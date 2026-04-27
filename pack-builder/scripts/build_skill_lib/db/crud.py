from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence

from ..utils.fs import die, DataIntegrityError
from ..utils.node_io import read_node_body_md_from_ref
from ..utils.text import extract_keywords, fts_tokens, markdown_to_plain
from ..types import AliasRecord, EdgeRecord, InputDoc, NodeRecord
from .schema import SCHEMA_SCRIPT, INDEX_SCRIPT
from ..utils.safe_sqlite import enable_wal, retry_on_locked, sqlite3_retry_exec, open_db_wal

if TYPE_CHECKING:
    from ..incremental.invalidation import ChangeSet


def _insert_records(
    conn: sqlite3.Connection,
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
    *,
    base_dir: Optional[Path] = None,
) -> None:
    """将记录批量插入已存在 schema 的 SQLite 连接中。供全量/增量写入共用。"""
    if docs:
        conn.executemany(
            """
            INSERT INTO docs(
              doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active
            ) VALUES (?,?,?,?,?,?,?)
            """,
            [
                (d.doc_id, d.title, d.path.name, str(d.path), d.doc_hash, d.source_version, 1 if d.is_active else 0)
                for d in docs
            ],
        )

    if nodes:
        node_rows: list[tuple[object, ...]] = []
        text_rows: list[tuple[object, ...]] = []
        fts_rows: list[tuple[object, ...]] = []
        for n in nodes:
            body_md = n.body_md
            if n.is_leaf and not body_md:
                if base_dir is None:
                    raise DataIntegrityError(
                        f"Missing body_md for leaf node and no base_dir provided: node_id={n.node_id} ref_path={n.ref_path!r}"
                    )
                body_md = read_node_body_md_from_ref(base_dir, n)
            body_plain = n.body_plain or markdown_to_plain(body_md)
            node_rows.append(
                (
                    n.node_key,
                    n.node_id,
                    n.doc_id,
                    n.source_version,
                    1 if n.is_active else 0,
                    n.kind,
                    n.label,
                    n.title,
                    n.heading_path,
                    n.parent_id,
                    n.prev_id,
                    n.next_id,
                    n.ordinal,
                    n.ref_path,
                    1 if n.is_leaf else 0,
                    n.raw_span_start,
                    n.raw_span_end,
                    n.node_hash,
                    n.confidence,
                )
            )
            kw_list = extract_keywords(body_plain, top_k=12, min_freq=1)
            text_rows.append((n.node_key, body_md, body_plain, " ".join(kw_list)))
            if n.is_leaf:
                tokens = fts_tokens(n.title + "\n" + body_plain)
                fts_rows.append((n.node_key, tokens))
        conn.executemany(
            """
            INSERT INTO nodes(
              node_key, node_id, doc_id, source_version, is_active, kind, label, title, heading_path, parent_id, prev_id, next_id,
              ordinal, ref_path, is_leaf, raw_span_start, raw_span_end, node_hash, confidence
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            node_rows,
        )
        if text_rows:
            conn.executemany(
                "INSERT INTO node_text(node_key, body_md, body_plain, keywords) VALUES (?,?,?,?)",
                text_rows,
            )
        if fts_rows:
            conn.executemany(
                "INSERT INTO node_fts(node_key, tokens) VALUES (?,?)",
                fts_rows,
            )
            conn.execute("INSERT INTO node_fts(node_fts) VALUES('optimize')")

    if edges:
        conn.executemany(
            """
            INSERT INTO edges(doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence)
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                (
                    e.doc_id,
                    e.edge_type,
                    e.from_node_id,
                    e.to_node_id,
                    e.source_version,
                    1 if e.is_active else 0,
                    e.confidence,
                )
                for e in edges
            ],
        )

    if aliases:
        conn.executemany(
            """
            INSERT INTO aliases(doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    a.doc_id,
                    a.alias,
                    a.normalized_alias,
                    a.target_node_id,
                    a.alias_level,
                    a.source_version,
                    1 if a.is_active else 0,
                    a.confidence,
                    a.source,
                )
                for a in aliases
            ],
        )


def write_kb_sqlite_db(
    db_path: Path,
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
    *,
    base_dir: Optional[Path] = None,
) -> None:
    tmp_path = db_path.with_suffix('.sqlite.tmp')
    if tmp_path.exists():
        tmp_path.unlink()
    try:
        conn = sqlite3.connect(str(tmp_path))
        try:
            enable_wal(conn)
            conn.execute("PRAGMA temp_store = MEMORY")

            conn.executescript(SCHEMA_SCRIPT)

            try:
                conn.execute("CREATE VIRTUAL TABLE node_fts USING fts5(node_key UNINDEXED, tokens)")
            except sqlite3.OperationalError as exc:
                raise DataIntegrityError(f"SQLite FTS5 is required but unavailable: {exc}") from exc

            conn.execute("BEGIN")
            _insert_records(conn, docs, nodes, edges, aliases, base_dir=base_dir)
            conn.commit()

            conn.executescript(INDEX_SCRIPT)
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
        finally:
            conn.close()
        import os
        os.replace(str(tmp_path), str(db_path))
    except OSError:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def incremental_update_kb_sqlite_db(
    db_path: Path,
    change_set: "ChangeSet",
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
    *,
    base_dir: Optional[Path] = None,
) -> None:
    """对现有 SQLite DB 做增量更新。

    策略：
    - removed_docs: DELETE 相关记录
    - rebuild_docs: DELETE 旧记录 + INSERT 新记录
    - metadata_only_docs: UPDATE docs 表 metadata（若 source_path/hash 有变化）
    - unchanged_docs: 无操作

    要求 db_path 指向的 DB 已经存在且 schema 完整。
    """
    conn = sqlite3.connect(str(db_path))
    try:
        enable_wal(conn)
        conn.execute("PRAGMA temp_store = MEMORY")

        dirty_doc_ids = change_set.rebuild_doc_ids | change_set.removed_doc_ids

        conn.execute("BEGIN")
        # 清理 dirty 文档的旧记录
        for doc_id in dirty_doc_ids:
            conn.execute("DELETE FROM docs WHERE doc_id = ?", (doc_id,))
            conn.execute(
                "DELETE FROM node_text WHERE node_key IN (SELECT node_key FROM nodes WHERE doc_id = ?)",
                (doc_id,),
            )
            conn.execute(
                "DELETE FROM node_fts WHERE node_key IN (SELECT node_key FROM nodes WHERE doc_id = ?)",
                (doc_id,),
            )
            conn.execute("DELETE FROM nodes WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM edges WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM aliases WHERE doc_id = ?", (doc_id,))

        # 插入 rebuild 文档的新记录（仅 is_active=True 的）
        rebuild_doc_set = change_set.rebuild_doc_ids
        rebuild_docs = [d for d in docs if d.doc_id in rebuild_doc_set and d.is_active]
        rebuild_nodes = [n for n in nodes if n.doc_id in rebuild_doc_set and n.is_active]
        rebuild_edges = [e for e in edges if e.doc_id in rebuild_doc_set and e.is_active]
        rebuild_aliases = [a for a in aliases if a.doc_id in rebuild_doc_set and a.is_active]

        _insert_records(conn, rebuild_docs, rebuild_nodes, rebuild_edges, rebuild_aliases, base_dir=base_dir)

        # metadata_only: 更新 docs 表中的路径/hash 等元数据
        for doc_id in change_set.metadata_only_doc_ids:
            doc = next((d for d in docs if d.doc_id == doc_id), None)
            if doc is not None:
                is_active_int = 1 if doc.is_active else 0
                conn.execute(
                    """
                    UPDATE docs
                    SET source_path = ?, doc_hash = ?, is_active = ?
                    WHERE doc_id = ? AND source_version = ?
                    """,
                    (str(doc.path), doc.doc_hash, is_active_int, doc.doc_id, doc.source_version),
                )
                # Cascade is_active to associated nodes/edges/aliases
                if not doc.is_active:
                    conn.execute(
                        "UPDATE nodes SET is_active = 0 WHERE doc_id = ? AND source_version = ?",
                        (doc.doc_id, doc.source_version),
                    )
                    conn.execute(
                        "UPDATE edges SET is_active = 0 WHERE doc_id = ? AND source_version = ?",
                        (doc.doc_id, doc.source_version),
                    )
                    conn.execute(
                        "UPDATE aliases SET is_active = 0 WHERE doc_id = ? AND source_version = ?",
                        (doc.doc_id, doc.source_version),
                    )
        conn.commit()

        # 重新创建索引（使用 IF NOT EXISTS 避免报错）
        conn.executescript(INDEX_SCRIPT.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS"))
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _fetchall(db_path: Path, sql: str) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    conn = open_db_wal(db_path)
    try:
        return sqlite3_retry_exec(conn, sql).fetchall()
    finally:
        conn.close()


def read_existing_docs(db_path: Path) -> List[InputDoc]:
    rows = _fetchall(db_path, "SELECT doc_id, doc_title, source_path, doc_hash, source_version, is_active FROM docs ORDER BY doc_id, source_version")
    return [
        InputDoc(
            path=Path(str(row["source_path"])),
            doc_id=str(row["doc_id"]),
            title=str(row["doc_title"]),
            source_version=str(row["source_version"]),
            doc_hash=str(row["doc_hash"]),
            is_active=bool(row["is_active"]),
        )
        for row in rows
    ]


def read_existing_nodes(db_path: Path, *, include_body: bool = True) -> List[NodeRecord]:
    """Read existing nodes from a prior build database.

    When *include_body* is False, body_md and body_plain are omitted
    (empty strings) to avoid loading large text fields when only
    structural metadata (IDs, hashes, ordinals) is needed for merge_history.
    """
    if include_body:
        sql = """
        SELECT
          n.node_id, n.doc_id, d.doc_title, n.kind, n.label, n.title, n.heading_path,
          n.parent_id, n.prev_id, n.next_id, n.ordinal, n.ref_path, n.is_leaf, t.body_md, t.body_plain,
          n.source_version, n.is_active, n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
        JOIN node_text t ON t.node_key = n.node_key
        ORDER BY n.doc_id, n.source_version, n.node_id
        """
    else:
        sql = """
        SELECT
          n.node_id, n.doc_id, d.doc_title, n.kind, n.label, n.title, n.heading_path,
          n.parent_id, n.prev_id, n.next_id, n.ordinal, n.ref_path, n.is_leaf, '' AS body_md, '' AS body_plain,
          n.source_version, n.is_active, n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
        ORDER BY n.doc_id, n.source_version, n.node_id
        """
    rows = _fetchall(db_path, sql)
    return [
        NodeRecord(
            node_id=str(row["node_id"]),
            doc_id=str(row["doc_id"]),
            doc_title=str(row["doc_title"]),
            kind=str(row["kind"]),
            label=str(row["label"]),
            title=str(row["title"]),
            heading_path=str(row["heading_path"] or ""),
            parent_id=row["parent_id"],
            prev_id=row["prev_id"],
            next_id=row["next_id"],
            ordinal=int(row["ordinal"]),
            ref_path=str(row["ref_path"]),
            is_leaf=bool(row["is_leaf"]),
            body_md=str(row["body_md"]),
            body_plain=str(row["body_plain"]),
            source_version=str(row["source_version"]),
            is_active=bool(row["is_active"]),
            raw_span_start=int(row["raw_span_start"]),
            raw_span_end=int(row["raw_span_end"]),
            node_hash=str(row["node_hash"]),
            confidence=float(row["confidence"]),
        )
        for row in rows
    ]


def read_existing_edges(db_path: Path) -> List[EdgeRecord]:
    rows = _fetchall(db_path, "SELECT doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence FROM edges")
    return [
        EdgeRecord(
            doc_id=str(row["doc_id"]),
            edge_type=str(row["edge_type"]),
            from_node_id=str(row["from_node_id"]),
            to_node_id=str(row["to_node_id"]),
            source_version=str(row["source_version"]),
            is_active=bool(row["is_active"]),
            confidence=float(row["confidence"]),
        )
        for row in rows
    ]


def read_existing_aliases(db_path: Path) -> List[AliasRecord]:
    rows = _fetchall(db_path, "SELECT doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source FROM aliases")
    return [
        AliasRecord(
            doc_id=str(row["doc_id"]),
            alias=str(row["alias"]),
            normalized_alias=str(row["normalized_alias"]),
            target_node_id=str(row["target_node_id"]),
            alias_level=str(row["alias_level"]),
            source_version=str(row["source_version"]),
            is_active=bool(row["is_active"]),
            confidence=float(row["confidence"]),
            source=str(row["source"]),
        )
        for row in rows
    ]
