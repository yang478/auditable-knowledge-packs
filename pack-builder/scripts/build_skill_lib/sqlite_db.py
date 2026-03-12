from __future__ import annotations

import re
import sqlite3
import unicodedata
from dataclasses import replace
from pathlib import Path
from typing import Callable, Iterable, List, Sequence, Tuple, TypeVar

from .fs_utils import die
from .text_utils import core_alias_title, fts_tokens, normalize_alias_text, normalize_article_ref, stable_hash
from .types import AliasRecord, EdgeRecord, InputDoc, NodeRecord


_REFERENCE_PATTERNS = (
    re.compile(r"参见第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
    re.compile(r"依据第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
)

ALIAS_EXACT = "exact"
ALIAS_ABBREVIATION = "abbreviation"
ALIAS_SOFT = "soft"


def extract_alias_rows(nodes: Sequence[NodeRecord]) -> List[AliasRecord]:
    rows: set[AliasRecord] = set()
    for node in nodes:
        core_title = core_alias_title(node.title)
        if not core_title:
            continue
        normalized_title = normalize_alias_text(core_title)
        if normalized_title:
            rows.add(
                AliasRecord(
                    doc_id=node.doc_id,
                    alias=core_title,
                    normalized_alias=normalized_title,
                    target_node_id=node.node_id,
                    alias_level=ALIAS_EXACT,
                    confidence=1.0,
                    source="title",
                    source_version=node.source_version,
                    is_active=node.is_active,
                )
            )

        if core_title.endswith("期限") and len(core_title) >= 6:
            abbreviation = core_title[0] + core_title[2] + core_title[-2]
            normalized_abbreviation = normalize_alias_text(abbreviation)
            if normalized_abbreviation:
                rows.add(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=abbreviation,
                        normalized_alias=normalized_abbreviation,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_ABBREVIATION,
                        confidence=0.92,
                        source="title_abbreviation",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )

        for alias in node.aliases:
            normalized_alias = normalize_alias_text(alias)
            if normalized_alias:
                rows.add(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=alias,
                        normalized_alias=normalized_alias,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_EXACT,
                        confidence=1.0,
                        source="frontmatter_alias",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )

        for match in re.finditer(r'(?:简称|以下简称)[“"]?([^”"、，。；;]{2,12})[”"]?', node.body_plain):
            alias = match.group(1).strip()
            normalized_alias = normalize_alias_text(alias)
            if normalized_alias:
                rows.add(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=alias,
                        normalized_alias=normalized_alias,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_EXACT,
                        confidence=0.98,
                        source="body_alias",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )
    return sorted(
        rows,
        key=lambda row: (row.doc_id, row.source_version, row.normalized_alias, row.target_node_id, row.alias_level),
    )


def extract_reference_edges(nodes: Sequence[NodeRecord]) -> List[EdgeRecord]:
    article_targets = {
        (node.doc_id, normalize_article_ref(node.label)): node.node_id
        for node in nodes
        if node.kind == "article"
    }
    edges: set[EdgeRecord] = set()
    for node in nodes:
        haystack = node.body_plain or node.body_md
        if not haystack:
            continue
        for pattern in _REFERENCE_PATTERNS:
            for match in pattern.finditer(haystack):
                label = normalize_article_ref(f"第{match.group(1)}条")
                target_node_id = article_targets.get((node.doc_id, label))
                if not target_node_id or target_node_id == node.node_id:
                    continue
                edges.add(
                    EdgeRecord(
                        doc_id=node.doc_id,
                        edge_type="references",
                        from_node_id=node.node_id,
                        to_node_id=target_node_id,
                        source_version=node.source_version,
                        is_active=node.is_active,
                        confidence=1.0,
                    )
                )
    return sorted(edges, key=lambda row: (row.doc_id, row.source_version, row.edge_type, row.from_node_id, row.to_node_id))


def write_kb_sqlite_db(
    db_path: Path,
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")

        conn.executescript(
            """
            CREATE TABLE docs (
              doc_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
              doc_id TEXT NOT NULL,
              doc_title TEXT NOT NULL,
              source_file TEXT NOT NULL,
              source_path TEXT NOT NULL,
              doc_hash TEXT NOT NULL,
              source_version TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              UNIQUE (doc_id, source_version)
            );

            CREATE TABLE nodes (
              node_key TEXT PRIMARY KEY,
              node_id TEXT NOT NULL,
              doc_id TEXT NOT NULL,
              source_version TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              kind TEXT NOT NULL,
              label TEXT NOT NULL,
              title TEXT NOT NULL,
              parent_id TEXT,
              prev_id TEXT,
              next_id TEXT,
              ordinal INTEGER NOT NULL,
              ref_path TEXT NOT NULL,
              is_leaf INTEGER NOT NULL,
              raw_span_start INTEGER NOT NULL,
              raw_span_end INTEGER NOT NULL,
              node_hash TEXT NOT NULL,
              confidence REAL NOT NULL,
              UNIQUE (node_id, source_version)
            );

            CREATE TABLE edges (
              doc_id TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              from_node_id TEXT NOT NULL,
              to_node_id TEXT NOT NULL,
              source_version TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              confidence REAL NOT NULL,
              PRIMARY KEY (edge_type, from_node_id, to_node_id, source_version)
            );

            CREATE TABLE aliases (
              doc_id TEXT NOT NULL,
              alias TEXT NOT NULL,
              normalized_alias TEXT NOT NULL,
              target_node_id TEXT NOT NULL,
              alias_level TEXT NOT NULL,
              source_version TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1,
              confidence REAL NOT NULL,
              source TEXT NOT NULL,
              PRIMARY KEY (normalized_alias, target_node_id, alias_level, source_version)
            );

            CREATE TABLE node_text (
              node_key TEXT PRIMARY KEY,
              body_md TEXT NOT NULL,
              body_plain TEXT NOT NULL,
              FOREIGN KEY (node_key) REFERENCES nodes(node_key)
            );
            """
        )

        for d in docs:
            conn.execute(
                """
                INSERT INTO docs(
                  doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (d.doc_id, d.title, d.path.name, str(d.path), d.doc_hash, d.source_version, 1 if d.is_active else 0),
            )

        for n in nodes:
            conn.execute(
                """
                INSERT INTO nodes(
                  node_key, node_id, doc_id, source_version, is_active, kind, label, title, parent_id, prev_id, next_id,
                  ordinal, ref_path, is_leaf, raw_span_start, raw_span_end, node_hash, confidence
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    n.node_key,
                    n.node_id,
                    n.doc_id,
                    n.source_version,
                    1 if n.is_active else 0,
                    n.kind,
                    n.label,
                    n.title,
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
                ),
            )
            conn.execute(
                "INSERT INTO node_text(node_key, body_md, body_plain) VALUES (?,?,?)",
                (n.node_key, n.body_md, n.body_plain),
            )

        for e in edges:
            conn.execute(
                """
                INSERT INTO edges(doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    e.doc_id,
                    e.edge_type,
                    e.from_node_id,
                    e.to_node_id,
                    e.source_version,
                    1 if e.is_active else 0,
                    e.confidence,
                ),
            )

        for a in aliases:
            conn.execute(
                """
                INSERT INTO aliases(doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
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
                ),
            )

        try:
            conn.execute("CREATE VIRTUAL TABLE node_fts USING fts5(node_key UNINDEXED, tokens)")
        except sqlite3.OperationalError as exc:
            die(f"SQLite FTS5 is required but unavailable: {exc}")

        for n in nodes:
            if not n.is_leaf:
                continue
            tokens = fts_tokens(n.title + "\n" + n.body_plain)
            conn.execute("INSERT INTO node_fts(node_key, tokens) VALUES (?,?)", (n.node_key, tokens))

        conn.execute("CREATE INDEX idx_nodes_doc_id_active ON nodes(doc_id, is_active)")
        conn.execute("CREATE INDEX idx_nodes_node_id_active ON nodes(node_id, is_active)")
        conn.execute("CREATE INDEX idx_docs_doc_id_active ON docs(doc_id, is_active)")
        conn.execute("CREATE INDEX idx_nodes_parent_id ON nodes(parent_id)")
        conn.execute("CREATE INDEX idx_nodes_prev_id ON nodes(prev_id)")
        conn.execute("CREATE INDEX idx_nodes_next_id ON nodes(next_id)")
        conn.execute("CREATE INDEX idx_edges_from_node_active ON edges(from_node_id, is_active)")
        conn.execute("CREATE INDEX idx_aliases_norm_active ON aliases(normalized_alias, is_active)")
        conn.commit()
    finally:
        conn.close()


def read_existing_docs(db_path: Path) -> List[InputDoc]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT doc_id, doc_title, source_path, doc_hash, source_version, is_active FROM docs ORDER BY doc_id, source_version"
        ).fetchall()
    finally:
        conn.close()
    out: List[InputDoc] = []
    for row in rows:
        out.append(
            InputDoc(
                path=Path(str(row["source_path"])),
                doc_id=str(row["doc_id"]),
                title=str(row["doc_title"]),
                source_version=str(row["source_version"]),
                doc_hash=str(row["doc_hash"]),
                is_active=bool(row["is_active"]),
            )
        )
    return out


def read_existing_nodes(db_path: Path) -> List[NodeRecord]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              n.node_id, n.doc_id, d.doc_title, n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id,
              n.ordinal, n.ref_path, n.is_leaf, t.body_md, t.body_plain,
              n.source_version, n.is_active, n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence
            FROM nodes n
            JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
            JOIN node_text t ON t.node_key = n.node_key
            ORDER BY n.doc_id, n.source_version, n.node_id
            """
        ).fetchall()
    finally:
        conn.close()
    out: List[NodeRecord] = []
    for row in rows:
        out.append(
            NodeRecord(
                node_id=str(row["node_id"]),
                doc_id=str(row["doc_id"]),
                doc_title=str(row["doc_title"]),
                kind=str(row["kind"]),
                label=str(row["label"]),
                title=str(row["title"]),
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
        )
    return out


def read_existing_edges(db_path: Path) -> List[EdgeRecord]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence FROM edges"
        ).fetchall()
    finally:
        conn.close()
    out: List[EdgeRecord] = []
    for row in rows:
        out.append(
            EdgeRecord(
                doc_id=str(row["doc_id"]),
                edge_type=str(row["edge_type"]),
                from_node_id=str(row["from_node_id"]),
                to_node_id=str(row["to_node_id"]),
                source_version=str(row["source_version"]),
                is_active=bool(row["is_active"]),
                confidence=float(row["confidence"]),
            )
        )
    return out


def read_existing_aliases(db_path: Path) -> List[AliasRecord]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source FROM aliases"
        ).fetchall()
    finally:
        conn.close()
    out: List[AliasRecord] = []
    for row in rows:
        out.append(
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
        )
    return out


T = TypeVar("T")


def merge_history(
    current_records: Sequence[T],
    rebuilt_records: Sequence[T],
    *,
    key_fn: Callable[[T], object],
    sort_key: Callable[[T], object],
) -> List[T]:
    rebuilt_keys = {key_fn(record) for record in rebuilt_records}
    merged: List[T] = [replace(record, is_active=True) for record in rebuilt_records]  # type: ignore[arg-type]
    for record in current_records:
        if key_fn(record) in rebuilt_keys:
            continue
        merged.append(replace(record, is_active=False))  # type: ignore[arg-type]
    return sorted(merged, key=sort_key)

