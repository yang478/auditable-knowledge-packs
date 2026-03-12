from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from .runtime import die, resolve_root
from .text import (
    core_alias_title,
    derive_source_version,
    fts_tokens,
    markdown_to_plain,
    node_key,
    normalize_alias_text,
    normalize_article_ref,
    parse_frontmatter,
    stable_hash,
    strip_frontmatter,
)


@dataclass
class NodeRow:
    node_id: str
    doc_id: str
    kind: str
    label: str
    title: str
    parent_id: str | None
    prev_id: str | None
    next_id: str | None
    ordinal: int
    ref_path: str
    is_leaf: bool
    body_md: str
    body_plain: str
    source_version: str = "current"
    is_active: bool = True
    aliases: Tuple[str, ...] = ()
    raw_span_start: int = 0
    raw_span_end: int = 0
    node_hash: str = ""
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not self.raw_span_end:
            self.raw_span_end = max(1, len(self.body_md))
        if not self.node_hash:
            self.node_hash = stable_hash(self.body_md)

    @property
    def node_key(self) -> str:
        return node_key(self.node_id, self.source_version)


@dataclass(frozen=True)
class DocRow:
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
    doc_hash: str = ""
    source_version: str = "current"
    is_active: bool = True


@dataclass(frozen=True)
class EdgeRow:
    doc_id: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    source_version: str
    is_active: bool = True
    confidence: float = 1.0


@dataclass(frozen=True)
class AliasRow:
    doc_id: str
    alias: str
    normalized_alias: str
    target_node_id: str
    alias_level: str
    confidence: float
    source: str
    source_version: str
    is_active: bool = True


def hash_doc_dir(doc_dir: Path) -> str:
    parts: List[str] = []
    for path in sorted(p for p in doc_dir.rglob("*.md") if p.is_file()):
        parts.append(str(path.relative_to(doc_dir)).replace("\\", "/"))
        parts.append(path.read_text(encoding="utf-8", errors="replace"))
    return stable_hash("\n\n".join(parts))


_REFERENCE_PATTERNS = (
    re.compile(r"参见第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
    re.compile(r"依据第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
)

ALIAS_EXACT = "exact"
ALIAS_ABBREVIATION = "abbreviation"
ALIAS_SOFT = "soft"


def extract_alias_rows(nodes: Sequence[NodeRow]) -> List[AliasRow]:
    rows: set[AliasRow] = set()
    for node in nodes:
        core_title = core_alias_title(node.title)
        if core_title:
            normalized_title = normalize_alias_text(core_title)
            if normalized_title:
                rows.add(
                    AliasRow(
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
                        AliasRow(
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
                    AliasRow(
                        doc_id=node.doc_id,
                        alias=alias,
                        normalized_alias=normalized_alias,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_SOFT,
                        confidence=0.85,
                        source="frontmatter",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )

    return sorted(
        rows,
        key=lambda row: (
            row.doc_id,
            row.source_version,
            row.normalized_alias,
            row.target_node_id,
            0 if row.is_active else 1,
        ),
    )


def extract_reference_edges(nodes: Sequence[NodeRow]) -> List[EdgeRow]:
    article_targets = {
        (node.doc_id, normalize_article_ref(node.label)): node.node_id
        for node in nodes
        if node.kind == "article"
    }
    edges: set[EdgeRow] = set()
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
                    EdgeRow(
                        doc_id=node.doc_id,
                        edge_type="references",
                        from_node_id=node.node_id,
                        to_node_id=target_node_id,
                        source_version=node.source_version,
                        is_active=node.is_active,
                        confidence=0.9,
                    )
                )
    return sorted(
        edges,
        key=lambda row: (row.doc_id, row.source_version, row.edge_type, row.from_node_id, row.to_node_id),
    )


def parse_doc_metadata(doc_dir: Path) -> Tuple[str, str, str, str]:
    """
    Best-effort doc_title + source_file from references/<doc_id>/metadata.md.
    """
    md_path = doc_dir / "metadata.md"
    if not md_path.exists():
        title = doc_dir.name
        return title, "(unknown)", derive_source_version(doc_dir.name, title), hash_doc_dir(doc_dir)
    md = md_path.read_text(encoding="utf-8", errors="replace")
    title = doc_dir.name
    for line in md.splitlines():
        if line.startswith("# "):
            title = line[2:].strip() or title
            break
    m = re.search(r"源文件：`([^`]+)`", md)
    source_file = m.group(1) if m else "(unknown)"
    version_match = re.search(r"版本：`([^`]+)`", md)
    doc_hash_match = re.search(r"文档哈希：`([^`]+)`", md)
    source_version = version_match.group(1) if version_match else derive_source_version(doc_dir.name, title)
    doc_hash = doc_hash_match.group(1) if doc_hash_match else hash_doc_dir(doc_dir)
    return title, source_file, source_version, doc_hash


def read_md_with_frontmatter(path: Path) -> Tuple[Dict[str, str], str]:
    md = path.read_text(encoding="utf-8", errors="replace")
    fm = parse_frontmatter(md)
    body = strip_frontmatter(md)
    return fm, body.strip() + "\n" if body.strip() else ""


def parse_int_suffix(text: str, *, default: int = 0) -> int:
    m = re.search(r"(\d+)$", text)
    return int(m.group(1)) if m else default


def parse_aliases_field(value: str) -> Tuple[str, ...]:
    raw = value.strip()
    if not raw:
        return ()

    items: List[str] = []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, list):
        items = [str(item).strip() for item in parsed]
    elif isinstance(parsed, str):
        items = [parsed.strip()]
    else:
        items = [part.strip() for part in re.split(r"[,，、;；]", raw)]

    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = normalize_alias_text(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(item)
    return tuple(out)


def build_nodes_from_references(root: Path) -> Tuple[List[DocRow], List[NodeRow]]:
    refs_dir = root / "references"
    if not refs_dir.exists():
        die(f"Missing references/: {refs_dir}")
    docs: List[DocRow] = []
    nodes: List[NodeRow] = []

    # Load documents
    for doc_dir in sorted((p for p in refs_dir.iterdir() if p.is_dir()), key=lambda p: p.name):
        doc_id = doc_dir.name
        doc_title, source_file, source_version, doc_hash = parse_doc_metadata(doc_dir)
        docs.append(
            DocRow(
                doc_id=doc_id,
                doc_title=doc_title,
                source_file=source_file,
                source_path=str(doc_dir),
                source_version=source_version,
                doc_hash=doc_hash,
                is_active=True,
            )
        )

        doc_nodes: List[NodeRow] = []

        def rel_to_root(path: Path) -> str:
            return str(path.relative_to(root)).replace("\\", "/")

        def add_kb_node(md: Path) -> None:
            fm, body = read_md_with_frontmatter(md)
            kind = str(fm.get("kind") or "").strip() or "section"
            label = str(fm.get("label") or "").strip() or md.stem
            title = str(fm.get("title") or "").strip() or label
            parent_id = str(fm.get("parent_id") or "").strip() or None
            node_id = str(fm.get("node_id") or "").strip() or f"{doc_id}:{kind}:{md.stem}"
            ordinal = parse_int_suffix(md.stem, default=0)
            is_leaf = bool(int(str(fm.get("is_leaf") or "1")))
            aliases = parse_aliases_field(fm.get("aliases", ""))
            doc_nodes.append(
                NodeRow(
                    node_id=node_id,
                    doc_id=doc_id,
                    kind=kind,
                    label=label,
                    title=title,
                    parent_id=parent_id,
                    prev_id=None,
                    next_id=None,
                    ordinal=ordinal,
                    ref_path=rel_to_root(md),
                    is_leaf=is_leaf,
                    body_md=body,
                    body_plain=markdown_to_plain(body),
                    source_version=source_version,
                    is_active=True,
                    aliases=aliases,
                    raw_span_start=0,
                    raw_span_end=len(body),
                    confidence=1.0,
                )
            )

        # Chapters: references/<doc_id>/chapters/chapter01.md
        chapters_dir = doc_dir / "chapters"
        if chapters_dir.exists():
            for md in sorted((p for p in chapters_dir.glob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                fm, body = read_md_with_frontmatter(md)
                chapter_id = md.stem
                title = str(fm.get("chapter_title") or "").strip() or chapter_id
                node_id = f"{doc_id}:chapter:{chapter_id}"
                doc_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="chapter",
                        label=chapter_id,
                        title=title,
                        parent_id=None,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(chapter_id, default=0),
                        ref_path=rel_to_root(md),
                        is_leaf=True,
                        body_md=body,
                        body_plain=markdown_to_plain(body),
                        source_version=source_version,
                        is_active=True,
                        aliases=(),
                        raw_span_start=0,
                        raw_span_end=len(body),
                        confidence=1.0,
                    )
                )

        # Sections: references/<doc_id>/sections/<chapter_id>/section-01-01.md
        sections_root = doc_dir / "sections"
        if sections_root.exists():
            for md in sorted((p for p in sections_root.rglob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                fm, body = read_md_with_frontmatter(md)
                chapter_id = md.parent.name
                section_id = md.stem
                title = str(fm.get("section_title") or "").strip() or section_id
                node_id = f"{doc_id}:section:{chapter_id}/{section_id}"
                parent_id = f"{doc_id}:chapter:{chapter_id}"
                doc_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="section",
                        label=section_id,
                        title=title,
                        parent_id=parent_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(section_id, default=0),
                        ref_path=rel_to_root(md),
                        is_leaf=True,
                        body_md=body,
                        body_plain=markdown_to_plain(body),
                        source_version=source_version,
                        is_active=True,
                        aliases=(),
                        raw_span_start=0,
                        raw_span_end=len(body),
                        confidence=1.0,
                    )
                )

        # Leaf nodes: articles/items/blocks are written with kb-node frontmatter.
        for sub in ("articles", "blocks", "items"):
            base = doc_dir / sub
            if not base.exists():
                continue
            for md in sorted((p for p in base.rglob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                add_kb_node(md)

        # Chapters/sections are navigation nodes: mark as non-leaf if they own children.
        has_child: set[str] = {n.parent_id for n in doc_nodes if n.parent_id}  # type: ignore[arg-type]
        for n in doc_nodes:
            if n.kind in {"chapter", "section"} and n.node_id in has_child:
                n.is_leaf = False

        # Rebuild stable prev/next links for siblings when absent.
        by_group: Dict[Tuple[str, Optional[str], str], List[NodeRow]] = {}
        for n in doc_nodes:
            by_group.setdefault((n.doc_id, n.parent_id, n.kind), []).append(n)
        for siblings in by_group.values():
            siblings.sort(key=lambda x: (x.ordinal, x.node_id))
            for idx, cur in enumerate(siblings):
                if cur.prev_id is None and idx > 0:
                    cur.prev_id = siblings[idx - 1].node_id
                if cur.next_id is None and idx + 1 < len(siblings):
                    cur.next_id = siblings[idx + 1].node_id

        nodes.extend(doc_nodes)

    return docs, nodes


def write_kb_sqlite_db(
    db_path: Path,
    docs: Sequence[DocRow],
    nodes: Sequence[NodeRow],
    edges: Sequence[EdgeRow],
    aliases: Sequence[AliasRow],
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
                (
                    d.doc_id,
                    d.doc_title,
                    d.source_file,
                    d.source_path,
                    d.doc_hash,
                    d.source_version,
                    1 if d.is_active else 0,
                ),
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
            tokens = " ".join(fts_tokens(n.title + "\n" + n.body_plain))
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


def read_existing_docs(db_path: Path) -> List[DocRow]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active
            FROM docs
            ORDER BY doc_id, source_version DESC
            """
        ).fetchall()
    finally:
        conn.close()
    out: List[DocRow] = []
    for row in rows:
        out.append(
            DocRow(
                doc_id=str(row[0]),
                doc_title=str(row[1]),
                source_file=str(row[2]),
                source_path=str(row[3]),
                doc_hash=str(row[4]),
                source_version=str(row[5]),
                is_active=bool(row[6]),
            )
        )
    return out


def read_existing_nodes(db_path: Path) -> List[NodeRow]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              n.node_id, n.doc_id, n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id, n.ordinal,
              n.ref_path, n.is_leaf, t.body_md, t.body_plain, n.source_version, n.is_active,
              n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence
            FROM nodes n
            JOIN node_text t ON t.node_key = n.node_key
            ORDER BY n.doc_id, n.source_version DESC, n.node_id
            """
        ).fetchall()
    finally:
        conn.close()
    out: List[NodeRow] = []
    for row in rows:
        out.append(
            NodeRow(
                node_id=str(row["node_id"]),
                doc_id=str(row["doc_id"]),
                kind=str(row["kind"]),
                label=str(row["label"]),
                title=str(row["title"]),
                parent_id=str(row["parent_id"]) if row["parent_id"] else None,
                prev_id=str(row["prev_id"]) if row["prev_id"] else None,
                next_id=str(row["next_id"]) if row["next_id"] else None,
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


def read_existing_edges(db_path: Path) -> List[EdgeRow]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence
            FROM edges
            ORDER BY doc_id, source_version DESC, edge_type, from_node_id, to_node_id
            """
        ).fetchall()
    finally:
        conn.close()
    out: List[EdgeRow] = []
    for row in rows:
        out.append(
            EdgeRow(
                doc_id=str(row[0]),
                edge_type=str(row[1]),
                from_node_id=str(row[2]),
                to_node_id=str(row[3]),
                source_version=str(row[4]),
                is_active=bool(row[5]),
                confidence=float(row[6]),
            )
        )
    return out


def read_existing_aliases(db_path: Path) -> List[AliasRow]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              doc_id, alias, normalized_alias, target_node_id, alias_level, confidence,
              source, source_version, is_active
            FROM aliases
            ORDER BY doc_id, source_version DESC, normalized_alias, target_node_id, alias_level
            """
        ).fetchall()
    finally:
        conn.close()
    out: List[AliasRow] = []
    for row in rows:
        out.append(
            AliasRow(
                doc_id=str(row["doc_id"]),
                alias=str(row["alias"]),
                normalized_alias=str(row["normalized_alias"]),
                target_node_id=str(row["target_node_id"]),
                alias_level=str(row["alias_level"]),
                confidence=float(row["confidence"]),
                source=str(row["source"]),
                source_version=str(row["source_version"]),
                is_active=bool(row["is_active"]),
            )
        )
    return out


def merge_history(current_records, rebuilt_records, *, key_fn, sort_key):
    rebuilt_keys = {key_fn(record) for record in rebuilt_records}
    merged = [replace(record, is_active=True) for record in rebuilt_records]
    for record in current_records:
        if key_fn(record) in rebuilt_keys:
            continue
        merged.append(replace(record, is_active=False))
    return sorted(merged, key=sort_key)


def validate_shadow_db(db_path: Path) -> Tuple[int, int, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        docs_count = int(conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0])
        nodes_count = int(conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0])
        leaf_count = int(conn.execute("SELECT COUNT(*) FROM nodes WHERE is_leaf = 1").fetchone()[0])
        invalid = conn.execute(
            """
            SELECT doc_id
            FROM docs
            GROUP BY doc_id
            HAVING SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) > 1
            """
        ).fetchall()
    finally:
        conn.close()
    if invalid:
        die(f"Shadow rebuild failed invariant: multiple active versions for {[row[0] for row in invalid]}")
    return docs_count, nodes_count, leaf_count


def atomic_replace(src: Path, dst: Path) -> None:
    src.replace(dst)


def rebuild_shadow_db(root: Path, db_path: Path) -> Tuple[Path, List[DocRow], List[NodeRow]]:
    shadow_path = db_path.with_suffix(db_path.suffix + ".next")
    print("[OK] shadow rebuild:", shadow_path)
    if shadow_path.exists():
        shadow_path.unlink()

    current_docs = read_existing_docs(db_path)
    current_nodes = read_existing_nodes(db_path)
    current_edges = read_existing_edges(db_path)
    current_aliases = read_existing_aliases(db_path)

    rebuilt_docs, nodes = build_nodes_from_references(root)
    rebuilt_edges = extract_reference_edges(nodes)
    rebuilt_aliases = extract_alias_rows(nodes)

    merged_docs = merge_history(
        current_docs,
        rebuilt_docs,
        key_fn=lambda record: (record.doc_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, 0 if record.is_active else 1),
    )
    merged_nodes = merge_history(
        current_nodes,
        nodes,
        key_fn=lambda record: (record.node_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.node_id, 0 if record.is_active else 1),
    )
    merged_edges = merge_history(
        current_edges,
        rebuilt_edges,
        key_fn=lambda record: (record.edge_type, record.from_node_id, record.to_node_id, record.source_version),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.edge_type,
            record.from_node_id,
            record.to_node_id,
            0 if record.is_active else 1,
        ),
    )
    merged_aliases = merge_history(
        current_aliases,
        rebuilt_aliases,
        key_fn=lambda record: (record.normalized_alias, record.target_node_id, record.alias_level, record.source_version),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.normalized_alias,
            record.target_node_id,
            record.alias_level,
            0 if record.is_active else 1,
        ),
    )
    write_kb_sqlite_db(shadow_path, merged_docs, merged_nodes, merged_edges, merged_aliases)
    docs_count, nodes_count, leaf_count = validate_shadow_db(shadow_path)
    atomic_replace(shadow_path, db_path)
    print(f"[OK] atomic switch: {db_path} (docs={docs_count} nodes={nodes_count} leaf={leaf_count})")
    return db_path, merged_docs, merged_nodes


def cmd_reindex(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    if not db_path.exists():
        die("Missing kb.sqlite. Rebuild the skill first.")
    dst, docs, nodes = rebuild_shadow_db(root, db_path)
    leaf = sum(1 for n in nodes if n.is_active and n.is_leaf)
    print("[OK] Reindexed:", dst)
    print(f"[OK] docs={len(docs)} nodes={len(nodes)} leaf={leaf}")
    return 0
