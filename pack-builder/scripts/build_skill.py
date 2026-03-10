#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET


@dataclass(frozen=True)
class InputDoc:
    path: Path
    doc_id: str
    title: str
    source_version: str = "current"
    doc_hash: str = ""
    is_active: bool = True


@dataclass
class NodeRecord:
    node_id: str
    doc_id: str
    doc_title: str
    kind: str
    label: str
    title: str
    parent_id: Optional[str]
    prev_id: Optional[str]
    next_id: Optional[str]
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
            self.node_hash = _stable_hash(self.body_md)

    @property
    def node_key(self) -> str:
        return _node_key(self.node_id, self.source_version)


@dataclass(frozen=True)
class EdgeRecord:
    doc_id: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    source_version: str
    is_active: bool = True
    confidence: float = 1.0


@dataclass(frozen=True)
class AliasRecord:
    doc_id: str
    alias: str
    normalized_alias: str
    target_node_id: str
    alias_level: str
    confidence: float
    source: str
    source_version: str
    is_active: bool = True


def _die(message: str, code: int = 2) -> "NoReturn":  # type: ignore[name-defined]
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(code)


def _safe_skill_name(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?", name):
        _die("Invalid --skill-name. Use lowercase letters/digits/hyphens only (e.g. my-books).")
    if name.startswith("-") or name.endswith("-") or "--" in name:
        _die("Invalid --skill-name. Avoid leading/trailing hyphens and consecutive '--'.")
    return name


def _slugify_ascii(text: str) -> str:
    s = unicodedata.normalize("NFKC", text)
    s = s.lower()
    s = re.sub(r"(?<![a-z0-9])v(?=\d+\b)", "versionkeep_", s)
    s = re.sub(r"([a-z])(\d)", r"\1-\2", s)
    s = re.sub(r"(\d)([a-z])", r"\1-\2", s)
    s = s.replace("versionkeep_", "v")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def _derive_doc_id(path: Path, used: set[str]) -> str:
    base = _slugify_ascii(path.stem)
    if not base:
        base = "doc"
    if len(base) > 48:
        base = base[:48].strip("-") or "doc"

    doc_id = base
    if doc_id in used:
        h = hashlib.sha1(str(path).encode("utf-8", errors="ignore")).hexdigest()[:8]
        doc_id = f"{base}-{h}"
    i = 2
    while doc_id in used:
        doc_id = f"{base}-{i}"
        i += 1
    used.add(doc_id)
    return doc_id


_CJK_RANGES = (
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xF900, 0xFAFF),  # CJK Compatibility Ideographs
)


def _is_cjk(ch: str) -> bool:
    o = ord(ch)
    for lo, hi in _CJK_RANGES:
        if lo <= o <= hi:
            return True
    return False


def _markdown_to_plain(md: str) -> str:
    out_lines: List[str] = []
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            out_lines.append("")
            continue
        line = re.sub(r"^#{1,6}\s+", "", line)
        line = re.sub(r"`([^`]*)`", r"\1", line)
        line = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", line)
        line = line.replace("**", "").replace("__", "").replace("*", "")
        out_lines.append(line)
    return "\n".join(out_lines).strip() + "\n"


_VERSION_RE = re.compile(r"\bV(?P<num>\d+)\b", re.IGNORECASE)


def _stable_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _node_key(node_id: str, source_version: str) -> str:
    return f"{node_id}@{source_version}"


def _derive_doc_title(path: Path, md: str) -> str:
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            title = re.sub(r"^#{1,6}\s+", "", line).strip()
            return title or path.stem
        break
    return path.stem


def _derive_source_version(path: Path, title: str) -> str:
    match = _VERSION_RE.search(title) or _VERSION_RE.search(path.stem)
    if match:
        return f"v{match.group('num')}"
    return "current"


_ASCII_WORD_RE = re.compile(r"[A-Za-z0-9_]{2,}")


def _tokenize_cjk_2gram(text: str) -> List[str]:
    tokens: List[str] = []
    run: List[str] = []

    def flush() -> None:
        nonlocal run
        if len(run) >= 2:
            tokens.extend("".join(run[i : i + 2]) for i in range(len(run) - 1))
        elif len(run) == 1:
            tokens.append(run[0])
        run = []

    for ch in text:
        if _is_cjk(ch):
            run.append(ch)
        else:
            flush()
    flush()
    return tokens


def _fts_tokens(text: str) -> str:
    tokens: List[str] = []
    tokens.extend(_tokenize_cjk_2gram(text))
    tokens.extend(m.group(0).lower() for m in _ASCII_WORD_RE.finditer(text))
    return " ".join(tokens)


_REFERENCE_PATTERNS = (
    re.compile(r"参见第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
    re.compile(r"依据第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
)

ALIAS_EXACT = "exact"
ALIAS_ABBREVIATION = "abbreviation"
ALIAS_SOFT = "soft"


def _normalize_article_ref(label: str) -> str:
    return re.sub(r"\s+", "", label).replace("條", "条")


def _normalize_alias_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).lower()
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", text)


def _core_alias_title(title: str) -> str:
    return re.sub(r"^第\s*[0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?\s*[条條]\s*", "", title).strip()


def _extract_alias_rows(nodes: Sequence[NodeRecord]) -> List[AliasRecord]:
    rows: set[AliasRecord] = set()
    for node in nodes:
        core_title = _core_alias_title(node.title)
        if not core_title:
            continue
        normalized_title = _normalize_alias_text(core_title)
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
            normalized_abbreviation = _normalize_alias_text(abbreviation)
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
            normalized_alias = _normalize_alias_text(alias)
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
            normalized_alias = _normalize_alias_text(alias)
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
    return sorted(rows, key=lambda row: (row.doc_id, row.source_version, row.normalized_alias, row.target_node_id, row.alias_level))


def _extract_reference_edges(nodes: Sequence[NodeRecord]) -> List[EdgeRecord]:
    article_targets = {
        (node.doc_id, _normalize_article_ref(node.label)): node.node_id
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
                label = _normalize_article_ref(f"第{match.group(1)}条")
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
                        confidence=0.9,
                    )
                )
    return sorted(edges, key=lambda row: (row.doc_id, row.source_version, row.edge_type, row.from_node_id, row.to_node_id))


def _write_kb_sqlite_db(
    db_path: Path,
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(
            """
            DROP TABLE IF EXISTS docs;
            DROP TABLE IF EXISTS aliases;
            DROP TABLE IF EXISTS edges;
            DROP TABLE IF EXISTS nodes;
            DROP TABLE IF EXISTS node_text;
            DROP TABLE IF EXISTS node_fts;

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
                    n.is_leaf and 1 or 0,
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

        for edge in edges:
            conn.execute(
                """
                INSERT INTO edges(doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    edge.doc_id,
                    edge.edge_type,
                    edge.from_node_id,
                    edge.to_node_id,
                    edge.source_version,
                    1 if edge.is_active else 0,
                    edge.confidence,
                ),
            )

        for alias in aliases:
            conn.execute(
                """
                INSERT INTO aliases(doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    alias.doc_id,
                    alias.alias,
                    alias.normalized_alias,
                    alias.target_node_id,
                    alias.alias_level,
                    alias.source_version,
                    1 if alias.is_active else 0,
                    alias.confidence,
                    alias.source,
                ),
            )

        try:
            conn.execute("CREATE VIRTUAL TABLE node_fts USING fts5(node_key UNINDEXED, tokens)")
        except sqlite3.OperationalError as e:
            _die(f"SQLite FTS5 is required but unavailable: {e}")

        for n in nodes:
            if not n.is_leaf:
                continue
            tokens = _fts_tokens(n.title + "\n" + n.body_plain)
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


def _write_kb_sqlite(out_skill_dir: Path, docs: Sequence[InputDoc], nodes: Sequence[NodeRecord]) -> None:
    db_path = out_skill_dir / "kb.sqlite"
    _write_kb_sqlite_db(db_path, docs, nodes, _extract_reference_edges(nodes), _extract_alias_rows(nodes))


def _read_existing_docs(db_path: Path) -> List[InputDoc]:
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


def _read_existing_nodes(db_path: Path) -> List[NodeRecord]:
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
    return [
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
        for row in rows
    ]


def _read_existing_edges(db_path: Path) -> List[EdgeRecord]:
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


def _read_existing_aliases(db_path: Path) -> List[AliasRecord]:
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


def _merge_history(current_records, rebuilt_records, *, key_fn, sort_key):
    rebuilt_keys = {key_fn(record) for record in rebuilt_records}
    merged = [replace(record, is_active=True) for record in rebuilt_records]
    for record in current_records:
        if key_fn(record) in rebuilt_keys:
            continue
        merged.append(replace(record, is_active=False))
    return sorted(merged, key=sort_key)


def _read_text(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "gb18030", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _which(cmd: str) -> Optional[str]:
    if cmd == "pdftotext" and os.environ.get("BOOK_SKILL_GENERATOR_NO_PDFTOTEXT"):
        return None
    return shutil.which(cmd)


def _extract_pdf_to_text(path: Path) -> str:
    pdftotext = _which("pdftotext")
    if not pdftotext:
        _die(
            "PDF input requires `pdftotext` (Poppler). Install it or convert PDF to TXT/MD first."
        )
    with tempfile.TemporaryDirectory() as tmp:
        out_txt = Path(tmp) / "out.txt"
        proc = subprocess.run(
            [pdftotext, "-layout", "-nopgbrk", str(path), str(out_txt)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if proc.returncode != 0:
            _die(f"pdftotext failed for {path.name}: {proc.stderr.strip()}")
        return _read_text(out_txt)


def _docx_paragraphs(docx_path: Path) -> List[Tuple[Optional[int], str]]:
    """
    Minimal OOXML extractor with heading detection from paragraph style.
    Returns a list of (heading_level, text) where heading_level may be None.
    """
    ns = {
        "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    }
    out: List[Tuple[Optional[int], str]] = []
    try:
        with zipfile.ZipFile(docx_path) as zf:
            try:
                xml = zf.read("word/document.xml")
            except KeyError:
                _die(f"DOCX missing word/document.xml: {docx_path.name}. Try converting DOCX → MD/TXT first.")
    except zipfile.BadZipFile:
        _die(f"Invalid DOCX (bad zip): {docx_path.name}. Try converting DOCX → MD/TXT first.")

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        _die(f"DOCX parse failed: {docx_path.name}. Try converting DOCX → MD/TXT first.")
    for p in root.findall(".//w:p", ns):
        texts = [t.text or "" for t in p.findall(".//w:t", ns)]
        text = "".join(texts).strip()
        if not text:
            continue

        level: Optional[int] = None
        pstyle = p.find("./w:pPr/w:pStyle", ns)
        if pstyle is not None:
            val = pstyle.attrib.get(f"{{{ns['w']}}}val", "")
            m = re.search(r"Heading\s*(\d+)", val, flags=re.IGNORECASE)
            if m:
                level = max(1, min(6, int(m.group(1))))

        out.append((level, text))

    return out


def _extract_docx_to_markdown(path: Path) -> str:
    paras = _docx_paragraphs(path)
    lines: List[str] = []
    for level, text in paras:
        if level:
            lines.append("#" * level + " " + text)
        else:
            lines.append(text)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _infer_text_headings_to_markdown(text: str) -> str:
    """
    Heuristic conversion for plain text to markdown headings to enable structure-first splitting.
    """
    out_lines: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            out_lines.append("")
            continue

        # Chinese chapter: 第六章 标题
        m = re.match(r"^第\s*([0-9一二三四五六七八九十百千]+)\s*章\s*(.*)$", line)
        if m:
            out_lines.append(f"# 第{m.group(1)}章 {m.group(2)}".rstrip())
            continue

        # Chinese appendix: 附录A ...
        m = re.match(r"^附录\s*([A-Z0-9])\s*(.*)$", line)
        if m:
            out_lines.append(f"# 附录{m.group(1)} {m.group(2)}".rstrip())
            continue

        # English chapter / appendix
        if re.match(r"^(chapter|appendix)\s+\w+", line, flags=re.IGNORECASE):
            out_lines.append("# " + line)
            continue

        # Numeric headings: 1 / 1.1 / 1.1.1
        m = re.match(r"^(?P<num>\d+(?:\.\d+){0,4})\s+(.+)$", line)
        if m:
            num = m.group("num")
            level = min(6, num.count(".") + 1)
            out_lines.append("#" * level + " " + line)
            continue

        out_lines.append(line)
    return "\n".join(out_lines).rstrip() + "\n"


def extract_to_markdown(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".md", ".markdown"}:
        return _read_text(path)
    if ext == ".txt":
        return _infer_text_headings_to_markdown(_read_text(path))
    if ext == ".docx":
        return _extract_docx_to_markdown(path)
    if ext == ".pdf":
        return _infer_text_headings_to_markdown(_extract_pdf_to_text(path))
    _die(f"Unsupported input type: {path.name} (supported: .md .txt .docx .pdf)")


def build_keywords_from_title(title: str) -> List[str]:
    raw = title.strip()
    parts = re.split(r"[\\s、/，,；;：:（）()《》“”\"'\\-]+|与|及|和|以及", raw)
    keywords: List[str] = []
    for p in parts:
        p = p.strip()
        if len(p) < 2:
            continue
        keywords.append(p)
    seen = set()
    out: List[str] = []
    for k in keywords:
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def write_tsv(path: Path, rows: Iterable[Tuple[str, ...]], header: Optional[Tuple[str, ...]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        if header:
            f.write("# " + "\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(row) + "\n")


_WINDOWS_INVALID_FILENAME_CHARS = set('<>:"/\\|?*')
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def _shard_name_from_key(key: str) -> str:
    if not key:
        return "_EMPTY"
    if len(key) > 32:
        key = key[:32]
    if key.upper() in _WINDOWS_RESERVED_NAMES:
        return "U" + "-".join(f"{ord(c):04X}" for c in key)
    for ch in key:
        if ch in {".", " "}:
            return "U" + "-".join(f"{ord(c):04X}" for c in key)
        if ch in _WINDOWS_INVALID_FILENAME_CHARS:
            return "U" + "-".join(f"{ord(c):04X}" for c in key)
        if ord(ch) < 32:
            return "U" + "-".join(f"{ord(c):04X}" for c in key)
    return key


def _first_visible_prefix(text: str, n: int) -> str:
    s = text.strip()
    if not s:
        return ""
    return s[: max(1, n)]


def _shard_rows_by_prefix(
    rows: List[Tuple[str, ...]],
    *,
    primary_index: int,
    max_rows: int = 200,
    max_prefix_len: int = 4,
) -> Dict[str, List[Tuple[str, ...]]]:
    def group(n: int, chunk: List[Tuple[str, ...]]) -> Dict[str, List[Tuple[str, ...]]]:
        out: Dict[str, List[Tuple[str, ...]]] = {}
        for r in chunk:
            key = _first_visible_prefix(r[primary_index], n)
            out.setdefault(key, []).append(r)
        return out

    shards = group(1, rows)
    for n in range(1, max_prefix_len):
        oversize = [k for k, v in shards.items() if len(v) > max_rows]
        if not oversize:
            break
        for k in oversize:
            chunk = shards.pop(k)
            for sk, sv in group(n + 1, chunk).items():
                shards[sk] = sv
    return shards


def _write_sharded_index(out_dir: Path, index_name: str, rows: List[Tuple[str, ...]], header: Tuple[str, ...]) -> None:
    idx_root = out_dir / "indexes" / index_name
    idx_root.mkdir(parents=True, exist_ok=True)

    shards = _shard_rows_by_prefix(rows, primary_index=0)
    shard_map_rows: List[Tuple[str, ...]] = []
    for key in sorted(shards.keys()):
        shard_file = _shard_name_from_key(key) + ".tsv"
        write_tsv(idx_root / shard_file, shards[key], header=header)
        shard_map_rows.append((key, shard_file))
    write_tsv(idx_root / "_shards.tsv", shard_map_rows, header=("key", "file"))


HEADING_RE = re.compile(r"^(?P<marks>#{1,6})\s+(?P<title>.+?)\s*$")

_CN_NUM_RE = r"[0-9一二三四五六七八九十百千]+"
ARTICLE_LINE_RE = re.compile(
    rf"^\s*第\s*(?P<num>{_CN_NUM_RE})(?:\s*之\s*(?P<zhi>{_CN_NUM_RE}))?\s*[条條]\s*(?P<rest>.*)$"
)
ITEM_LINE_RE = re.compile(r"^\s*[（(]\s*(?P<mark>[一二三四五六七八九十0-9]+)\s*[）)]\s*(?P<rest>.*)$")


@dataclass
class Heading:
    level: int
    title: str
    line_index: int


def _parse_headings(md: str) -> List[Heading]:
    headings: List[Heading] = []
    for i, line in enumerate(md.splitlines()):
        m = HEADING_RE.match(line)
        if not m:
            continue
        headings.append(Heading(level=len(m.group("marks")), title=m.group("title").strip(), line_index=i))
    return headings


def _slice_lines(lines: List[str], start: int, end: int) -> List[str]:
    seg = lines[start:end]
    while seg and seg[0].strip() == "":
        seg.pop(0)
    while seg and seg[-1].strip() == "":
        seg.pop()
    return seg


def _strip_first_heading_line(lines: List[str]) -> List[str]:
    if not lines:
        return lines
    if HEADING_RE.match(lines[0].strip()):
        return lines[1:]
    return lines


def _strip_markdown_heading_prefix(line: str) -> str:
    s = line.strip()
    if not s.startswith("#"):
        return s
    return re.sub(r"^#{1,6}\s*", "", s).strip()


def _article_label(line: str) -> Optional[str]:
    m = ARTICLE_LINE_RE.match(_strip_markdown_heading_prefix(line))
    if not m:
        return None
    num = (m.group("num") or "").replace(" ", "")
    zhi = (m.group("zhi") or "").replace(" ", "")
    return f"第{num}{('之' + zhi) if zhi else ''}条"


def _item_label(line: str) -> Optional[str]:
    m = ITEM_LINE_RE.match(_strip_markdown_heading_prefix(line))
    if not m:
        return None
    mark = (m.group("mark") or "").strip()
    if not mark:
        return None
    return f"（{mark}）"


def _split_blocks_by_starts(lines: List[str], start_fn) -> List[List[str]]:
    blocks: List[List[str]] = []
    start: Optional[int] = None
    for i, line in enumerate(lines):
        if start_fn(line):
            if start is not None:
                blocks.append(lines[start:i])
            start = i
    if start is not None:
        blocks.append(lines[start:])
    return blocks


def _split_paragraphs(lines: List[str]) -> List[List[str]]:
    paragraphs: List[List[str]] = []
    cur: List[str] = []
    for line in lines:
        if line.strip() == "":
            if cur:
                paragraphs.append(cur)
                cur = []
            continue
        cur.append(line)
    if cur:
        paragraphs.append(cur)
    return paragraphs


def _pack_paragraphs_into_blocks(paragraphs: List[List[str]], *, max_chars: int) -> List[List[str]]:
    if max_chars <= 0:
        return []
    blocks: List[List[str]] = []

    def split_long_paragraph(p: List[str]) -> None:
        chunk: List[str] = []
        for line in p:
            if len(line) > max_chars:
                if chunk:
                    blocks.append(chunk)
                    chunk = []
                for i in range(0, len(line), max_chars):
                    blocks.append([line[i : i + max_chars]])
                continue
            if not chunk:
                chunk = [line]
                continue
            candidate = "\n".join(chunk + [line])
            if len(candidate) > max_chars:
                blocks.append(chunk)
                chunk = [line]
            else:
                chunk.append(line)
        if chunk:
            blocks.append(chunk)

    for p in paragraphs:
        if not p:
            continue
        if len("\n".join(p)) <= max_chars:
            blocks.append(list(p))
        else:
            split_long_paragraph(p)
    return blocks


def _frontmatter_kb_node(
    doc: InputDoc,
    *,
    node_id: str,
    kind: str,
    label: str,
    title: str,
    parent_id: str,
    ref_path: str,
) -> str:
    base = [
        "---",
        f'doc_id: "{doc.doc_id}"',
        f'doc_title: "{doc.title}"',
        f'source_file: "{doc.path.name}"',
        f'node_id: "{node_id}"',
        f'kind: "{kind}"',
        f'label: "{label}"',
        f'title: "{title}"',
        f'parent_id: "{parent_id}"',
        f'ref_path: "{ref_path}"',
        "---\n",
    ]
    return "\n".join(base) + "\n"


def _split_by_heading_level(md: str, *, level: int) -> List[Tuple[str, List[str]]]:
    lines = md.splitlines()
    headings = [h for h in _parse_headings(md) if h.level == level]
    if not headings:
        return []
    blocks: List[Tuple[str, List[str]]] = []
    for idx, h in enumerate(headings):
        start = h.line_index
        end = headings[idx + 1].line_index if idx + 1 < len(headings) else len(lines)
        seg = _slice_lines(lines, start, end)
        blocks.append((h.title, seg))
    return blocks


def _frontmatter(doc: InputDoc, *, chapter_id: str, chapter_title: str, section_id: Optional[str] = None, section_title: Optional[str] = None) -> str:
    base = [
        "---",
        f'doc_id: "{doc.doc_id}"',
        f'doc_title: "{doc.title}"',
        f'source_file: "{doc.path.name}"',
        f'chapter_id: "{chapter_id}"',
        f'chapter_title: "{chapter_title}"',
    ]
    if section_id and section_title:
        base.append(f'section_id: "{section_id}"')
        base.append(f'section_title: "{section_title}"')
        base.append("keywords: " + json.dumps(build_keywords_from_title(section_title), ensure_ascii=False))
    else:
        base.append("keywords: " + json.dumps(build_keywords_from_title(chapter_title), ensure_ascii=False))
    base.append("---\n")
    return "\n".join(base) + "\n"


def _generate_doc(
    doc: InputDoc, md: str, out_skill_dir: Path
) -> Tuple[List[Tuple[str, str, str, str, str, str]], List[NodeRecord]]:
    """
    Returns:
      - headings rows: (title, doc_id, doc_title, type, id, path)
      - node records for kb.sqlite (chapter/section nodes)
    """
    doc_dir = out_skill_dir / "references" / doc.doc_id
    chapters_dir = doc_dir / "chapters"
    sections_root = doc_dir / "sections"
    articles_dir = doc_dir / "articles"
    items_root = doc_dir / "items"
    blocks_dir = doc_dir / "blocks"

    write_text(
        doc_dir / "metadata.md",
        (
            f"# {doc.title}\n\n"
            f"- 源文件：`{doc.path.name}`\n"
            f"- 版本：`{doc.source_version}`\n"
            f"- 文档哈希：`{doc.doc_hash}`\n"
        ),
    )

    chapter_blocks = _split_by_heading_level(md, level=1)
    chapter_level = 1
    if not chapter_blocks:
        chapter_blocks = _split_by_heading_level(md, level=2)
        chapter_level = 2

    lines = md.splitlines()
    toc_rows: List[Dict[str, str]] = []
    heading_rows: List[Tuple[str, str, str, str, str, str]] = []
    nodes: List[NodeRecord] = []
    prev_chapter: Optional[NodeRecord] = None
    article_counter = 0
    block_counter = 0

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    def write_articles_and_items(parent_node: NodeRecord, content_lines: List[str]) -> int:
        """
        Parse Chinese legal "第X条" and "（一）" structures and write:
          - references/<doc_id>/articles/article-0001.md ...
          - references/<doc_id>/items/article-0001/item-01.md ...
        Returns number of articles written.
        """
        nonlocal article_counter

        body_lines = _strip_first_heading_line(content_lines)
        body_lines = _slice_lines(body_lines, 0, len(body_lines))
        if not body_lines:
            return 0

        article_blocks = _split_blocks_by_starts(body_lines, _article_label)
        if not article_blocks:
            return 0

        prev_article: Optional[NodeRecord] = None
        wrote = 0

        for a_lines in article_blocks:
            first_raw = a_lines[0] if a_lines else ""
            first = _strip_markdown_heading_prefix(first_raw)
            label = _article_label(first_raw) or "条"
            title = first or label

            article_counter += 1
            article_id = f"article-{article_counter:04d}"
            article_path = articles_dir / f"{article_id}.md"
            article_rel = rel(article_path)

            article_node_id = f"{doc.doc_id}:article:{article_counter:04d}"
            article_body_md = "\n".join(a_lines).strip() + "\n"

            article_node = NodeRecord(
                node_id=article_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="article",
                label=label,
                title=title,
                parent_id=parent_node.node_id,
                prev_id=prev_article.node_id if prev_article else None,
                next_id=None,
                ordinal=article_counter,
                ref_path=article_rel,
                is_leaf=True,
                body_md=article_body_md,
                body_plain=_markdown_to_plain(article_body_md),
                source_version=doc.source_version,
            )
            if prev_article:
                prev_article.next_id = article_node.node_id
            prev_article = article_node
            nodes.append(article_node)
            heading_rows.append((title, doc.doc_id, doc.title, "article", article_node_id, article_rel))

            # Write article file
            write_text(
                article_path,
                _frontmatter_kb_node(
                    doc,
                    node_id=article_node_id,
                    kind="article",
                    label=label,
                    title=title,
                    parent_id=parent_node.node_id,
                    ref_path=article_rel,
                )
                + article_body_md,
            )

            # Split items within article
            item_blocks = _split_blocks_by_starts(a_lines[1:], _item_label)
            if item_blocks:
                prev_item: Optional[NodeRecord] = None
                for item_idx, i_lines in enumerate(item_blocks, start=1):
                    i_first_raw = i_lines[0] if i_lines else ""
                    i_first = _strip_markdown_heading_prefix(i_first_raw)
                    i_label = _item_label(i_first_raw) or f"（{item_idx}）"
                    i_title = i_first or i_label
                    item_node_id = f"{doc.doc_id}:item:{article_counter:04d}:{item_idx:02d}"

                    item_dir = items_root / article_id
                    item_path = item_dir / f"item-{item_idx:02d}.md"
                    item_rel = rel(item_path)
                    item_body_md = "\n".join(i_lines).strip() + "\n"

                    item_node = NodeRecord(
                        node_id=item_node_id,
                        doc_id=doc.doc_id,
                        doc_title=doc.title,
                        kind="item",
                        label=i_label,
                        title=i_title,
                        parent_id=article_node_id,
                        prev_id=prev_item.node_id if prev_item else None,
                        next_id=None,
                        ordinal=item_idx,
                        ref_path=item_rel,
                        is_leaf=True,
                        body_md=item_body_md,
                        body_plain=_markdown_to_plain(item_body_md),
                        source_version=doc.source_version,
                    )
                    if prev_item:
                        prev_item.next_id = item_node.node_id
                    prev_item = item_node
                    nodes.append(item_node)
                    heading_rows.append((i_title, doc.doc_id, doc.title, "item", item_node_id, item_rel))

                    write_text(
                        item_path,
                        _frontmatter_kb_node(
                            doc,
                            node_id=item_node_id,
                            kind="item",
                            label=i_label,
                            title=i_title,
                            parent_id=article_node_id,
                            ref_path=item_rel,
                        )
                        + item_body_md,
                    )

            wrote += 1

        return wrote

    def write_blocks(parent_node: NodeRecord, content_lines: List[str]) -> int:
        """
        Fallback for documents without headings/articles: split into stable blocks for search.
        Writes:
          - references/<doc_id>/blocks/block-0001.md ...
        Returns number of blocks written.
        """
        nonlocal block_counter

        body_lines = _strip_first_heading_line(content_lines)
        body_lines = _slice_lines(body_lines, 0, len(body_lines))
        if not body_lines:
            return 0

        paragraphs = _split_paragraphs(body_lines)
        if not paragraphs:
            return 0
        blocks = _pack_paragraphs_into_blocks(paragraphs, max_chars=6000)
        if not blocks:
            return 0

        prev_block: Optional[NodeRecord] = None
        wrote = 0

        for b_lines in blocks:
            block_counter += 1
            block_id = f"block-{block_counter:04d}"
            block_node_id = f"{doc.doc_id}:block:{block_counter:04d}"
            block_path = blocks_dir / f"{block_id}.md"
            block_rel = rel(block_path)
            block_body_md = "\n".join(b_lines).strip() + "\n"

            title = block_id
            for raw in b_lines:
                s = raw.strip()
                if not s:
                    continue
                s = re.sub(r"^#{1,6}\\s+", "", s)
                s = s.replace('"', "").strip()
                if len(s) > 80:
                    s = (s[:80].rstrip() + "…").strip()
                title = f"{block_id} {s}" if s else block_id
                break

            block_node = NodeRecord(
                node_id=block_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="block",
                label=block_id,
                title=title,
                parent_id=parent_node.node_id,
                prev_id=prev_block.node_id if prev_block else None,
                next_id=None,
                ordinal=block_counter,
                ref_path=block_rel,
                is_leaf=True,
                body_md=block_body_md,
                body_plain=_markdown_to_plain(block_body_md),
                source_version=doc.source_version,
            )
            if prev_block:
                prev_block.next_id = block_node.node_id
            prev_block = block_node
            nodes.append(block_node)

            write_text(
                block_path,
                _frontmatter_kb_node(
                    doc,
                    node_id=block_node_id,
                    kind="block",
                    label=block_id,
                    title=title,
                    parent_id=parent_node.node_id,
                    ref_path=block_rel,
                )
                + block_body_md,
            )
            wrote += 1
        return wrote

    def write_chapter(chapter_index: int, title: str, content_lines: List[str]) -> Tuple[str, int]:
        nonlocal prev_chapter
        chapter_id = f"chapter{chapter_index:02d}"
        chapter_path = chapters_dir / f"{chapter_id}.md"
        body_md = "\n".join(content_lines).strip() + "\n"
        write_text(chapter_path, _frontmatter(doc, chapter_id=chapter_id, chapter_title=title) + body_md)
        chapter_rel = rel(chapter_path)
        heading_rows.append((title, doc.doc_id, doc.title, "chapter", chapter_id, chapter_rel))

        # Split sections inside this chapter
        section_level = min(6, chapter_level + 1)
        chapter_md = "\n".join(content_lines) + "\n"
        section_blocks = _split_by_heading_level(chapter_md, level=section_level)
        section_count = 0

        chapter_node_id = f"{doc.doc_id}:chapter:{chapter_id}"
        chapter_node = NodeRecord(
            node_id=chapter_node_id,
            doc_id=doc.doc_id,
            doc_title=doc.title,
            kind="chapter",
            label=chapter_id,
            title=title,
            parent_id=None,
            prev_id=prev_chapter.node_id if prev_chapter else None,
            next_id=None,
            ordinal=chapter_index,
            ref_path=chapter_rel,
            is_leaf=not bool(section_blocks),
            body_md=body_md,
            body_plain=_markdown_to_plain(body_md),
            source_version=doc.source_version,
        )
        if prev_chapter:
            prev_chapter.next_id = chapter_node.node_id
        prev_chapter = chapter_node
        nodes.append(chapter_node)

        if section_blocks:
            prev_section: Optional[NodeRecord] = None
            for sec_idx, (sec_title, sec_lines) in enumerate(section_blocks, start=1):
                section_id = f"section-{chapter_index:02d}-{sec_idx:02d}"
                section_path = sections_root / chapter_id / f"{section_id}.md"
                sec_body_md = "\n".join(sec_lines).strip() + "\n"
                write_text(
                    section_path,
                    _frontmatter(
                        doc,
                        chapter_id=chapter_id,
                        chapter_title=title,
                        section_id=section_id,
                        section_title=sec_title,
                    )
                    + sec_body_md,
                )
                section_rel = rel(section_path)
                heading_rows.append(
                    (
                        sec_title,
                        doc.doc_id,
                        doc.title,
                        "section",
                        f"{chapter_id}/{section_id}",
                        section_rel,
                    )
                )
                section_node_id = f"{doc.doc_id}:section:{chapter_id}/{section_id}"
                section_node = NodeRecord(
                    node_id=section_node_id,
                    doc_id=doc.doc_id,
                    doc_title=doc.title,
                    kind="section",
                    label=section_id,
                    title=sec_title,
                    parent_id=chapter_node_id,
                    prev_id=prev_section.node_id if prev_section else None,
                    next_id=None,
                    ordinal=sec_idx,
                    ref_path=section_rel,
                    is_leaf=True,
                    body_md=sec_body_md,
                    body_plain=_markdown_to_plain(sec_body_md),
                    source_version=doc.source_version,
                )
                if prev_section:
                    prev_section.next_id = section_node.node_id
                prev_section = section_node
                nodes.append(section_node)

                # Parse legal article/item blocks. If present, make section a navigation node (not indexed),
                # and index both articles and items (bundle later elevates item hits to article).
                wrote_articles = write_articles_and_items(section_node, sec_lines)
                if wrote_articles:
                    section_node.is_leaf = False
                section_count += 1
        else:
            # No sections: attempt to split into articles/items directly inside chapter.
            wrote_articles = write_articles_and_items(chapter_node, content_lines)
            if wrote_articles:
                chapter_node.is_leaf = False
        return chapter_id, section_count

    if not chapter_blocks:
        # No headings: create a single chapter from all content.
        all_lines = _slice_lines(lines, 0, len(lines))
        chapter_id, section_count = write_chapter(1, "正文", all_lines)
        chapter_node_id = f"{doc.doc_id}:chapter:{chapter_id}"
        chapter_node = next((n for n in reversed(nodes) if n.node_id == chapter_node_id), None)
        if chapter_node and chapter_node.is_leaf:
            wrote_blocks = write_blocks(chapter_node, all_lines)
            if wrote_blocks:
                chapter_node.is_leaf = False
        toc_rows.append(
            {
                "chapter_id": chapter_id,
                "chapter_title": "正文",
                "path": f"references/{doc.doc_id}/chapters/{chapter_id}.md",
                "sections": str(section_count),
            }
        )
    else:
        # Preface: content before first chapter heading.
        first_start = HEADING_RE.match(lines[0]) and 0
        first_ch_line = None
        for h in _parse_headings(md):
            if h.level == chapter_level:
                first_ch_line = h.line_index
                break
        if first_ch_line and first_ch_line > 0:
            preface_lines = _slice_lines(lines, 0, first_ch_line)
            if preface_lines:
                preface_id, section_count = write_chapter(0, "前置内容", preface_lines)
                toc_rows.append(
                    {
                        "chapter_id": preface_id,
                        "chapter_title": "前置内容",
                        "path": f"references/{doc.doc_id}/chapters/{preface_id}.md",
                        "sections": str(section_count),
                    }
                )

        for idx, (title, content_lines) in enumerate(chapter_blocks, start=1):
            chapter_id, section_count = write_chapter(idx, title, content_lines)
            toc_rows.append(
                {
                    "chapter_id": chapter_id,
                    "chapter_title": title,
                    "path": f"references/{doc.doc_id}/chapters/{chapter_id}.md",
                    "sections": str(section_count),
                }
            )

    # toc.md
    toc_md = [f"# {doc.title} 目录\n\n", "## 章节列表\n\n", "| 章节 | 标题 | 文件 | 小节数 |\n|---|---|---|---|\n"]
    for row in toc_rows:
        toc_md.append(
            f"| `{row['chapter_id']}` | {row['chapter_title']} | `{row['path']}` | {row['sections']} |\n"
        )
    write_text(doc_dir / "toc.md", "".join(toc_md))

    return heading_rows, nodes


def _render_generated_skill_md(skill_name: str, title: str, docs: List[InputDoc]) -> str:
    doc_list = ", ".join(d.title for d in docs[:5])
    if len(docs) > 5:
        doc_list += "…"
    desc = (
        f"Use when querying the generated documents knowledge base ({doc_list}) and needing "
        "deterministic search→context bundling with citations (no embeddings)."
    )
    frontmatter = f"---\nname: {skill_name}\ndescription: {desc}\n---\n\n"

    lines: List[str] = [frontmatter]
    lines.append(f"# {title}\n\n")
    lines.append("## Documents\n\n")
    for d in docs:
        lines.append(f"- {d.doc_id}: `{d.path.name}`（标题：{d.title}）\n")

    lines.append("\n## Recommended Workflow (Deterministic)\n\n")
    lines.append("- 运行路径：标题/正文/术语 三路召回 → 确定性融合排序 → 最多一轮补查 → `bundle.md`。\n")
    lines.append("- 补查只做一轮，动作限定为 definition / references / version_metadata。\n")
    lines.append("\n1. (Optional) Preview ranked hits:\n\n")
    lines.append("   `python3 scripts/kbtool.py search --query \"...\" --out search.md`\n\n")
    lines.append("2. Generate a single evidence bundle:\n\n")
    lines.append("   `python3 scripts/kbtool.py bundle --query \"...\" --out bundle.md`\n\n")
    lines.append("   Tips:\n")
    lines.append("   - For noisy queries: add `--must \"...\"` (repeatable) or `--query-mode and`.\n")
    lines.append("   - For timeline questions: add `--order chronological`.\n")
    lines.append("   - If the query suggests definitions / scope / exceptions, inspect `## 补查记录` for the one-round expansion.\n\n")
    lines.append("3. Open `bundle.md`, then answer using only its contents.\n")
    lines.append("4. Copy/paste the auto-generated `## 参考依据` section from `bundle.md` at the end of your answer.\n")
    lines.append("\n## Direct Lookup (Fallback)\n\n")
    lines.append("- If the user specifies a document/chapter/section, open `references/<doc_id>/toc.md`, then open the referenced `references/` file directly.\n")
    lines.append("\n## Rebuild\n\n")
    lines.append("- If you edit `references/`, rebuild `kb.sqlite`: `python3 scripts/kbtool.py reindex`\n")
    lines.append("- `reindex` uses shadow rebuild + 原子重建 / atomic switch, and keeps older document versions as inactive rows when the version changes.\n")
    lines.append("- If you add/remove input documents, rerun the generator.\n")
    lines.append("- Optional (TSV only): `python3 scripts/reindex.py`\n")
    return "".join(lines)


def _write_reindex_script(out_skill_dir: Path) -> None:
    script_path = out_skill_dir / "scripts" / "reindex.py"
    template = Path(__file__).resolve().parents[1] / "templates" / "reindex.py"
    if not template.exists():
        _die("Missing template: templates/reindex.py (pack-builder installation is incomplete)")
    write_text(script_path, template.read_text(encoding="utf-8"))
    script_path.chmod(0o755)


def _write_kbtool_script(out_skill_dir: Path) -> None:
    script_path = out_skill_dir / "scripts" / "kbtool.py"
    template = Path(__file__).resolve().parents[1] / "templates" / "kbtool.py"
    if not template.exists():
        _die("Missing template: templates/kbtool.py (pack-builder installation is incomplete)")
    write_text(script_path, template.read_text(encoding="utf-8"))
    script_path.chmod(0o755)


def build_skill(skill_name: str, title: str, inputs: Sequence[Path], out_dir: Path, force: bool) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / skill_name
    tmp = out_dir / f".{skill_name}.tmp-{os.getpid()}"
    if tmp.exists():
        shutil.rmtree(tmp)
    if target.exists() and not force:
        _die(f"Output already exists: {target} (use --force to overwrite)")
    tmp.mkdir(parents=True, exist_ok=True)

    docs: List[InputDoc] = []
    used_doc_ids: set[str] = set()
    all_heading_rows: List[Tuple[str, str, str, str, str, str]] = []
    all_nodes: List[NodeRecord] = []
    for p in inputs:
        md = extract_to_markdown(p)
        title_for_doc = _derive_doc_title(p, md)
        doc = InputDoc(
            path=p,
            doc_id=_derive_doc_id(p, used_doc_ids),
            title=title_for_doc,
            source_version=_derive_source_version(p, title_for_doc),
            doc_hash=_stable_hash(md),
        )
        docs.append(doc)
        headings, nodes = _generate_doc(doc, md, tmp)
        all_heading_rows.extend(headings)
        all_nodes.extend(nodes)

    # Global indexes
    (tmp / "indexes" / "headings").mkdir(parents=True, exist_ok=True)
    (tmp / "indexes" / "kw").mkdir(parents=True, exist_ok=True)

    all_heading_rows.sort(key=lambda r: r[0])
    headings_rows = [(t, doc_id, doc_title, kind, item_id, path) for (t, doc_id, doc_title, kind, item_id, path) in all_heading_rows]
    _write_sharded_index(tmp, "headings", headings_rows, header=("title", "doc_id", "doc_title", "type", "id", "path"))

    kw_rows: List[Tuple[str, ...]] = []
    seen = set()
    for (t, doc_id, doc_title, kind, item_id, path) in all_heading_rows:
        for kw in build_keywords_from_title(t):
            key = (kw, doc_id, kind, item_id, path)
            if key in seen:
                continue
            seen.add(key)
            kw_rows.append((kw, doc_id, doc_title, kind, item_id, path))
    kw_rows.sort(key=lambda r: r[0])
    _write_sharded_index(tmp, "kw", kw_rows, header=("keyword", "doc_id", "doc_title", "type", "id", "path"))

    # Generated skill SKILL.md
    write_text(tmp / "SKILL.md", _render_generated_skill_md(skill_name, title, docs))
    _write_reindex_script(tmp)
    _write_kbtool_script(tmp)

    existing_db = target / "kb.sqlite"
    current_docs = _read_existing_docs(existing_db) if target.exists() and force else []
    current_nodes = _read_existing_nodes(existing_db) if target.exists() and force else []
    current_edges = _read_existing_edges(existing_db) if target.exists() and force else []
    current_aliases = _read_existing_aliases(existing_db) if target.exists() and force else []

    rebuilt_edges = _extract_reference_edges(all_nodes)
    rebuilt_aliases = _extract_alias_rows(all_nodes)
    merged_docs = _merge_history(
        current_docs,
        docs,
        key_fn=lambda record: (record.doc_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, 0 if record.is_active else 1),
    )
    merged_nodes = _merge_history(
        current_nodes,
        all_nodes,
        key_fn=lambda record: (record.node_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.node_id, 0 if record.is_active else 1),
    )
    merged_edges = _merge_history(
        current_edges,
        rebuilt_edges,
        key_fn=lambda record: (record.edge_type, record.from_node_id, record.to_node_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.edge_type, record.from_node_id, record.to_node_id, 0 if record.is_active else 1),
    )
    merged_aliases = _merge_history(
        current_aliases,
        rebuilt_aliases,
        key_fn=lambda record: (record.normalized_alias, record.target_node_id, record.alias_level, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.normalized_alias, record.target_node_id, record.alias_level, 0 if record.is_active else 1),
    )
    _write_kb_sqlite_db(tmp / "kb.sqlite", merged_docs, merged_nodes, merged_edges, merged_aliases)

    manifest = {
        "generator": "pack-builder",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "skill_name": skill_name,
        "title": title,
        "docs": [
            {
                "doc_id": d.doc_id,
                "title": d.title,
                "source_file": d.path.name,
                "source_path": str(d.path),
                "doc_hash": d.doc_hash,
                "source_version": d.source_version,
                "active_version": True,
            }
            for d in docs
        ],
        "layout": {
            "references_root": "references/",
            "indexes_root": "indexes/",
            "headings_index": "indexes/headings/",
            "keyword_index": "indexes/kw/",
        },
    }
    write_text(tmp / "manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")

    # Finalize
    if target.exists():
        shutil.rmtree(target)
    tmp.rename(target)
    return target


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a monitor-style knowledge base skill from documents.")
    parser.add_argument("--skill-name", required=True, help="Output skill folder name (lowercase letters/digits/hyphens).")
    parser.add_argument("--out-dir", default=".claude/skills", help="Directory to write the generated skill into (default: .claude/skills).")
    parser.add_argument("--inputs", nargs="+", required=True, help="Input documents (.md .txt .docx .pdf).")
    parser.add_argument("--title", default="Document Knowledge Base", help="Human-friendly title for the generated skill.")
    parser.add_argument("--force", action="store_true", help="Overwrite output folder if it already exists.")
    args = parser.parse_args(argv)

    skill_name = _safe_skill_name(args.skill_name)
    out_dir = Path(args.out_dir)
    inputs = [Path(p) for p in args.inputs]
    for p in inputs:
        if not p.exists() or not p.is_file():
            _die(f"Missing input file: {p}")

    build_skill(skill_name=skill_name, title=args.title, inputs=inputs, out_dir=out_dir, force=args.force)
    print(f"[OK] Generated skill: {out_dir / skill_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
