#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import unicodedata
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def die(message: str, code: int = 2) -> "NoReturn":  # type: ignore[name-defined]
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(code)


_CJK_RANGES = (
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xF900, 0xFAFF),  # CJK Compatibility Ideographs
)


def is_cjk(ch: str) -> bool:
    o = ord(ch)
    for lo, hi in _CJK_RANGES:
        if lo <= o <= hi:
            return True
    return False


_ASCII_WORD_RE = re.compile(r"[A-Za-z0-9_]{2,}")


def tokenize_cjk_2gram(text: str) -> List[str]:
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
        if is_cjk(ch):
            run.append(ch)
        else:
            flush()
    flush()
    return tokens


def fts_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    tokens.extend(tokenize_cjk_2gram(text))
    tokens.extend(m.group(0).lower() for m in _ASCII_WORD_RE.finditer(text))
    return tokens


def build_match_query(tokens: Sequence[str], *, max_tokens: int = 64) -> str:
    safe: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        t = t.replace('"', "")
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        safe.append(f'"{t}"')
        if len(safe) >= max_tokens:
            break
    if not safe:
        return ""
    return " OR ".join(safe)

def build_match_all(tokens: Sequence[str], *, max_tokens: int = 16) -> str:
    safe: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        t = t.replace('"', "")
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        safe.append(f'"{t}"')
        if len(safe) >= max_tokens:
            break
    if not safe:
        return ""
    return " AND ".join(safe)


def build_match_expression(
    raw_query: str,
    *,
    query_mode: str,
    must_terms: Sequence[str],
    max_tokens: int = 64,
) -> str:
    must_clauses: List[str] = []
    for t in must_terms:
        clause = build_match_all(fts_tokens(t), max_tokens=16)
        if clause:
            must_clauses.append(f"({clause})" if " AND " in clause else clause)

    query_clause = ""
    if query_mode == "and":
        parts = query_terms(raw_query)
        q_clauses: List[str] = []
        for p in parts:
            clause = build_match_all(fts_tokens(p), max_tokens=16)
            if not clause:
                continue
            q_clauses.append(f"({clause})" if " AND " in clause else clause)
        query_clause = " AND ".join(q_clauses)
    else:
        # default: OR across all query tokens
        query_clause = build_match_query(fts_tokens(raw_query), max_tokens=max_tokens)

    clauses = [c for c in must_clauses + ([query_clause] if query_clause else []) if c]
    if not clauses:
        return ""
    if len(clauses) == 1:
        return clauses[0]
    return " AND ".join(f"({c})" if (" OR " in c or " AND " in c) else c for c in clauses)


def query_terms(raw_query: str) -> List[str]:
    q = raw_query.strip()
    if not q:
        return []
    parts = [p.strip() for p in re.split(r"\s+", q) if p.strip()]
    if parts:
        return parts
    return [q]


def count_occurrences(haystack: str, needle: str) -> int:
    if not haystack or not needle:
        return 0
    return haystack.count(needle)


def extract_window(text: str, terms: Sequence[str], max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    s = text
    idx = -1
    hit = ""
    for t in terms:
        if not t:
            continue
        j = s.find(t)
        if j != -1 and (idx == -1 or j < idx):
            idx = j
            hit = t
    if idx == -1:
        return s[:max_chars]
    start = max(0, idx - max_chars // 3)
    end = min(len(s), start + max_chars)
    if end - start < max_chars:
        start = max(0, end - max_chars)
    snippet = s[start:end]
    if start > 0:
        snippet = "… " + snippet
    if end < len(s):
        snippet = snippet + " …"
    if hit and hit not in snippet:
        # best-effort: do nothing if windowing missed due to truncation
        pass
    return snippet


def parse_frontmatter(md: str) -> Dict[str, str]:
    if not md.startswith("---"):
        return {}
    parts = md.split("---", 2)
    if len(parts) < 3:
        return {}
    fm: Dict[str, str] = {}
    for line in parts[1].splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        fm[k.strip()] = v.strip().strip('"')
    return fm


def strip_frontmatter(md: str) -> str:
    if not md.startswith("---"):
        return md
    parts = md.split("---", 2)
    if len(parts) < 3:
        return md
    body = parts[2]
    return body.lstrip("\r\n")


def markdown_to_plain(md: str) -> str:
    out_lines: List[str] = []
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            out_lines.append("")
            continue
        line = re.sub(r"^#{1,6}\\s+", "", line)
        line = re.sub(r"`([^`]*)`", r"\\1", line)
        line = re.sub(r"\\[(.*?)\\]\\((.*?)\\)", r"\\1", line)
        line = line.replace("**", "").replace("__", "").replace("*", "")
        out_lines.append(line)
    return "\n".join(out_lines).strip() + "\n"


@dataclass
class Node:
    node_id: str
    doc_id: str
    doc_title: str
    source_file: str
    source_version: str
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


@dataclass
class NodeRow:
    node_id: str
    doc_id: str
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


@dataclass(frozen=True)
class NormalizedQuery:
    raw: str
    normalized: str
    article_terms: List[str]
    title_terms: List[str]
    alias_terms: List[str]


_VERSION_RE = re.compile(r"\bV(?P<num>\d+)\b", re.IGNORECASE)
_CN_NUMERAL_MAP = {"零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
_CN_UNIT_MAP = {"十": 10, "百": 100, "千": 1000}


def stable_hash(text: str) -> str:
    import hashlib

    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def node_key(node_id: str, source_version: str) -> str:
    return f"{node_id}@{source_version}"


def derive_source_version(name: str, title: str) -> str:
    match = _VERSION_RE.search(title) or _VERSION_RE.search(name)
    if match:
        return f"v{match.group('num')}"
    return "current"


def cn_numeral_to_int(text: str) -> int:
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    total = 0
    current = 0
    for ch in text:
        if ch in _CN_NUMERAL_MAP:
            current = _CN_NUMERAL_MAP[ch]
            continue
        unit = _CN_UNIT_MAP.get(ch)
        if not unit:
            continue
        total += (current or 1) * unit
        current = 0
    return total + current


def normalize_query(raw_query: str) -> NormalizedQuery:
    raw = unicodedata.normalize("NFKC", raw_query.strip())
    article_terms: List[str] = []
    for match in re.finditer(r"第\s*([0-9一二三四五六七八九十百千]+)\s*[条條]", raw):
        article_num = cn_numeral_to_int(match.group(1))
        if article_num > 0:
            article_terms.append(f"第{article_num}条")

    title_fragment = re.sub(r"第\s*[0-9一二三四五六七八九十百千]+\s*[条條]", " ", raw)
    title_fragment = title_fragment.replace("是什么", " ").replace("什么", " ")
    title_fragment = re.sub(r"[的与和]\s*", " ", title_fragment)
    title_fragment = title_fragment.strip()
    title_terms = [t for t in query_terms(title_fragment) if t] if title_fragment else []
    if not title_terms and raw:
        title_terms = [raw]

    alias_terms = [term for term in [raw, *title_terms] if term]
    return NormalizedQuery(
        raw=raw_query,
        normalized=normalize_alias_text(raw),
        article_terms=article_terms,
        title_terms=title_terms,
        alias_terms=alias_terms,
    )


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
TRIGGER_TERMS = {"定义", "适用", "例外", "但书", "条件", "流程", "版本", "修订", "区别"}


def normalize_article_ref(label: str) -> str:
    return re.sub(r"\s+", "", label).replace("條", "条")


def normalize_alias_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).lower()
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", text)


def core_alias_title(title: str) -> str:
    return re.sub(r"^第\s*[0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?\s*[条條]\s*", "", title).strip()



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
                    AliasRow(
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



def search_alias_hits(conn: sqlite3.Connection, raw_query: str, *, include_soft: bool) -> List[str]:
    normalized_query = normalize_alias_text(raw_query)
    if not normalized_query:
        return []
    allowed = [ALIAS_EXACT, ALIAS_ABBREVIATION]
    if include_soft:
        allowed.append(ALIAS_SOFT)
    placeholders = ",".join("?" for _ in allowed)
    rows = conn.execute(
        f"""
        SELECT target_node_id, alias_level, confidence
        FROM aliases
        WHERE normalized_alias = ? AND is_active = 1 AND alias_level IN ({placeholders})
        ORDER BY CASE alias_level
          WHEN 'exact' THEN 0
          WHEN 'abbreviation' THEN 1
          ELSE 2
        END, confidence DESC, target_node_id ASC
        """,
        (normalized_query, *allowed),
    ).fetchall()
    out: List[str] = []
    seen: set[str] = set()
    for row in rows:
        node_id = str(row[0])
        if node_id in seen:
            continue
        seen.add(node_id)
        out.append(node_id)
    return out


def is_short_query(raw_query: str) -> bool:
    normalized_query = normalize_alias_text(raw_query)
    return bool(normalized_query) and len(normalized_query) <= 2



def search_title_nodes(conn: sqlite3.Connection, normalized_query: NormalizedQuery, *, limit: int) -> List[str]:
    rows = conn.execute(
        "SELECT node_id, kind, title FROM nodes WHERE is_leaf = 1 AND is_active = 1 ORDER BY node_id ASC"
    ).fetchall()
    scored: List[Tuple[int, str]] = []
    for row in rows:
        if str(row["kind"]) == "block":
            continue
        title_text = str(row["title"])
        title_norm = normalize_alias_text(title_text)
        score = 0
        for term in normalized_query.article_terms:
            normalized_term = normalize_alias_text(term)
            if normalized_term and normalized_term in title_norm:
                score += 2000
        for term in normalized_query.title_terms:
            normalized_term = normalize_alias_text(term)
            if normalized_term and normalized_term in title_norm:
                score += 1000
        if score > 0:
            scored.append((score, str(row["node_id"])))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [node_id for _, node_id in scored[:limit]]


def search_body_nodes(
    conn: sqlite3.Connection,
    normalized_query: NormalizedQuery,
    *,
    limit: int,
    query_mode: str,
    must_terms: Sequence[str],
) -> List[str]:
    match = build_match_expression(normalized_query.raw, query_mode=query_mode, must_terms=must_terms)
    if not match:
        return []
    rows = conn.execute(
        """
        SELECT n.node_id, bm25(node_fts) AS bm25
        FROM node_fts
        JOIN nodes n ON n.node_key = node_fts.node_key
        WHERE node_fts MATCH ? AND n.is_active = 1 AND n.is_leaf = 1
        ORDER BY bm25(node_fts), n.node_id
        LIMIT ?
        """,
        (match, limit),
    ).fetchall()

    terms: List[str] = []
    seen_terms: set[str] = set()
    for term in [*normalized_query.title_terms, *must_terms]:
        if not term or term in seen_terms:
            continue
        seen_terms.add(term)
        terms.append(term)

    scored: List[Tuple[int, float, str]] = []
    for row in rows:
        node_id = str(row["node_id"])
        bm25 = float(row["bm25"])
        node = get_node(conn, node_id)
        title_hay = node.title.lower()
        body_hay = node.body_plain.lower()
        occ_title = sum(count_occurrences(title_hay, term.lower()) for term in terms)
        occ_body = sum(count_occurrences(body_hay, term.lower()) for term in terms)
        score = occ_title * 1000 + occ_body * 100
        scored.append((score, -bm25, node_id))
    scored.sort(reverse=True)
    return [node_id for _, _, node_id in scored[:limit]]


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


def expand_reference_hits(conn: sqlite3.Connection, node_ids: Sequence[str]) -> List[str]:
    expanded: List[str] = []
    seen = set(node_ids)
    for node_id in node_ids:
        rows = conn.execute(
            """
            SELECT to_node_id
            FROM edges
            WHERE edge_type = 'references' AND from_node_id = ? AND is_active = 1
            ORDER BY to_node_id
            """,
            (node_id,),
        ).fetchall()
        for row in rows:
            target = elevate_to_article(conn, str(row[0]))
            if target in seen:
                continue
            seen.add(target)
            expanded.append(target)
    return expanded


def should_trigger_expansion(normalized_query: NormalizedQuery, hits: Sequence[str]) -> Tuple[List[str], List[str]]:
    del hits
    actions: List[str] = []
    reasons: List[str] = []
    raw = normalized_query.raw
    if "定义" in raw:
        actions.append("definition")
        reasons.append("问法包含“定义”")
    if any(term in raw for term in ("适用", "例外", "但书", "条件", "流程", "区别")):
        actions.append("references")
        reasons.append("问法包含范围/例外类词")
    if any(term in raw for term in ("版本", "修订")):
        actions.append("version_metadata")
        reasons.append("问法包含版本类词")
    deduped_actions: List[str] = []
    seen_actions: set[str] = set()
    for action in actions:
        if action in seen_actions:
            continue
        seen_actions.add(action)
        deduped_actions.append(action)
    return deduped_actions, reasons


def apply_triggered_expansion(
    conn: sqlite3.Connection,
    normalized_query: NormalizedQuery,
    hits: Sequence[str],
    *,
    force_debug: bool,
) -> Tuple[List[str], List[str]]:
    actions, reasons = should_trigger_expansion(normalized_query, hits)
    if not actions and not force_debug:
        return list(hits), []

    expanded = list(hits)
    seen = set(hits)
    added: List[str] = []

    if "definition" in actions:
        for node_id in search_title_nodes(conn, normalize_query("定义"), limit=3):
            article_id = elevate_to_article(conn, node_id)
            if article_id in seen:
                continue
            seen.add(article_id)
            expanded.append(article_id)
            added.append(article_id)

    if "references" in actions:
        for node_id in expand_reference_hits(conn, hits):
            if node_id in seen:
                continue
            seen.add(node_id)
            expanded.append(node_id)
            added.append(node_id)

    diagnostics: List[str] = []
    for reason in reasons:
        diagnostics.append(f"补查触发：{reason}")
    if actions:
        diagnostics.append("补查动作：" + ", ".join(actions))
    for node_id in added:
        diagnostics.append(f"新增节点：`{display_node_id(node_id)}`")
    if force_debug and not diagnostics:
        diagnostics.append("补查触发：手动调试")
    return expanded, diagnostics


def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        die(f"Missing kb.sqlite: {db_path} (run build or reindex first)")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn



def get_node(conn: sqlite3.Connection, node_id: str) -> Node:
    row = conn.execute(
        """
        SELECT
          n.node_id, n.doc_id, d.doc_title, d.source_file, n.source_version, n.kind, n.label, n.title,
          n.parent_id, n.prev_id, n.next_id, n.ordinal, n.ref_path, n.is_leaf,
          t.body_md, t.body_plain
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version AND d.is_active = 1
        JOIN node_text t ON t.node_key = n.node_key
        WHERE n.node_id = ? AND n.is_active = 1
        ORDER BY n.source_version DESC
        LIMIT 1
        """,
        (node_id,),
    ).fetchone()
    if not row:
        die(f"Unknown node_id: {node_id}")
    return Node(
        node_id=row["node_id"],
        doc_id=row["doc_id"],
        doc_title=row["doc_title"],
        source_file=row["source_file"],
        source_version=row["source_version"],
        kind=row["kind"],
        label=row["label"],
        title=row["title"],
        parent_id=row["parent_id"],
        prev_id=row["prev_id"],
        next_id=row["next_id"],
        ordinal=int(row["ordinal"]),
        ref_path=row["ref_path"],
        is_leaf=bool(row["is_leaf"]),
        body_md=row["body_md"],
        body_plain=row["body_plain"],
    )


def elevate_to_article(conn: sqlite3.Connection, node_id: str) -> str:
    n = get_node(conn, node_id)
    if n.kind == "article":
        return n.node_id
    cur = n
    while cur.parent_id:
        parent = get_node(conn, cur.parent_id)
        if parent.kind == "article":
            return parent.node_id
        cur = parent
    return n.node_id


def iter_parents(conn: sqlite3.Connection, node: Node) -> List[str]:
    out: List[str] = []
    cur = node
    while cur.parent_id:
        pid = cur.parent_id
        out.append(pid)
        cur = get_node(conn, pid)
    out.reverse()
    return out


def iter_neighbors(conn: sqlite3.Connection, node: Node, neighbors: int) -> List[str]:
    if neighbors <= 0:
        return [node.node_id]
    out: List[str] = []

    prev_ids: List[str] = []
    cur = node
    for _ in range(neighbors):
        if not cur.prev_id:
            break
        p = get_node(conn, cur.prev_id)
        if p.parent_id != node.parent_id:
            break
        prev_ids.append(p.node_id)
        cur = p
    prev_ids.reverse()
    out.extend(prev_ids)
    out.append(node.node_id)

    cur = node
    for _ in range(neighbors):
        if not cur.next_id:
            break
        n = get_node(conn, cur.next_id)
        if n.parent_id != node.parent_id:
            break
        out.append(n.node_id)
        cur = n
    return out


def search_leaf_nodes(
    conn: sqlite3.Connection,
    raw_query: str,
    *,
    limit: int = 20,
    query_mode: str = "or",
    must_terms: Sequence[str] = (),
) -> List[str]:
    normalized_query = normalize_query(raw_query)
    title_hits = search_title_nodes(conn, normalized_query, limit=limit)
    body_hits = search_body_nodes(
        conn,
        normalized_query,
        limit=limit,
        query_mode=query_mode,
        must_terms=must_terms,
    )
    alias_hits: List[str] = []
    seen_alias_hits: set[str] = set()
    for term in normalized_query.alias_terms:
        for node_id in search_alias_hits(conn, term, include_soft=not (title_hits or body_hits)):
            if node_id in seen_alias_hits:
                continue
            seen_alias_hits.add(node_id)
            alias_hits.append(node_id)

    title_rank = {node_id: idx for idx, node_id in enumerate(title_hits)}
    body_rank = {node_id: idx for idx, node_id in enumerate(body_hits)}
    alias_rank = {node_id: idx for idx, node_id in enumerate(alias_hits)}

    combined = [*title_hits, *alias_hits, *body_hits]
    ordered_scores: List[Tuple[int, str]] = []
    seen_nodes: set[str] = set()
    has_primary_support = bool(title_hits or alias_hits)
    for node_id in combined:
        if node_id in seen_nodes:
            continue
        seen_nodes.add(node_id)
        if is_short_query(raw_query) and has_primary_support and node_id not in title_rank and node_id not in alias_rank:
            continue
        score = 0
        if node_id in title_rank:
            score += 3000 - title_rank[node_id]
        if node_id in alias_rank:
            score += 2500 - alias_rank[node_id]
        if node_id in body_rank:
            score += 1000 - body_rank[node_id]
        if node_id in title_rank and node_id in body_rank:
            score += 250
        if node_id in alias_rank and node_id in body_rank:
            score += 100
        ordered_scores.append((score, node_id))
    ordered_scores.sort(key=lambda item: (-item[0], item[1]))
    return [node_id for _, node_id in ordered_scores[:limit]]


def display_node_id(node_id: str) -> str:
    match = re.search(r":article:(\d+)$", node_id)
    if not match:
        return node_id
    return re.sub(r":article:\d+$", f":article:{int(match.group(1)):03d}", node_id)

def chronological_key(conn: sqlite3.Connection, node_id: str) -> Tuple[str, Tuple[int, ...], str]:
    n = get_node(conn, node_id)
    ords: List[int] = [n.ordinal]
    cur = n
    while cur.parent_id:
        cur = get_node(conn, cur.parent_id)
        ords.append(cur.ordinal)
    ords.reverse()
    return (n.doc_id, tuple(ords), n.node_id)


def render_bundle(
    conn: sqlite3.Connection,
    hits: Sequence[str],
    *,
    raw_query: str,
    neighbors: int,
    max_chars: int,
    per_node_max_chars: int,
    order: str,
    diagnostics: Sequence[str] = (),
) -> Tuple[str, List[Node]]:
    included: List[str] = []
    seen: set[str] = set()

    for node_id in hits:
        node = get_node(conn, node_id)
        for pid in iter_parents(conn, node):
            if pid not in seen:
                seen.add(pid)
                included.append(pid)

        for nid in iter_neighbors(conn, node, neighbors):
            if nid not in seen:
                seen.add(nid)
                included.append(nid)

    if order == "chronological":
        included.sort(key=lambda nid: chronological_key(conn, nid))

    terms = query_terms(raw_query)
    out_parts: List[str] = [f"# Bundle\n\n- Query: `{raw_query}`\n\n"]
    if diagnostics:
        out_parts.append("## 补查记录\n")
        for line in diagnostics:
            out_parts.append(f"- {line}\n")
        out_parts.append("\n")
    rendered_nodes: List[Node] = []

    remaining = max_chars

    for node_id in included:
        node = get_node(conn, node_id)
        header = (
            f"## {node.doc_title} — {node.title}\n\n"
            f"- node_id: `{display_node_id(node.node_id)}`\n"
            f"- source_file: `{node.source_file}`\n"
            f"- source: `{node.ref_path}`\n\n"
        )

        body = ""
        if node.is_leaf:
            body = node.body_md.strip() + "\n"
            if len(body) > per_node_max_chars or len(header) + len(body) > remaining:
                snippet = extract_window(
                    node.body_plain, terms, min(per_node_max_chars, max(200, remaining - len(header)))
                )
                body = snippet.strip() + "\n\n*(TRUNCATED)*\n"

        chunk = header + body + ("\n" if body else "")
        if len(chunk) > remaining:
            break
        out_parts.append(chunk)
        remaining -= len(chunk)
        rendered_nodes.append(node)

    out_parts.append("## 参考依据\n")
    seen_cites: set[str] = set()
    for n in rendered_nodes:
        if not n.is_leaf:
            continue
        key = f"{n.doc_title}::{n.ref_path}"
        if key in seen_cites:
            continue
        seen_cites.add(key)
        out_parts.append(f"- {n.doc_title}（{n.source_file}） {n.title}（{n.ref_path}）\n")

    return "".join(out_parts), rendered_nodes


def cmd_bundle(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve() if args.root else Path.cwd()
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        hits = search_leaf_nodes(
            conn,
            args.query,
            limit=args.limit,
            query_mode=args.query_mode,
            must_terms=args.must,
        )
        if not hits:
            die("No matches. Try a different query or rebuild indexes.")
        elevated: List[str] = []
        seen = set()
        for node_id in hits:
            article_id = elevate_to_article(conn, node_id)
            if article_id in seen:
                continue
            seen.add(article_id)
            elevated.append(article_id)
        normalized_query = normalize_query(args.query)
        elevated, diagnostics = apply_triggered_expansion(
            conn,
            normalized_query,
            elevated,
            force_debug=args.debug_triggers,
        )
        content, _ = render_bundle(
            conn,
            elevated,
            raw_query=args.query,
            neighbors=args.neighbors,
            max_chars=args.max_chars,
            per_node_max_chars=args.per_node_max_chars,
            order=args.order,
            diagnostics=diagnostics,
        )
        out_path = (root / args.out).resolve()
        out_path.write_text(content, encoding="utf-8", newline="\n")
        print("[OK] Wrote bundle:", out_path)
        return 0
    finally:
        conn.close()

def render_search_md(
    conn: sqlite3.Connection,
    hits: Sequence[str],
    *,
    raw_query: str,
    query_mode: str,
    must_terms: Sequence[str],
    snippet_chars: int,
) -> str:
    parts: List[str] = [f"# Search\n\n- Query: `{raw_query}`\n- query_mode: `{query_mode}`\n"]
    if must_terms:
        must = ", ".join(f"`{t}`" for t in must_terms if t)
        if must:
            parts.append(f"- must: {must}\n")
    parts.append("\n## Hits\n\n")

    terms: List[str] = []
    seen: set[str] = set()
    for t in list(query_terms(raw_query)) + list(must_terms):
        if not t or t in seen:
            continue
        seen.add(t)
        terms.append(t)

    for i, node_id in enumerate(hits, start=1):
        node = get_node(conn, node_id)
        parts.append(f"### {i}. {node.doc_title} — {node.title}\n\n")
        parts.append(f"- node_id: `{display_node_id(node.node_id)}`\n")
        parts.append(f"- kind: `{node.kind}`\n")
        parts.append(f"- source_file: `{node.source_file}`\n")
        parts.append(f"- source: `{node.ref_path}`\n\n")
        snippet = extract_window(node.body_plain, terms, snippet_chars).strip()
        if snippet:
            snippet_md = "\n".join("> " + ln for ln in snippet.splitlines())
            parts.append(snippet_md + "\n\n")
    return "".join(parts)


def cmd_search(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve() if args.root else Path.cwd()
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        hits = search_leaf_nodes(
            conn,
            args.query,
            limit=args.limit,
            query_mode=args.query_mode,
            must_terms=args.must,
        )
        if not hits:
            die("No matches. Try a different query or rebuild indexes.")
        elevated: List[str] = []
        seen = set()
        for node_id in hits:
            article_id = elevate_to_article(conn, node_id)
            if article_id in seen:
                continue
            seen.add(article_id)
            elevated.append(article_id)

        content = render_search_md(
            conn,
            elevated,
            raw_query=args.query,
            query_mode=args.query_mode,
            must_terms=args.must,
            snippet_chars=args.snippet_chars,
        )
        out_path = (root / args.out).resolve()
        out_path.write_text(content, encoding="utf-8", newline="\n")
        print("[OK] Wrote search:", out_path)
        return 0
    finally:
        conn.close()


def load_manifest_docs(root: Path) -> Dict[str, DocRow]:
    manifest = root / "manifest.json"
    if not manifest.exists():
        return {}
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        die(f"Invalid manifest.json: {e}")
    out: Dict[str, DocRow] = {}
    for d in data.get("docs", []) if isinstance(data, dict) else []:
        if not isinstance(d, dict):
            continue
        doc_id = str(d.get("doc_id") or "").strip()
        if not doc_id:
            continue
        out[doc_id] = DocRow(
            doc_id=doc_id,
            doc_title=str(d.get("title") or doc_id),
            source_file=str(d.get("source_file") or "(unknown)"),
            source_path=str(d.get("source_path") or str(root / "references" / doc_id)),
            doc_hash=str(d.get("doc_hash") or ""),
            source_version=str(d.get("source_version") or derive_source_version(doc_id, str(d.get("title") or doc_id))),
            is_active=bool(d.get("active_version", d.get("is_active", True))),
        )
    return out


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
    refs_root = root / "references"
    if not refs_root.exists():
        die(f"Missing references/: {refs_root}")

    manifest_docs = load_manifest_docs(root)
    docs: Dict[str, DocRow] = {}
    nodes: List[NodeRow] = []

    for doc_dir in sorted([p for p in refs_root.iterdir() if p.is_dir()]):
        doc_id = doc_dir.name
        title, source_file, source_version, doc_hash = parse_doc_metadata(doc_dir)
        if doc_id in manifest_docs:
            manifest_doc = manifest_docs[doc_id]
            docs[doc_id] = DocRow(
                doc_id=doc_id,
                doc_title=title or manifest_doc.doc_title,
                source_file=source_file or manifest_doc.source_file,
                source_path=manifest_doc.source_path,
                doc_hash=doc_hash or manifest_doc.doc_hash,
                source_version=source_version or manifest_doc.source_version,
                is_active=True,
            )
        else:
            docs[doc_id] = DocRow(
                doc_id=doc_id,
                doc_title=title,
                source_file=source_file,
                source_path=str(doc_dir),
                doc_hash=doc_hash,
                source_version=source_version,
                is_active=True,
            )
        doc_source_version = docs[doc_id].source_version

        def rel(p: Path) -> str:
            return str(p.relative_to(root)).replace("\\", "/")

        chapters_dir = doc_dir / "chapters"
        chapter_nodes: List[NodeRow] = []
        if chapters_dir.exists():
            for chapter_path in sorted(chapters_dir.glob("chapter*.md")):
                fm, body_md = read_md_with_frontmatter(chapter_path)
                chapter_id = (fm.get("chapter_id") or chapter_path.stem).strip()
                chapter_title = (fm.get("chapter_title") or chapter_id).strip()
                node_id = f"{doc_id}:chapter:{chapter_id}"
                chapter_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="chapter",
                        label=chapter_id,
                        title=chapter_title,
                        parent_id=None,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(chapter_id, default=0),
                        ref_path=rel(chapter_path),
                        is_leaf=True,
                        body_md=body_md,
                        body_plain=markdown_to_plain(body_md),
                        source_version=doc_source_version,
                        aliases=parse_aliases_field(fm.get("aliases", "")),
                    )
                )
        nodes.extend(chapter_nodes)

        sections_root = doc_dir / "sections"
        section_nodes: List[NodeRow] = []
        if sections_root.exists():
            for chapter_folder in sorted([p for p in sections_root.iterdir() if p.is_dir()]):
                chapter_id = chapter_folder.name
                chapter_node_id = f"{doc_id}:chapter:{chapter_id}"
                for sec_path in sorted(chapter_folder.glob("*.md")):
                    fm, body_md = read_md_with_frontmatter(sec_path)
                    sec_id = (fm.get("section_id") or sec_path.stem).strip()
                    sec_title = (fm.get("section_title") or sec_id).strip()
                    sec_node_id = f"{doc_id}:section:{chapter_id}/{sec_id}"
                    section_nodes.append(
                        NodeRow(
                            node_id=sec_node_id,
                            doc_id=doc_id,
                            kind="section",
                            label=sec_id,
                            title=sec_title,
                            parent_id=chapter_node_id,
                            prev_id=None,
                            next_id=None,
                            ordinal=parse_int_suffix(sec_id, default=0),
                            ref_path=rel(sec_path),
                            is_leaf=True,
                            body_md=body_md,
                            body_plain=markdown_to_plain(body_md),
                            source_version=doc_source_version,
                            aliases=parse_aliases_field(fm.get("aliases", "")),
                        )
                    )
        nodes.extend(section_nodes)

        articles_dir = doc_dir / "articles"
        article_nodes: List[NodeRow] = []
        if articles_dir.exists():
            for a_path in sorted(articles_dir.glob("article-*.md")):
                fm, body_md = read_md_with_frontmatter(a_path)
                node_id = (fm.get("node_id") or "").strip()
                if not node_id:
                    num = parse_int_suffix(a_path.stem, default=0)
                    node_id = f"{doc_id}:article:{num:04d}"
                parent_id = (fm.get("parent_id") or "").strip() or None
                label = (fm.get("label") or a_path.stem).strip()
                title = (fm.get("title") or label).strip()
                article_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="article",
                        label=label,
                        title=title,
                        parent_id=parent_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(a_path.stem, default=0),
                        ref_path=rel(a_path),
                        is_leaf=True,
                        body_md=body_md,
                        body_plain=markdown_to_plain(body_md),
                        source_version=doc_source_version,
                        aliases=parse_aliases_field(fm.get("aliases", "")),
                    )
                )
        nodes.extend(article_nodes)

        items_root = doc_dir / "items"
        item_nodes: List[NodeRow] = []
        if items_root.exists():
            for a_folder in sorted([p for p in items_root.iterdir() if p.is_dir()]):
                for i_path in sorted(a_folder.glob("item-*.md")):
                    fm, body_md = read_md_with_frontmatter(i_path)
                    node_id = (fm.get("node_id") or "").strip()
                    if not node_id:
                        a_num = parse_int_suffix(a_folder.name, default=0)
                        i_num = parse_int_suffix(i_path.stem, default=0)
                        node_id = f"{doc_id}:item:{a_num:04d}:{i_num:02d}"
                    parent_id = (fm.get("parent_id") or "").strip() or None
                    label = (fm.get("label") or i_path.stem).strip()
                    title = (fm.get("title") or label).strip()
                    item_nodes.append(
                        NodeRow(
                            node_id=node_id,
                            doc_id=doc_id,
                            kind="item",
                            label=label,
                            title=title,
                            parent_id=parent_id,
                            prev_id=None,
                            next_id=None,
                            ordinal=parse_int_suffix(i_path.stem, default=0),
                            ref_path=rel(i_path),
                            is_leaf=True,
                            body_md=body_md,
                            body_plain=markdown_to_plain(body_md),
                            source_version=doc_source_version,
                            aliases=parse_aliases_field(fm.get("aliases", "")),
                        )
                    )
        nodes.extend(item_nodes)

        blocks_dir = doc_dir / "blocks"
        block_nodes: List[NodeRow] = []
        if blocks_dir.exists():
            for b_path in sorted(blocks_dir.glob("block-*.md")):
                fm, body_md = read_md_with_frontmatter(b_path)
                node_id = (fm.get("node_id") or "").strip()
                if not node_id:
                    num = parse_int_suffix(b_path.stem, default=0)
                    node_id = f"{doc_id}:block:{num:04d}"
                parent_id = (fm.get("parent_id") or "").strip() or None
                label = (fm.get("label") or b_path.stem).strip()
                title = (fm.get("title") or label).strip()
                block_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="block",
                        label=label,
                        title=title,
                        parent_id=parent_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(b_path.stem, default=0),
                        ref_path=rel(b_path),
                        is_leaf=True,
                        body_md=body_md,
                        body_plain=markdown_to_plain(body_md),
                        source_version=doc_source_version,
                        aliases=parse_aliases_field(fm.get("aliases", "")),
                    )
                )
        nodes.extend(block_nodes)

        children_by_parent: Dict[str, int] = {}
        for n in nodes:
            if n.doc_id != doc_id or not n.parent_id:
                continue
            children_by_parent[n.parent_id] = children_by_parent.get(n.parent_id, 0) + 1
        for n in nodes:
            if n.doc_id != doc_id:
                continue
            if n.kind in {"chapter", "section"} and n.node_id in children_by_parent:
                n.is_leaf = False

    by_group: Dict[Tuple[str, Optional[str], str], List[NodeRow]] = {}
    for n in nodes:
        by_group.setdefault((n.doc_id, n.parent_id, n.kind), []).append(n)
    for siblings in by_group.values():
        siblings.sort(key=lambda x: x.ordinal)
        for i, cur in enumerate(siblings):
            cur.prev_id = siblings[i - 1].node_id if i > 0 else None
            cur.next_id = siblings[i + 1].node_id if i + 1 < len(siblings) else None

    doc_list = [docs[k] for k in sorted(docs.keys())]
    return doc_list, nodes



def write_kb_sqlite_db(
        db_path: Path,
        docs: Sequence[DocRow],
        nodes: Sequence[NodeRow],
        edges: Sequence[EdgeRow],
        aliases: Sequence[AliasRow],
    ) -> None:
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.executescript(
                """
                DROP TABLE IF EXISTS node_fts;
                DROP TABLE IF EXISTS aliases;
                DROP TABLE IF EXISTS edges;
                DROP TABLE IF EXISTS node_text;
                DROP TABLE IF EXISTS nodes;
                DROP TABLE IF EXISTS docs;

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
            conn.execute("PRAGMA foreign_keys=ON")

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
                die(f"SQLite FTS5 is required but unavailable: {e}")

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


def write_kb_sqlite(db_path: Path, docs: Sequence[DocRow], nodes: Sequence[NodeRow]) -> None:
        write_kb_sqlite_db(db_path, docs, nodes, extract_reference_edges(nodes), extract_alias_rows(nodes))


def read_existing_docs(db_path: Path) -> List[DocRow]:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active
                FROM docs
                ORDER BY doc_id, source_version
                """
            ).fetchall()
        finally:
            conn.close()
        return [
            DocRow(
                doc_id=str(row["doc_id"]),
                doc_title=str(row["doc_title"]),
                source_file=str(row["source_file"]),
                source_path=str(row["source_path"]),
                doc_hash=str(row["doc_hash"]),
                source_version=str(row["source_version"]),
                is_active=bool(row["is_active"]),
            )
            for row in rows
        ]


def read_existing_nodes(db_path: Path) -> List[NodeRow]:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT
                  n.node_id, n.doc_id, n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id,
                  n.ordinal, n.ref_path, n.is_leaf, n.source_version, n.is_active,
                  n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence,
                  t.body_md, t.body_plain
                FROM nodes n
                JOIN node_text t ON t.node_key = n.node_key
                ORDER BY n.doc_id, n.source_version, n.node_id
                """
            ).fetchall()
        finally:
            conn.close()
        return [
            NodeRow(
                node_id=str(row["node_id"]),
                doc_id=str(row["doc_id"]),
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
                aliases=(),
                raw_span_start=int(row["raw_span_start"]),
                raw_span_end=int(row["raw_span_end"]),
                node_hash=str(row["node_hash"]),
                confidence=float(row["confidence"]),
            )
            for row in rows
        ]


def read_existing_edges(db_path: Path) -> List[EdgeRow]:
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
            EdgeRow(
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


def read_existing_aliases(db_path: Path) -> List[AliasRow]:
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
            for row in rows
        ]


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
        sort_key=lambda record: (record.doc_id, record.source_version, record.edge_type, record.from_node_id, record.to_node_id, 0 if record.is_active else 1),
    )
    merged_aliases = merge_history(
        current_aliases,
        rebuilt_aliases,
        key_fn=lambda record: (record.normalized_alias, record.target_node_id, record.alias_level, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.normalized_alias, record.target_node_id, record.alias_level, 0 if record.is_active else 1),
    )
    write_kb_sqlite_db(shadow_path, merged_docs, merged_nodes, merged_edges, merged_aliases)
    return shadow_path, merged_docs, merged_nodes


def cmd_reindex(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve() if args.root else Path.cwd()
    db_path = root / args.db
    shadow_path, docs, nodes = rebuild_shadow_db(root, db_path)
    docs_count, nodes_count, leaf_count = validate_shadow_db(shadow_path)
    print(f"[OK] shadow rebuild: {shadow_path} (docs={docs_count}, nodes={nodes_count}, leaf={leaf_count})")
    atomic_replace(shadow_path, db_path)
    print(f"[OK] atomic switch: {shadow_path} -> {db_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="KB tool for generated skills (search, bundle).")
    p.add_argument("--root", default="", help="Skill root directory (default: cwd).")
    p.add_argument("--db", default="kb.sqlite", help="SQLite DB path relative to root (default: kb.sqlite).")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bundle", help="Search + expand + write a single evidence bundle markdown file.")
    b.add_argument("--query", required=True, help="User query.")
    b.add_argument("--out", default="bundle.md", help="Output markdown path (relative to root).")
    b.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    b.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    b.add_argument("--must", action="append", default=[], help="Term that must appear (repeatable).")
    b.add_argument("--neighbors", type=int, default=1, help="Expand to prev/next leaf nodes within same parent.")
    b.add_argument("--order", choices=["relevance", "chronological"], default="relevance", help="Output order.")
    b.add_argument("--max-chars", type=int, default=40000, help="Max output size (characters).")
    b.add_argument("--per-node-max-chars", type=int, default=6000, help="Max chars per node before truncation.")
    b.add_argument("--debug-triggers", action="store_true", help="Emit reference-trigger diagnostics and one-hop reference expansion.")
    b.set_defaults(func=cmd_bundle)

    s = sub.add_parser("search", help="Search leaf nodes and write ranked hits with snippets.")
    s.add_argument("--query", required=True, help="User query.")
    s.add_argument("--out", default="search.md", help="Output markdown path (relative to root).")
    s.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    s.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    s.add_argument("--must", action="append", default=[], help="Term that must appear (repeatable).")
    s.add_argument("--snippet-chars", type=int, default=400, help="Max chars per hit snippet.")
    s.set_defaults(func=cmd_search)

    r = sub.add_parser("reindex", help="Rebuild kb.sqlite from references/ (after manual edits).")
    r.set_defaults(func=cmd_reindex)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
