from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .runtime import SqliteTimeout, die, open_db, print_json, resolve_root, run_hook, safe_output_path, sha1_file
from .text import (
    build_match_expression,
    build_match_query,
    core_alias_title,
    count_occurrences,
    extract_window,
    fts_tokens,
    is_cjk,
    markdown_to_plain,
    normalize_alias_text,
    query_terms,
)


@dataclass
class Node:
    node_id: str
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
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


@dataclass(frozen=True)
class NodeLinks:
    node_id: str
    doc_id: str
    kind: str
    parent_id: Optional[str]
    prev_id: Optional[str]
    next_id: Optional[str]
    ordinal: int


def node_to_dict(node: Node, *, include_body: bool) -> Dict[str, object]:
    out: Dict[str, object] = {
        "node_id": node.node_id,
        "doc_id": node.doc_id,
        "doc_title": node.doc_title,
        "source_file": node.source_file,
        "source_path": node.source_path,
        "source_version": node.source_version,
        "kind": node.kind,
        "label": node.label,
        "title": node.title,
        "parent_id": node.parent_id,
        "prev_id": node.prev_id,
        "next_id": node.next_id,
        "ordinal": int(node.ordinal),
        "ref_path": node.ref_path,
        "is_leaf": bool(node.is_leaf),
    }
    if include_body:
        out["body_md"] = node.body_md
        out["body_plain"] = node.body_plain
    return out


@dataclass(frozen=True)
class NormalizedQuery:
    raw: str
    normalized: str
    article_terms: List[str]
    title_terms: List[str]
    alias_terms: List[str]


_CN_NUMERAL_MAP = {"零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
_CN_UNIT_MAP = {"十": 10, "百": 100, "千": 1000}


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

    raw_query = raw_query.strip()
    alias_terms = [term for term in [raw, *title_terms] if term]
    return NormalizedQuery(
        raw=raw_query,
        normalized=normalize_alias_text(raw),
        article_terms=article_terms,
        title_terms=title_terms,
        alias_terms=alias_terms,
    )


def search_alias_hits(conn: sqlite3.Connection, raw_query: str, *, include_soft: bool) -> List[str]:
    normalized = normalize_alias_text(raw_query)
    if not normalized:
        return []

    level_filter = ""
    if not include_soft:
        level_filter = " AND a.alias_level IN ('exact', 'abbreviation') "

    rows = conn.execute(
        f"""
        SELECT DISTINCT a.target_node_id
        FROM aliases a
        JOIN nodes n ON n.node_id = a.target_node_id AND n.source_version = a.source_version
        WHERE a.normalized_alias = ? AND a.is_active = 1 AND n.is_active = 1 AND n.is_leaf = 1
        {level_filter}
        ORDER BY a.target_node_id
        LIMIT 10
        """,
        (normalized,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def is_short_query(raw_query: str) -> bool:
    raw = raw_query.strip()
    if not raw:
        return True
    if len(raw) <= 3:
        return True
    return False


def search_title_nodes(conn: sqlite3.Connection, normalized_query: NormalizedQuery, *, limit: int) -> List[str]:
    scores: Dict[str, int] = {}
    fetch_limit = max(int(limit) * 20, 50)
    for term in [*normalized_query.article_terms, *normalized_query.title_terms]:
        q = term.strip()
        if not q:
            continue
        for row in conn.execute(
            """
            SELECT node_id, kind, title
            FROM nodes
            WHERE title LIKE ? AND is_active = 1 AND is_leaf = 1
            ORDER BY node_id
            LIMIT ?
            """,
            (f"%{q}%", fetch_limit),
        ).fetchall():
            node_id = str(row["node_id"])
            title = str(row["title"] or "")
            kind = str(row["kind"] or "")
            occ = count_occurrences(title.lower(), q.lower())
            if occ <= 0:
                continue
            score = scores.get(node_id, 0)
            score += occ * 1000
            if kind == "article":
                score += 200
            scores[node_id] = score
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [node_id for node_id, _ in ranked[:limit]]


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
        SELECT n.node_id, n.title, t.body_plain, bm25(node_fts) AS bm25
        FROM node_fts
        JOIN nodes n ON n.node_key = node_fts.node_key
        JOIN node_text t ON t.node_key = n.node_key
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
        title_hay = str(row["title"] or "").lower()
        body_hay = str(row["body_plain"] or "").lower()
        occ_title = sum(count_occurrences(title_hay, term.lower()) for term in terms)
        occ_body = sum(count_occurrences(body_hay, term.lower()) for term in terms)
        score = occ_title * 1000 + occ_body * 100
        scored.append((score, -bm25, node_id))
    scored.sort(reverse=True)
    return [node_id for _, _, node_id in scored[:limit]]


def get_node_links(conn: sqlite3.Connection, node_id: str) -> NodeLinks:
    row = conn.execute(
        """
        SELECT node_id, doc_id, kind, parent_id, prev_id, next_id, ordinal
        FROM nodes
        WHERE node_id = ? AND is_active = 1
        ORDER BY source_version DESC
        LIMIT 1
        """,
        (node_id,),
    ).fetchone()
    if not row:
        die(f"Unknown node_id: {node_id}")
    return NodeLinks(
        node_id=str(row["node_id"]),
        doc_id=str(row["doc_id"]),
        kind=str(row["kind"]),
        parent_id=str(row["parent_id"]) if row["parent_id"] else None,
        prev_id=str(row["prev_id"]) if row["prev_id"] else None,
        next_id=str(row["next_id"]) if row["next_id"] else None,
        ordinal=int(row["ordinal"]),
    )


def open_node(conn: sqlite3.Connection, node_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT
          n.node_id, n.doc_id, d.doc_title,
          d.source_file, d.source_path, n.source_version,
          n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id, n.ordinal, n.ref_path, n.is_leaf,
          t.body_md, t.body_plain
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
        JOIN node_text t ON t.node_key = n.node_key
        WHERE n.node_id = ? AND n.is_active = 1
        ORDER BY n.source_version DESC
        LIMIT 1
        """,
        (node_id,),
    ).fetchone()
    if not row:
        die(f"Unknown node_id: {node_id}")
    return row


def get_node(conn: sqlite3.Connection, node_id: str) -> Node:
    row = open_node(conn, node_id)
    return Node(
        node_id=row["node_id"],
        doc_id=row["doc_id"],
        doc_title=row["doc_title"],
        source_file=row["source_file"],
        source_path=row["source_path"],
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
    n = get_node_links(conn, node_id)
    if n.kind == "article":
        return n.node_id
    cur = n
    while cur.parent_id:
        parent = get_node_links(conn, cur.parent_id)
        if parent.kind == "article":
            return parent.node_id
        cur = parent
    return n.node_id


def iter_parents(conn: sqlite3.Connection, node: Node) -> List[str]:
    out: List[str] = []
    parent_id = node.parent_id
    while parent_id:
        out.append(parent_id)
        parent_id = get_node_links(conn, parent_id).parent_id
    out.reverse()
    return out


def iter_neighbors(conn: sqlite3.Connection, node: Node, neighbors: int) -> List[str]:
    if neighbors <= 0:
        return [node.node_id]
    out: List[str] = []

    prev_ids: List[str] = []
    prev_id = node.prev_id
    for _ in range(neighbors):
        if not prev_id:
            break
        p = get_node_links(conn, prev_id)
        if p.parent_id != node.parent_id:
            break
        prev_ids.append(p.node_id)
        prev_id = p.prev_id
    prev_ids.reverse()
    out.extend(prev_ids)
    out.append(node.node_id)

    next_id = node.next_id
    for _ in range(neighbors):
        if not next_id:
            break
        n = get_node_links(conn, next_id)
        if n.parent_id != node.parent_id:
            break
        out.append(n.node_id)
        next_id = n.next_id
    return out


def _doc_id_from_node_id(node_id: str) -> str:
    # node_id format: "{doc_id}:{kind}:{label...}"
    return str(node_id).split(":", 1)[0]


_DOC_CODE_HINT_RE = re.compile(r"(?<!\d)([1-9]\d{3}(?:-\d+){1,4})(?!\d)")


def extract_doc_code_hints(raw_query: str) -> List[str]:
    hints: List[str] = []
    seen: set[str] = set()
    for m in _DOC_CODE_HINT_RE.finditer(str(raw_query or "")):
        code = str(m.group(1) or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        hints.append(code)
    return hints


def _compile_doc_code_hint_re(code: str) -> re.Pattern[str]:
    # Avoid prefix matches like "1993-1-1" matching "1993-1-12".
    return re.compile(rf"(?<!\d){re.escape(str(code or '').strip())}(?!\d)")


def search_leaf_nodes(
    conn: sqlite3.Connection,
    raw_query: str,
    *,
    limit: int = 20,
    query_mode: str = "or",
    must_terms: Sequence[str] = (),
) -> List[str]:
    raw_must_terms = [str(t).strip() for t in must_terms if str(t).strip()]
    must_content_terms: List[str] = []
    must_doc_code_hints: List[str] = []
    hinted_terms: List[Tuple[str, List[str]]] = []
    candidate_doc_hints: List[str] = []
    for t in raw_must_terms:
        hints = extract_doc_code_hints(t)
        if not hints:
            must_content_terms.append(t)
            continue
        hinted_terms.append((t, hints))
        for h in hints:
            if h and h not in candidate_doc_hints:
                candidate_doc_hints.append(h)

    if hinted_terms and candidate_doc_hints:
        doc_ids = [
            str(row[0])
            for row in conn.execute("SELECT DISTINCT doc_id FROM nodes WHERE is_active = 1").fetchall()
        ]
        matched: set[str] = set()
        for h in candidate_doc_hints:
            r = _compile_doc_code_hint_re(h)
            if any(r.search(doc_id) for doc_id in doc_ids):
                matched.add(h)
        for t, hints in hinted_terms:
            if any(h in matched for h in hints):
                for h in hints:
                    if h in matched and h not in must_doc_code_hints:
                        must_doc_code_hints.append(h)
            else:
                must_content_terms.append(t)

    normalized_query = normalize_query(raw_query)
    title_hits = search_title_nodes(conn, normalized_query, limit=limit)
    body_hits = search_body_nodes(
        conn,
        normalized_query,
        limit=limit,
        query_mode=query_mode,
        must_terms=must_content_terms,
    )
    doc_code_hints = extract_doc_code_hints(raw_query)
    for h in must_doc_code_hints:
        if h and h not in doc_code_hints:
            doc_code_hints.append(h)
    doc_code_hint_res = [_compile_doc_code_hint_re(c) for c in doc_code_hints if str(c).strip()]
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
    ordered_scores: List[Tuple[int, int, int, str]] = []
    seen_nodes: set[str] = set()
    has_primary_support = bool(title_hits or alias_hits)
    query_low = str(raw_query or "").lower()
    boilerplate_titles = {
        "preface",
        "foreword",
        "contents",
        "table of contents",
        "toc",
        "前言",
        "目录",
    }
    boilerplate_query_terms = {"preface", "foreword", "contents", "table of contents", "toc", "前言", "目录"}
    for node_id in combined:
        if node_id in seen_nodes:
            continue
        seen_nodes.add(node_id)
        if (
            is_short_query(raw_query)
            and has_primary_support
            and node_id not in title_rank
            and node_id not in alias_rank
        ):
            continue
        downrank_boilerplate = False
        node_kind = ""
        node_title_low = ""
        if must_content_terms or query_mode == "and":
            node = get_node(conn, node_id)
            node_kind = str(node.kind or "")
            node_title_low = str(node.title or "").strip().lower()
            hay = (node.title + "\n" + node.body_plain).lower()
            if any(str(t).lower() not in hay for t in must_content_terms if str(t).strip()):
                continue
            if query_mode == "and":
                parts = [p.lower() for p in query_terms(raw_query) if p.strip()]
                if any(p not in hay for p in parts):
                    continue
        else:
            row = conn.execute(
                """
                SELECT kind, title
                FROM nodes
                WHERE node_id = ? AND is_active = 1
                ORDER BY source_version DESC
                LIMIT 1
                """,
                (node_id,),
            ).fetchone()
            if row:
                node_kind = str(row["kind"] or "")
                node_title_low = str(row["title"] or "").strip().lower()

        if (
            node_kind == "block"
            and node_title_low in boilerplate_titles
            and not any(t in query_low for t in boilerplate_query_terms)
        ):
            downrank_boilerplate = True
        if downrank_boilerplate:
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
        if doc_code_hint_res:
            doc_id = _doc_id_from_node_id(node_id)
            if any(r.search(doc_id) for r in doc_code_hint_res):
                score += 5000
        ordered_scores.append(
            (
                score,
                body_rank.get(node_id, 1_000_000),
                title_rank.get(node_id, 1_000_000),
                node_id,
            )
        )
    ordered_scores.sort(key=lambda item: (-item[0], item[1], item[2], item[3]))
    ordered_node_ids = [node_id for _, _, _, node_id in ordered_scores]
    if doc_code_hint_res:
        filtered = [
            nid
            for nid in ordered_node_ids
            if any(r.search(_doc_id_from_node_id(nid)) for r in doc_code_hint_res)
        ]
        if filtered:
            ordered_node_ids = filtered
    return ordered_node_ids[:limit]


def _query_has_multiple_terms(raw_query: str) -> bool:
    parts = [p for p in query_terms(raw_query) if p.strip()]
    return len(parts) >= 2


_EN_STOPWORDS: set[str] = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "bs",
    "by",
    "en",
    "for",
    "from",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "such",
    "that",
    "the",
    "their",
    "then",
    "these",
    "this",
    "those",
    "to",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
    "without",
}


def _is_low_signal_term(term: str) -> bool:
    t = str(term or "").strip()
    if not t:
        return True
    # Only apply stopword filtering to non-CJK terms.
    if any(is_cjk(ch) for ch in t):
        return False
    return t.lower() in _EN_STOPWORDS


def _format_trace_must(must_terms: Sequence[str]) -> str:
    return json.dumps([str(t) for t in must_terms if str(t).strip()], ensure_ascii=False)


def _focus_unit_id(conn: sqlite3.Connection, node_id: str) -> str:
    n = get_node_links(conn, node_id)
    if n.kind in {"article", "clause"}:
        return n.node_id
    cur = n
    while cur.parent_id:
        parent = get_node_links(conn, cur.parent_id)
        if parent.kind in {"article", "clause"}:
            return parent.node_id
        cur = parent
    return f"doc:{n.doc_id}"


def _article_focus_metrics(
    conn: sqlite3.Connection,
    leaf_hits: Sequence[str],
    *,
    focus_k: int,
) -> Tuple[int, float, float]:
    k = max(0, min(int(focus_k), len(leaf_hits)))
    if k <= 0:
        return (0, 0.0, 0.0)

    total = 0
    article_weights: Dict[str, int] = {}
    for idx, node_id in enumerate(leaf_hits[:k]):
        article_id = _focus_unit_id(conn, node_id)
        w = k - idx
        total += w
        article_weights[article_id] = article_weights.get(article_id, 0) + w

    ranked = sorted(article_weights.items(), key=lambda item: (-item[1], item[0]))
    diversity = len(ranked)
    top3 = sum(w for _, w in ranked[:3])
    mass_top3 = float(top3) / float(total) if total else 0.0

    top1 = ranked[0][1] if ranked else 0
    top2 = ranked[1][1] if len(ranked) > 1 else 0
    margin = float(top1 - top2) / float(total) if total else 0.0

    return diversity, mass_top3, margin


@dataclass(frozen=True)
class IterativeSearchResult:
    hits: List[str]
    query_mode: str
    must_terms: List[str]
    trace_lines: List[str]


def iterative_search_leaf_nodes(
    conn: sqlite3.Connection,
    raw_query: str,
    *,
    limit: int,
    query_mode: str,
    must_terms: Sequence[str],
    max_rounds: int = 5,
    focus_k: int = 12,
    focus_max_articles: int = 3,
    mass_top3_threshold: float = 0.8,
) -> IterativeSearchResult:
    focus_k = max(1, int(focus_k))
    focus_max_articles = max(1, int(focus_max_articles))
    try:
        mass_top3_threshold = float(mass_top3_threshold)
    except (TypeError, ValueError):
        mass_top3_threshold = 0.8
    mass_top3_threshold = max(0.0, min(1.0, mass_top3_threshold))

    base_must = [str(t).strip() for t in must_terms if str(t).strip()]
    auto_must: List[str] = []
    trace: List[str] = []

    doc_code_hints = extract_doc_code_hints(raw_query)
    for t in base_must:
        for h in extract_doc_code_hints(t):
            if h and h not in doc_code_hints:
                doc_code_hints.append(h)

    normalized = normalize_query(raw_query)
    candidate_must: List[str] = []
    seen_terms: set[str] = set()
    for t in [*normalized.article_terms, *normalized.title_terms, *fts_tokens(raw_query)]:
        term = str(t).strip()
        if not term or term in seen_terms or _is_low_signal_term(term):
            continue
        seen_terms.add(term)
        candidate_must.append(term)

    multi_terms = _query_has_multiple_terms(raw_query)
    attempted_and = query_mode == "and"

    best_obj: Optional[float] = None
    best_div: int = 1_000_000
    best_mass: float = 0.0
    best_margin: float = 0.0
    best_round: int = 0
    best_hits: List[str] = []
    best_qm: str = query_mode
    best_must: List[str] = list(base_must)

    stop_reason = "max_rounds"
    stop_detail = ""

    seen_states: set[Tuple[str, Tuple[str, ...]]] = set()

    rounds = max(1, min(int(max_rounds), 5))
    single_round = rounds <= 1
    trace.append(
        " ".join(
            [
                "params:",
                f"max_rounds={rounds}",
                f"focus_k={focus_k}",
                f"focus_max_articles={focus_max_articles}",
                f"mass_top3_threshold={mass_top3_threshold:.2f}",
                f"doc_code_hints={doc_code_hints}" if doc_code_hints else "",
            ]
        )
        .strip()
    )
    for i in range(rounds):
        round_qm = query_mode
        effective_must = [*base_must, *auto_must]
        state_key = (round_qm, tuple(effective_must))
        if state_key in seen_states:
            stop_reason = "repeat_state"
            stop_detail = f"query_mode={round_qm} must={_format_trace_must(effective_must)}"
            break
        seen_states.add(state_key)

        hits = search_leaf_nodes(
            conn,
            raw_query,
            limit=limit,
            query_mode=round_qm,
            must_terms=effective_must,
        )

        if not hits:
            action = "stop"
            reason = "no_hits"
            if auto_must:
                removed = auto_must.pop()
                action = f"relax:pop_must={removed}"
                reason = "no_hits"
                stop_reason = "relax_then_retry"
            elif round_qm == "and" and multi_terms:
                query_mode = "or"
                action = "relax:set_query_mode=or"
                reason = "no_hits"
                stop_reason = "relax_then_retry"
                # Avoid immediate repeat_state (or, []) after a failed AND round: inject one
                # additional must term so we can make progress on the next round.
                used = set(effective_must)
                next_term = ""
                for term in candidate_must:
                    if term in used:
                        continue
                    next_term = term
                    break
                if next_term:
                    auto_must.append(next_term)
                    action = f"{action}+add_must={next_term}"
                    reason = "no_hits_relax_add_must"
            else:
                stop_reason = "no_hits"
            trace.append(
                " ".join(
                    [
                        f"round={i}",
                        f"query_mode={round_qm}",
                        f"must={_format_trace_must(effective_must)}",
                        "hits=0",
                        "articles@0=0",
                        "mass_top3=0.00",
                        "margin=0.00",
                        f"action={action}",
                        f"reason={reason}",
                    ]
                )
            )
            if stop_reason == "no_hits":
                break
            continue

        k = min(int(focus_k), len(hits), limit)
        diversity, mass_top3, margin = _article_focus_metrics(conn, hits, focus_k=k)
        objective = 100.0 * mass_top3 - 10.0 * float(diversity) + 5.0 * margin

        is_better = False
        if best_obj is None or objective > best_obj + 1e-9:
            is_better = True
        elif best_obj is not None and abs(objective - best_obj) <= 1e-9:
            # Stable tiebreakers: fewer articles, higher mass, higher margin, earlier round
            if diversity < best_div:
                is_better = True
            elif diversity == best_div and mass_top3 > best_mass + 1e-9:
                is_better = True
            elif diversity == best_div and abs(mass_top3 - best_mass) <= 1e-9 and margin > best_margin + 1e-9:
                is_better = True

        if is_better:
            best_obj = objective
            best_div = diversity
            best_mass = mass_top3
            best_margin = margin
            best_round = i
            best_hits = list(hits)
            best_qm = round_qm
            best_must = list(effective_must)

        converged = diversity <= int(focus_max_articles) and mass_top3 >= float(mass_top3_threshold)

        action = "stop" if converged else "tighten"
        reason = "converged" if converged else "focus_articles"

        if not converged and single_round:
            action = "stop"
            reason = "max_rounds"
            stop_reason = "max_rounds"
            stop_detail = "single_round"
        elif not converged:
            if round_qm == "or" and multi_terms and not attempted_and:
                query_mode = "and"
                attempted_and = True
                action = "tighten:set_query_mode=and"
                reason = "articles_too_diverse"
            else:
                next_term = ""
                used = set(effective_must)
                for term in candidate_must:
                    if term in used:
                        continue
                    next_term = term
                    break
                if next_term:
                    auto_must.append(next_term)
                    action = f"tighten:add_must={next_term}"
                    reason = "articles_too_diverse"
                else:
                    stop_reason = "no_more_terms"
                    action = "stop"
                    reason = "no_more_terms"

        trace.append(
            " ".join(
                [
                    f"round={i}",
                    f"query_mode={round_qm}",
                    f"must={_format_trace_must(effective_must)}",
                    f"hits={len(hits)}",
                    f"articles@{k}={diversity}",
                    f"mass_top3={mass_top3:.2f}",
                    f"margin={margin:.2f}",
                    f"objective={objective:.2f}",
                    f"action={action}",
                    f"reason={reason}",
                ]
            )
        )

        if converged:
            stop_reason = "converged"
            stop_detail = f"articles@{k}={diversity} mass_top3={mass_top3:.2f}"
            break
        if single_round:
            break
        if stop_reason == "no_more_terms":
            stop_detail = "candidate_must exhausted"
            break

    if best_obj is None or not best_hits:
        # No successful round; return empty hits and trace for caller to error out.
        return IterativeSearchResult(hits=[], query_mode=best_qm, must_terms=best_must, trace_lines=trace)

    trace.append(
        f"stop: reason={stop_reason} {stop_detail}".strip()
        + f" select_round={best_round} query_mode={best_qm} must={_format_trace_must(best_must)}"
        + f" objective={best_obj:.2f} articles@K={best_div} mass_top3={best_mass:.2f}"
    )

    return IterativeSearchResult(hits=best_hits, query_mode=best_qm, must_terms=best_must, trace_lines=trace)


def display_node_id(node_id: str) -> str:
    match = re.search(r":article:(\d+)$", node_id)
    if not match:
        return node_id
    return re.sub(r":article:\d+$", f":article:{int(match.group(1)):03d}", node_id)


def chronological_key(conn: sqlite3.Connection, node_id: str) -> Tuple[str, Tuple[int, ...], str]:
    n = get_node_links(conn, node_id)
    ords: List[int] = [n.ordinal]
    cur = n
    while cur.parent_id:
        cur = get_node_links(conn, cur.parent_id)
        ords.append(cur.ordinal)
    ords.reverse()
    return (n.doc_id, tuple(ords), n.node_id)


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


def _strip_common_noise_lines(md: str) -> Tuple[str, int]:
    licensed_copy_re = re.compile(
        r"(?:BSI)?Licensed\s*(?:Copy)?\s*:.*?(?:Uncontrolled\s*Copy,\s*)?(?:\(\s*c\s*\)|©)\s*BSI",
        flags=re.IGNORECASE,
    )
    removed = 0
    out_lines: List[str] = []
    for raw in str(md or "").splitlines():
        line = str(raw)
        if "licensed" in line.lower():
            line = licensed_copy_re.sub("", line)
        s = line.strip()
        if not s:
            if raw:
                removed += 1
            else:
                out_lines.append(line)
            continue
        low = s.lower()
        # Common standards headers/footers (e.g., Eurocodes via IHS).
        if "provided by ihs" in low:
            removed += 1
            continue
        if "no reproduction" in low and "ihs" in low:
            removed += 1
            continue
        if "not for resale" in low:
            removed += 1
            continue
        if "without license from ihs" in low:
            removed += 1
            continue
        if low.startswith("copyright") and ("ihs" in low or "cen" in low or "standardization" in low):
            removed += 1
            continue
        out_lines.append(line)
    cleaned = "\n".join(out_lines).strip()
    return cleaned, removed


def render_bundle(
    conn: sqlite3.Connection,
    hits: Sequence[str],
    *,
    raw_query: str,
    neighbors: int,
    max_chars: int,
    per_node_max_chars: int,
    body_mode: str,
    order: str,
    trace_lines: Sequence[str] = (),
    diagnostics: Sequence[str] = (),
    hooks_root: Optional[Path] = None,
    enable_hooks: bool = False,
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

        # Structured outline: when a clause is included, automatically include its direct
        # table/figure children to avoid "table extracted but missing from bundle".
        if node.kind == "clause":
            added = 0
            for child_id in child_node_ids(conn, node.node_id):
                child = get_node_links(conn, child_id)
                if child.kind not in {"table", "figure"}:
                    continue
                if child_id in seen:
                    continue
                seen.add(child_id)
                included.append(child_id)
                added += 1
                if added >= 8:
                    break

    if order == "chronological":
        included.sort(key=lambda nid: chronological_key(conn, nid))

    terms = query_terms(raw_query)
    out_parts: List[str] = [
        f"# Bundle\n\n- Query: `{raw_query}`\n\n",
        "## 回答约束（Evidence-only）\n"
        "- 只允许引用本 bundle 中确实出现的条款/式号/原文。\n"
        "- 不要从记忆补公式/数值；如果公式被抽空/缺失（例如只有式号 `(6.xx)`），请明确写：当前抽取缺失，仅能引用式号 `(6.xx)`。\n"
        "- 最终回答请附上文末自动生成的 `## 参考依据`。\n\n",
    ]
    if trace_lines:
        out_parts.append("## 检索轨迹\n")
        for line in trace_lines:
            ln = str(line).rstrip()
            if not ln:
                continue
            out_parts.append(f"- {ln}\n")
        out_parts.append("\n")
    if diagnostics:
        out_parts.append("## 补查记录\n")
        for line in diagnostics:
            out_parts.append(f"- {line}\n")
        out_parts.append("\n")
    rendered_nodes: List[Node] = []

    planned_nodes = len(included)
    truncated_nodes = 0
    snippet_nodes = 0
    noise_lines_stripped = 0
    remaining = max_chars

    for node_id in included:
        node = get_node(conn, node_id)
        if enable_hooks and hooks_root is not None:
            out, _ = run_hook(
                hooks_root,
                "pre_render",
                {
                    "stage": "pre_render",
                    "query": raw_query,
                    "node": {
                        "node_id": node.node_id,
                        "doc_id": node.doc_id,
                        "doc_title": node.doc_title,
                        "kind": node.kind,
                        "label": node.label,
                        "title": node.title,
                        "ref_path": node.ref_path,
                        "is_leaf": node.is_leaf,
                        "source_file": node.source_file,
                        "source_version": node.source_version,
                        "body_md": node.body_md,
                        "body_plain": node.body_plain,
                    },
                },
            )
            if out.get("skip") is True:
                continue
            if "title" in out:
                node.title = str(out.get("title") or node.title)
            if "body_md" in out:
                node.body_md = str(out.get("body_md") or "")
                node.body_plain = markdown_to_plain(node.body_md)

        if node.is_leaf and node.body_md:
            cleaned, removed = _strip_common_noise_lines(node.body_md)
            if removed:
                noise_lines_stripped += removed
                node.body_md = cleaned
                node.body_plain = markdown_to_plain(node.body_md)
        header = (
            f"## {node.doc_title} — {node.title}\n\n"
            f"- node_id: `{display_node_id(node.node_id)}`\n"
            f"- source_file: `{node.source_file}`\n"
            f"- source_path: `{node.source_path}`\n"
            f"- source: `{node.ref_path}`\n\n"
        )

        body = ""
        if node.is_leaf:
            if body_mode == "none":
                body = ""
            elif body_mode == "snippet":
                snippet = extract_window(
                    node.body_plain, terms, min(per_node_max_chars, max(200, remaining - len(header)))
                )
                body = snippet.strip() + "\n\n*(SNIPPET)*\n"
                snippet_nodes += 1
            else:
                body = node.body_md.strip() + "\n"
                if len(body) > per_node_max_chars or len(header) + len(body) > remaining:
                    snippet = extract_window(
                        node.body_plain, terms, min(per_node_max_chars, max(200, remaining - len(header)))
                    )
                    body = snippet.strip() + "\n\n*(TRUNCATED)*\n"
                    truncated_nodes += 1

        chunk = header + body + ("\n" if body else "")
        if len(chunk) > remaining:
            break
        out_parts.append(chunk)
        remaining -= len(chunk)
        rendered_nodes.append(node)

    budget_exhausted = len(rendered_nodes) < planned_nodes
    if budget_exhausted or truncated_nodes or snippet_nodes or noise_lines_stripped:
        out_parts.append("## 渲染提示\n")
        out_parts.append(
            f"- planned_nodes={planned_nodes} rendered_nodes={len(rendered_nodes)} budget_exhausted={str(budget_exhausted).lower()}\n"
        )
        out_parts.append(
            f"- render: neighbors={int(neighbors)} order={order} max_chars={int(max_chars)} per_node_max_chars={int(per_node_max_chars)} body={body_mode}\n"
        )
        if truncated_nodes:
            out_parts.append(f"- markers: truncated_nodes={truncated_nodes}\n")
        if snippet_nodes:
            out_parts.append(f"- markers: snippet_nodes={snippet_nodes}\n")
        if noise_lines_stripped:
            out_parts.append(f"- markers: noise_lines_stripped={noise_lines_stripped}\n")
        if budget_exhausted:
            out_parts.append(
                "- 建议：优先减小 `--neighbors` / `--limit`，或增大 `--max-chars` / `--per-node-max-chars`，必要时使用 `--body snippet`。\n"
            )
        out_parts.append("\n")

    out_parts.append("## 参考依据\n")
    seen_cites: set[str] = set()
    for n in rendered_nodes:
        if not n.is_leaf:
            continue
        cite = f"{n.doc_id}:{n.ref_path}"
        if cite in seen_cites:
            continue
        seen_cites.add(cite)
        out_parts.append(
            f"- {n.doc_title}（`{n.source_path}`） {n.title}（node_id: `{display_node_id(n.node_id)}`; source: `{n.ref_path}`）\n"
        )

    return "".join(out_parts), rendered_nodes


def cmd_bundle(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
        with SqliteTimeout(conn, timeout_ms) as timeout:
            try:
                bundle_result = _run_bundle_pipeline(conn, root, args)
                out_path = safe_output_path(root, args.out)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(bundle_result.content, encoding="utf-8", newline="\n")
                print("[OK] Wrote bundle:", out_path)
                return 0
            except sqlite3.OperationalError as e:
                if timeout.timed_out:
                    die(f"SQLite query timed out after {timeout_ms}ms")
                raise
    finally:
        conn.close()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_sha256_hex(s: str) -> bool:
    s = (s or "").strip().lower()
    return bool(re.fullmatch(r"[0-9a-f]{64}", s))


def _load_manifest_summary(root: Path) -> Dict[str, object]:
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    docs = data.get("docs")
    doc_count = len(docs) if isinstance(docs, list) else 0
    return {
        "skill_name": data.get("skill_name") or root.name,
        "title": data.get("title") or "",
        "generated_at": data.get("generated_at") or "",
        "doc_count": doc_count,
    }


def _default_run_dir_name() -> str:
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    suffix = os.urandom(3).hex()
    return f"research_runs/{ts}-{suffix}"


def _detect_next_round(run_dir: Path) -> int:
    best = 0
    if not run_dir.exists():
        return 1
    for p in run_dir.glob("bundle.round*.md"):
        m = re.search(r"bundle\.round(\d+)\.md$", p.name)
        if not m:
            continue
        try:
            n = int(m.group(1))
        except ValueError:
            continue
        if n > best:
            best = n
    return best + 1 if best > 0 else 1


@dataclass(frozen=True)
class BundlePipelineResult:
    raw_query: str
    query_mode: str
    must_terms: List[str]
    leaf_hits: List[str]
    trace_lines: List[str]
    elevated_hits: List[str]
    expanded_hits: List[str]
    diagnostics: List[str]
    content: str
    rendered_nodes: List[Node]
    included_node_ids: List[str]


def _run_bundle_pipeline(conn: sqlite3.Connection, root: Path, args: argparse.Namespace) -> BundlePipelineResult:
    raw_query = args.query
    query_mode = args.query_mode
    must_terms = list(args.must)
    enable_hooks = bool(getattr(args, "enable_hooks", False))
    diagnostics: List[str] = []

    if enable_hooks:
        out, digest = run_hook(
            root,
            "pre_search",
            {
                "stage": "pre_search",
                "query": raw_query,
                "query_mode": query_mode,
                "must": must_terms,
            },
        )
        if digest:
            diagnostics.append(f"hook: pre_search sha1={digest} path=hooks/pre_search.py")
        if "query" in out:
            candidate = str(out.get("query") or "").strip()
            if candidate:
                raw_query = candidate
        if "query_mode" in out:
            qm = str(out.get("query_mode") or "").strip().lower()
            if qm in {"or", "and"}:
                query_mode = qm
        if "must" in out:
            value = out.get("must")
            if isinstance(value, list):
                must_terms = [str(v).strip() for v in value if str(v).strip()]

    iter_result = iterative_search_leaf_nodes(
        conn,
        raw_query,
        limit=args.limit,
        query_mode=query_mode,
        must_terms=must_terms,
        max_rounds=(
            1
            if bool(getattr(args, "no_iter", False))
            else max(1, min(5, int(getattr(args, "iter_max_rounds", 5))))
        ),
        focus_k=max(1, int(getattr(args, "iter_focus_k", 12))),
        focus_max_articles=max(1, int(getattr(args, "iter_focus_max_articles", 3))),
        mass_top3_threshold=max(
            0.0, min(1.0, float(getattr(args, "iter_mass_top3_threshold", 0.8)))
        ),
    )
    leaf_hits = list(iter_result.hits)
    query_mode = str(iter_result.query_mode or query_mode)
    must_terms = list(iter_result.must_terms or must_terms)
    trace_lines = list(iter_result.trace_lines or [])
    if not leaf_hits:
        die("No matches. Try a different query or rebuild indexes.")

    if enable_hooks:
        out, digest = run_hook(
            root,
            "post_search",
            {
                "stage": "post_search",
                "query": raw_query,
                "query_mode": query_mode,
                "must": must_terms,
                "hits": list(leaf_hits),
            },
        )
        if digest:
            diagnostics.append(f"hook: post_search sha1={digest} path=hooks/post_search.py")
        if "hits" in out:
            value = out.get("hits")
            if not isinstance(value, list):
                die("Hook post_search must return {'hits': [...]} list")
            leaf_hits = [str(v) for v in value if str(v)]

    elevated: List[str] = []
    seen = set()
    for node_id in leaf_hits:
        article_id = elevate_to_article(conn, node_id)
        if article_id in seen:
            continue
        seen.add(article_id)
        elevated.append(article_id)

    if enable_hooks:
        out, digest = run_hook(
            root,
            "pre_expand",
            {
                "stage": "pre_expand",
                "query": raw_query,
                "hits": list(elevated),
            },
        )
        if digest:
            diagnostics.append(f"hook: pre_expand sha1={digest} path=hooks/pre_expand.py")
        if "hits" in out:
            value = out.get("hits")
            if not isinstance(value, list):
                die("Hook pre_expand must return {'hits': [...]} list")
            elevated = [str(v) for v in value if str(v)]

    normalized_query = normalize_query(raw_query)
    expanded, expansion_diags = apply_triggered_expansion(
        conn,
        normalized_query,
        elevated,
        force_debug=args.debug_triggers,
    )
    diagnostics.extend(expansion_diags)
    if enable_hooks:
        pre_render_path = root / "hooks" / "pre_render.py"
        if pre_render_path.exists():
            diagnostics.append(
                f"hook: pre_render sha1={sha1_file(pre_render_path)} path=hooks/pre_render.py"
            )

    included_node_ids: List[str] = []
    seen_nodes: set[str] = set()
    for node_id in expanded:
        node = get_node(conn, node_id)
        for pid in iter_parents(conn, node):
            if pid not in seen_nodes:
                seen_nodes.add(pid)
                included_node_ids.append(pid)
        for nid in iter_neighbors(conn, node, int(args.neighbors)):
            if nid not in seen_nodes:
                seen_nodes.add(nid)
                included_node_ids.append(nid)
        if node.kind == "clause":
            added = 0
            for child_id in child_node_ids(conn, node.node_id):
                child = get_node_links(conn, child_id)
                if child.kind not in {"table", "figure"}:
                    continue
                if child_id in seen_nodes:
                    continue
                seen_nodes.add(child_id)
                included_node_ids.append(child_id)
                added += 1
                if added >= 8:
                    break
    if args.order == "chronological":
        included_node_ids.sort(key=lambda nid: chronological_key(conn, nid))

    content, rendered_nodes = render_bundle(
        conn,
        expanded,
        raw_query=raw_query,
        neighbors=args.neighbors,
        max_chars=args.max_chars,
        per_node_max_chars=args.per_node_max_chars,
        body_mode=args.body,
        order=args.order,
        trace_lines=trace_lines,
        diagnostics=diagnostics,
        hooks_root=root if enable_hooks else None,
        enable_hooks=enable_hooks,
    )

    return BundlePipelineResult(
        raw_query=raw_query,
        query_mode=query_mode,
        must_terms=must_terms,
        leaf_hits=leaf_hits,
        trace_lines=trace_lines,
        elevated_hits=elevated,
        expanded_hits=expanded,
        diagnostics=diagnostics,
        content=content,
        rendered_nodes=rendered_nodes,
        included_node_ids=included_node_ids,
    )


def _detect_hollow_equations(content: str) -> List[str]:
    labels: List[str] = []
    seen: set[str] = set()
    for ln in (content or "").splitlines():
        if "=" not in ln:
            continue
        eqs = re.findall(r"\((\d+(?:\.\d+)+)\)", ln)
        if not eqs:
            continue
        if not (re.search(r"=\s*\(", ln) or re.search(r"=\s*but\\b", ln, flags=re.IGNORECASE)):
            continue
        for e in eqs:
            if e in seen:
                continue
            seen.add(e)
            labels.append(e)
    return labels


def _verify_bundle(
    bundle_result: BundlePipelineResult,
    *,
    focus_k: int,
    focus_max_articles: int,
    mass_top3_threshold: float,
    focus_diversity: int,
    focus_mass_top3: float,
    focus_margin: float,
    search_limit: int,
    render_neighbors: int,
    render_order: str,
    render_max_chars: int,
    render_per_node_max_chars: int,
    render_body_mode: str,
) -> Dict[str, object]:
    content = bundle_result.content
    normalized = normalize_query(bundle_result.raw_query)
    key_terms: List[str] = []
    seen_terms: set[str] = set()
    for t in [*normalized.article_terms, *normalized.title_terms]:
        term = str(t).strip()
        if not term or term in seen_terms:
            continue
        seen_terms.add(term)
        key_terms.append(term)

    present = [t for t in key_terms if t in content]
    missing = [t for t in key_terms if t not in content]
    coverage = float(len(present)) / float(len(key_terms)) if key_terms else 1.0

    budget_exhausted = len(bundle_result.rendered_nodes) < len(bundle_result.included_node_ids)
    truncated_nodes = content.count("*(TRUNCATED)*")
    snippet_nodes = content.count("*(SNIPPET)*")

    checks: List[Dict[str, object]] = []
    blocking_issues: List[str] = []
    non_blocking_issues: List[str] = []

    key_terms_ok = len(missing) == 0
    checks.append(
        {
            "name": "key_term_coverage",
            "ok": key_terms_ok,
            "terms": key_terms,
            "present": present,
            "missing": missing,
            "coverage": coverage,
        }
    )
    if not key_terms_ok:
        blocking_issues.append("missing_key_terms")

    checks.append(
        {
            "name": "budget_exhausted",
            "ok": not budget_exhausted,
            "budget_exhausted": budget_exhausted,
            "planned_nodes": len(bundle_result.included_node_ids),
            "rendered_nodes": len(bundle_result.rendered_nodes),
        }
    )
    if budget_exhausted:
        blocking_issues.append("budget_exhausted")

    if truncated_nodes or snippet_nodes:
        non_blocking_issues.append("nodes_truncated_or_snippet")
    checks.append(
        {
            "name": "render_markers",
            "ok": True,
            "truncated_nodes": truncated_nodes,
            "snippet_nodes": snippet_nodes,
        }
    )

    hollow_eqs = _detect_hollow_equations(content)
    if hollow_eqs:
        non_blocking_issues.append("hollow_equations")
    checks.append(
        {
            "name": "hollow_equations",
            "ok": not hollow_eqs,
            "equations": hollow_eqs[:25],
            "count": len(hollow_eqs),
        }
    )

    focus_ok = (focus_diversity <= int(focus_max_articles)) and (
        float(focus_mass_top3) >= float(mass_top3_threshold)
    )
    checks.append(
        {
            "name": "focus",
            "ok": focus_ok,
            "focus_k": int(focus_k),
            "focus_max_articles": int(focus_max_articles),
            "mass_top3_threshold": float(mass_top3_threshold),
            "focus": {
                "diversity": int(focus_diversity),
                "mass_top3": float(focus_mass_top3),
                "margin": float(focus_margin),
            },
        }
    )
    if not focus_ok:
        non_blocking_issues.append("focus_not_converged")

    suggestions: List[str] = []
    suggested_next_params: List[Dict[str, object]] = []

    def _suggest_set(params: Dict[str, object], reason: str) -> None:
        if not params:
            return
        suggested_next_params.append({"set": params, "reason": reason})

    def _suggest_add_must(terms: Sequence[str], reason: str) -> None:
        items = [str(t).strip() for t in terms if str(t).strip() and not _is_low_signal_term(str(t))]
        if not items:
            return
        suggested_next_params.append({"add_must": items, "reason": reason})

    if "missing_key_terms" in blocking_issues:
        suggestions.append("Revise query or add --must terms so key terms appear in the evidence bundle.")
        _suggest_add_must(missing[:3], "missing_key_terms")
    if "budget_exhausted" in blocking_issues:
        suggestions.append("Increase --max-chars / --per-node-max-chars or reduce --neighbors to avoid truncation.")
        if int(render_neighbors) > 0:
            _suggest_set({"neighbors": 0}, "budget_exhausted")
        if int(render_max_chars) < 120000:
            _suggest_set({"max_chars": min(120000, int(render_max_chars) * 2)}, "budget_exhausted")
        if int(render_per_node_max_chars) < 12000:
            _suggest_set({"per_node_max_chars": min(12000, max(int(render_per_node_max_chars) + 2000, 8000))}, "budget_exhausted")
        if str(render_body_mode) == "full":
            _suggest_set({"body": "snippet"}, "budget_exhausted")
        if int(search_limit) > 12:
            _suggest_set({"limit": 12}, "budget_exhausted")
    if truncated_nodes and "budget_exhausted" not in blocking_issues:
        suggestions.append("Consider increasing --per-node-max-chars if important nodes are marked *(TRUNCATED)*.")
        _suggest_set({"per_node_max_chars": min(12000, max(int(render_per_node_max_chars) + 2000, 8000))}, "nodes_truncated_or_snippet")
    if not focus_ok:
        suggestions.append("Consider adding --must terms or using --query-mode and to focus on fewer articles.")
        if str(bundle_result.query_mode) != "and" and _query_has_multiple_terms(bundle_result.raw_query):
            _suggest_set({"query_mode": "and"}, "focus_not_converged")
        normalized_q = normalize_query(bundle_result.raw_query)
        candidate = [*normalized_q.article_terms, *normalized_q.title_terms, *fts_tokens(bundle_result.raw_query)]
        existing = {str(t).strip() for t in (bundle_result.must_terms or []) if str(t).strip()}
        must_candidates: List[str] = []
        for t in candidate:
            term = str(t).strip()
            if not term or term in existing or _is_low_signal_term(term):
                continue
            must_candidates.append(term)
            if len(must_candidates) >= 3:
                break
        _suggest_add_must(must_candidates, "focus_not_converged")

    if hollow_eqs:
        suggestions.append(
            "Some equations look hollow/missing in the extracted evidence. Do NOT invent formulas; cite only the equation numbers (e.g. (6.xx)) and state extraction is missing."
        )

    ok = not blocking_issues
    verdict = "pass" if ok else "fail"
    stop_recommended = bool(ok and focus_ok and key_terms_ok)
    stop_reason = "pass_and_converged" if stop_recommended else ""

    doc_code_hints = extract_doc_code_hints(bundle_result.raw_query)
    for t in bundle_result.must_terms:
        for h in extract_doc_code_hints(str(t)):
            if h and h not in doc_code_hints:
                doc_code_hints.append(h)

    return {
        "schema": "kbtool.verify.v1",
        "ts": _utc_now_iso(),
        "ok": ok,
        "verdict": verdict,
        "stop_recommended": stop_recommended,
        "stop_reason": stop_reason,
        "blocking_issues": blocking_issues,
        "non_blocking_issues": non_blocking_issues,
        "checks": checks,
        "suggestions": suggestions,
        "suggested_next_params": suggested_next_params,
        "hints": {"doc_code_hints": doc_code_hints},
        "params": {
            "focus_k": int(focus_k),
            "focus_max_articles": int(focus_max_articles),
            "mass_top3_threshold": float(mass_top3_threshold),
        },
        "render": {
            "limit": int(search_limit),
            "query_mode": str(bundle_result.query_mode),
            "must": list(bundle_result.must_terms),
            "neighbors": int(render_neighbors),
            "order": str(render_order),
            "max_chars": int(render_max_chars),
            "per_node_max_chars": int(render_per_node_max_chars),
            "body_mode": str(render_body_mode),
        },
    }


def cmd_research(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
        with SqliteTimeout(conn, timeout_ms) as timeout:
            try:
                run_dir_arg = str(getattr(args, "run_dir", "") or "").strip()
                if not run_dir_arg:
                    run_dir_arg = _default_run_dir_name()
                run_dir = safe_output_path(root, run_dir_arg)
                if run_dir.exists() and run_dir.is_file():
                    die(f"Invalid --run-dir (points to a file): {run_dir_arg!r}")
                run_dir.mkdir(parents=True, exist_ok=True)

                round_arg = int(getattr(args, "round", 0) or 0)
                round_n = round_arg if round_arg > 0 else _detect_next_round(run_dir)
                if round_n <= 0:
                    die(f"Invalid round number: {round_n}")
                round_tag = f"{round_n:02d}"

                bundle_name = f"bundle.round{round_tag}.md"
                trace_name = f"trace.round{round_tag}.json"
                verify_name = f"verify.round{round_tag}.json"
                trace_jsonl_name = "trace.jsonl"

                bundle_path = run_dir / bundle_name
                trace_path = run_dir / trace_name
                verify_path = run_dir / verify_name
                trace_jsonl_path = run_dir / trace_jsonl_name

                bundle_result = _run_bundle_pipeline(conn, root, args)
                bundle_path.write_text(bundle_result.content, encoding="utf-8", newline="\n")

                focus_k = max(1, int(getattr(args, "iter_focus_k", 12)))
                focus_max_articles = max(1, int(getattr(args, "iter_focus_max_articles", 3)))
                mass_top3_threshold = max(0.0, min(1.0, float(getattr(args, "iter_mass_top3_threshold", 0.8))))
                div, mass_top3, margin = _article_focus_metrics(conn, bundle_result.leaf_hits, focus_k=focus_k)

                planner_json_raw = str(getattr(args, "planner_json", "") or "").strip()
                planner_sha256 = ""
                if planner_json_raw:
                    planner_sha256 = hashlib.sha256(planner_json_raw.encode("utf-8", errors="ignore")).hexdigest()
                planner_meta: object = {}
                if planner_json_raw:
                    try:
                        planner_meta = json.loads(planner_json_raw)
                    except (json.JSONDecodeError, ValueError):
                        planner_meta = {"raw": planner_json_raw}

                planner_artifacts: Dict[str, object] = {}
                planner_missing: List[str] = []
                if not planner_json_raw:
                    planner_missing.append("planner_json")
                elif not isinstance(planner_meta, dict):
                    planner_missing.extend(["planner.model", "planner.temperature", "planner.prompt_sha256"])
                else:
                    model = str(planner_meta.get("model") or "").strip()
                    if not model:
                        planner_missing.append("planner.model")
                    temp_raw = planner_meta.get("temperature")
                    try:
                        float(temp_raw)
                    except (TypeError, ValueError):
                        planner_missing.append("planner.temperature")

                    prompt_sha = str(planner_meta.get("prompt_sha256") or "").strip()
                    prompt_text = planner_meta.get("prompt")
                    prompt_path = str(planner_meta.get("prompt_path") or "").strip()
                    if prompt_path:
                        src = Path(prompt_path)
                        if not src.is_absolute():
                            src = (root / src).resolve()
                        if not src.exists() or not src.is_file():
                            planner_missing.append("planner.prompt_path")
                        else:
                            sha = _sha256_file(src)
                            dest_ext = src.suffix if src.suffix else ".txt"
                            dest_name = f"planner.round{round_tag}.prompt{dest_ext}"
                            dest = run_dir / dest_name
                            dest.write_bytes(src.read_bytes())
                            planner_meta = {**planner_meta, "prompt_sha256": sha, "prompt_path": str(src)}
                            planner_artifacts["prompt"] = {
                                "path": str(dest.relative_to(root)).replace("\\", "/"),
                                "sha256": sha,
                                "bytes": int(dest.stat().st_size),
                            }
                    elif isinstance(prompt_text, str) and prompt_text.strip():
                        sha = hashlib.sha256(prompt_text.encode("utf-8", errors="ignore")).hexdigest()
                        dest = run_dir / f"planner.round{round_tag}.prompt.txt"
                        dest.write_text(prompt_text, encoding="utf-8", newline="\n")
                        planner_meta = {**planner_meta, "prompt_sha256": sha}
                        planner_artifacts["prompt"] = {
                            "path": str(dest.relative_to(root)).replace("\\", "/"),
                            "sha256": sha,
                            "bytes": int(dest.stat().st_size),
                        }
                    elif not _is_sha256_hex(prompt_sha):
                        planner_missing.append("planner.prompt_sha256")

                note = str(getattr(args, "note", "") or "").strip()

                kbtool_sha = ""
                sha_path = root / "kbtool.sha1"
                if sha_path.exists():
                    kbtool_sha = sha_path.read_text(encoding="utf-8", errors="ignore").strip()

                manifest_summary = _load_manifest_summary(root)

                bundle_rel = str(bundle_path.relative_to(root)).replace("\\", "/")
                trace_rel = str(trace_path.relative_to(root)).replace("\\", "/")
                verify_rel = str(verify_path.relative_to(root)).replace("\\", "/")
                doc_code_hints = extract_doc_code_hints(bundle_result.raw_query)
                for t in bundle_result.must_terms:
                    for h in extract_doc_code_hints(str(t)):
                        if h and h not in doc_code_hints:
                            doc_code_hints.append(h)

                input_params = {
                    "query_mode": str(getattr(args, "query_mode", "")),
                    "must": list(getattr(args, "must", []) or []),
                    "limit": int(getattr(args, "limit", 20)),
                    "neighbors": int(getattr(args, "neighbors", 0) or 0),
                    "order": str(getattr(args, "order", "")),
                    "max_chars": int(getattr(args, "max_chars", 0) or 0),
                    "per_node_max_chars": int(getattr(args, "per_node_max_chars", 0) or 0),
                    "body": str(getattr(args, "body", "")),
                    "debug_triggers": bool(getattr(args, "debug_triggers", False)),
                    "enable_hooks": bool(getattr(args, "enable_hooks", False)),
                    "timeout_ms": int(getattr(args, "timeout_ms", 0) or 0),
                    "no_iter": bool(getattr(args, "no_iter", False)),
                    "iter": {
                        "max_rounds": (
                            1
                            if bool(getattr(args, "no_iter", False))
                            else max(1, min(5, int(getattr(args, "iter_max_rounds", 5))))
                        ),
                        "focus_k": focus_k,
                        "focus_max_articles": focus_max_articles,
                        "mass_top3_threshold": mass_top3_threshold,
                    },
                }

                trace_payload: Dict[str, object] = {
                    "schema": "kbtool.research_round.v1",
                    "ts": _utc_now_iso(),
                    "round": round_n,
                    "skill": {
                        **manifest_summary,
                        "db": str(Path(args.db)),
                        "kbtool_sha1": kbtool_sha,
                    },
                    "input": {
                        "query": str(getattr(args, "query", "")),
                        "params": input_params,
                        "planner": planner_meta,
                        "planner_sha256": planner_sha256,
                        "planner_artifacts": planner_artifacts,
                        "planner_missing": planner_missing,
                        "note": note,
                    },
                    "effective": {
                        "query": bundle_result.raw_query,
                        "query_mode": bundle_result.query_mode,
                        "must": bundle_result.must_terms,
                        "doc_code_hints": doc_code_hints,
                        "iter": {
                            "max_rounds": (1 if bool(getattr(args, "no_iter", False)) else max(1, min(5, int(getattr(args, "iter_max_rounds", 5))))),
                            "focus_k": focus_k,
                            "focus_max_articles": focus_max_articles,
                            "mass_top3_threshold": mass_top3_threshold,
                        },
                    },
                    "retrieval": {
                        "iterative": {
                            "leaf_hits": bundle_result.leaf_hits,
                            "trace_lines": bundle_result.trace_lines,
                            "focus": {
                                "diversity": div,
                                "mass_top3": mass_top3,
                                "margin": margin,
                            },
                        },
                        "articles": {
                            "elevated": bundle_result.elevated_hits,
                            "expanded": bundle_result.expanded_hits,
                        },
                        "render": {
                            "neighbors": int(args.neighbors),
                            "order": str(args.order),
                            "max_chars": int(args.max_chars),
                            "per_node_max_chars": int(args.per_node_max_chars),
                            "body_mode": str(args.body),
                            "planned_node_ids": bundle_result.included_node_ids,
                            "rendered_nodes": [node_to_dict(n, include_body=False) for n in bundle_result.rendered_nodes],
                            "budget_exhausted": len(bundle_result.rendered_nodes) < len(bundle_result.included_node_ids),
                            "markers": {
                                "truncated_nodes": bundle_result.content.count("*(TRUNCATED)*"),
                                "snippet_nodes": bundle_result.content.count("*(SNIPPET)*"),
                            },
                        },
                        "diagnostics": bundle_result.diagnostics,
                    },
                    "outputs": {
                        "bundle_md": bundle_rel,
                        "trace_json": trace_rel,
                        "verify_json": verify_rel,
                        "trace_jsonl": str(trace_jsonl_path.relative_to(root)).replace("\\", "/"),
                    },
                }

                trace_path.write_text(json.dumps(trace_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")

                verify_payload = _verify_bundle(
                    bundle_result,
                    focus_k=focus_k,
                    focus_max_articles=focus_max_articles,
                    mass_top3_threshold=mass_top3_threshold,
                    focus_diversity=div,
                    focus_mass_top3=mass_top3,
                    focus_margin=margin,
                    search_limit=int(getattr(args, "limit", 20)),
                    render_neighbors=int(args.neighbors),
                    render_order=str(args.order),
                    render_max_chars=int(args.max_chars),
                    render_per_node_max_chars=int(args.per_node_max_chars),
                    render_body_mode=str(args.body),
                )
                verify_payload["round"] = round_n
                verify_payload["focus"] = {"diversity": div, "mass_top3": mass_top3, "margin": margin}
                verify_payload["audit"] = {
                    "planner_ok": not planner_missing,
                    "planner_missing": planner_missing,
                    "planner_sha256": planner_sha256,
                    "planner_artifacts": planner_artifacts,
                }

                checks = verify_payload.get("checks")
                if isinstance(checks, list):
                    checks.append(
                        {
                            "name": "planner_audit_metadata",
                            "ok": not planner_missing,
                            "missing": planner_missing,
                            "planner_sha256": planner_sha256,
                            "planner_artifacts": planner_artifacts,
                        }
                    )

                if planner_missing:
                    blocking = verify_payload.get("blocking_issues")
                    if isinstance(blocking, list) and "audit_incomplete" not in blocking:
                        blocking.append("audit_incomplete")
                    verify_payload["ok"] = False
                    verify_payload["verdict"] = "fail"
                    verify_payload["stop_recommended"] = False
                    verify_payload["stop_reason"] = ""
                    suggestions = verify_payload.get("suggestions")
                    if isinstance(suggestions, list):
                        suggestions.insert(
                            0,
                            "Audit metadata incomplete. Provide --planner-json with at least: {model, temperature, prompt_sha256 or prompt_path}.",
                        )
                verify_path.write_text(json.dumps(verify_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")

                planned_nodes = len(bundle_result.included_node_ids)
                rendered_nodes = len(bundle_result.rendered_nodes)
                truncated_nodes = bundle_result.content.count("*(TRUNCATED)*")
                snippet_nodes = bundle_result.content.count("*(SNIPPET)*")
                budget_exhausted = rendered_nodes < planned_nodes

                event = {
                    "schema": "kbtool.research_event.v1",
                    "ts": _utc_now_iso(),
                    "round": round_n,
                    "run_dir": str(run_dir.relative_to(root)).replace("\\", "/"),
                    "input": {
                        "query": str(getattr(args, "query", "")),
                        "params": input_params,
                        "planner_sha256": planner_sha256,
                        "planner_missing": planner_missing,
                        "note": note,
                    },
                    "effective": {
                        "query": bundle_result.raw_query,
                        "query_mode": bundle_result.query_mode,
                        "must": bundle_result.must_terms,
                        "doc_code_hints": doc_code_hints,
                    },
                    "render": {
                        "limit": int(getattr(args, "limit", 20)),
                        "neighbors": int(args.neighbors),
                        "order": str(args.order),
                        "max_chars": int(args.max_chars),
                        "per_node_max_chars": int(args.per_node_max_chars),
                        "body_mode": str(args.body),
                    },
                    "stats": {
                        "planned_nodes": planned_nodes,
                        "rendered_nodes": rendered_nodes,
                        "budget_exhausted": budget_exhausted,
                        "truncated_nodes": truncated_nodes,
                        "snippet_nodes": snippet_nodes,
                    },
                    "focus": {"diversity": div, "mass_top3": mass_top3, "margin": margin},
                    "bundle": {"path": bundle_rel, "sha256": _sha256_file(bundle_path)},
                    "trace": {"path": trace_rel, "sha256": _sha256_file(trace_path)},
                    "verify": {"path": verify_rel, "sha256": _sha256_file(verify_path)},
                    "ok": bool(verify_payload.get("ok")),
                    "stop_recommended": bool(verify_payload.get("stop_recommended")),
                    "stop_reason": str(verify_payload.get("stop_reason") or ""),
                    "blocking_issues": verify_payload.get("blocking_issues", []),
                    "non_blocking_issues": verify_payload.get("non_blocking_issues", []),
                    "suggestions": verify_payload.get("suggestions", []),
                    "suggested_next_params": verify_payload.get("suggested_next_params", []),
                }
                with trace_jsonl_path.open("a", encoding="utf-8", newline="\n") as f:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")

                result = {
                    "tool": "kbtool",
                    "cmd": "research",
                    "round": round_n,
                    "ok": bool(verify_payload.get("ok")),
                    "stop_recommended": bool(verify_payload.get("stop_recommended")),
                    "stop_reason": str(verify_payload.get("stop_reason") or ""),
                    "run_dir": str(run_dir.relative_to(root)).replace("\\", "/"),
                    "paths": {
                        "bundle_md": bundle_rel,
                        "trace_json": trace_rel,
                        "verify_json": verify_rel,
                        "trace_jsonl": str(trace_jsonl_path.relative_to(root)).replace("\\", "/"),
                    },
                    "input": {
                        "query": str(getattr(args, "query", "")),
                        "params": input_params,
                        "planner_sha256": planner_sha256,
                        "planner_missing": planner_missing,
                        "note": note,
                    },
                    "audit": verify_payload.get("audit", {}),
                    "effective": {
                        "query": bundle_result.raw_query,
                        "query_mode": bundle_result.query_mode,
                        "must": bundle_result.must_terms,
                        "doc_code_hints": doc_code_hints,
                    },
                    "render": {
                        "limit": int(getattr(args, "limit", 20)),
                        "neighbors": int(args.neighbors),
                        "order": str(args.order),
                        "max_chars": int(args.max_chars),
                        "per_node_max_chars": int(args.per_node_max_chars),
                        "body_mode": str(args.body),
                    },
                    "stats": {
                        "planned_nodes": planned_nodes,
                        "rendered_nodes": rendered_nodes,
                        "budget_exhausted": budget_exhausted,
                        "truncated_nodes": truncated_nodes,
                        "snippet_nodes": snippet_nodes,
                    },
                    "focus": {"diversity": div, "mass_top3": mass_top3, "margin": margin},
                    "blocking_issues": verify_payload.get("blocking_issues", []),
                    "non_blocking_issues": verify_payload.get("non_blocking_issues", []),
                    "suggestions": verify_payload.get("suggestions", []),
                    "suggested_next_params": verify_payload.get("suggested_next_params", []),
                }
                print_json(result)
                return 0
            except sqlite3.OperationalError:
                if timeout.timed_out:
                    die(f"SQLite query timed out after {timeout_ms}ms")
                raise
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
        parts.append(f"- source_path: `{node.source_path}`\n")
        parts.append(f"- source: `{node.ref_path}`\n\n")
        snippet = extract_window(node.body_plain, terms, snippet_chars).strip()
        if snippet:
            snippet_md = "\n".join("> " + ln for ln in snippet.splitlines())
            parts.append(snippet_md + "\n\n")
    return "".join(parts)


def cmd_search(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
        with SqliteTimeout(conn, timeout_ms) as timeout:
            try:
                raw_query = args.query
                query_mode = args.query_mode
                must_terms = list(args.must)
                enable_hooks = bool(getattr(args, "enable_hooks", False))

                if enable_hooks:
                    out, _ = run_hook(
                        root,
                        "pre_search",
                        {
                            "stage": "pre_search",
                            "query": raw_query,
                            "query_mode": query_mode,
                            "must": must_terms,
                        },
                    )
                    if "query" in out:
                        candidate = str(out.get("query") or "").strip()
                        if candidate:
                            raw_query = candidate
                    if "query_mode" in out:
                        qm = str(out.get("query_mode") or "").strip().lower()
                        if qm in {"or", "and"}:
                            query_mode = qm
                    if "must" in out:
                        value = out.get("must")
                        if isinstance(value, list):
                            must_terms = [str(v).strip() for v in value if str(v).strip()]

                hits = search_leaf_nodes(
                    conn,
                    raw_query,
                    limit=args.limit,
                    query_mode=query_mode,
                    must_terms=must_terms,
                )
                if not hits:
                    die("No matches. Try a different query or rebuild indexes.")
                if enable_hooks:
                    out, digest = run_hook(
                        root,
                        "post_search",
                        {
                            "stage": "post_search",
                            "query": raw_query,
                            "query_mode": query_mode,
                            "must": must_terms,
                            "hits": list(hits),
                        },
                    )
                    if "hits" in out:
                        value = out.get("hits")
                        if not isinstance(value, list):
                            die("Hook post_search must return {'hits': [...]} list")
                        hits = [str(v) for v in value if str(v)]
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
                    raw_query=raw_query,
                    query_mode=query_mode,
                    must_terms=must_terms,
                    snippet_chars=args.snippet_chars,
                )
                out_path = safe_output_path(root, args.out)
                out_path.write_text(content, encoding="utf-8", newline="\n")
                print("[OK] Wrote search:", out_path)
                return 0
            except sqlite3.OperationalError as e:
                if timeout.timed_out:
                    die(f"SQLite query timed out after {timeout_ms}ms")
                raise
    finally:
        conn.close()


def cmd_get_node(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        node_id = str(getattr(args, "node_id", "") or "").strip()
        node_id_flag = str(getattr(args, "node_id_flag", "") or "").strip()
        if node_id and node_id_flag and node_id != node_id_flag:
            die("Conflicting node id: provide positional node_id OR --node-id (not both).")
        node_id = node_id or node_id_flag
        if not node_id:
            die("Missing node_id.")

        payload = node_to_dict(get_node(conn, node_id), include_body=True)
        out = str(getattr(args, "out", "") or "").strip()
        if out:
            out_path = safe_output_path(root, out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
        print_json(payload)
        return 0
    finally:
        conn.close()


def child_node_ids(conn: sqlite3.Connection, parent_id: str) -> List[str]:
    rows = conn.execute(
        """
        SELECT node_id
        FROM nodes
        WHERE parent_id = ? AND is_active = 1
        ORDER BY ordinal, node_id
        """,
        (parent_id,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def cmd_get_children(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        ids = child_node_ids(conn, args.node_id)
        nodes = [node_to_dict(get_node(conn, nid), include_body=False) for nid in ids]
        print_json({"node_id": args.node_id, "children": nodes})
        return 0
    finally:
        conn.close()


def cmd_get_parent(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        n = get_node(conn, args.node_id)
        if not n.parent_id:
            print_json({"node_id": args.node_id, "parent": None})
            return 0
        parent = get_node(conn, n.parent_id)
        print_json({"node_id": args.node_id, "parent": node_to_dict(parent, include_body=False)})
        return 0
    finally:
        conn.close()


def cmd_get_siblings(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        n = get_node(conn, args.node_id)
        ids = iter_neighbors(conn, n, int(args.neighbors))
        nodes = [node_to_dict(get_node(conn, nid), include_body=False) for nid in ids]
        print_json({"node_id": args.node_id, "neighbors": int(args.neighbors), "nodes": nodes})
        return 0
    finally:
        conn.close()


def cmd_follow_references(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        node_id = args.node_id
        direction = str(args.direction or "out").lower()
        if direction not in {"out", "in", "both"}:
            die("--direction must be out/in/both")
        found: set[str] = set()

        if direction in {"out", "both"}:
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
                found.add(elevate_to_article(conn, str(row[0])))

        if direction in {"in", "both"}:
            rows = conn.execute(
                """
                SELECT from_node_id
                FROM edges
                WHERE edge_type = 'references' AND to_node_id = ? AND is_active = 1
                ORDER BY from_node_id
                """,
                (node_id,),
            ).fetchall()
            for row in rows:
                found.add(elevate_to_article(conn, str(row[0])))

        nodes = [node_to_dict(get_node(conn, nid), include_body=False) for nid in sorted(found)]
        print_json({"node_id": node_id, "direction": direction, "nodes": nodes})
        return 0
    finally:
        conn.close()
