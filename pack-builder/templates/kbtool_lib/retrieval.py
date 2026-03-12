from __future__ import annotations

import argparse
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .runtime import die, open_db, print_json, resolve_root, run_hook, sha1_file
from .text import (
    build_match_expression,
    build_match_query,
    core_alias_title,
    count_occurrences,
    extract_window,
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
    ordered_scores: List[Tuple[int, int, int, str]] = []
    seen_nodes: set[str] = set()
    has_primary_support = bool(title_hits or alias_hits)
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
        if must_terms or query_mode == "and":
            node = get_node(conn, node_id)
            hay = (node.title + "\n" + node.body_plain).lower()
            if any(str(t).lower() not in hay for t in must_terms if str(t).strip()):
                continue
            if query_mode == "and":
                parts = [p.lower() for p in query_terms(raw_query) if p.strip()]
                if any(p not in hay for p in parts):
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
        ordered_scores.append(
            (
                score,
                body_rank.get(node_id, 1_000_000),
                title_rank.get(node_id, 1_000_000),
                node_id,
            )
        )
    ordered_scores.sort(key=lambda item: (-item[0], item[1], item[2], item[3]))
    return [node_id for _, _, _, node_id in ordered_scores[:limit]]


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
            else:
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
            if digest:
                diagnostics.append(f"hook: post_search sha1={digest} path=hooks/post_search.py")
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
        elevated, expansion_diags = apply_triggered_expansion(
            conn,
            normalized_query,
            elevated,
            force_debug=args.debug_triggers,
        )
        diagnostics.extend(expansion_diags)
        if enable_hooks:
            pre_render_path = root / "hooks" / "pre_render.py"
            if pre_render_path.exists():
                diagnostics.append(f"hook: pre_render sha1={sha1_file(pre_render_path)} path=hooks/pre_render.py")
        content, _ = render_bundle(
            conn,
            elevated,
            raw_query=raw_query,
            neighbors=args.neighbors,
            max_chars=args.max_chars,
            per_node_max_chars=args.per_node_max_chars,
            body_mode=args.body,
            order=args.order,
            diagnostics=diagnostics,
            hooks_root=root if enable_hooks else None,
            enable_hooks=enable_hooks,
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
        out_path = (root / args.out).resolve()
        out_path.write_text(content, encoding="utf-8", newline="\n")
        print("[OK] Wrote search:", out_path)
        return 0
    finally:
        conn.close()


def cmd_get_node(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = root / args.db
    conn = open_db(db_path)
    try:
        node = get_node(conn, args.node_id)
        print_json(node_to_dict(node, include_body=True))
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
