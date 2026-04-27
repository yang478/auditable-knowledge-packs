from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence

from . import memory
from .tokenizer_core import _ordered_unique
from .runtime import SqliteTimeout, die, open_db, print_json, resolve_db_path, resolve_root, safe_output_path
from .sql_utils import build_in_placeholders
from .text import build_match_expression, extract_window, fts_tokens, query_terms

logger = logging.getLogger(__name__)

DEFAULT_GRAPH_EDGE_TYPES = ("prev", "next", "references", "alias_mention", "title_mention")

_BUNDLE_DEFAULTS: dict[str, object] = {
    "limit": 20,
    "neighbors": 1,
    "max_chars": 40000,
    "per_node_max_chars": 6000,
    "body": "full",
}

_BUNDLE_PRESETS: dict[str, dict[str, object]] = {
    # Small output suitable for weak / small-context models.
    "quick": {
        "limit": 5,
        "neighbors": 0,
        "max_chars": 12000,
        "per_node_max_chars": 200,
        "body": "snippet",
        "max_nodes": 10,  # quick 模式最多 10 个节点
    },
    # Keep current defaults (explicit, so callers can switch back after using quick).
    "standard": dict(_BUNDLE_DEFAULTS),
}


def apply_bundle_preset(args: argparse.Namespace) -> None:
    preset = str(getattr(args, "preset", "") or "").strip().lower()
    if not preset:
        return
    settings = _BUNDLE_PRESETS.get(preset)
    if settings is None:
        die(f"Unknown --preset: {preset!r} (expected: {sorted(_BUNDLE_PRESETS)})")

    for key, value in settings.items():
        default = _BUNDLE_DEFAULTS.get(key)
        current = getattr(args, key, None)
        if current is None or current == default:
            setattr(args, key, value)


@dataclass(frozen=True)
class NodeLinks:
    node_id: str
    doc_id: str
    kind: str
    parent_id: Optional[str]
    prev_id: Optional[str]
    next_id: Optional[str]
    ordinal: int


@dataclass(frozen=True)
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
    heading_path: str = ""
    keywords: str = ""


def node_to_dict(node: Node, *, include_body: bool, include_ref_path: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
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
        "is_leaf": bool(node.is_leaf),
    }
    if include_ref_path:
        payload["ref_path"] = node.ref_path
    if include_body:
        payload["body_md"] = node.body_md
        payload["body_plain"] = node.body_plain
    return payload


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
          n.heading_path,
          t.body_md, t.body_plain, t.keywords
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
    return _row_to_node(row)


def _row_to_node(row: sqlite3.Row) -> Node:
    return Node(
        node_id=str(row["node_id"]),
        doc_id=str(row["doc_id"]),
        doc_title=str(row["doc_title"]),
        source_file=str(row["source_file"]),
        source_path=str(row["source_path"]),
        source_version=str(row["source_version"]),
        kind=str(row["kind"]),
        label=str(row["label"]),
        title=str(row["title"]),
        parent_id=str(row["parent_id"]) if row["parent_id"] else None,
        prev_id=str(row["prev_id"]) if row["prev_id"] else None,
        next_id=str(row["next_id"]) if row["next_id"] else None,
        ordinal=int(row["ordinal"]),
        ref_path=str(row["ref_path"]),
        is_leaf=bool(row["is_leaf"]),
        body_md=str(row["body_md"] or ""),
        body_plain=str(row["body_plain"] or ""),
        heading_path=str(row["heading_path"] if "heading_path" in row.keys() else ""),
        keywords=str(row["keywords"] if "keywords" in row.keys() else ""),
    )


def get_nodes_batch(conn: sqlite3.Connection, node_ids: Sequence[str]) -> dict[str, Node]:
    """Fetch multiple nodes in a single query. Returns node_id → Node mapping."""
    if not node_ids:
        return {}
    unique_ids = list(dict.fromkeys(str(n) for n in node_ids if str(n).strip()))
    if not unique_ids:
        return {}
    placeholders = build_in_placeholders(unique_ids)
    rows = conn.execute(
        f"""
        SELECT
          n.node_id, n.doc_id, d.doc_title,
          d.source_file, d.source_path, n.source_version,
          n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id, n.ordinal, n.ref_path, n.is_leaf,
          n.heading_path,
          t.body_md, t.body_plain, t.keywords
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
        JOIN node_text t ON t.node_key = n.node_key
        WHERE n.node_id IN ({placeholders}) AND n.is_active = 1
        ORDER BY n.source_version DESC
        """,
        tuple(unique_ids),
    ).fetchall()
    # Keep only the latest source_version per node_id (first seen due to ORDER BY).
    result: dict[str, Node] = {}
    for row in rows:
        nid = str(row["node_id"])
        if nid not in result:
            result[nid] = _row_to_node(row)
    return result


def _fetch_neighbor_chain(
    conn: sqlite3.Connection,
    start_id: str,
    parent_id: Optional[str],
    neighbors: int,
    direction: str,
) -> List[str]:
    """Fetch a prev/next neighbor chain using a single recursive CTE query.

    direction: 'prev' or 'next' — determines which link column to follow.
    """
    link_col = "prev_id" if direction == "prev" else "next_id"
    sql = f"""
    WITH RECURSIVE chain(node_id, {link_col}, parent_id, depth) AS (
        SELECT node_id, {link_col}, parent_id, 0
        FROM nodes
        WHERE node_id = ? AND is_active = 1
        UNION ALL
        SELECT n.node_id, n.{link_col}, n.parent_id, chain.depth + 1
        FROM chain
        JOIN nodes n ON n.node_id = chain.{link_col}
        WHERE chain.depth < ? AND n.is_active = 1
    )
    SELECT node_id, parent_id FROM chain WHERE depth > 0 ORDER BY depth
    """
    rows = conn.execute(sql, (start_id, neighbors)).fetchall()
    out: List[str] = []
    for row in rows:
        if parent_id is not None and str(row["parent_id"] or "") != parent_id:
            break
        out.append(str(row["node_id"]))
    return out


def iter_neighbors(conn: sqlite3.Connection, node: NodeLinks, neighbors: int) -> List[str]:
    if neighbors <= 0:
        return [node.node_id]
    out: List[str] = []

    if node.prev_id:
        prev_ids = _fetch_neighbor_chain(
            conn, node.prev_id, node.parent_id, neighbors, "prev"
        )
        prev_ids.reverse()
        out.extend(prev_ids)
    out.append(node.node_id)

    if node.next_id:
        next_ids = _fetch_neighbor_chain(
            conn, node.next_id, node.parent_id, neighbors, "next"
        )
        out.extend(next_ids)
    return out


def _filter_exclude_terms(conn: sqlite3.Connection, node_ids: Sequence[str], exclude_terms: Sequence[str]) -> List[str]:
    terms = [str(t).strip().lower() for t in exclude_terms if str(t).strip()]
    if not terms:
        return [str(n) for n in node_ids if str(n).strip()]

    nodes = get_nodes_batch(conn, node_ids)
    out: List[str] = []
    for node_id in node_ids:
        n = nodes.get(str(node_id))
        if n is None:
            continue
        hay = (n.title + "\n" + n.body_plain).lower()
        if any(term in hay for term in terms):
            continue
        out.append(n.node_id)
    return out


def search_chunk_ids(
    conn: sqlite3.Connection,
    raw_query: str,
    *,
    query_mode: str,
    must_terms: Sequence[str],
    limit: int,
) -> tuple[str, List[str]]:
    q = str(raw_query or "").strip()
    if not q:
        die("Missing --query.")
    qm = str(query_mode or "or").strip().lower()
    if qm not in {"or", "and"}:
        die("--query-mode must be or/and")

    # B2: 别名查询自动扩展
    def _expand_with_aliases(conn, raw_query):
        """查询 aliases 表，提取与查询 token 相关的别名 token。"""
        tokens = fts_tokens(raw_query)
        alias_tokens = []
        for token in tokens[:8]:
            rows = conn.execute(
                "SELECT DISTINCT normalized_alias FROM aliases WHERE alias LIKE ? AND is_active=1 LIMIT 3",
                (f"%{token}%",),
            ).fetchall()
            for r in rows:
                alias_tokens.append(str(r[0]))
        return alias_tokens

    alias_tokens = _expand_with_aliases(conn, q)
    if alias_tokens:
        extra = " ".join(alias_tokens[:16])
        q = f"{q} {extra}" if q else extra

    # Extract first meaningful token for title/heading_path boosting
    title_tokens = fts_tokens(q)
    title_term = title_tokens[0] if title_tokens else q.split()[0] if q.split() else ""

    # 转义 LIKE 特殊字符，防止 title_term 中的 % 和 _ 被解释为通配符
    escaped_title = title_term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _try_search(match_expr: str) -> list[sqlite3.Row]:
        """用给定 FTS 表达式执行搜索，返回匹配行或空列表。"""
        if not match_expr:
            return []
        return conn.execute(
            """SELECT n.node_id, bm25(node_fts) AS bm25
            FROM node_fts
            JOIN nodes n ON n.node_key = node_fts.node_key
            WHERE node_fts MATCH ? AND n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk'
            ORDER BY
                bm25(node_fts, 1.2, 0.4),
                (CASE WHEN n.heading_path LIKE '%' || ? || '%' ESCAPE '\\' THEN 1 ELSE 0 END) DESC,
                n.title LIKE '%' || ? || '%' ESCAPE '\\' DESC,
                n.doc_id, n.ordinal, n.node_id
            LIMIT ?""",
            (match_expr, escaped_title, escaped_title, int(limit)),
        ).fetchall()

    def _escape_fts_token(t: str) -> str:
        return t.replace('"', '""')

    # Level 1: 原始查询模式
    match = build_match_expression(q, query_mode=qm, must_terms=list(must_terms), max_tokens=64)
    if match:
        rows = _try_search(match)
        if rows:
            return match, [str(r["node_id"]) for r in rows]

    # Level 2: must_terms AND + 主查询 OR (仅当 query_mode=and 且有 must_terms)
    if qm == "and" and must_terms:
        match = build_match_expression(q, query_mode="or", must_terms=list(must_terms), max_tokens=64)
        if match:
            rows = _try_search(match)
            if rows:
                return match, [str(r["node_id"]) for r in rows]

    # Level 3: 全 OR 无 must_terms
    match = build_match_expression(q, query_mode="or", must_terms=[], max_tokens=64)
    if match:
        rows = _try_search(match)
        if rows:
            return match, [str(r["node_id"]) for r in rows]

    # Level 4: 去停用词重试
    tokens_no_stop = [t for t in fts_tokens(q) if t]  # 已去停用词
    if tokens_no_stop:
        simplified = " OR ".join(f'"{_escape_fts_token(t)}"' for t in tokens_no_stop[:32])
        rows = _try_search(simplified)
        if rows:
            return simplified, [str(r["node_id"]) for r in rows]

    return "", []


def expand_with_neighbors(conn: sqlite3.Connection, hit_ids: Sequence[str], *, neighbors: int) -> List[str]:
    included: set[str] = set()
    for node_id in hit_ids:
        if not str(node_id).strip():
            continue
        links = get_node_links(conn, str(node_id))
        for nid in iter_neighbors(conn, links, int(neighbors)):
            included.add(nid)

    if not included:
        return []

    # Preserve seed order: build result in the order hits were first seen.
    placeholders = build_in_placeholders(included)
    rows = conn.execute(
        f"""
        SELECT node_id
        FROM nodes
        WHERE is_active = 1 AND kind = 'chunk' AND is_leaf = 1 AND node_id IN ({placeholders})
        """,
        tuple(sorted(included)),
    ).fetchall()
    id_set = {str(row["node_id"]) for row in rows}
    # Return in seed order (first hit first), then neighbors discovered later.
    result: list[str] = []
    seen: set[str] = set()
    for node_id in hit_ids:
        nid = str(node_id)
        if nid in id_set and nid not in seen:
            result.append(nid)
            seen.add(nid)
    for nid in id_set:
        if nid not in seen:
            result.append(nid)
    return result


def graph_edge_types(raw: Sequence[str] | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_GRAPH_EDGE_TYPES
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        for part in str(item or "").replace(",", " ").split():
            edge_type = part.strip()
            if not edge_type or edge_type in seen:
                continue
            seen.add(edge_type)
            out.append(edge_type)
    return tuple(out) or DEFAULT_GRAPH_EDGE_TYPES


def expand_with_graph(
    conn: sqlite3.Connection,
    hit_ids: Sequence[str],
    *,
    graph_depth: int,
    edge_types: Sequence[str],
    limit: int,
) -> List[str]:
    if graph_depth <= 0 or not hit_ids or not edge_types:
        return list(hit_ids)
    try:
        conn.execute("SELECT 1 FROM edges LIMIT 0")
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            # Retry once after a short delay
            import time
            time.sleep(0.1)
            try:
                conn.execute("SELECT 1 FROM edges LIMIT 0")
            except sqlite3.Error:
                return list(hit_ids)
        else:
            return list(hit_ids)

    depth = min(max(0, int(graph_depth)), 3)
    edge_type_values = tuple(str(edge_type) for edge_type in edge_types if str(edge_type).strip())
    if not edge_type_values:
        return list(hit_ids)

    result_limit = max(1, int(limit))
    seed_ids = [str(h) for h in hit_ids if str(h).strip()]
    if not seed_ids:
        return list(hit_ids)

    # Single recursive CTE for ALL seed nodes (was: one CTE per seed in a loop).
    seed_ph = build_in_placeholders(seed_ids)
    edge_ph = build_in_placeholders(edge_type_values)
    sql = f"""
    WITH RECURSIVE graph_walk(node_id, depth) AS (
        SELECT node_id, 0
        FROM nodes
        WHERE node_id IN ({seed_ph}) AND is_active = 1 AND is_leaf = 1

        UNION ALL

        SELECT e.to_node_id, graph_walk.depth + 1
        FROM graph_walk
        JOIN edges e ON e.from_node_id = graph_walk.node_id
        WHERE graph_walk.depth < ?
          AND e.is_active = 1
          AND e.confidence >= 0.5
          AND e.edge_type IN ({edge_ph})

        UNION ALL

        SELECT e.from_node_id, graph_walk.depth + 1
        FROM graph_walk
        JOIN edges e ON e.to_node_id = graph_walk.node_id
        WHERE graph_walk.depth < ?
          AND e.is_active = 1
          AND e.confidence >= 0.5
          AND e.edge_type IN ({edge_ph})
    )
    SELECT n.node_id
    FROM graph_walk
    JOIN nodes n ON n.node_id = graph_walk.node_id
    WHERE n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk'
    GROUP BY n.node_id
    ORDER BY MIN(graph_walk.depth), n.doc_id, n.ordinal, n.node_id
    LIMIT ?
    """

    params = [*seed_ids, depth, *edge_type_values, depth, *edge_type_values, result_limit]
    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            import time
            time.sleep(0.1)
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.Error as exc2:
                logger.warning("Graph expansion failed: %s", exc2)
                return list(hit_ids)
        else:
            logger.warning("Graph expansion failed: %s", exc)
            return list(hit_ids)

    out: List[str] = []
    for row in rows:
        node_id = str(row["node_id"] if isinstance(row, sqlite3.Row) else row[0])
        out.append(node_id)
    return out


# _ordered_unique imported from .tokenizer_core


def expand_hits(
    conn: sqlite3.Connection,
    hit_ids: Sequence[str],
    *,
    neighbors: int,
    graph_depth: int,
    edge_types: Sequence[str],
    limit: int,
    max_nodes: int | None = None,
) -> List[str]:
    seeds = _ordered_unique(list(hit_ids))
    if not seeds:
        return []
    if int(graph_depth) > 0:
        graph_ids = expand_with_graph(
            conn,
            seeds,
            graph_depth=int(graph_depth),
            edge_types=edge_types,
            limit=max(1, int(limit)),
        )
        expanded = _ordered_unique([*seeds, *graph_ids])
        if len(expanded) > len(seeds) or int(neighbors) <= 0:
            if max_nodes is not None and max_nodes > 0 and len(expanded) > max_nodes:
                expanded = expanded[:max_nodes]
            return expanded
    neighbor_ids = expand_with_neighbors(conn, seeds, neighbors=max(0, int(neighbors)))
    result = _ordered_unique([*seeds, *neighbor_ids]) or seeds
    if max_nodes is not None and max_nodes > 0 and len(result) > max_nodes:
        result = result[:max_nodes]
    return result


def render_bundle_md(
    conn: sqlite3.Connection,
    node_ids: Sequence[str],
    *,
    raw_query: str,
    match: str,
    neighbors: int,
    limit: int,
    max_chars: int,
    per_node_max_chars: int,
    body_mode: str,
    graph_depth: int = 0,
    edge_types: Sequence[str] = (),
    show_keywords: bool = False,
    keyword_count: int = 6,
    suggested_queries: Sequence[str] = (),
    suggested_neighbors: int = 0,
) -> tuple[str, List[Node], bool]:
    mode = str(body_mode or "full").strip().lower()
    if mode not in {"full", "snippet", "none"}:
        die("--body must be full/snippet/none")

    max_chars_value = max(0, int(max_chars))
    per_node_value = max(0, int(per_node_max_chars))
    terms = query_terms(raw_query)

    header_lines = [
        "# Bundle\n\n",
        f"- query: `{raw_query}`\n",
        f"- query_mode: `{ 'and' if ' AND ' in match else 'or' }`\n",
        f"- fts_match: `{match}`\n",
        f"- limit: {int(limit)}\n",
        f"- neighbors: {int(neighbors)}\n",
        f"- graph_depth: {int(graph_depth)}\n",
    ]
    if int(graph_depth) > 0:
        header_lines.append(f"- edge_types: {', '.join(edge_types)}\n")
    header_lines.append(f"- nodes: {len(list(node_ids))}\n")
    if suggested_queries:
        header_lines.append(f"- suggested_rewrites: {', '.join(suggested_queries)}\n")
    if suggested_neighbors:
        header_lines.append(f"- recommended_neighbors: {suggested_neighbors}\n")
    header_lines.append("\n")
    content_parts: List[str] = ["".join(header_lines), "## Evidence\n\n"]

    rendered: List[Node] = []
    budget_exhausted = False
    used = sum(len(part) for part in content_parts)

    nodes_map = get_nodes_batch(conn, node_ids)

    for node_id in node_ids:
        node = nodes_map.get(str(node_id))
        if node is None:
            continue
        rendered.append(node)

        body_md = node.body_md or ""
        marker = ""
        if mode == "none":
            body_md = ""
        elif mode == "snippet":
            snippet = extract_window(node.body_plain or "", terms, max_chars=min(per_node_value or 800, 1200))
            body_md = snippet.strip() + "\n" if snippet.strip() else ""
            marker = "*(SNIPPET)*\n\n" if body_md else ""
        else:
            if per_node_value > 0 and len(body_md) > per_node_value:
                body_md = body_md[:per_node_value].rstrip() + "\n"
                marker = "*(TRUNCATED)*\n\n"

        block = [
            f"### `{node.node_id}` {node.title}\n\n",
            f"- doc: {node.doc_title} (`{node.doc_id}`)\n",
            f"- ref: `{node.ref_path}`\n",
        ]
        if show_keywords and node.keywords:
            kws = node.keywords.split()
            kw_count = max(1, min(keyword_count, len(kws)))
            selected = kws[:kw_count]
            block.append(f"- keywords: {', '.join(f'`{k}`' for k in selected)}\n")
        block.append("\n")
        if marker:
            block.append(marker)
        if body_md:
            block.append(body_md + "\n")

        block_text = "".join(block)
        if max_chars_value > 0 and used + len(block_text) > max_chars_value:
            budget_exhausted = True
            break
        content_parts.append(block_text)
        used += len(block_text)

    content_parts.append("## References\n\n")
    for node in rendered:
        content_parts.append(f"- `{node.ref_path}`\n")
    if budget_exhausted:
        content_parts.append(
            "\n> *(BUDGET EXHAUSTED)* 输出达到 `--max-chars` 限制。可减小 `--neighbors/--limit` 或增大预算。\n"
        )

    return "".join(content_parts), rendered, budget_exhausted


@dataclass(frozen=True)
class _BundleResult:
    """Shared result from bundle core execution."""
    payload: dict[str, Any]
    content: str
    raw_query: str
    match: str
    hits: list[str]
    expanded: list[str]
    neighbors: int
    graph_depth: int
    edge_types: tuple[str, ...]


_NO_HITS_SUGGESTIONS = [
    "尝试更宽泛或不同的关键词",
    "使用 --query-mode or（当前可能是 and 模式）",
    "检查关键词是否使用了文档原文语言",
    "如果连续 2 轮以上都无命中，可能是数据损坏或索引问题，请停止搜索并报告",
]


def _proximity_rerank(conn, hit_ids, raw_query):
    """基于查询 token 在原文中的邻近度重排 hit_ids。"""
    tokens = fts_tokens(raw_query)
    if len(tokens) < 2:
        return hit_ids

    bonuses = {}
    for node_id in hit_ids[:50]:
        row = conn.execute(
            "SELECT t.body_plain FROM node_text t JOIN nodes n ON n.node_key = t.node_key WHERE n.node_id = ?",
            (node_id,)
        ).fetchone()
        if not row:
            continue
        body = str(row[0]).lower()
        # 对每个 token，找到所有出现位置（不只是第一次）
        positions = []
        for token in tokens:
            token_lower = token.lower()
            occ_list = []
            pos = 0
            while True:
                pos = body.find(token_lower, pos)
                if pos == -1:
                    break
                occ_list.append((pos, pos + len(token)))
                pos += 1
            if occ_list:
                positions.append(occ_list[:10])  # 限制每个 token 最多取前 10 个出现位置，避免组合爆炸
        if len(positions) < 2:
            bonuses[node_id] = 0.0
            continue
        # 找最小窗口（贪心：对每个 token 的出现位置列表，取笛卡尔积，找到最小跨度）
        from itertools import product
        min_window = float('inf')
        for combo in product(*positions):
            starts = [p[0] for p in combo]
            window = max(starts) - min(starts)
            if window < min_window:
                min_window = window
        window = min_window
        if window <= 50:
            bonuses[node_id] = 2.0
        elif window <= 200:
            bonuses[node_id] = 1.0
        elif window <= 500:
            bonuses[node_id] = 0.5
        else:
            bonuses[node_id] = 0.0

    if not bonuses:
        return hit_ids

    # 分组：高邻近度节点提到最前
    high = [nid for nid in hit_ids if bonuses.get(nid, 0) >= 2.0]
    mid = [nid for nid in hit_ids if 0.5 <= bonuses.get(nid, 0) < 2.0]
    rest = [nid for nid in hit_ids if bonuses.get(nid, 0) < 0.5]
    return high + mid + rest


def _execute_bundle_core(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
) -> _BundleResult:
    """Core bundle logic shared between cmd_bundle and triage._run_bundle_task."""
    apply_bundle_preset(args)
    raw_query = str(getattr(args, "query", "") or "").strip()
    query_mode = str(getattr(args, "query_mode", "or") or "or")
    must_terms = list(getattr(args, "require_terms", []) or [])
    exclude_terms = list(getattr(args, "exclude_terms", []) or [])
    limit = max(1, int(getattr(args, "limit", 20) or 20))
    neighbors_raw = getattr(args, "neighbors", 1)
    neighbors_value = 1 if neighbors_raw is None else int(neighbors_raw)
    neighbors = max(0, neighbors_value)
    graph_depth = max(0, int(getattr(args, "graph_depth", 0) or 0))
    edge_types = graph_edge_types(getattr(args, "edge_types", None))
    max_nodes = int(getattr(args, "max_nodes", 0) or 0)

    match, hits = search_chunk_ids(
        conn,
        raw_query,
        query_mode=query_mode,
        must_terms=must_terms,
        limit=limit * 2,
    )
    hits = _filter_exclude_terms(conn, hits, exclude_terms)
    hits = memory.apply_learned_boost(conn, hits, raw_query)
    hits = hits[:limit]

    if not hits:
        # B6: 渐进降级 — n-gram fallback: LIKE 扫描 body_plain
        tokens = fts_tokens(raw_query)
        if tokens:
            conditions = []
            params = []
            for token in tokens[:5]:
                conditions.append("t.body_plain LIKE ? ESCAPE '\\'")
                escaped = token.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                params.append(f"%{escaped}%")
            where = " OR ".join(conditions)
            rows = conn.execute(
                f"SELECT n.node_id FROM node_text t JOIN nodes n ON n.node_key = t.node_key WHERE n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk' AND ({where}) LIMIT ?",
                params + [int(limit)],
            ).fetchall()
            fallback_ids = [str(r[0]) for r in rows]
            if fallback_ids:
                logger.info("FTS returned 0 hits, LIKE fallback: %d hits", len(fallback_ids))
                hits = fallback_ids

        if not hits:
            return _BundleResult(
                payload={
                    "hits": 0,
                    "stop_reason": "no_hits",
                    "query": raw_query,
                    "suggestions": list(_NO_HITS_SUGGESTIONS),
                },
                content="",
                raw_query=raw_query,
                match=match,
                hits=[],
                expanded=[],
                neighbors=neighbors,
                graph_depth=graph_depth,
                edge_types=edge_types,
            )

    # B5: 邻近度评分后处理
    hits = _proximity_rerank(conn, hits, raw_query)

    expanded = expand_hits(
        conn,
        hits,
        neighbors=neighbors,
        graph_depth=graph_depth,
        edge_types=edge_types,
        limit=max(limit * 3, limit),
    )

    # 节点总数上界控制
    if max_nodes > 0 and len(expanded) > max_nodes:
        logger.info("Truncating from %d to %d nodes (--max-nodes)", len(expanded), max_nodes)
        expanded = expanded[:max_nodes]

    suggested_queries = memory.suggest_rewrites(conn, raw_query, expanded)
    suggested_neighbors = memory.recommend_neighbors(conn, raw_query)

    content, rendered, budget_exhausted = render_bundle_md(
        conn,
        expanded,
        raw_query=raw_query,
        match=match,
        neighbors=neighbors,
        limit=limit,
        max_chars=int(getattr(args, "max_chars", 40000) or 40000),
        per_node_max_chars=int(getattr(args, "per_node_max_chars", 6000) or 6000),
        body_mode=str(getattr(args, "body", "full") or "full"),
        graph_depth=graph_depth,
        edge_types=edge_types,
        show_keywords=bool(getattr(args, "show_keywords", False)),
        keyword_count=int(getattr(args, "keyword_count", 6) or 6),
        suggested_queries=suggested_queries,
        suggested_neighbors=suggested_neighbors,
    )

    payload = {
        "tool": "kbtool",
        "cmd": "bundle",
        "query": raw_query,
        "fts_match": match,
        "hits": list(hits),
        "expanded": list(expanded),
        "graph_depth": graph_depth,
        "edge_types": list(edge_types),
        "rendered": [node_to_dict(n, include_body=False, include_ref_path=False) for n in rendered],
        "budget_exhausted": bool(budget_exhausted),
        "suggested_queries": suggested_queries,
        "suggested_neighbors": suggested_neighbors,
    }
    return _BundleResult(
        payload=payload,
        content=content,
        raw_query=raw_query,
        match=match,
        hits=list(hits),
        expanded=list(expanded),
        neighbors=neighbors,
        graph_depth=graph_depth,
        edge_types=edge_types,
    )


def cmd_bundle(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    conn = open_db(resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite")))
    try:
        # 缓存检查
        _query_text = str(getattr(args, "query", "") or "").strip()
        if _query_text and not getattr(args, "no_cache", False):
            cached = memory.get_cached_result(conn, _query_text)
            if cached:
                logger.info("Cache hit for query: %s", _query_text)
                print_json(cached)
                return 0

        timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
        with SqliteTimeout(conn, timeout_ms):
            result = _execute_bundle_core(conn, args)

            if not result.hits:
                print_json(result.payload)
                memory.log_query(
                    conn,
                    query_text=result.raw_query,
                    cmd="bundle",
                    preset=str(getattr(args, "preset", "") or ""),
                    hit_ids=[],
                    neighbors=result.neighbors,
                )
                return 1

            out_path = safe_output_path(root, getattr(args, "out", "bundle.md"))
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(result.content, encoding="utf-8", newline="\n")
            logger.info("Wrote bundle: %s", out_path)

            out_rel = str(out_path.relative_to(root)).replace("\\", "/")
            result.payload["out"] = out_rel

            print_json(result.payload)
            if result.hits:
                memory.cache_result(conn, _query_text, result.payload, result.expanded)
            memory.log_query(
                conn,
                query_text=result.raw_query,
                cmd="bundle",
                preset=str(getattr(args, "preset", "") or ""),
                hit_ids=result.expanded,
                bundle_path=out_rel,
                neighbors=result.neighbors,
            )
            return 0
    finally:
        conn.close()


def cmd_get_node(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    conn = open_db(resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite")))
    try:
        # Collect all node_ids: positional (space-separated) + --node-id (single flag) + --node-ids (comma-separated)
        node_ids: list[str] = list(getattr(args, "node_id", []) or [])
        node_id_flag = str(getattr(args, "node_id_flag", "") or "").strip()
        if node_id_flag:
            node_ids.append(node_id_flag)
        node_ids_flag = str(getattr(args, "node_ids_flag", "") or "").strip()
        if node_ids_flag:
            node_ids.extend(nid.strip() for nid in node_ids_flag.split(",") if nid.strip())

        if not node_ids:
            die("Missing node_id(s).")

        # Deduplicate while preserving input order
        seen: set[str] = set()
        unique_ids: list[str] = []
        for nid in node_ids:
            if nid not in seen:
                seen.add(nid)
                unique_ids.append(nid)

        # Batch fetch in a single SQL query
        nodes_map = get_nodes_batch(conn, unique_ids)
        nodes = [nodes_map[nid] for nid in unique_ids if nid in nodes_map]

        if not nodes:
            die(f"No active nodes found for: {', '.join(unique_ids[:5])}")

        # Audit trail: log accessed node_ids
        try:
            memory.log_query(
                conn,
                query_text=f"__audit_get_node__:{','.join(unique_ids[:8])}",
                cmd="get-node",
                hit_ids=unique_ids,
            )
        except Exception as exc:
            logger.warning("Audit log failed: %s", exc)

        fmt = str(getattr(args, "format", "json") or "json").strip().lower()
        if fmt == "body":
            # Multi-node body output: each node prefixed with a metadata header, separated by ---
            parts: list[str] = []
            for node in nodes:
                keywords = node.keywords or ""
                keywords_str = ", ".join(keywords.split()[:8]) if keywords else ""
                meta_header = (
                    f"<!--\n"
                    f"node_id: {node.node_id}\n"
                    f"doc: {node.doc_title} ({node.doc_id})\n"
                    f"heading: {node.heading_path or node.title}\n"
                    f"keywords: [{keywords_str}]\n"
                    f"prev: {node.prev_id or 'N/A'}  next: {node.next_id or 'N/A'}\n"
                    f"-->\n\n"
                )
                body = node.body_md or ""
                parts.append(meta_header + body)
            separator = "\n\n---\n\n"
            full_body = separator.join(parts)

            out = str(getattr(args, "out", "") or "").strip()
            if out:
                out_path = safe_output_path(root, out)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(full_body + "\n", encoding="utf-8", newline="\n")
            sys.stdout.write(full_body + "\n")
            return 0

        # JSON format: return array
        payload_list: list[dict[str, object]] = []
        for node in nodes:
            payload_list.append(node_to_dict(node, include_body=True))

        out = str(getattr(args, "out", "") or "").strip()
        if out:
            out_path = safe_output_path(root, out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(payload_list, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8", newline="\n",
            )
        print_json(payload_list)
        return 0
    finally:
        conn.close()
