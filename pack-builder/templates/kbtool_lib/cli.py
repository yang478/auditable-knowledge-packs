from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import traceback
from typing import Optional, Sequence

from .catalog import cmd_docs
from .grep import cmd_search
from .locate import cmd_files
from .retrieval import cmd_bundle, cmd_get_node
from .runtime import configure_logging, die, open_db, print_json, resolve_db_path, resolve_root
from .triage import cmd_triage
from . import memory

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="KB tool for generated skills (Occam chunking, FTS5).")
    p.add_argument("--root", default="", help="Skill root directory (default: auto-detect).")
    p.add_argument("--db", default="kb.sqlite", help="SQLite DB path relative to root (default: kb.sqlite).")
    p.add_argument("--skill", action="store_true", help="Print JSON tool usage for LLMs and exit.")
    sub = p.add_subparsers(dest="cmd", metavar="{bundle,get-node,docs,search,files,triage,history,feedback,vacuum}")

    b = sub.add_parser("bundle", help="Search chunks and write an evidence bundle (merged search+bundle).")
    b.add_argument("--query", required=True, help="User query.")
    b.add_argument(
        "--preset",
        choices=["quick", "standard"],
        default="quick",
        help="Output budget preset (default: quick small/snippet). Does not override explicitly provided flags.",
    )
    b.add_argument("--out", default="runs/bundle.md", help="Output markdown path (relative to root).")
    b.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    b.add_argument("--max-nodes", type=int, default=None, dest="max_nodes", help="扩展后的最大节点总数。超出部分按 BM25 排序截断。默认无限制。")
    b.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    b.add_argument(
        "--require-term",
        action="append",
        default=[],
        dest="require_terms",
        help="Term that must appear in matched text (repeatable).",
    )
    b.add_argument(
        "--exclude-term",
        action="append",
        default=[],
        dest="exclude_terms",
        help="Term that must not appear in matched text (repeatable).",
    )
    b.add_argument("--neighbors", type=int, default=1, help="Expand to prev/next chunks (context window).")
    b.add_argument("--max-chars", type=int, default=40000, help="Max output size (characters).")
    b.add_argument("--per-node-max-chars", type=int, default=6000, help="Per-chunk max chars before truncation.")
    b.add_argument("--body", choices=["full", "snippet", "none"], default="full", help="Chunk body rendering mode.")
    b.add_argument("--graph-depth", type=int, default=1, dest="graph_depth", help="Graph traversal depth (default: 1; 0 disables graph).")
    b.add_argument("--edge-types", nargs="+", default=None, dest="edge_types", help="Graph edge types to traverse (default excludes co_occurrence).")
    b.add_argument("--timeout-ms", type=int, default=30000, help="查询超时毫秒 (默认 30000)")
    b.add_argument("--no-cache", action="store_true", help="跳过查询结果缓存")
    b_kw_group = b.add_mutually_exclusive_group()
    b_kw_group.add_argument("--show-keywords", action="store_true", dest="show_keywords", default=True, help="Show extracted keywords for each chunk in output (default).")
    b_kw_group.add_argument("--no-show-keywords", action="store_false", dest="show_keywords", help="Hide extracted keywords for each chunk in output.")
    b.add_argument("--keyword-count", type=int, default=6, dest="keyword_count", help="Max keywords per chunk when --show-keywords (default: 6).")
    b.set_defaults(func=cmd_bundle)

    gn = sub.add_parser("get-node", help="Fetch one or more nodes (includes body_md).")
    gn.add_argument("node_id", nargs="*", default=[], help="Node id(s), space-separated (positional).")
    gn.add_argument("--node-id", dest="node_id_flag", default="", help="Node id (flag, single).")
    gn.add_argument("--node-ids", dest="node_ids_flag", default="", help="Node ids, comma-separated (flag).")
    gn.add_argument("--out", default="", help="Optional output path (relative to root).")
    gn.add_argument("--format", choices=["json", "body"], default="json", help="Output format: json=full node JSON; body=body_md plain text only.")
    gn.set_defaults(func=cmd_get_node)

    d = sub.add_parser("docs", help="List documents to markdown.")
    d.add_argument("--query", default="", help="Filter by title substring.")
    d.add_argument("--limit", type=int, default=0, help="Max docs to list (0=all).")
    d.add_argument("--out", default="runs/docs.md", help="Output markdown path (relative to root).")
    d.set_defaults(func=cmd_docs)

    g = sub.add_parser("search", help="Precise content search via ripgrep (rg) in references/.")
    g.add_argument("--pattern", default="", help="Regex pattern to search for.")
    # Weak-model compatibility: accept `--query` as a keyword query that is split on whitespace and OR'ed.
    # This avoids the common failure mode where models pass `--query \"a b c\"` and rg treats it as a literal phrase.
    g.add_argument("--query", default="", help="Keyword query (split on whitespace, OR terms; fixed-strings).")
    g.add_argument(
        "-F",
        "--fixed",
        action="store_true",
        help="Treat --pattern as a literal string (ripgrep fixed-strings, no regex).",
    )
    g.add_argument("--limit", type=int, default=20, help="Max results to return.")
    g.add_argument("--out", default="", help="Output markdown path for audit trail (relative to root). If omitted, only stdout JSON is emitted.")
    # Weak-model compatibility: these flags belong to `bundle` but models sometimes copy them to `search`.
    # Accept them as no-ops so `search` doesn't fail hard and can still provide evidence.
    g.add_argument("--neighbors", type=int, default=0, help=argparse.SUPPRESS)
    g.add_argument("--max-chars", type=int, default=0, help=argparse.SUPPRESS)
    g.add_argument("--per-node-max-chars", type=int, default=0, help=argparse.SUPPRESS)
    g.add_argument("--body", choices=["full", "snippet", "none"], default="", help=argparse.SUPPRESS)
    g.set_defaults(func=cmd_search)

    loc = sub.add_parser("files", help="Precise file search via fd in references/.")
    loc.add_argument("--pattern", required=True, help="File name pattern (glob/regex).")
    loc.add_argument("--limit", type=int, default=50, help="Max files to return.")
    loc.add_argument("--out", default="", help="Output markdown path for audit trail (relative to root). If omitted, only stdout JSON is emitted.")
    loc.set_defaults(func=cmd_files)

    t = sub.add_parser("triage", help="Parallel fuzzy+exact search, writing a combined evidence bundle.")
    t.add_argument(
        "--query",
        default="",
        help="Optional user query for BM25 bundle. If omitted, falls back to --pattern (or --file-pattern).",
    )
    t.add_argument(
        "--preset",
        choices=["quick", "standard"],
        default="quick",
        help="Bundle output budget preset (default: quick).",
    )
    t.add_argument("--pattern", default="", help="Optional rg pattern for exact content search.")
    t.add_argument("--fixed", action="store_true", help="Treat --pattern as a literal string (ripgrep fixed-strings).")
    t.add_argument("--file-pattern", default="", dest="file_pattern", help="Optional fd file name pattern for file locate.")
    t.add_argument("--graph-depth", type=int, default=1, dest="graph_depth", help="Bundle graph traversal depth (default: 1; 0 disables graph).")
    t.add_argument("--edge-types", nargs="+", default=None, dest="edge_types", help="Graph edge types to traverse (default excludes co_occurrence).")
    t.add_argument("--search-limit", type=int, default=12, dest="search_limit", help="Max rg matches in triage cascade.")
    t.add_argument("--files-limit", type=int, default=20, dest="files_limit", help="Max fd matches in triage.")
    t.add_argument("--max-nodes", type=int, default=None, dest="max_nodes", help="扩展后的最大节点总数。超出部分按 BM25 排序截断。默认无限制。")
    t.add_argument("--timeout-ms", type=int, default=30000, help="查询超时毫秒 (默认 30000)")
    t.add_argument("--out", default="runs/triage.md", help="Output markdown path (relative to root).")
    kw_group = t.add_mutually_exclusive_group()
    kw_group.add_argument("--show-keywords", action="store_true", dest="show_keywords", default=True, help="Show extracted keywords for each chunk in output (default).")
    kw_group.add_argument("--no-show-keywords", action="store_false", dest="show_keywords", help="Hide extracted keywords for each chunk in output.")
    t.add_argument("--keyword-count", type=int, default=6, dest="keyword_count", help="Max keywords per chunk when --show-keywords (default: 6).")
    t.set_defaults(func=cmd_triage)

    h = sub.add_parser("history", help="Show recent query history and learning statistics.")
    h.add_argument("--limit", type=int, default=20, help="Max history entries to show.")
    h.add_argument("--query", default="", help="Filter history by query text substring.")
    h.add_argument("--stats", action="store_true", help="Show statistics instead of history list.")
    h.add_argument("--out", default="", help="Optional markdown output path (relative to root).")
    h.set_defaults(func=cmd_history)

    fb = sub.add_parser("feedback", help="Record explicit feedback for a query-node pair to improve ranking.")
    fb.add_argument("--query-id", required=True, help="Query ID from history.")
    fb.add_argument("--node-id", required=True, help="Node ID to rate.")
    fb.add_argument("--type", required=True, choices=["positive", "negative", "neutral"], help="Feedback type.")
    fb.add_argument("--context", default="", help="Optional context note.")
    fb.set_defaults(func=cmd_feedback)

    vc = sub.add_parser("vacuum", help="Purge soft-deleted rows and reclaim disk space (safe, non-destructive to active data).")
    vc.add_argument("--purge-query-history", action="store_true", help="Also remove query log entries older than --history-max-age-days (default: keep).")
    vc.add_argument("--history-max-age-days", type=int, default=90, help="Max age in days for query log retention (default: 90, used with --purge-query-history).")
    vc.set_defaults(func=cmd_vacuum)

    return p


def cmd_history(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    conn = open_db(resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite")))
    try:
        show_stats = bool(getattr(args, "stats", False))
        if show_stats:
            stats = memory.get_stats(conn)
            print_json({"tool": "kbtool", "cmd": "history", "stats": stats})
            return 0

        limit = max(1, min(int(getattr(args, "limit", 20) or 20), 10000))
        query_filter = str(getattr(args, "query", "") or "")
        rows = memory.get_query_history(conn, limit=limit, query_substring=query_filter)

        lines: list[str] = ["# Query History\n\n"]
        for r in rows:
            ts = str(r.get("timestamp", ""))[:19].replace("T", " ")
            lines.append(f"## {r['query_id']}\n")
            lines.append(f"- **query**: `{r['query_text']}`\n")
            lines.append(f"- **norm**: `{r['query_norm']}`\n")
            lines.append(f"- **time**: {ts}\n")
            lines.append(f"- **cmd**: {r['cmd']}\n")
            lines.append(f"- **hits**: {r['hits_count']}\n")
            if r.get("bundle_path"):
                lines.append(f"- **bundle**: `{r['bundle_path']}`\n")
            lines.append("\n")

        md = "".join(lines) if lines else "# Query History\n\nNo records yet.\n"
        out = str(getattr(args, "out", "") or "").strip()
        if out:
            from .runtime import safe_output_path
            out_path = safe_output_path(root, out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(md, encoding="utf-8", newline="\n")

        print_json({"tool": "kbtool", "cmd": "history", "count": len(rows), "rows": rows})
        return 0
    finally:
        conn.close()


def cmd_feedback(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    conn = open_db(resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite")))
    try:
        qid = str(getattr(args, "query_id", "") or "").strip()
        nid = str(getattr(args, "node_id", "") or "").strip()
        ftype = str(getattr(args, "type", "") or "").strip()
        ctx = str(getattr(args, "context", "") or "").strip()
        fid = memory.record_feedback(
            conn, query_id=qid, node_id=nid, feedback_type=ftype, context=ctx
        )
        print_json({
            "tool": "kbtool",
            "cmd": "feedback",
            "feedback_id": fid,
            "query_id": qid,
            "node_id": nid,
            "type": ftype,
        })
        return 0
    finally:
        conn.close()


def cmd_vacuum(args: argparse.Namespace) -> int:
    """Purge soft-deleted rows (is_active=0) from entity tables and reclaim disk space.

    Safety: only removes rows where is_active=0 — never touches active data.
    Runs PRAGMA integrity_check before and after to confirm database health.
    """
    root = resolve_root(args.root)
    db_path = resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite"))
    conn = open_db(db_path)
    try:
        # Pre-check integrity
        pre_ok = conn.execute("PRAGMA integrity_check").fetchone()
        if pre_ok and str(pre_ok[0]).lower() != "ok":
            die(f"Database integrity check failed BEFORE vacuum: {pre_ok[0]}\n  File: {db_path}\n  Aborting to prevent data loss.")

        # Gather statistics before purge
        stats_before = _vacuum_stats(conn)
        logger.info(
            "Vacuum: starting — %d docs, %d nodes, %d edges, %d aliases (total, including inactive).",
            stats_before["docs"], stats_before["nodes"],
            stats_before["edges"], stats_before["aliases"],
        )

        # --- Purge inactive rows from entity tables ---
        purged_docs = _purge_inactive(conn, "docs")
        purged_nodes = _purge_inactive(conn, "nodes")
        purged_edges = _purge_inactive(conn, "edges")
        purged_aliases = _purge_inactive(conn, "aliases")

        # --- Purge orphaned node_text rows ---
        purged_nt = conn.execute(
            """
            DELETE FROM node_text
            WHERE node_key NOT IN (SELECT node_key FROM nodes WHERE is_active = 1)
            """
        ).rowcount

        # --- Optionally purge old query history ---
        purged_history = 0
        if getattr(args, "purge_query_history", False):
            max_age = max(1, int(getattr(args, "history_max_age_days", 90) or 90))
            memory.ensure_memory_tables(conn)
            purged_history = conn.execute(
                "DELETE FROM query_log WHERE timestamp < datetime('now', '-' || ? || ' days')",
                (max_age,),
            ).rowcount
            conn.execute(
                "DELETE FROM query_node_weights WHERE last_used < datetime('now', '-' || ? || ' days')",
                (max_age,),
            )
            conn.execute(
                "DELETE FROM node_feedback WHERE timestamp < datetime('now', '-' || ? || ' days')",
                (max_age,),
            )

        conn.commit()

        # --- Reclaim disk space ---
        logger.info("Vacuum: running VACUUM to reclaim disk space...")
        conn.execute("VACUUM")
        conn.commit()

        # Post-check integrity
        post_ok = conn.execute("PRAGMA integrity_check").fetchone()
        stats_after = _vacuum_stats(conn)

        result = {
            "tool": "kbtool",
            "cmd": "vacuum",
            "status": "ok" if (post_ok and str(post_ok[0]).lower() == "ok") else "integrity_warning",
            "purged": {
                "docs": purged_docs,
                "nodes": purged_nodes,
                "edges": purged_edges,
                "aliases": purged_aliases,
                "node_text_orphans": purged_nt,
                "query_history": purged_history,
            },
            "before": stats_before,
            "after": stats_after,
        }
        print_json(result)

        if result["status"] != "ok":
            return 1
        return 0
    finally:
        conn.close()


def _purge_inactive(conn: sqlite3.Connection, table: str) -> int:
    """Delete rows where is_active=0 from the given table. Returns count of deleted rows."""
    # Whitelist to prevent SQL injection via table name
    _SAFE_ENTITY_TABLES = {"docs", "nodes", "edges", "aliases"}
    if table not in _SAFE_ENTITY_TABLES:
        raise ValueError(f"Invalid table name for vacuum: {table!r}")
    cur = conn.execute(f"DELETE FROM {table} WHERE is_active = 0")
    return cur.rowcount


def _vacuum_stats(conn: sqlite3.Connection) -> dict[str, int]:
    """Return row counts for all entity tables."""
    stats: dict[str, int] = {}
    for table in ("docs", "nodes", "edges", "aliases", "node_text"):
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        stats[table] = int(row[0]) if row else 0
    return stats


def main(argv: Optional[Sequence[str]] = None) -> int:
    configure_logging()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)

        if args.skill:
            payload = {
                "tool": "kbtool",
                "schema": "kbtool.skill.v1",
                "description": "知识库文档检索与证据打包工具。",
                "commands": [
                    {
                        "name": "bundle",
                        "description": "检索 chunk 叶子节点并写出确定性的 bundle.md（search+bundle 合并为一步）。",
                        "args": [
                            {"flag": "--query", "type": "string", "required": True},
                            {"flag": "--preset", "type": "enum", "choices": ["quick", "standard"], "default": "quick", "note": "输出预算预设：quick=小上下文/片段；standard=较完整"},
                            {"flag": "--out", "type": "string", "default": "runs/bundle.md"},
                            {"flag": "--limit", "type": "int", "default": 20, "note": "控制命中数量上限（输出体积的关键旋钮）"},
                            {"flag": "--max-nodes", "type": "int", "default": 0, "note": "扩展后的最大节点总数。超出部分按 BM25 排序截断。quick preset 默认 10。"},
                            {"flag": "--query-mode", "type": "enum", "choices": ["or", "and"], "default": "or"},
                            {"flag": "--neighbors", "type": "int", "default": 1},
                            {"flag": "--max-chars", "type": "int", "default": 40000},
                            {"flag": "--per-node-max-chars", "type": "int", "default": 6000, "note": "每个 chunk 的最大字符数（输出体积的关键旋钮）"},
                            {"flag": "--body", "type": "enum", "choices": ["full", "snippet", "none"], "default": "full"},
                            {"flag": "--graph-depth", "type": "int", "default": 1, "note": "证据导航图跳数；噪声大时设为 0"},
                            {"flag": "--edge-types", "type": "list", "default": "prev next references alias_mention title_mention", "note": "默认不走 co_occurrence；需要时显式指定"},
                            {"flag": "--show-keywords", "type": "bool", "default": True, "note": "在每个 chunk 后面显示提取的关键词，帮助发现下一跳线索"},
                            {"flag": "--no-show-keywords", "type": "bool", "default": False, "note": "隐藏提取的关键词"},
                            {"flag": "--keyword-count", "type": "int", "default": 6, "note": "每个 chunk 显示的关键词数量（--show-keywords 开启时生效）"},
                        ],
                    },
                    {"name": "get-node", "description": "按 node_id 获取节点内容。默认输出 JSON；加 --format body 直接输出 body_md 纯文本。"},
                    {"name": "docs", "description": "输出文档列表（markdown）。"},
                    {
                        "name": "search",
                        "description": "精确内容搜索：使用 ripgrep 在 references/ 中搜索正则模式。",
                        "args": [
                            {"flag": "--pattern", "type": "string", "required": False, "note": "正则模式（精确控制，推荐用于 OR/分组/变体匹配）"},
                            {"flag": "--query", "type": "string", "required": False, "note": "关键词查询：按空格拆词并 OR（固定字符串匹配，更适合弱模型的多词输入）"},
                            {"flag": "--fixed", "type": "bool", "default": False, "note": "将 --pattern 当作字面量匹配（ripgrep -F）；--query 默认就是固定字符串 OR"},
                            {"flag": "--limit", "type": "int", "default": 20},
                            {"flag": "--out", "type": "string", "default": "", "note": "审计输出路径，留空则仅输出stdout"},
                        ],
                    },
                    {
                        "name": "files",
                        "description": "精确文件定位：使用 fd 按文件名模式查找 references/ 中的文件。",
                        "args": [
                            {"flag": "--pattern", "type": "string", "required": True},
                            {"flag": "--limit", "type": "int", "default": 50},
                            {"flag": "--out", "type": "string", "default": "", "note": "审计输出路径，留空则仅输出stdout"},
                        ],
                    },
                    {
                        "name": "triage",
                        "description": "并行运行 bundle（BM25）+ search（rg）+ files（fd），输出一个合并的 triage.md 证据包。",
                        "args": [
                            {"flag": "--query", "type": "string", "required": False, "note": "可选：省略时默认使用 --pattern（或 --file-pattern）作为 query"},
                            {"flag": "--preset", "type": "enum", "choices": ["quick", "standard"], "default": "quick", "note": "bundle 输出预算预设（默认 quick）"},
                            {"flag": "--pattern", "type": "string", "default": "", "note": "可选：rg 精确内容搜索模式"},
                            {"flag": "--fixed", "type": "bool", "default": False, "note": "可选：rg 字面量匹配（-F）"},
                            {"flag": "--file-pattern", "type": "string", "default": "", "note": "可选：fd 文件名匹配模式"},
                            {"flag": "--graph-depth", "type": "int", "default": 1, "note": "证据导航图跳数；噪声大时设为 0"},
                            {"flag": "--edge-types", "type": "list", "default": "prev next references alias_mention title_mention"},
                            {"flag": "--search-limit", "type": "int", "default": 12},
                            {"flag": "--files-limit", "type": "int", "default": 20},
                            {"flag": "--out", "type": "string", "default": "runs/triage.md"},
                            {"flag": "--show-keywords", "type": "bool", "default": True, "note": "在每个 chunk 后面显示提取的关键词，帮助发现下一跳线索"},
                            {"flag": "--no-show-keywords", "type": "bool", "default": False, "note": "隐藏提取的关键词"},
                            {"flag": "--keyword-count", "type": "int", "default": 6, "note": "每个 chunk 显示的关键词数量（--show-keywords 开启时生效）"},
                        ],
                    },
                    {
                        "name": "history",
                        "description": "查看查询历史和学习统计。",
                        "args": [
                            {"flag": "--limit", "type": "int", "default": 20},
                            {"flag": "--query", "type": "string", "default": "", "note": "按查询文本子串过滤"},
                            {"flag": "--stats", "type": "bool", "default": False, "note": "显示统计信息而非历史列表"},
                        ],
                    },
                    {
                        "name": "feedback",
                        "description": "对查询结果中的节点记录反馈，用于优化后续排序。",
                        "args": [
                            {"flag": "--query-id", "type": "string", "required": True},
                            {"flag": "--node-id", "type": "string", "required": True},
                            {"flag": "--type", "type": "enum", "choices": ["positive", "negative", "neutral"], "required": True},
                            {"flag": "--context", "type": "string", "default": ""},
                        ],
                    },
                ],
                "workflow": [
                    "【核心原则】查询是查询，生成是生成。不要用直觉代替证据；所有结论必须来自 runs/ 或 references/ 的文本。",
                    "",
                    "【单点查询】默认从小上下文开始：运行 `triage --query ...`，生成 `runs/r1-triage.md`，先读再决定。",
                    "需要穷举/精确匹配时用 `search --pattern ...`；需要定位文件时用 `files --pattern ...`。",
                    "需要更多上下文时，再逐步增加 `--neighbors/--per-node-max-chars/--limit`（不要一次拉满）。",
                    "精读原文时，优先用 `get-node` 或直接打开少量 `references/.../chunks/*.md`。",
                    "图扩展默认只走低噪边；如果结果发散，下一轮加 `--graph-depth 0`。",
                    "",
                    "【关键词辅助——推理链利器】开启 `--show-keywords` 后，每个 chunk 后面会列出高频关键词。",
                    "  - 基础用法：直接看 chunk 的 keywords，从中挑选下一跳的人名/物品/事件作为下一轮查询词。",
                    "  - 进阶用法：调高 `--keyword-count`（如 12）来发现更多线索，或结合 `--preset standard` 获取更大上下文。",
                    "  - 每轮查询时加 `--show-keywords`，让工具帮你提取关键词，不用自己从长文本中找。",
                    "",
                    "【推理链查询——关键】当问题包含多跳因果关系（A→B→C→...→答案）时，禁止一次性查询所有关键词。必须分轮迭代：",
                    "  1. 从推理链起点开始，每轮只查询 1-2 个环节的关键词。",
                    "  2. 开启 `--show-keywords`，读完 chunk 后直接从 keywords 中挑选下一跳的实体名。",
                    "  3. 用新实体名作为下一轮查询词，逐步推进到答案。",
                    "  4. 每轮使用新的 `--out` 文件名（r1, r2, r3...），保留审计轨迹。",
                    "  5. 如果某一轮没有命中，不要跳过——调整关键词重新查询，直到确认这一跳。",
                    "",
                    "【推理链示例】问题：'某角色因某物品与另一角色交好，引来第三方上门，导致某人受罚。谁上门？'",
                    "  分轮迭代（每轮用 --show-keywords）：",
                    "    R1: triage --query '角色A' --show-keywords → 看到 keywords 有 [角色B, 物品X]，确认交好物件。",
                    "    R2: triage --query '角色B 物品X' --show-keywords → 看到 keywords 有 [第三方C]，确认上门者。",
                    "    R3: triage --query '第三方C' --show-keywords → 看到 keywords 有 [角色D, 受罚]，确认受罚者。",
                    "  加速收敛：发现线索密集时，用 `--preset standard --show-keywords --keyword-count 10` 一次获取更多线索。",
                    "  最终答案：基于 references/... 原文确认各跳关系后给出。",
                ],
                "trigger_conditions": {
                    "match": [
                        "用户提问涉及知识库文档内容（概念/定义/流程/数据等）",
                        "用户需要从文档中检索证据并给出引用依据（references/...）",
                    ],
                    "do_not_match": [
                        "与文档无关的通用知识问题",
                        "创意写作等与文档无关的任务",
                    ],
                },
                "presets": {
                    "quick": "--preset quick",
                    "standard": "--preset standard",
                },
                "error_recovery": {
                    "no_hits": "尝试更宽泛关键词或使用 --query-mode or。",
                    "truncated_output": "减小 --neighbors 或增大 --max-chars。",
                },
            }
            print_json(payload)
            return 0

        fn = getattr(args, "func", None)
        if not callable(fn):
            parser.error("missing command (or use --skill)")
        return int(fn(args))
    except SystemExit:
        raise
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        if os.environ.get("KBTOOL_TRACEBACK") or os.environ.get("KBTOOL_DEBUG"):
            detail += "\n" + traceback.format_exc()
        die(detail)
