from __future__ import annotations

import argparse
import json
import os
import traceback
from typing import Dict, Optional, Sequence

from .catalog import cmd_categories, cmd_docs
from .reindex import cmd_reindex
from .retrieval import (
    cmd_bundle,
    cmd_follow_references,
    cmd_get_children,
    cmd_get_node,
    cmd_get_parent,
    cmd_research,
    cmd_get_siblings,
    cmd_search,
)
from .runtime import print_json, resolve_root


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="KB tool for generated skills (search, bundle).")
    p.add_argument("--root", default="", help="Skill root directory (default: auto-detect).")
    p.add_argument("--db", default="kb.sqlite", help="SQLite DB path relative to root (default: kb.sqlite).")
    p.add_argument("--skill", action="store_true", help="Print JSON tool usage for LLMs and exit.")
    sub = p.add_subparsers(dest="cmd")

    b = sub.add_parser("bundle", help="Search + expand + write a single evidence bundle markdown file.")
    b.add_argument("--query", required=True, help="User query.")
    b.add_argument("--out", default="bundle.md", help="Output markdown path (relative to root).")
    b.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    b.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    b.add_argument("--must", action="append", default=[], help="Term that must appear (repeatable).")
    b.add_argument("--iter-max-rounds", type=int, default=5, help="Iterative retrieval rounds (1-5, default: 5).")
    b.add_argument("--iter-focus-k", type=int, default=12, help="Top-K hits used for focus metrics (default: 12).")
    b.add_argument(
        "--iter-focus-max-articles",
        type=int,
        default=3,
        help="Try to converge to <= N articles (default: 3).",
    )
    b.add_argument(
        "--iter-mass-top3-threshold",
        type=float,
        default=0.8,
        help="Converged when top3 article mass >= T (0-1, default: 0.8).",
    )
    b.add_argument(
        "--timeout-ms",
        type=int,
        default=0,
        help="Abort SQLite queries if they exceed this duration in ms (0 = disabled).",
    )
    b.add_argument("--no-iter", action="store_true", help="Disable iterative retrieval (single-pass search).")
    b.add_argument("--neighbors", type=int, default=0, help="Expand to prev/next leaf nodes within same parent.")
    b.add_argument("--order", choices=["relevance", "chronological"], default="relevance", help="Output order.")
    b.add_argument("--max-chars", type=int, default=40000, help="Max output size (characters).")
    b.add_argument("--per-node-max-chars", type=int, default=6000, help="Max chars per node before truncation.")
    b.add_argument("--body", choices=["full", "snippet", "none"], default="full", help="Leaf body rendering mode.")
    b.add_argument("--debug-triggers", action="store_true", help="Emit reference-trigger diagnostics and one-hop reference expansion.")
    b.add_argument("--enable-hooks", action="store_true", help="Enable optional hooks from hooks/ directory (executes local python).")
    b.set_defaults(func=cmd_bundle)

    rr = sub.add_parser("research", help="Run ONE research round: bundle + verify + structured trace outputs (LLM-in-the-loop).")
    rr.add_argument("--query", required=True, help="User query.")
    rr.add_argument("--run-dir", default="", help="Run output directory (relative to root). If empty, a new run dir is created.")
    rr.add_argument("--round", type=int, default=0, help="Round number (0 = auto-increment based on existing bundle.roundNN.md).")
    rr.add_argument(
        "--planner-json",
        default="",
        help="Planner metadata JSON (audit). Recommended keys: model, temperature, prompt_sha256 (or prompt_path). Saved into trace.",
    )
    rr.add_argument("--note", default="", help="Optional note saved into trace (free-form text).")
    rr.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    rr.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    rr.add_argument("--must", action="append", default=[], help="Term that must appear (repeatable).")
    rr.add_argument("--iter-max-rounds", type=int, default=5, help="Iterative retrieval rounds (1-5, default: 5).")
    rr.add_argument("--iter-focus-k", type=int, default=12, help="Top-K hits used for focus metrics (default: 12).")
    rr.add_argument(
        "--iter-focus-max-articles",
        type=int,
        default=3,
        help="Try to converge to <= N articles (default: 3).",
    )
    rr.add_argument(
        "--iter-mass-top3-threshold",
        type=float,
        default=0.8,
        help="Converged when top3 article mass >= T (0-1, default: 0.8).",
    )
    rr.add_argument(
        "--timeout-ms",
        type=int,
        default=0,
        help="Abort SQLite queries if they exceed this duration in ms (0 = disabled).",
    )
    rr.add_argument("--no-iter", action="store_true", help="Disable iterative retrieval (single-pass search).")
    rr.add_argument("--neighbors", type=int, default=0, help="Expand to prev/next leaf nodes within same parent.")
    rr.add_argument("--order", choices=["relevance", "chronological"], default="relevance", help="Output order.")
    rr.add_argument("--max-chars", type=int, default=40000, help="Max output size (characters).")
    rr.add_argument("--per-node-max-chars", type=int, default=6000, help="Max chars per node before truncation.")
    rr.add_argument("--body", choices=["full", "snippet", "none"], default="full", help="Leaf body rendering mode.")
    rr.add_argument("--debug-triggers", action="store_true", help="Emit reference-trigger diagnostics and one-hop reference expansion.")
    rr.add_argument("--enable-hooks", action="store_true", help="Enable optional hooks from hooks/ directory (executes local python).")
    rr.set_defaults(func=cmd_research)

    s = sub.add_parser("search", help="Search leaf nodes and write ranked hits with snippets.")
    s.add_argument("--query", required=True, help="User query.")
    s.add_argument("--out", default="search.md", help="Output markdown path (relative to root).")
    s.add_argument("--limit", type=int, default=20, help="Max FTS candidates to consider.")
    s.add_argument("--query-mode", choices=["or", "and"], default="or", help="FTS query composition mode.")
    s.add_argument("--must", action="append", default=[], help="Term that must appear (repeatable).")
    s.add_argument("--snippet-chars", type=int, default=400, help="Max chars per hit snippet.")
    s.add_argument(
        "--timeout-ms",
        type=int,
        default=0,
        help="Abort SQLite queries if they exceed this duration in ms (0 = disabled).",
    )
    s.add_argument("--enable-hooks", action="store_true", help="Enable optional hooks from hooks/ directory (executes local python).")
    s.set_defaults(func=cmd_search)

    gn = sub.add_parser("get-node", help="Get one node as JSON.")
    gn.add_argument("node_id", nargs="?", default="", help="Node ID to fetch.")
    gn.add_argument("--node-id", dest="node_id_flag", default="", help="Node ID to fetch (same as positional).")
    gn.add_argument("--out", default="", help="Optional JSON output path (relative to root).")
    gn.set_defaults(func=cmd_get_node)

    gc = sub.add_parser("get-children", help="List children of a node as JSON.")
    gc.add_argument("node_id", help="Parent node ID.")
    gc.set_defaults(func=cmd_get_children)

    gp = sub.add_parser("get-parent", help="Get parent of a node as JSON.")
    gp.add_argument("node_id", help="Node ID.")
    gp.set_defaults(func=cmd_get_parent)

    gs = sub.add_parser("get-siblings", help="Get prev/next neighbors of a node as JSON.")
    gs.add_argument("node_id", help="Node ID.")
    gs.add_argument("--neighbors", type=int, default=1, help="How many neighbors on each side.")
    gs.set_defaults(func=cmd_get_siblings)

    fr = sub.add_parser("follow-references", help="Follow references edges as JSON.")
    fr.add_argument("node_id", help="Node ID.")
    fr.add_argument("--direction", choices=["out", "in", "both"], default="out", help="Edge direction to follow.")
    fr.set_defaults(func=cmd_follow_references)

    r = sub.add_parser("reindex", help="Rebuild kb.sqlite from references/ (after manual edits).")
    r.set_defaults(func=cmd_reindex)

    c = sub.add_parser("categories", help="List document categories derived from manifest titles.")
    c.add_argument("--out", default="categories.md", help="Output markdown path (relative to root).")
    c.set_defaults(func=cmd_categories)

    d = sub.add_parser("docs", help="List documents in manifest (filterable).")
    d.add_argument("--category", default="", help="Filter by category (derived from 'X - Y').")
    d.add_argument("--query", default="", help="Substring filter on title/source_file.")
    d.add_argument("--limit", type=int, default=200, help="Max docs to output (0 = no limit).")
    d.add_argument("--out", default="docs.md", help="Output markdown path (relative to root).")
    d.set_defaults(func=cmd_docs)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(list(argv) if argv is not None else None)
        if bool(getattr(args, "skill", False)):
            root = resolve_root(str(getattr(args, "root", "")) or "")
            manifest_path = root / "manifest.json"
            manifest: Dict[str, object] = {}
            if manifest_path.exists():
                try:
                    data = json.loads(manifest_path.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        manifest = data
                except (OSError, json.JSONDecodeError, ValueError):
                    manifest = {}

            skill_name = str(manifest.get("skill_name") or root.name)
            title = str(manifest.get("title") or "Document Knowledge Base")
            docs = manifest.get("docs")
            doc_count = len(docs) if isinstance(docs, list) else 0

            payload: Dict[str, object] = {
                "tool": "kbtool",
                "deterministic": True,
                "skill": {"name": skill_name, "title": title, "docs": doc_count},
                "contracts": {
                    "answering": [
                        "Evidence-only: only cite clauses/equations/quotes that appear in bundle.roundNN.md.",
                        "Never invent formulas/numbers from memory. If an equation is missing/blank, cite only its number (e.g. (6.xx)) and state extraction is missing.",
                        "Include the auto-generated '## 参考依据' section at the end of your answer.",
                    ],
                    "planner_json": {
                        "minimum": {
                            "model": "string",
                            "temperature": "number",
                            "prompt_sha256": "sha256 hex (or use prompt_path)",
                        },
                        "prompt_path": "Optional: path to a text file containing the planner prompt; kbtool will copy+hash it into run_dir.",
                    },
                },
                "commands": [
                    {
                        "name": "research",
                        "description": "Run ONE round: bundle + verify + write structured trace for iterative deep research (LLM decides next round).",
                        "options": [
                            {"flag": "--run-dir", "type": "string", "default": "", "note": "Empty = create new run dir"},
                            {"flag": "--round", "type": "int", "default": 0, "note": "0 = auto-increment"},
                            {
                                "flag": "--planner-json",
                                "type": "string",
                                "default": "",
                                "note": "Planner metadata JSON (audit). Include at least: {model, temperature, prompt_sha256 or prompt_path}.",
                            },
                            {"flag": "--note", "type": "string", "default": ""},
                            {"flag": "--query-mode", "type": "enum", "choices": ["or", "and"], "default": "or"},
                            {
                                "flag": "--must",
                                "type": "string",
                                "default": [],
                                "repeatable": True,
                                "note": "Usually 'text must appear'. Doc-code-like terms (e.g. 1993-1-1) are treated as doc hints when they match a doc_id.",
                            },
                            {"flag": "--limit", "type": "int", "default": 20},
                            {"flag": "--iter-max-rounds", "type": "int", "default": 5, "range": [1, 5]},
                            {"flag": "--iter-focus-max-articles", "type": "int", "default": 3},
                            {"flag": "--iter-mass-top3-threshold", "type": "float", "default": 0.8, "range": [0.0, 1.0]},
                            {"flag": "--timeout-ms", "type": "int", "default": 0, "note": "0 = disabled"},
                            {"flag": "--no-iter", "type": "bool", "default": False},
                            {"flag": "--neighbors", "type": "int", "default": 0},
                            {"flag": "--order", "type": "enum", "choices": ["relevance", "chronological"], "default": "relevance"},
                            {"flag": "--max-chars", "type": "int", "default": 40000},
                            {"flag": "--per-node-max-chars", "type": "int", "default": 6000},
                            {"flag": "--body", "type": "enum", "choices": ["full", "snippet", "none"], "default": "full"},
                            {"flag": "--debug-triggers", "type": "bool", "default": False},
                            {"flag": "--enable-hooks", "type": "bool", "default": False},
                        ],
                        "outputs": [
                            "Writes run_dir/bundle.roundNN.md",
                            "Writes run_dir/trace.roundNN.json",
                            "Writes run_dir/verify.roundNN.json",
                            "Appends run_dir/trace.jsonl",
                            "Prints a JSON summary to stdout",
                        ],
                    },
                    {
                        "name": "search",
                        "description": "Search leaf nodes and write ranked hits markdown.",
                        "options": [
                            {"flag": "--query", "type": "string", "required": True},
                            {"flag": "--out", "type": "string", "default": "search.md"},
                            {"flag": "--query-mode", "type": "enum", "choices": ["or", "and"], "default": "or"},
                            {
                                "flag": "--must",
                                "type": "string",
                                "default": [],
                                "repeatable": True,
                                "note": "Usually 'text must appear'. Doc-code-like terms (e.g. 1993-1-1) are treated as doc hints when they match a doc_id.",
                            },
                            {"flag": "--limit", "type": "int", "default": 20},
                            {"flag": "--snippet-chars", "type": "int", "default": 280},
                            {"flag": "--timeout-ms", "type": "int", "default": 0, "note": "0 = disabled"},
                            {"flag": "--enable-hooks", "type": "bool", "default": False},
                        ],
                    },
                    {"name": "get-node", "description": "Fetch a node as JSON (includes body_md)."},
                    {"name": "get-children", "description": "List children of a node as JSON."},
                    {"name": "get-parent", "description": "Fetch parent node as JSON."},
                    {"name": "get-siblings", "description": "Fetch prev/next neighbors as JSON."},
                    {"name": "follow-references", "description": "Follow references edges as JSON."},
                    {"name": "categories", "description": "List document categories to markdown."},
                    {"name": "docs", "description": "List documents to markdown."},
                    {"name": "reindex", "description": "Rebuild kb.sqlite from references/."},
                ],
                "workflow": [
                    "Prefer `research --query ...` to generate auditable evidence + verify + trace (one round).",
                    "If verify fails, revise query/params and run the NEXT `research` round (same `--run-dir`, omit `--round` so it auto-increments).",
                    "Answer strictly from the generated bundle.roundNN.md and append its `## 参考依据` section.",
                ],
                "security": {
                    "hooks": {
                        "opt_in_flag": "--enable-hooks",
                        "path": "hooks/",
                        "note": "Hooks execute local python and may reduce determinism unless controlled.",
                    }
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
        from .runtime import die

        die(detail)
