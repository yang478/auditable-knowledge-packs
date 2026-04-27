"""kbtool search — precise content search via ripgrep (rg).

Wraps rg to search within references/ directory and returns
structured JSON results.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from typing import List

from .runtime import die, escape_markdown_inline, print_json, resolve_root, write_audit_markdown
from .safe_subprocess import run_subprocess_safe
from .text import build_punctuation_tolerant_regex
from .tools import parse_rg_output, resolve_tool_binary


def cmd_search(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    pattern = str(getattr(args, "pattern", "") or "").strip()
    query = str(getattr(args, "query", "") or "").strip()
    fixed = bool(getattr(args, "fixed", False))
    mode = "pattern" if pattern else "query" if query else ""
    if not mode:
        die("Missing --pattern or --query.")

    rg_path = resolve_tool_binary("rg", search_paths=[root / "bin"])
    if rg_path is None:
        die(
            "ripgrep (rg) not found. "
            "Install rg or place the binary in <skill-root>/bin/."
        )

    limit = max(1, int(getattr(args, "limit", 50) or 50))
    search_dir = root / "references"

    rg_args = [
        str(rg_path),
        "--line-number",
        "--no-heading",
        "--with-filename",
        "--color", "never",
        "--max-count", str(limit),
    ]
    # `--query` uses fixed-string OR across whitespace-separated terms to reduce weak-model failures.
    if mode == "query":
        terms = [t for t in query.split() if t.strip()]
        if not terms:
            die("Missing --query.")
        # Avoid pathological command sizes from overly long queries.
        terms = terms[:32]
        rg_args.append("--fixed-strings")
        for term in terms:
            rg_args.extend(["-e", term])
        rg_args.extend(["--", str(search_dir)])
    else:
        rg_args.append("--fixed-strings" if fixed else "")
        rg_args.extend(["--", pattern, str(search_dir)])
    # Remove empty args
    rg_args = [a for a in rg_args if a]

    try:
        proc = run_subprocess_safe(
            rg_args,
            timeout=60,
            check=False,
            text=True,
        )
    except subprocess.TimeoutExpired:
        result = {
            "tool": "kbtool",
            "cmd": "search",
            "pattern": pattern,
            "error": f"rg timed out after 60s for pattern: {pattern}",
            "matches": [],
        }
        print_json(result)
        return 1
    except OSError as exc:
        die(f"Failed to run rg: {exc}")

    matches = parse_rg_output(proc.stdout if proc.returncode == 0 else "", root, limit)
    used_punct_fallback = False
    pattern_regex = ""

    # When fixed-string search finds no hits, try a punctuation-tolerant regex fallback.
    if mode == "pattern" and fixed and not matches:
        regex = build_punctuation_tolerant_regex(pattern)
        if regex and regex != pattern:
            rg_args2 = [
                str(rg_path),
                "--line-number",
                "--no-heading",
                "--with-filename",
                "--color",
                "never",
                "--max-count",
                str(limit),
                "--",
                regex,
                str(search_dir),
            ]
            try:
                proc2 = run_subprocess_safe(
                    rg_args2,
                    timeout=60,
                    check=False,
                    text=True,
                )
                matches2 = parse_rg_output(proc2.stdout if proc2.returncode == 0 else "", root, limit)
                if matches2:
                    matches = matches2
                    used_punct_fallback = True
                    pattern_regex = regex
            except subprocess.TimeoutExpired:
                pass

    result = {
        "tool": "kbtool",
        "cmd": "search",
        "mode": mode,
        "pattern": pattern,
        "pattern_regex": pattern_regex,
        "query": query,
        "fixed": bool(fixed),
        "punct_fallback": bool(used_punct_fallback),
        "matches": matches,
    }
    print_json(result)

    # Write audit trail file if --out is specified
    out = str(getattr(args, "out", "") or "").strip()
    if out:
        entries = [
            f"- `{m['file']}:{m['line_number']}`: {escape_markdown_inline(m['line_text'])}"
            for m in matches
        ]
        write_audit_markdown(
            root, out,
            title="Search Audit",
            header_fields=[
                ("mode", mode),
                ("pattern", f"`{pattern}`" if pattern else ""),
                ("pattern_regex", f"`{pattern_regex}`" if pattern_regex else ""),
                ("query", f"`{query}`" if query else ""),
                ("fixed", "true" if fixed else "false"),
                ("punct_fallback", "true" if used_punct_fallback else "false"),
                ("limit", str(limit)),
                ("matches", str(len(matches))),
            ],
            entries=entries,
        )
    return 0
