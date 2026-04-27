"""kbtool files — precise file search via fd.

Wraps fd to find files by name pattern within the skill root
and returns structured JSON results.
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import List

from .runtime import die, print_json, resolve_root, write_audit_markdown
from .safe_subprocess import run_subprocess_safe
from .tools import resolve_tool_binary


def cmd_files(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    pattern = str(getattr(args, "pattern", "") or "").strip()
    if not pattern:
        die("Missing --pattern.")

    fd_path = resolve_tool_binary("fd", search_paths=[root / "bin"])
    if fd_path is None:
        die(
            "fd not found. "
            "Install fd or place the binary in <skill-root>/bin/."
        )

    limit = max(1, int(getattr(args, "limit", 50) or 50))
    search_dir = root / "references"

    fd_args = [
        str(fd_path),
        "--type", "f",
        "--max-results", str(limit),
        "--color", "never",
        "--absolute-path",
        pattern,
        str(search_dir),
    ]

    try:
        proc = run_subprocess_safe(
            fd_args,
            timeout=60,
            check=False,
            text=True,
        )
    except subprocess.TimeoutExpired:
        result = {
            "tool": "kbtool",
            "cmd": "files",
            "pattern": pattern,
            "error": f"fd timed out after 60s for pattern: {pattern}",
            "files": [],
        }
        print_json(result)
        return 1
    except OSError as exc:
        die(f"Failed to run fd: {exc}")

    files: List[str] = []
    if proc.returncode == 0 and proc.stdout:
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            # Make path relative to root
            try:
                rel_path = str(
                    Path(line).resolve().relative_to(root.resolve())
                ).replace("\\", "/")
            except ValueError:
                rel_path = line.replace("\\", "/")
            files.append(rel_path)

    result = {
        "tool": "kbtool",
        "cmd": "files",
        "pattern": pattern,
        "files": files,
    }
    print_json(result)

    # Write audit trail file if --out is specified
    out = str(getattr(args, "out", "") or "").strip()
    if out:
        entries = [f"- `{f}`" for f in files]
        write_audit_markdown(
            root, out,
            title="Files Audit",
            header_fields=[
                ("pattern", f"`{pattern}`"),
                ("limit", str(limit)),
                ("files", str(len(files))),
            ],
            entries=entries,
        )
    return 0
