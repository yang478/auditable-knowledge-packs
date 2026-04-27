"""Binary tool resolution for ripgrep (rg) and fd.

Searches for CLI tools in order:
  1. Explicit search_paths (e.g., skill root bin/)
  2. System PATH via shutil.which
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional, Sequence


def resolve_tool_binary(name: str, *, search_paths: Sequence[Path] = ()) -> Optional[Path]:
    """Resolve a CLI tool binary.

    Args:
        name: Tool binary name (e.g., "rg", "fd").
        search_paths: Directories to search first (bundled binaries).

    Returns:
        Absolute path to the binary, or None if not found.
    """
    # 1. Check explicit search paths (bundled binaries)
    for search_dir in search_paths:
        candidate = Path(search_dir) / name
        if candidate.is_file():
            return candidate.resolve()

    # 2. Fallback to system PATH
    found = shutil.which(name)
    if found:
        return Path(found).resolve()

    return None


def parse_rg_output(stdout: str, root: Path, limit: int) -> list[dict[str, object]]:
    """Parse rg stdout into structured match dicts.

    Each match has keys: file (relative to root), line_number, line_text.
    """
    out: list[dict[str, object]] = []
    if not stdout:
        return out
    root_resolved = root.resolve()
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split(":", 2)
        if len(parts) < 3:
            continue
        file_path = parts[0]
        line_number = parts[1]
        line_text = parts[2]
        try:
            rel_path = str(Path(file_path).resolve().relative_to(root_resolved)).replace("\\", "/")
        except ValueError:
            rel_path = file_path.replace("\\", "/")
        out.append(
            {
                "file": rel_path,
                "line_number": int(line_number) if line_number.isdigit() else line_number,
                "line_text": line_text,
            }
        )
        if len(out) >= limit:
            break
    return out
