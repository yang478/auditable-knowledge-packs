from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..types import NodeRecord
from .fs import BuildError
from .text import markdown_to_plain, strip_frontmatter


def _safe_resolve_ref(base_dir: Path, node: NodeRecord) -> Path:
    """Resolve node.ref_path under base_dir, raising ValueError if it escapes."""
    path = base_dir / node.ref_path
    try:
        path.resolve().relative_to(base_dir.resolve())
    except ValueError:
        raise ValueError(
            f"ref_path escapes base_dir: {node.ref_path} (node_id={node.node_id})"
        )
    return path


def read_node_body_md_from_ref(base_dir: Path, node: NodeRecord) -> str:
    """Read a leaf node's body from its ref_path file, stripping frontmatter."""
    if not node.ref_path:
        raise BuildError(f"Leaf node missing ref_path: node_id={node.node_id}")
    path = _safe_resolve_ref(base_dir, node)
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise BuildError(f"Failed to read node ref file: {path} (node_id={node.node_id}, {type(exc).__name__}: {exc})")
    body = strip_frontmatter(raw)
    return body.rstrip() + "\n"


def leaf_haystack_plain(base_dir: Optional[Path], node: NodeRecord) -> str:
    """Return plain text haystack for a leaf node (for alias/reference extraction)."""
    if node.body_plain:
        return node.body_plain
    body_md = node.body_md
    if not body_md and base_dir is not None:
        body_md = read_node_body_md_from_ref(base_dir, node)
    if not body_md:
        return ""
    return markdown_to_plain(body_md)
