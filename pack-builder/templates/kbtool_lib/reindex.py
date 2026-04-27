from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from . import artifact_contract, state_contract
from .canonical_text import canonical_text_sha256, normalize_canonical_text
from .fingerprint_core import sha256_bytes as _sha256_bytes, sha256_text as _sha256_text
from .runtime import die, resolve_db_path, resolve_root
from .safe_sqlite import enable_wal

logger = logging.getLogger(__name__)
from .text import (
    core_alias_title,
    derive_source_version,
    extract_keywords,
    fts_tokens,
    markdown_to_plain,
    node_key,
    normalize_alias_text,
    normalize_article_ref,
    parse_frontmatter,
    stable_hash,
    strip_frontmatter,
)


@dataclass
class NodeRow:
    node_id: str
    doc_id: str
    kind: str
    label: str
    title: str
    parent_id: str | None
    prev_id: str | None
    next_id: str | None
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
        if self.raw_span_end == 0:
            self.raw_span_end = len(self.body_md)
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
    active_parser: str = ""
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
class RefreshResult:
    dirty_doc_ids: Tuple[str, ...]
    refreshed_indexes: Tuple[str, ...]
    rewritten_rows: int
    full_rewrite_rows: int
    uses_atomic_activation: bool = True


def _parse_bool_robust(value: object, default: bool = True) -> bool:
    """Parse boolean from various frontmatter formats."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("true", "yes", "1"):
        return True
    if s in ("false", "no", "0", ""):
        return False
    return default


def _safe_canonical_version(source_version: str) -> str:
    value = re.sub(r"[^0-9A-Za-z._-]+", "-", str(source_version or "current")).strip("-")
    return value or "current"


def canonical_text_rel_path(doc_id: str, source_version: str) -> str:
    return f"canonical_text/{doc_id}--{_safe_canonical_version(source_version)}.txt"


def _safe_join_under(root: Path, rel_path: str) -> Path:
    root_resolved = root.resolve()
    path = (root / rel_path).resolve()
    try:
        path.relative_to(root_resolved)
    except ValueError:
        die(f"Invalid ref_path outside skill root: {rel_path!r}")
    return path


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
PHASE_A_ARTIFACT_EXPORT = artifact_contract.PHASE_A_ARTIFACT_EXPORT
BUILD_STATE_FILENAME = state_contract.BUILD_STATE_FILENAME
ARTIFACT_VERSION = "kbtool.artifact.v1"


def extract_alias_rows(nodes: Sequence[NodeRow]) -> List[AliasRow]:
    rows: set[AliasRow] = set()
    for node in nodes:
        if not node.is_leaf:
            continue
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
                        alias_level=ALIAS_SOFT,
                        confidence=0.85,
                        source="frontmatter",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )

    return sorted(
        rows,
        key=lambda row: (
            row.doc_id,
            row.source_version,
            row.normalized_alias,
            row.target_node_id,
            0 if row.is_active else 1,
        ),
    )


def extract_reference_edges(nodes: Sequence[NodeRow]) -> List[EdgeRow]:
    article_targets = {
        (node.doc_id, normalize_article_ref(node.label)): node.node_id for node in nodes if node.kind == "article"
    }
    edges: set[EdgeRow] = set()
    for node in nodes:
        if not node.is_leaf:
            continue
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


def parse_doc_metadata(doc_dir: Path) -> Tuple[str, str, str, str, str, str]:
    """
    Best-effort doc_title + source_file from references/<doc_id>/metadata.md.
    """
    md_path = doc_dir / "metadata.md"
    if not md_path.exists():
        title = doc_dir.name
        return title, "(unknown)", str(doc_dir), derive_source_version(doc_dir.name, title), hash_doc_dir(doc_dir), ""
    md = md_path.read_text(encoding="utf-8", errors="replace")
    title = doc_dir.name
    for line in md.splitlines():
        if line.startswith("# "):
            title = line[2:].strip() or title
            break
    m = re.search(r"源文件：`([^`]+)`", md)
    source_path_match = re.search(r"源路径：`([^`]+)`", md)
    source_file = m.group(1) if m else "(unknown)"
    version_match = re.search(r"版本：`([^`]+)`", md)
    doc_hash_match = re.search(r"文档哈希：`([^`]+)`", md)
    parser_match = re.search(r"解析器：`([^`]+)`", md)
    source_path = source_path_match.group(1) if source_path_match else str(doc_dir)
    source_version = version_match.group(1) if version_match else derive_source_version(doc_dir.name, title)
    doc_hash = doc_hash_match.group(1) if doc_hash_match else hash_doc_dir(doc_dir)
    active_parser = parser_match.group(1) if parser_match else ""
    return title, source_file, source_path, source_version, doc_hash, active_parser


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
    refs_dir = root / "references"
    if not refs_dir.exists():
        die(f"Missing references/: {refs_dir}")
    docs: List[DocRow] = []
    nodes: List[NodeRow] = []

    # Load documents
    for doc_dir in sorted((p for p in refs_dir.iterdir() if p.is_dir()), key=lambda p: p.name):
        doc_id = doc_dir.name
        doc_title, source_file, source_path, source_version, doc_hash, active_parser = parse_doc_metadata(doc_dir)
        docs.append(
            DocRow(
                doc_id=doc_id,
                doc_title=doc_title,
                source_file=source_file,
                source_path=source_path,
                source_version=source_version,
                doc_hash=doc_hash,
                active_parser=active_parser,
                is_active=True,
            )
        )

        doc_nodes: List[NodeRow] = []

        def rel_to_root(path: Path) -> str:
            return str(path.relative_to(root)).replace("\\", "/")

        def add_kb_node(md: Path) -> None:
            fm, body = read_md_with_frontmatter(md)
            kind = str(fm.get("kind") or "").strip() or "section"
            label = str(fm.get("label") or "").strip() or md.stem
            title = str(fm.get("title") or "").strip() or label
            parent_id = str(fm.get("parent_id") or "").strip() or None
            node_id = str(fm.get("node_id") or "").strip() or f"{doc_id}:{kind}:{md.stem}"
            ordinal = parse_int_suffix(md.stem, default=0)
            is_leaf = _parse_bool_robust(fm.get("is_leaf"), default=True)
            aliases = parse_aliases_field(fm.get("aliases", ""))
            doc_nodes.append(
                NodeRow(
                    node_id=node_id,
                    doc_id=doc_id,
                    kind=kind,
                    label=label,
                    title=title,
                    parent_id=parent_id,
                    prev_id=None,
                    next_id=None,
                    ordinal=ordinal,
                    ref_path=rel_to_root(md),
                    is_leaf=is_leaf,
                    body_md=body,
                    body_plain=markdown_to_plain(body),
                    source_version=source_version,
                    is_active=True,
                    aliases=aliases,
                    raw_span_start=0,
                    raw_span_end=len(body),
                    confidence=1.0,
                )
            )

        # Chapters: references/<doc_id>/chapters/chapter01.md
        chapters_dir = doc_dir / "chapters"
        if chapters_dir.exists():
            for md in sorted((p for p in chapters_dir.glob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                fm, body = read_md_with_frontmatter(md)
                chapter_id = md.stem
                title = str(fm.get("chapter_title") or "").strip() or chapter_id
                node_id = f"{doc_id}:chapter:{chapter_id}"
                doc_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="chapter",
                        label=chapter_id,
                        title=title,
                        parent_id=None,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(chapter_id, default=0),
                        ref_path=rel_to_root(md),
                        is_leaf=True,
                        body_md=body,
                        body_plain=markdown_to_plain(body),
                        source_version=source_version,
                        is_active=True,
                        aliases=(),
                        raw_span_start=0,
                        raw_span_end=len(body),
                        confidence=1.0,
                    )
                )

        # Sections: references/<doc_id>/sections/<chapter_id>/section-01-01.md
        sections_root = doc_dir / "sections"
        if sections_root.exists():
            for md in sorted((p for p in sections_root.rglob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                fm, body = read_md_with_frontmatter(md)
                chapter_id = md.parent.name
                section_id = md.stem
                title = str(fm.get("section_title") or "").strip() or section_id
                node_id = f"{doc_id}:section:{chapter_id}/{section_id}"
                parent_id = f"{doc_id}:chapter:{chapter_id}"
                doc_nodes.append(
                    NodeRow(
                        node_id=node_id,
                        doc_id=doc_id,
                        kind="section",
                        label=section_id,
                        title=title,
                        parent_id=parent_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=parse_int_suffix(section_id, default=0),
                        ref_path=rel_to_root(md),
                        is_leaf=True,
                        body_md=body,
                        body_plain=markdown_to_plain(body),
                        source_version=source_version,
                        is_active=True,
                        aliases=(),
                        raw_span_start=0,
                        raw_span_end=len(body),
                        confidence=1.0,
                    )
                )

        # Leaf nodes: articles/items/blocks are written with kb-node frontmatter.
        for sub in ("articles", "blocks", "items"):
            base = doc_dir / sub
            if not base.exists():
                continue
            for md in sorted((p for p in base.rglob("*.md") if p.is_file()), key=lambda p: p.as_posix()):
                add_kb_node(md)

        # Chapters/sections are navigation nodes: mark as non-leaf if they own children.
        has_child: set[str] = {n.parent_id for n in doc_nodes if n.parent_id}  # type: ignore[arg-type]
        for n in doc_nodes:
            if n.kind in {"chapter", "section"} and n.node_id in has_child:
                n.is_leaf = False
        for n in doc_nodes:
            if not n.is_leaf:
                n.body_md = ""
                n.body_plain = ""
                n.raw_span_start = 0
                n.raw_span_end = 0
                n.node_hash = stable_hash("")

        # Rebuild stable prev/next links for siblings when absent.
        by_group: Dict[Tuple[str, Optional[str], str], List[NodeRow]] = {}
        for n in doc_nodes:
            by_group.setdefault((n.doc_id, n.parent_id, n.kind), []).append(n)
        for siblings in by_group.values():
            siblings.sort(key=lambda x: (x.ordinal, x.node_id))
            for idx, cur in enumerate(siblings):
                if cur.prev_id is None and idx > 0:
                    cur.prev_id = siblings[idx - 1].node_id
                if cur.next_id is None and idx + 1 < len(siblings):
                    cur.next_id = siblings[idx + 1].node_id

        nodes.extend(doc_nodes)

    return docs, nodes


def _load_existing_source_paths(root: Path) -> Dict[Tuple[str, str], str]:
    out: Dict[Tuple[str, str], str] = {}
    for manifest_name in ("corpus_manifest.json", "manifest.json"):
        manifest_path = root / manifest_name
        if not manifest_path.exists():
            continue
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        docs = data.get("docs")
        if not isinstance(docs, list):
            continue
        for row in docs:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("doc_id") or "").strip()
            source_version = str(row.get("source_version") or "current").strip() or "current"
            source_path = str(row.get("source_path") or "").strip()
            if not doc_id or not source_path:
                continue
            out[(doc_id, source_version)] = source_path
            out.setdefault((doc_id, ""), source_path)
    return out


def _load_existing_corpus_title(root: Path) -> str:
    for manifest_name in ("corpus_manifest.json", "manifest.json"):
        manifest_path = root / manifest_name
        if not manifest_path.exists():
            continue
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        title = str(data.get("title") or "").strip()
        if title:
            return title
    return root.name


def _document_row_footprint(doc_state: Dict[str, object]) -> int:
    span_fingerprints = doc_state.get("span_fingerprints", {})
    node_fingerprints = doc_state.get("node_fingerprints", {})
    span_count = len(span_fingerprints) if isinstance(span_fingerprints, dict) else 0
    node_count = len(node_fingerprints) if isinstance(node_fingerprints, dict) else 0
    return max(1, 1 + span_count + node_count)


def _summarize_incremental_refresh(
    previous_state: Dict[str, object], current_state: Dict[str, object]
) -> RefreshResult:
    previous_docs = previous_state.get("documents", {})
    current_docs = current_state.get("documents", {})
    previous_map = previous_docs if isinstance(previous_docs, dict) else {}
    current_map = current_docs if isinstance(current_docs, dict) else {}

    dirty_doc_ids: List[str] = []
    for doc_id in sorted(set(previous_map.keys()) | set(current_map.keys())):
        if previous_map.get(doc_id) != current_map.get(doc_id):
            dirty_doc_ids.append(str(doc_id))

    full_rewrite_rows = sum(_document_row_footprint(doc) for doc in previous_map.values() if isinstance(doc, dict))
    if not full_rewrite_rows:
        full_rewrite_rows = sum(_document_row_footprint(doc) for doc in current_map.values() if isinstance(doc, dict))
    full_rewrite_rows = max(1, full_rewrite_rows)

    rewritten_rows = sum(_document_row_footprint(current_map.get(doc_id, {})) for doc_id in dirty_doc_ids)
    if dirty_doc_ids:
        rewritten_rows = max(1, rewritten_rows)

    refreshed_indexes: List[str] = []
    if dirty_doc_ids:
        refreshed_indexes.extend(["sqlite", "fts", "aliases", "edges"])

    return RefreshResult(
        dirty_doc_ids=tuple(dirty_doc_ids),
        refreshed_indexes=tuple(refreshed_indexes),
        rewritten_rows=rewritten_rows,
        full_rewrite_rows=full_rewrite_rows,
        uses_atomic_activation=True,
    )


def _load_existing_corpus_docs(root: Path) -> Dict[Tuple[str, str], Dict[str, object]]:
    manifest_path = root / "corpus_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}

    docs = data.get("docs")
    if not isinstance(docs, list):
        return {}

    out: Dict[Tuple[str, str], Dict[str, object]] = {}
    for row in docs:
        if not isinstance(row, dict):
            continue
        doc_id = str(row.get("doc_id") or "").strip()
        source_version = str(row.get("source_version") or "current").strip() or "current"
        if not doc_id:
            continue
        out[(doc_id, source_version)] = {
            "title": str(row.get("title") or ""),
            "source_file": str(row.get("source_file") or ""),
            "source_path": str(row.get("source_path") or ""),
            "doc_hash": str(row.get("doc_hash") or ""),
            "active_version": bool(row.get("active_version", row.get("is_active", True))),
            "canonical_text_path": str(row.get("canonical_text_path") or ""),
            "canonical_text_sha256": str(row.get("canonical_text_sha256") or ""),
        }
    return out


def _read_json(path: Path) -> Dict[str, object]:
    return state_contract.read_json(path)


def _read_ref_body(root: Path, rel_path: str) -> str:
    path = _safe_join_under(root, rel_path)
    md = path.read_text(encoding="utf-8", errors="replace")
    body = strip_frontmatter(md)
    return body.strip() + "\n" if body.strip() else ""


def _node_body_md(root: Path, node: NodeRow) -> str:
    body = node.body_md
    if not body and node.ref_path:
        body = _read_ref_body(root, node.ref_path)
    return body.strip() + "\n" if body.strip() else ""


def _first_nonempty_line(text: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _container_prefix(container_body: str, first_child_body: str) -> str:
    container = container_body.strip()
    child = first_child_body.strip()
    if not container or not child:
        return ""

    idx = container.find(child)
    if idx >= 0:
        prefix = container[:idx].strip()
        return prefix

    child_anchor = _first_nonempty_line(child)
    if not child_anchor:
        return ""
    idx = container.find(child_anchor)
    if idx >= 0:
        prefix = container[:idx].strip()
        return prefix
    return ""


def _sort_nodes(nodes: Sequence[NodeRow]) -> List[NodeRow]:
    return sorted(nodes, key=lambda node: (node.ordinal, node.ref_path, node.node_id))


def _render_canonical_node(
    root: Path,
    node: NodeRow,
    *,
    children_by_parent: Dict[str, List[NodeRow]],
) -> str:
    """Render a canonical node and its children using explicit stack (iterative DFS).

    Avoids recursion depth limits for deeply nested documents (e.g. legal texts).
    """
    direct_children = [child for child in children_by_parent.get(node.node_id, []) if child.kind != "item"]
    if not direct_children:
        return _node_body_md(root, node).strip()

    # Stack holds (node, state) where state is False=enter, True=exit(after children)
    stack: List[tuple[NodeRow, bool]] = [(node, False)]
    # Map node_id -> list of rendered parts for that node
    rendered_map: Dict[str, List[str]] = {}

    while stack:
        cur, processed = stack.pop()
        if processed:
            # Assemble this node's rendered text from its children
            cur_children = [c for c in children_by_parent.get(cur.node_id, []) if c.kind != "item"]
            parts: List[str] = []
            container_body = _node_body_md(root, cur)
            if cur_children:
                first_child_body = _node_body_md(root, cur_children[0])
                prefix = _container_prefix(container_body, first_child_body)
                if prefix:
                    parts.append(prefix)
                for child in cur_children:
                    child_rendered = rendered_map.pop(child.node_id, [])
                    text = "\n\n".join(child_rendered).strip()
                    if text:
                        parts.append(text)
            else:
                body = container_body.strip()
                if body:
                    parts.append(body)
            rendered_map[cur.node_id] = parts
            continue

        # Push exit marker, then children (reversed so first child processed first)
        stack.append((cur, True))
        cur_children = [c for c in children_by_parent.get(cur.node_id, []) if c.kind != "item"]
        for child in reversed(cur_children):
            stack.append((child, False))

    return "\n\n".join(rendered_map.get(node.node_id, [])).strip()


def _canonical_text_from_doc_nodes(
    root: Path,
    doc: DocRow,
    nodes: Sequence[NodeRow],
    *,
    include_inactive: bool,
) -> str:
    doc_nodes = _sort_nodes(
        [
            node
            for node in nodes
            if node.doc_id == doc.doc_id
            and node.source_version == doc.source_version
            and (include_inactive or node.is_active)
        ]
    )
    if not doc_nodes:
        return normalize_canonical_text("")

    children_by_parent: Dict[str, List[NodeRow]] = {}
    top_level: List[NodeRow] = []
    for node in doc_nodes:
        if node.parent_id:
            children_by_parent.setdefault(node.parent_id, []).append(node)
        else:
            top_level.append(node)
    for parent_id, siblings in children_by_parent.items():
        children_by_parent[parent_id] = _sort_nodes(siblings)

    rendered_sections: List[str] = []
    for node in _sort_nodes(top_level):
        if node.kind == "item":
            continue
        rendered = _render_canonical_node(root, node, children_by_parent=children_by_parent).strip()
        if rendered:
            rendered_sections.append(rendered)
    return normalize_canonical_text("\n\n".join(rendered_sections))


def _load_existing_canonical_text(root: Path, row: Dict[str, object]) -> str | None:
    rel_path = str(row.get("canonical_text_path") or "").strip()
    if not rel_path:
        return None
    return _load_canonical_text_from_rel_path(root, rel_path)


def _load_canonical_text_from_rel_path(root: Path, rel_path: str) -> str | None:
    if not rel_path:
        return None
    path = root / rel_path
    if not path.exists():
        return None
    return normalize_canonical_text(path.read_text(encoding="utf-8"))


def write_corpus_manifest(root: Path, docs: Sequence[DocRow], nodes: Sequence[NodeRow]) -> Path:
    source_paths = _load_existing_source_paths(root)
    existing_docs = _load_existing_corpus_docs(root)
    payload_docs = []
    for doc in sorted(docs, key=lambda item: (item.doc_id, item.source_version, 0 if item.is_active else 1)):
        existing_doc = existing_docs.get((doc.doc_id, doc.source_version), {})
        if doc.is_active:
            canonical_text = _canonical_text_from_doc_nodes(root, doc, nodes, include_inactive=False)
        else:
            canonical_text = _load_existing_canonical_text(root, existing_doc)
            if canonical_text is None:
                canonical_text = _load_canonical_text_from_rel_path(
                    root, canonical_text_rel_path(doc.doc_id, doc.source_version)
                )
            if canonical_text is None:
                canonical_text = _canonical_text_from_doc_nodes(root, doc, nodes, include_inactive=True)
        if canonical_text == normalize_canonical_text("") and existing_doc:
            preserved = _load_existing_canonical_text(root, existing_doc)
            if preserved is not None:
                canonical_text = preserved
        rel_path = canonical_text_rel_path(doc.doc_id, doc.source_version)
        out_path = root / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(canonical_text, encoding="utf-8", newline="\n")
        source_path = (
            source_paths.get((doc.doc_id, doc.source_version)) or source_paths.get((doc.doc_id, "")) or doc.source_path
        )
        payload_docs.append(
            {
                "doc_id": doc.doc_id,
                "title": doc.doc_title,
                "source_file": doc.source_file,
                "source_path": source_path,
                "doc_hash": doc.doc_hash,
                "source_version": doc.source_version,
                "active_version": bool(doc.is_active),
                "canonical_text_path": rel_path,
                "canonical_text_sha256": canonical_text_sha256(canonical_text),
            }
        )

    payload = {
        "title": _load_existing_corpus_title(root),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "docs": payload_docs,
    }
    out_path = root / "corpus_manifest.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return out_path


def write_phase_a_artifact_export(
    root: Path,
    docs: Sequence[DocRow],
    nodes: Sequence[NodeRow],
    edges: Sequence[EdgeRow],
    aliases: Sequence[AliasRow],
) -> Path:
    return artifact_contract.write_phase_a_artifact_export(
        root,
        docs=docs,
        nodes=nodes,
        edges=edges,
        aliases=aliases,
    )


def _stable_payload(value: object) -> str:
    return state_contract.stable_payload(value)


def _default_model_registry_json() -> str:
    return json.dumps(
        {
            "components": {},
            "reranker": {
                "version": "",
                "fallback": "rules_only",
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _iter_block_ranges(canonical_text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    block_start: int | None = None
    block_end = 0
    cursor = 0

    for line in canonical_text.splitlines(keepends=True):
        line_start = cursor
        cursor += len(line)
        if line.strip():
            if block_start is None:
                block_start = line_start
            block_end = cursor
            continue
        if block_start is not None:
            ranges.append((block_start, block_end))
            block_start = None

    if block_start is not None:
        ranges.append((block_start, block_end))
    return ranges


# NOTE: The fingerprint functions below (_span_fingerprints, _node_fingerprint,
# _alias_fingerprint, _edge_fingerprint) are duplicated in:
#   pack-builder/scripts/build_skill_lib/fingerprint_utils.py
# This file is a runtime template and must remain self-contained, so we do not
# import from the build-time module. Keep both implementations in sync manually.


def _span_fingerprints(doc_id: str, canonical_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for start, end in _iter_block_ranges(canonical_text):
        span_id = _sha256_text(_stable_payload([doc_id, start, end]))[:16]
        out[span_id] = _sha256_text(canonical_text[start:end])
    return out


def _node_fingerprint(node: NodeRow) -> str:
    return _sha256_text(
        _stable_payload(
            {
                "node_id": node.node_id,
                "kind": node.kind,
                "label": node.label,
                "title": node.title,
                "parent_id": node.parent_id,
                "prev_id": node.prev_id,
                "next_id": node.next_id,
                "ordinal": node.ordinal,
                "ref_path": node.ref_path,
                "is_leaf": node.is_leaf,
                "aliases": list(node.aliases),
                "raw_span_start": node.raw_span_start,
                "raw_span_end": node.raw_span_end,
                "node_hash": node.node_hash,
            }
        )
    )


def _alias_fingerprint(alias: AliasRow) -> str:
    return _sha256_text(
        _stable_payload(
            {
                "normalized_alias": alias.normalized_alias,
                "target_node_id": alias.target_node_id,
                "alias_level": alias.alias_level,
                "confidence": alias.confidence,
                "source": alias.source,
            }
        )
    )


def _edge_fingerprint(edge: EdgeRow) -> str:
    return _sha256_text(
        _stable_payload(
            {
                "edge_type": edge.edge_type,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "confidence": edge.confidence,
            }
        )
    )


def _infer_active_parser(nodes: Sequence[NodeRow]) -> str:
    kinds = {node.kind for node in nodes if node.is_active}
    if {"clause", "table", "figure"} & kinds:
        return "outline"
    if "chapter" in kinds or "section" in kinds:
        return "markdown_headings"
    if "block" in kinds:
        return "block_fallback"
    return ""


def _index_binding_payload(name: str, rows: Sequence[object]) -> dict[str, str]:
    return state_contract.index_binding_payload(name, rows)


def _export_sha_by_doc(root: Path) -> dict[tuple[str, str], str]:
    return state_contract.export_sha_by_doc(_read_json(root / PHASE_A_ARTIFACT_EXPORT))


def write_build_state(
    root: Path,
    docs: Sequence[DocRow],
    nodes: Sequence[NodeRow],
    edges: Sequence[EdgeRow],
    aliases: Sequence[AliasRow],
) -> Path:
    manifest_rows = _load_existing_corpus_docs(root)
    previous_state = _read_json(root / "build_state.json")
    previous_documents = previous_state.get("documents")
    if not isinstance(previous_documents, dict):
        previous_documents = {}
    export_sha_by_doc = _export_sha_by_doc(root)
    manifest_path = root / "corpus_manifest.json"
    state = state_contract.empty_build_state()
    state["artifact_version"] = ARTIFACT_VERSION
    state["created_at"] = datetime.now(timezone.utc).isoformat()
    state["corpus_manifest_sha256"] = (
        _sha256_text(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else ""
    )
    state["documents"] = {}
    state["indexes"] = {}
    state["model_registry_sha256"] = _sha256_text(_default_model_registry_json())

    for doc in sorted((row for row in docs if row.is_active), key=lambda item: (item.doc_id, item.source_version)):
        key = (doc.doc_id, doc.source_version)
        manifest_row = manifest_rows.get(key, {})
        previous_doc = previous_documents.get(doc.doc_id)
        if not isinstance(previous_doc, dict):
            previous_doc = {}
        canonical_text = _load_existing_canonical_text(root, manifest_row) or ""
        doc_nodes = [
            node
            for node in nodes
            if node.doc_id == doc.doc_id and node.source_version == doc.source_version and node.is_active
        ]
        doc_aliases = [
            alias
            for alias in aliases
            if alias.doc_id == doc.doc_id and alias.source_version == doc.source_version and alias.is_active
        ]
        doc_edges = [
            edge
            for edge in edges
            if edge.doc_id == doc.doc_id and edge.source_version == doc.source_version and edge.is_active
        ]
        source_path_value = str(manifest_row.get("source_path") or previous_doc.get("source_path") or doc.source_path)
        source_path = Path(source_path_value)
        try:
            source_fingerprint = _sha256_bytes(source_path.read_bytes())
        except OSError:
            source_fingerprint = str(previous_doc.get("source_fingerprint") or "") or _sha256_text(doc.doc_hash)
        state["documents"][doc.doc_id] = {
            "source_path": source_path_value,
            "source_fingerprint": source_fingerprint,
            "extracted_text_fingerprint": _sha256_text(canonical_text),
            "span_fingerprints": _span_fingerprints(doc.doc_id, canonical_text),
            "node_fingerprints": {node.node_id: _node_fingerprint(node) for node in doc_nodes},
            "alias_fingerprints": {
                f"{alias.normalized_alias}|{alias.target_node_id}|{alias.alias_level}": _alias_fingerprint(alias)
                for alias in doc_aliases
            },
            "edge_fingerprints": {
                f"{edge.edge_type}|{edge.from_node_id}|{edge.to_node_id}": _edge_fingerprint(edge) for edge in doc_edges
            },
            "active_parser": str(doc.active_parser or "") or _infer_active_parser(doc_nodes),
            "export_sha256": export_sha_by_doc.get(key, ""),
        }

    active_node_rows = [
        {
            "doc_id": node.doc_id,
            "source_version": node.source_version,
            "node_id": node.node_id,
            "node_hash": node.node_hash,
            "is_leaf": node.is_leaf,
        }
        for node in nodes
        if node.is_active
    ]
    active_leaf_rows = [
        {
            "doc_id": node.doc_id,
            "source_version": node.source_version,
            "node_id": node.node_id,
            "node_hash": node.node_hash,
        }
        for node in nodes
        if node.is_active and node.is_leaf
    ]
    active_alias_rows = [
        {
            "doc_id": alias.doc_id,
            "source_version": alias.source_version,
            "normalized_alias": alias.normalized_alias,
            "target_node_id": alias.target_node_id,
            "alias_level": alias.alias_level,
        }
        for alias in aliases
        if alias.is_active
    ]
    active_edge_rows = [
        {
            "doc_id": edge.doc_id,
            "source_version": edge.source_version,
            "edge_type": edge.edge_type,
            "from_node_id": edge.from_node_id,
            "to_node_id": edge.to_node_id,
        }
        for edge in edges
        if edge.is_active
    ]
    state["indexes"] = {
        "sqlite": _index_binding_payload("sqlite", active_node_rows),
        "fts": _index_binding_payload("fts", active_leaf_rows),
        "aliases": _index_binding_payload("aliases", active_alias_rows),
        "edges": _index_binding_payload("edges", active_edge_rows),
    }

    out_path = root / BUILD_STATE_FILENAME
    out_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return out_path


def write_kb_sqlite_db(
    db_path: Path,
    docs: Sequence[DocRow],
    nodes: Sequence[NodeRow],
    edges: Sequence[EdgeRow],
    aliases: Sequence[AliasRow],
) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    try:
        enable_wal(conn)
        conn.execute("PRAGMA temp_store = MEMORY")

        conn.executescript(
            """
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
              keywords TEXT NOT NULL DEFAULT '',
              FOREIGN KEY (node_key) REFERENCES nodes(node_key)
            );
            """
        )

        doc_rows = [
            (d.doc_id, d.doc_title, d.source_file, d.source_path,
             d.doc_hash, d.source_version, 1 if d.is_active else 0)
            for d in docs
        ]
        conn.executemany(
            "INSERT INTO docs(doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active)"
            " VALUES (?,?,?,?,?,?,?)",
            doc_rows,
        )

        node_rows = [
            (n.node_key, n.node_id, n.doc_id, n.source_version,
             1 if n.is_active else 0, n.kind, n.label, n.title,
             n.parent_id, n.prev_id, n.next_id, n.ordinal,
             n.ref_path, 1 if n.is_leaf else 0,
             n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence)
            for n in nodes
        ]
        conn.executemany(
            "INSERT INTO nodes("
            "  node_key, node_id, doc_id, source_version, is_active, kind, label, title,"
            "  parent_id, prev_id, next_id, ordinal, ref_path, is_leaf,"
            "  raw_span_start, raw_span_end, node_hash, confidence"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            node_rows,
        )
        node_text_rows = [
            (n.node_key, n.body_md, n.body_plain,
             " ".join(extract_keywords(n.body_plain, top_k=12)))
            for n in nodes
        ]
        conn.executemany(
            "INSERT INTO node_text(node_key, body_md, body_plain, keywords) VALUES (?,?,?,?)",
            node_text_rows,
        )

        edge_rows = [
            (e.doc_id, e.edge_type, e.from_node_id, e.to_node_id,
             e.source_version, 1 if e.is_active else 0, e.confidence)
            for e in edges
        ]
        conn.executemany(
            "INSERT INTO edges(doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence)"
            " VALUES (?,?,?,?,?,?,?)",
            edge_rows,
        )

        alias_rows = [
            (a.doc_id, a.alias, a.normalized_alias, a.target_node_id,
             a.alias_level, a.source_version, 1 if a.is_active else 0,
             a.confidence, a.source)
            for a in aliases
        ]
        conn.executemany(
            "INSERT INTO aliases(doc_id, alias, normalized_alias, target_node_id, alias_level, source_version, is_active, confidence, source)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            alias_rows,
        )

        try:
            conn.execute("CREATE VIRTUAL TABLE node_fts USING fts5(node_key UNINDEXED, tokens)")
        except sqlite3.OperationalError as exc:
            die(f"SQLite FTS5 is required but unavailable: {exc}")

        fts_rows = [
            (n.node_key, " ".join(fts_tokens(n.title + "\n" + n.body_plain)))
            for n in nodes if n.is_leaf
        ]
        conn.executemany(
            "INSERT INTO node_fts(node_key, tokens) VALUES (?,?)",
            fts_rows,
        )

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


def read_existing_docs(db_path: Path) -> List[DocRow]:
    if not db_path.exists():
        return []
    from .safe_sqlite import open_db_wal, sqlite3_retry_exec
    conn = open_db_wal(db_path)
    try:
        rows = sqlite3_retry_exec(
            conn,
            """
            SELECT doc_id, doc_title, source_file, source_path, doc_hash, source_version, is_active
            FROM docs
            ORDER BY doc_id, source_version DESC
            """,
        ).fetchall()
    finally:
        conn.close()
    out: List[DocRow] = []
    for row in rows:
        out.append(
            DocRow(
                doc_id=str(row[0]),
                doc_title=str(row[1]),
                source_file=str(row[2]),
                source_path=str(row[3]),
                doc_hash=str(row[4]),
                source_version=str(row[5]),
                is_active=bool(row[6]),
            )
        )
    return out


def read_existing_nodes(db_path: Path) -> List[NodeRow]:
    if not db_path.exists():
        return []
    from .safe_sqlite import open_db_wal, sqlite3_retry_exec
    conn = open_db_wal(db_path)
    try:
        rows = sqlite3_retry_exec(
            conn,
            """
            SELECT
              n.node_id, n.doc_id, n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id, n.ordinal,
              n.ref_path, n.is_leaf, t.body_md, t.body_plain, n.source_version, n.is_active,
              n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence
            FROM nodes n
            JOIN node_text t ON t.node_key = n.node_key
            ORDER BY n.doc_id, n.source_version DESC, n.node_id
            """,
        ).fetchall()
    finally:
        conn.close()
    out: List[NodeRow] = []
    for row in rows:
        out.append(
            NodeRow(
                node_id=str(row["node_id"]),
                doc_id=str(row["doc_id"]),
                kind=str(row["kind"]),
                label=str(row["label"]),
                title=str(row["title"]),
                parent_id=str(row["parent_id"]) if row["parent_id"] else None,
                prev_id=str(row["prev_id"]) if row["prev_id"] else None,
                next_id=str(row["next_id"]) if row["next_id"] else None,
                ordinal=int(row["ordinal"]),
                ref_path=str(row["ref_path"]),
                is_leaf=bool(row["is_leaf"]),
                body_md=str(row["body_md"]),
                body_plain=str(row["body_plain"]),
                source_version=str(row["source_version"]),
                is_active=bool(row["is_active"]),
                raw_span_start=int(row["raw_span_start"]),
                raw_span_end=int(row["raw_span_end"]),
                node_hash=str(row["node_hash"]),
                confidence=float(row["confidence"]),
            )
        )
    return out


def read_existing_edges(db_path: Path) -> List[EdgeRow]:
    if not db_path.exists():
        return []
    from .safe_sqlite import open_db_wal, sqlite3_retry_exec
    conn = open_db_wal(db_path)
    try:
        rows = sqlite3_retry_exec(
            conn,
            """
            SELECT doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence
            FROM edges
            ORDER BY doc_id, source_version DESC, edge_type, from_node_id, to_node_id
            """,
        ).fetchall()
    finally:
        conn.close()
    out: List[EdgeRow] = []
    for row in rows:
        out.append(
            EdgeRow(
                doc_id=str(row[0]),
                edge_type=str(row[1]),
                from_node_id=str(row[2]),
                to_node_id=str(row[3]),
                source_version=str(row[4]),
                is_active=bool(row[5]),
                confidence=float(row[6]),
            )
        )
    return out


def read_existing_aliases(db_path: Path) -> List[AliasRow]:
    if not db_path.exists():
        return []
    from .safe_sqlite import open_db_wal, sqlite3_retry_exec
    conn = open_db_wal(db_path)
    try:
        rows = sqlite3_retry_exec(
            conn,
            """
            SELECT
              doc_id, alias, normalized_alias, target_node_id, alias_level, confidence,
              source, source_version, is_active
            FROM aliases
            ORDER BY doc_id, source_version DESC, normalized_alias, target_node_id, alias_level
            """,
        ).fetchall()
    finally:
        conn.close()
    out: List[AliasRow] = []
    for row in rows:
        out.append(
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
        )
    return out


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
    """Atomically replace *dst* with *src* using os.replace (same-fs atomic).

    Also handles WAL sidecars (.sqlite-wal, .sqlite-shm) atomically.
    """
    # os.replace is POSIX-atomic on the same filesystem.
    os.replace(str(src), str(dst))
    # Move WAL sidecars if present
    for suffix in ("-wal", "-shm"):
        src_sidecar = src.with_suffix(src.suffix + suffix)
        dst_sidecar = dst.with_suffix(dst.suffix + suffix)
        if src_sidecar.exists():
            os.replace(str(src_sidecar), str(dst_sidecar))
        elif dst_sidecar.exists() and not src_sidecar.exists():
            # Source rebuild did not generate sidecars; old ones are stale.
            try:
                dst_sidecar.unlink()
            except OSError:
                pass


def rebuild_shadow_db(
    root: Path, db_path: Path
) -> Tuple[Path, List[DocRow], List[NodeRow], List[EdgeRow], List[AliasRow]]:
    shadow_path = db_path.with_suffix(db_path.suffix + ".next")
    logger.info("shadow rebuild: %s", shadow_path)
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
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.edge_type,
            record.from_node_id,
            record.to_node_id,
            0 if record.is_active else 1,
        ),
    )
    merged_aliases = merge_history(
        current_aliases,
        rebuilt_aliases,
        key_fn=lambda record: (
            record.normalized_alias,
            record.target_node_id,
            record.alias_level,
            record.source_version,
        ),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.normalized_alias,
            record.target_node_id,
            record.alias_level,
            0 if record.is_active else 1,
        ),
    )
    write_kb_sqlite_db(shadow_path, merged_docs, merged_nodes, merged_edges, merged_aliases)
    docs_count, nodes_count, leaf_count = validate_shadow_db(shadow_path)
    atomic_replace(shadow_path, db_path)
    logger.info("atomic switch: %s (docs=%s nodes=%s leaf=%s)", db_path, docs_count, nodes_count, leaf_count)
    return db_path, merged_docs, merged_nodes, merged_edges, merged_aliases


def cmd_reindex(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    db_path = resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite"))
    if not db_path.exists():
        die("Missing kb.sqlite. Rebuild the skill first.")
    previous_state = {}
    state_path = root / BUILD_STATE_FILENAME
    if state_path.exists():
        try:
            previous_state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            previous_state = {}
    dst, docs, nodes, edges, aliases = rebuild_shadow_db(root, db_path)
    write_corpus_manifest(root, docs, nodes)
    write_phase_a_artifact_export(root, docs, nodes, edges, aliases)
    write_build_state(root, docs, nodes, edges, aliases)
    current_state = {}
    if state_path.exists():
        try:
            current_state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            current_state = {}
    refresh = _summarize_incremental_refresh(previous_state, current_state)
    leaf = sum(1 for n in nodes if n.is_active and n.is_leaf)
    logger.info("Reindexed: %s", dst)
    logger.info("docs=%s nodes=%s leaf=%s", len(docs), len(nodes), leaf)
    logger.info(
        "incremental refresh: dirty_docs=%s rewritten_rows=%s full_rewrite_rows=%s indexes=%s",
        len(refresh.dirty_doc_ids),
        refresh.rewritten_rows,
        refresh.full_rewrite_rows,
        ",".join(refresh.refreshed_indexes),
    )
    return 0
