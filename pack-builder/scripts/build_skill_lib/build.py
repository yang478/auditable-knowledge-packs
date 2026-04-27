from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .chunking import DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP, DEFAULT_SEPARATORS, validate_chunk_params
from .db import (
    append_graph_edges_to_db,
    incremental_update_kb_sqlite_db,
    merge_history,
    read_existing_aliases,
    read_existing_docs,
    read_existing_edges,
    read_existing_nodes,
    write_kb_sqlite_db,
)
from .doc import (
    extract_alias_rows,
    extract_reference_edges,
    generate_doc,
    generate_doc_from_ir,
)
from .extract import extract_to_markdown, spans_from_markdown
from .fingerprint.utils import sha256_text, source_fingerprint_for_path
from .incremental import (
    ARTIFACT_VERSION,
    BUILD_STATE_FILENAME,
    ChangeSet,
    build_state_from_artifact,
    compute_toolchain_checksum,
    write_build_state,
)
from .index import build_keywords_from_title, write_sharded_index
from .ir import read_ir_jsonl, write_phase_a_artifact_export
from .kbtool_assets import (
    copy_search_binaries,
    maybe_package_kbtool_pyinstaller,
    write_kbtool_script,
    write_kbtool_sha1,
    write_reindex_script,
    write_root_kbtool_entrypoints,
)
from .render import render_generated_skill_md
from concurrent.futures import ThreadPoolExecutor, as_completed

from .utils.fs import derive_doc_id, derive_doc_title, die, write_text
from .utils.text import canonical_text_from_markdown, canonical_text_sha256, derive_source_version, normalize_canonical_text, stable_hash
from .types import HeadingRow, InputDoc, NodeRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Manifest & canonical text helpers
# ---------------------------------------------------------------------------


def _write_manifest(out_skill_dir: Path, *, skill_name: str, title: str, docs: Sequence[InputDoc]) -> None:
    payload = {
        "skill_name": skill_name,
        "title": title,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "docs": [
            {
                "doc_id": d.doc_id,
                "title": d.title,
                "source_file": d.path.name,
                "source_path": str(d.path),
                "doc_hash": d.doc_hash,
                "source_version": d.source_version,
                "active_version": bool(d.is_active),
            }
            for d in docs
        ],
    }
    write_text(out_skill_dir / "manifest.json", json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _safe_canonical_version(source_version: str) -> str:
    value = re.sub(r"[^0-9A-Za-z._-]+", "-", str(source_version or "current")).strip("-")
    return value or "current"


def _canonical_text_rel_path(doc_id: str, source_version: str) -> str:
    return f"canonical_text/{doc_id}--{_safe_canonical_version(source_version)}.txt"


def _canonical_text_from_ir_nodes(nodes: Sequence[NodeRecord], *, doc_id: str, source_version: str) -> str:
    base = [
        node
        for node in nodes
        if node.doc_id == doc_id and node.source_version == source_version and node.is_active
    ]
    candidates = [n for n in base if n.is_leaf] or base
    candidates.sort(key=lambda node: (node.ordinal, node.ref_path, node.node_id))
    text = "\n\n".join(
        part.strip()
        for part in ((node.body_md or node.body_plain or "") for node in candidates)
        if part.strip()
    )
    return canonical_text_from_markdown(text)


def _spans_from_doc_markdown(doc: InputDoc, markdown_text: str):
    return spans_from_markdown(markdown_text, doc_id=doc.doc_id)


def _load_existing_corpus_manifest(skill_root: Path) -> Tuple[str, Dict[Tuple[str, str], Dict[str, str]]]:
    manifest_path = skill_root / "corpus_manifest.json"
    if not manifest_path.exists():
        return "", {}
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return "", {}

    title = str(data.get("title") or "")
    docs = data.get("docs")
    if not isinstance(docs, list):
        return title, {}

    out: Dict[Tuple[str, str], Dict[str, str]] = {}
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
            "canonical_text_path": str(row.get("canonical_text_path") or ""),
        }
    return title, out


def _load_existing_canonical_text(
    skill_root: Path,
    *,
    doc_id: str,
    source_version: str,
    existing_doc: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    rel_candidates = []
    if existing_doc is not None:
        existing_rel_path = str(existing_doc.get("canonical_text_path") or "").strip()
        if existing_rel_path:
            rel_candidates.append(existing_rel_path)
    rel_candidates.append(_canonical_text_rel_path(doc_id, source_version))

    for rel_path in dict.fromkeys(rel_candidates):
        if not rel_path:
            continue
        path = skill_root / rel_path
        if path.exists():
            return normalize_canonical_text(path.read_text(encoding="utf-8"))
    return None


def _write_corpus_manifest(
    out_skill_dir: Path,
    *,
    title: str,
    docs: Sequence[InputDoc],
    canonical_texts: Dict[Tuple[str, str], str],
    existing_root: Optional[Path] = None,
    existing_docs: Optional[Dict[Tuple[str, str], Dict[str, str]]] = None,
) -> None:
    payload_docs = []
    for doc in sorted(docs, key=lambda item: (item.doc_id, item.source_version, 0 if item.is_active else 1)):
        key = (doc.doc_id, doc.source_version)
        canonical_text = canonical_texts.get(key)
        existing_doc = (existing_docs or {}).get(key, {})
        if canonical_text is None and existing_root is not None:
            canonical_text = _load_existing_canonical_text(
                existing_root,
                doc_id=doc.doc_id,
                source_version=doc.source_version,
                existing_doc=existing_doc,
            )
        if canonical_text is None:
            canonical_text = normalize_canonical_text("")

        rel_path = _canonical_text_rel_path(doc.doc_id, doc.source_version)
        write_text(out_skill_dir / rel_path, canonical_text)
        payload_docs.append(
            {
                "doc_id": doc.doc_id,
                "title": doc.title or str(existing_doc.get("title") or doc.doc_id),
                "source_file": doc.path.name if doc.path.name else str(existing_doc.get("source_file") or "(unknown)"),
                "source_path": str(doc.path) if str(doc.path) else str(existing_doc.get("source_path") or ""),
                "doc_hash": doc.doc_hash or str(existing_doc.get("doc_hash") or ""),
                "source_version": doc.source_version,
                "active_version": bool(doc.is_active),
                "canonical_text_path": rel_path,
                "canonical_text_sha256": canonical_text_sha256(canonical_text),
            }
        )

    payload = {
        "title": title,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "docs": payload_docs,
    }
    write_text(out_skill_dir / "corpus_manifest.json", json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


# ---------------------------------------------------------------------------
# Incremental build helpers
# ---------------------------------------------------------------------------


def _load_build_state(target: Path) -> dict[str, Any]:
    """读取先前构建的状态文件，失败时返回空字典。"""
    path = target / BUILD_STATE_FILENAME
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def _find_previous_state_for_path(
    previous_docs: dict[str, Any], resolved_path: Path
) -> Tuple[Optional[str], str, Optional[dict[str, Any]]]:
    """在 previous_state.documents 中查找对应路径的文档状态。

    返回 (doc_id, source_version, state_dict) 或 (None, "current", None)。
    兼容新旧两种 documents_state 结构。
    """
    if not isinstance(previous_docs, dict):
        return None, "current", None

    for doc_id, versions in previous_docs.items():
        if not isinstance(versions, dict):
            continue
        # 新结构：doc_id -> {source_version: state}
        if "source_fingerprint" not in versions:
            for sv, state in versions.items():
                if isinstance(state, dict) and state.get("source_path") == str(resolved_path):
                    return doc_id, sv, state
        else:
            # 旧结构：doc_id -> state（单版本）
            if versions.get("source_path") == str(resolved_path):
                return doc_id, "current", versions

    return None, "current", None


def _prepare_incremental_inputs(
    inputs: Sequence[Path],
    previous_state: dict[str, Any],
    pdf_fallback: str,
) -> Tuple[ChangeSet, List[InputDoc]]:
    """为增量构建准备 InputDoc 列表并计算变更集。

    对于 unchanged 文档，从 previous_state 恢复 metadata，避免文本提取。
    对于 changed 文档，执行提取并分类为 REBUILD 或 METADATA_ONLY。
    """
    previous_docs = previous_state.get("documents", {})
    if not isinstance(previous_docs, dict):
        previous_docs = {}

    changed_doc_ids: set[str] = set()
    unchanged_doc_ids: set[str] = set()
    metadata_only_doc_ids: set[str] = set()
    rebuild_doc_ids: set[str] = set()
    current_doc_ids: set[str] = set()
    docs: List[InputDoc] = []
    used_doc_ids: set[str] = set()

    for p in inputs:
        resolved = p.resolve()
        src_fp = source_fingerprint_for_path(resolved)

        prev_doc_id, prev_sv, prev_state = _find_previous_state_for_path(previous_docs, resolved)

        if prev_state is None or prev_doc_id is None:
            # 全新文档
            md = extract_to_markdown(resolved, pdf_fallback=pdf_fallback)
            title = derive_doc_title(resolved, md)
            doc_id = derive_doc_id(resolved, used_doc_ids)
            sv = derive_source_version(resolved.stem, title)
            doc_hash = stable_hash(md)
            docs.append(
                InputDoc(
                    path=resolved,
                    doc_id=doc_id,
                    title=title,
                    source_version=sv,
                    doc_hash=doc_hash,
                    active_parser="occam_chunking",
                )
            )
            changed_doc_ids.add(doc_id)
            rebuild_doc_ids.add(doc_id)
            current_doc_ids.add(doc_id)
            continue

        doc_id = prev_doc_id
        current_doc_ids.add(doc_id)
        used_doc_ids.add(doc_id)

        if prev_state.get("source_fingerprint") == src_fp:
            # 源文件完全未变
            unchanged_doc_ids.add(doc_id)
            docs.append(
                InputDoc(
                    path=resolved,
                    doc_id=doc_id,
                    title=prev_state.get("doc_title", derive_doc_title(resolved, "")),
                    source_version=prev_sv,
                    doc_hash=prev_state.get("doc_hash", ""),
                    active_parser=prev_state.get("active_parser", "occam_chunking"),
                )
            )
            continue

        # 源文件变了，提取文本进一步判断
        md = extract_to_markdown(resolved, pdf_fallback=pdf_fallback)
        text_fp = sha256_text(canonical_text_from_markdown(md))
        prev_text_fp = str(prev_state.get("extracted_text_fingerprint") or "")

        changed_doc_ids.add(doc_id)
        if text_fp == prev_text_fp:
            metadata_only_doc_ids.add(doc_id)
        else:
            rebuild_doc_ids.add(doc_id)

        title = derive_doc_title(resolved, md)
        sv = derive_source_version(resolved.stem, title)
        doc_hash = stable_hash(md)
        docs.append(
            InputDoc(
                path=resolved,
                doc_id=doc_id,
                title=title,
                source_version=sv,
                doc_hash=doc_hash,
                active_parser="occam_chunking",
            )
        )

    removed_doc_ids = set(previous_docs.keys()) - current_doc_ids
    rebuild_doc_ids |= removed_doc_ids
    changed_doc_ids |= removed_doc_ids

    change_set = ChangeSet(
        changed_doc_ids=changed_doc_ids,
        unchanged_doc_ids=unchanged_doc_ids,
        metadata_only_doc_ids=metadata_only_doc_ids,
        rebuild_doc_ids=rebuild_doc_ids,
        removed_doc_ids=removed_doc_ids,
    )
    return change_set, docs



# ---------------------------------------------------------------------------
# Phase functions extracted from build_skill()
# ---------------------------------------------------------------------------


def _process_single_full_input(
    p: Path,
    pdf_fallback: str,
    tmp: Path,
    chunking_config: dict,
    chunk_size: int,
    overlap: int,
    doc_id: str,
) -> Tuple[InputDoc, List[HeadingRow], List[NodeRecord], str, str]:
    """Extract, chunk, and generate nodes for one document — thread-safe.

    Returns (doc, heading_rows, nodes, source_version, canonical_md).
    Each doc has a unique doc_id so that generate_doc writes to a
    non-conflicting references/ subdirectory.
    """
    resolved = p.resolve()
    md = extract_to_markdown(resolved, pdf_fallback=pdf_fallback)
    canonical_md = canonical_text_from_markdown(md)
    title = derive_doc_title(resolved, md)
    sv = derive_source_version(resolved.stem, title)
    doc = InputDoc(
        path=resolved,
        doc_id=doc_id,
        title=title,
        source_version=sv,
        doc_hash=stable_hash(md),
        active_parser="occam_chunking",
    )
    headings, nodes = generate_doc(
        doc, md, tmp,
        chunking_config=chunking_config,
        chunk_size=chunk_size,
        overlap=overlap,
        canonical_md=canonical_md,
    )
    return doc, headings, nodes, sv, canonical_md


def _extract_documents(
    inputs: Sequence[Path],
    pdf_fallback: str,
    ir_jsonl: Optional[Path],
    tmp: Path,
    chunking_config: dict,
    chunk_size: int,
    overlap: int,
    *,
    change_set: Optional[ChangeSet] = None,
    target: Optional[Path] = None,
    workers: int | None = None,
) -> Tuple[List[InputDoc], List[HeadingRow], List[NodeRecord], Dict[Tuple[str, str], str]]:
    """Phase 1: Extract documents from inputs or IR JSONL (unified full/incremental).

    Three paths:
    - ir_jsonl provided → full extraction from prebuilt IR
    - change_set=None → full build: extract all inputs from scratch
    - change_set provided → incremental: reuse existing data for unchanged/metadata-only docs
    """
    docs: List[InputDoc] = []
    all_heading_rows: List[HeadingRow] = []
    all_nodes: List[NodeRecord] = []
    canonical_texts: Dict[Tuple[str, str], str] = {}

    # ── IR JSONL path (always full) ──────────────────────────────────────
    if ir_jsonl is not None:
        docs, all_nodes = read_ir_jsonl(ir_jsonl)
        for doc in docs:
            canonical_texts[(doc.doc_id, doc.source_version)] = _canonical_text_from_ir_nodes(
                all_nodes,
                doc_id=doc.doc_id,
                source_version=doc.source_version,
            )
            all_heading_rows.extend(generate_doc_from_ir(doc, all_nodes, tmp))
        return docs, all_heading_rows, all_nodes, canonical_texts

    # ── Full build path ──────────────────────────────────────────────────
    if change_set is None:
        # Pre-compute doc_ids to avoid locking during parallel extraction
        used_doc_ids: set[str] = set()
        input_doc_ids: list[tuple[Path, str]] = []
        for p in inputs:
            doc_id = derive_doc_id(p.resolve(), used_doc_ids)
            input_doc_ids.append((p, doc_id))

        # Parallel extraction: each doc is fully processed in its own thread
        max_workers = workers if workers else min(8, max(1, len(inputs)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_path: dict = {}
            for p, doc_id in input_doc_ids:
                future = executor.submit(
                    _process_single_full_input,
                    p, pdf_fallback, tmp, chunking_config, chunk_size, overlap, doc_id,
                )
                future_to_path[future] = p

            failed_paths: list[Path] = []
            for future in as_completed(future_to_path):
                src_path = future_to_path[future]
                try:
                    doc, headings, nodes, sv, ct = future.result()
                except Exception as exc:
                    failed_paths.append(src_path)
                    logger.error("Document extraction failed: %s — %s", src_path, exc)
                    continue
                docs.append(doc)
                all_heading_rows.extend(headings)
                all_nodes.extend(nodes)
                canonical_texts[(doc.doc_id, sv)] = ct

            if failed_paths:
                raise BuildError(
                    f"{len(failed_paths)} document(s) failed during parallel extraction: "
                    + ", ".join(str(p) for p in failed_paths)
                )

        # Sort docs to match input order for deterministic output
        input_order = {p.resolve(): i for i, (p, _) in enumerate(input_doc_ids)}
        docs.sort(key=lambda d: input_order.get(d.path, 0))

        return docs, all_heading_rows, all_nodes, canonical_texts

    # ── Incremental path ─────────────────────────────────────────────────
    assert target is not None, "target is required for incremental builds"
    previous_state = _load_build_state(target)
    previous_docs = previous_state.get("documents", {})
    if not isinstance(previous_docs, dict):
        previous_docs = {}

    reuse_doc_ids = change_set.unchanged_doc_ids | change_set.metadata_only_doc_ids

    # Preload existing nodes to avoid repeated DB queries
    existing_nodes_by_doc: Dict[Tuple[str, str], List[NodeRecord]] = {}
    db_path = target / "kb.sqlite"
    if db_path.exists():
        for node in read_existing_nodes(db_path, include_body=True):
            existing_nodes_by_doc.setdefault((node.doc_id, node.source_version), []).append(node)

    used_doc_ids: set[str] = set()
    for p in inputs:
        resolved = p.resolve()
        prev_doc_id, prev_sv, prev_state = _find_previous_state_for_path(previous_docs, resolved)

        if prev_doc_id is None:
            # New document – full extraction
            md = extract_to_markdown(resolved, pdf_fallback=pdf_fallback)
            canonical_md = canonical_text_from_markdown(md)
            title = derive_doc_title(resolved, md)
            doc_id = derive_doc_id(resolved, used_doc_ids)
            sv = derive_source_version(resolved.stem, title)
            doc_hash = stable_hash(md)
            doc = InputDoc(
                path=resolved,
                doc_id=doc_id,
                title=title,
                source_version=sv,
                doc_hash=doc_hash,
                active_parser="occam_chunking",
            )
            docs.append(doc)
            headings, nodes = generate_doc(
                doc, md, tmp,
                chunking_config=chunking_config,
                chunk_size=chunk_size,
                overlap=overlap,
                canonical_md=canonical_md,
            )
            all_heading_rows.extend(headings)
            all_nodes.extend(nodes)
            canonical_texts[(doc_id, sv)] = canonical_md
            continue

        doc_id = prev_doc_id
        used_doc_ids.add(doc_id)

        if doc_id in reuse_doc_ids:
            # Unchanged or metadata-only – reuse existing data
            doc = InputDoc(
                path=resolved,
                doc_id=doc_id,
                title=prev_state.get("doc_title", derive_doc_title(resolved, "")),
                source_version=prev_sv,
                doc_hash=prev_state.get("doc_hash", ""),
                active_parser=prev_state.get("active_parser", "occam_chunking"),
            )
            docs.append(doc)

            # Copy references directory from existing target
            src_ref = target / "references" / doc_id
            dst_ref = tmp / "references" / doc_id
            if src_ref.exists():
                shutil.copytree(src_ref, dst_ref, dirs_exist_ok=True)

            # Load nodes from existing DB
            key = (doc_id, prev_sv)
            doc_nodes = existing_nodes_by_doc.get(key, [])
            all_nodes.extend(doc_nodes)
            for node in doc_nodes:
                if node.is_active:
                    all_heading_rows.append(
                        (node.title, doc_id, doc.title, node.kind, node.node_id, node.ref_path)
                    )

            # Load canonical text
            ct = _load_existing_canonical_text(target, doc_id=doc_id, source_version=prev_sv)
            if ct is None:
                ct = normalize_canonical_text("")
            canonical_texts[key] = ct
        else:
            # Changed document – full extraction
            md = extract_to_markdown(resolved, pdf_fallback=pdf_fallback)
            canonical_md = canonical_text_from_markdown(md)
            title = derive_doc_title(resolved, md)
            sv = derive_source_version(resolved.stem, title)
            doc_hash = stable_hash(md)
            doc = InputDoc(
                path=resolved,
                doc_id=doc_id,
                title=title,
                source_version=sv,
                doc_hash=doc_hash,
                active_parser="occam_chunking",
            )
            docs.append(doc)
            headings, nodes = generate_doc(
                doc, md, tmp,
                chunking_config=chunking_config,
                chunk_size=chunk_size,
                overlap=overlap,
                canonical_md=canonical_md,
            )
            all_heading_rows.extend(headings)
            all_nodes.extend(nodes)
            canonical_texts[(doc_id, sv)] = canonical_md

    return docs, all_heading_rows, all_nodes, canonical_texts


def _write_indexes_and_assets(
    tmp: Path,
    skill_name: str,
    title: str,
    docs: Sequence[InputDoc],
    all_heading_rows: List[HeadingRow],
    package_kbtool: bool,
) -> str:
    """Phase 2: Write manifest, indexes, SKILL.md, scripts, and binaries."""
    _write_manifest(tmp, skill_name=skill_name, title=title, docs=docs)

    # Global indexes
    all_heading_rows.sort(key=lambda r: r[0])
    headings_rows = [
        (t, doc_id, doc_title, kind, item_id, path) for (t, doc_id, doc_title, kind, item_id, path) in all_heading_rows
    ]
    write_sharded_index(tmp, "headings", headings_rows, header=("title", "doc_id", "doc_title", "type", "id", "path"))

    kw_map: dict[tuple[str, str, str, str, str], tuple[str, ...]] = {}
    for t, doc_id, doc_title, kind, item_id, path in all_heading_rows:
        for kw in build_keywords_from_title(t):
            kw_map[(kw, doc_id, kind, item_id, path)] = (kw, doc_id, doc_title, kind, item_id, path)
    kw_rows = sorted(kw_map.values(), key=lambda r: r[0])
    write_sharded_index(tmp, "kw", kw_rows, header=("keyword", "doc_id", "doc_title", "type", "id", "path"))

    # Generated skill SKILL.md
    write_text(
        tmp / "SKILL.md",
        render_generated_skill_md(skill_name, title, docs),
    )
    write_reindex_script(tmp)
    write_kbtool_script(tmp)
    copy_search_binaries(tmp)
    kbtool_sha = write_kbtool_sha1(tmp)
    write_root_kbtool_entrypoints(tmp)
    if package_kbtool:
        exe = maybe_package_kbtool_pyinstaller(tmp)
        if exe is not None:
            write_text(exe.parent / "kbtool.sha1", kbtool_sha + "\n")

    return kbtool_sha


def _merge_build_history(
    target: Path,
    force: bool,
    docs: Sequence[InputDoc],
    all_nodes: Sequence[NodeRecord],
    tmp: Path,
    *,
    incremental: bool = False,
):
    """Phase 3: Load existing DB records, extract edges/aliases, merge all 4 record types."""
    existing_db = target / "kb.sqlite"
    read_existing = target.exists() and (force or incremental)
    current_docs = read_existing_docs(existing_db) if read_existing else []
    current_nodes = read_existing_nodes(existing_db, include_body=False) if read_existing else []
    current_edges = read_existing_edges(existing_db) if read_existing else []
    current_aliases = read_existing_aliases(existing_db) if read_existing else []

    rebuilt_edges = extract_reference_edges(all_nodes, base_dir=tmp)
    rebuilt_aliases = extract_alias_rows(all_nodes, base_dir=tmp)
    merged_docs = merge_history(
        current_docs,
        docs,
        key_fn=lambda record: (record.doc_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, 0 if record.is_active else 1),
    )
    merged_nodes = merge_history(
        current_nodes,
        all_nodes,
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
        key_fn=lambda record: (record.normalized_alias, record.target_node_id, record.alias_level, record.source_version),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.normalized_alias,
            record.target_node_id,
            record.alias_level,
            0 if record.is_active else 1,
        ),
    )

    return merged_docs, merged_nodes, merged_edges, merged_aliases


def _write_database_and_graph(
    tmp: Path,
    merged_docs,
    merged_nodes,
    merged_edges,
    merged_aliases,
    enable_graph_edges: bool,
):
    """Phase 4: Write SQLite database and optional graph edges."""
    db_path = tmp / "kb.sqlite"
    write_kb_sqlite_db(db_path, merged_docs, merged_nodes, merged_edges, merged_aliases, base_dir=tmp)
    edges_for_artifacts = merged_edges
    if enable_graph_edges:
        graph_edge_count, graph_elapsed = append_graph_edges_to_db(db_path, merged_nodes)
        print(f"[graph_builder] Added {graph_edge_count} graph edges ({graph_elapsed:.2f}s)")
        edges_for_artifacts = read_existing_edges(db_path)
    return edges_for_artifacts


def _write_database_and_graph_incremental(
    tmp: Path,
    target: Path,
    change_set: ChangeSet,
    merged_docs,
    merged_nodes,
    merged_edges,
    merged_aliases,
    enable_graph_edges: bool,
):
    """Phase 4 (incremental): Copy existing DB and apply incremental updates."""
    db_path = tmp / "kb.sqlite"
    target_db = target / "kb.sqlite"

    if not target_db.exists():
        # Fallback to full write if no existing DB
        return _write_database_and_graph(
            tmp, merged_docs, merged_nodes, merged_edges, merged_aliases, enable_graph_edges
        )

    shutil.copy2(str(target_db), str(db_path))
    # Copy WAL sidecars if they exist so the copied DB sees uncheckpointed data.
    for suffix in ("-wal", "-shm"):
        src_wal = target_db.parent / (target_db.name + suffix)
        if src_wal.exists():
            dst_wal = db_path.parent / (db_path.name + suffix)
            shutil.copy2(str(src_wal), str(dst_wal))

    incremental_update_kb_sqlite_db(
        db_path,
        change_set,
        merged_docs,
        merged_nodes,
        merged_edges,
        merged_aliases,
        base_dir=tmp,
    )

    edges_for_artifacts = merged_edges
    if enable_graph_edges:
        rebuild_doc_ids = change_set.rebuild_doc_ids
        rebuild_nodes = [n for n in merged_nodes if n.doc_id in rebuild_doc_ids] if rebuild_doc_ids else merged_nodes
        graph_edge_count, graph_elapsed = append_graph_edges_to_db(db_path, rebuild_nodes, rebuild_doc_ids=rebuild_doc_ids)
        print(f"[graph_builder] Added/updated {graph_edge_count} graph edges ({graph_elapsed:.2f}s)")
        edges_for_artifacts = read_existing_edges(db_path)
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("INSERT INTO node_fts(node_fts) VALUES('optimize')")
    finally:
        conn.close()
    return edges_for_artifacts


def _write_final_artifacts(
    tmp: Path,
    target: Path,
    force: bool,
    title: str,
    merged_docs,
    merged_nodes,
    edges_for_artifacts,
    merged_aliases,
    canonical_texts: Dict[Tuple[str, str], str],
) -> None:
    """Phase 5: Write corpus manifest, phase A export, and build state."""
    should_merge = target.exists() and force
    existing_title, existing_corpus_docs = _load_existing_corpus_manifest(target) if should_merge else ("", {})
    _write_corpus_manifest(
        tmp,
        title=title or existing_title,
        docs=merged_docs,
        canonical_texts=canonical_texts,
        existing_root=target if should_merge else None,
        existing_docs=existing_corpus_docs,
    )
    write_phase_a_artifact_export(
        tmp,
        docs=merged_docs,
        nodes=merged_nodes,
        edges=edges_for_artifacts,
        aliases=merged_aliases,
    )
    write_build_state(
        tmp / BUILD_STATE_FILENAME,
        build_state_from_artifact(
            root=tmp,
            docs=merged_docs,
            nodes=merged_nodes,
            edges=edges_for_artifacts,
            aliases=merged_aliases,
            canonical_texts=canonical_texts,
        ),
    )


def _atomic_replace(tmp: Path, target: Path) -> Path:
    """Phase 6: Atomic-ish directory swap with backup & restore.

    Safety improvements:
    - Uses os.replace() for the final file-level atomic swap on same fs.
    - For directories, uses shutil.move + backup restore on OSError only.
    - Handles WAL/shm files atomically alongside the main DB.
    - Checks shutdown flag before destructive operations.
    """
    from .utils.signals import is_shutdown_requested, raise_if_shutdown
    raise_if_shutdown()

    # Checkpoint WAL so sidecar files are empty before the directory swap.
    db_path_wal = tmp / "kb.sqlite"
    if db_path_wal.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path_wal))
            try:
                for _ in range(3):
                    try:
                        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        break
                    except sqlite3.OperationalError:
                        continue
                conn.close()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass

    backup = target.parent / (target.name + ".old")
    if target.exists():
        if backup.exists():
            shutil.rmtree(backup)
        target.rename(backup)

    try:
        shutil.move(str(tmp), str(target))
    except OSError:
        if backup.exists():
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            shutil.move(str(backup), str(target))
        raise

    if backup.exists():
        shutil.rmtree(backup)
    return target


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _build_summary(skill_dir):
    import sqlite3
    db_path = str(skill_dir / "kb.sqlite")
    conn = sqlite3.connect(db_path)
    try:
        docs = conn.execute("SELECT COUNT(*) FROM docs WHERE is_active=1").fetchone()[0]
        chunks = conn.execute("SELECT COUNT(*) FROM nodes WHERE kind='chunk' AND is_active=1").fetchone()[0]
        edges = conn.execute("SELECT COUNT(*) FROM edges WHERE is_active=1").fetchone()[0]
        aliases = conn.execute("SELECT COUNT(*) FROM aliases WHERE is_active=1").fetchone()[0]
        tokens = conn.execute("SELECT COUNT(*) FROM node_fts_data").fetchone()[0]
        return f"{docs} doc(s), {chunks} chunk(s), {edges} edge(s), {aliases} alias(es), {tokens} FTS token(s)"
    finally:
        conn.close()


def build_skill(
    skill_name: str,
    title: str,
    inputs: Sequence[Path],
    out_dir: Path,
    force: bool,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overlap: int = DEFAULT_OVERLAP,
    pdf_fallback: str = "none",
    ir_jsonl: Optional[Path] = None,
    package_kbtool: bool = False,
    enable_graph_edges: bool = True,
    incremental: bool = False,
    workers: int | None = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / skill_name

    # Validate chunking args
    chunk_size_value = int(chunk_size)
    overlap_value = int(overlap)
    try:
        validate_chunk_params(chunk_size_value, overlap_value)
    except ValueError as exc:
        msg = str(exc)
        msg = msg.replace("chunk_size", "--chunk-size").replace("overlap", "--overlap")
        die(msg)

    chunking_config = {
        "schema": "pack_builder.chunking.v1",
        "algorithm": "recursive_character",
        "chunk_size_chars": chunk_size_value,
        "overlap_chars": overlap_value,
        "separators": list(DEFAULT_SEPARATORS),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    from .utils.signals import graceful_shutdown_context

    if target.exists() and not force and not incremental:
        die(f"Output already exists: {target} (use --force to overwrite or --incremental to update)")

    with graceful_shutdown_context():
        with tempfile.TemporaryDirectory(prefix=f".{skill_name}.tmp-", dir=out_dir) as tmp_name:
            tmp = Path(tmp_name)

            # Preserve user-managed files when rebuilding (e.g., multiple platform binaries).
            if target.exists():
                for keep in ("bin", "hooks"):
                    src = target / keep
                    if not src.exists():
                        continue
                    shutil.copytree(src, tmp / keep, dirs_exist_ok=True)

            write_text(tmp / "chunking.json", json.dumps(chunking_config, ensure_ascii=False, indent=2) + "\n")

            # Determine build mode
            previous_state = _load_build_state(target)
            is_incremental = (
                incremental
                and target.exists()
                and previous_state.get("artifact_version") == ARTIFACT_VERSION
                and (target / "kb.sqlite").exists()
            )

            change_set: Optional[ChangeSet] = None

            if is_incremental:
                # Toolchain checksum comparison
                previous_checksum = str(previous_state.get("build_toolchain_checksum") or "")
                current_checksum = compute_toolchain_checksum(tmp)
                if previous_checksum != current_checksum:
                    print("[incremental] Toolchain changed, falling back to full rebuild.")
                    is_incremental = False
                elif not inputs and ir_jsonl is None:
                    print("[incremental] No inputs, falling back to full rebuild.")
                    is_incremental = False
                else:
                    change_set, _ = _prepare_incremental_inputs(inputs, previous_state, pdf_fallback)
                    if not change_set.changed_doc_ids and not change_set.removed_doc_ids:
                        print("[incremental] No changes detected. Skipping rebuild.")
                        return target
                    print(
                        f"[incremental] {len(change_set.rebuild_doc_ids)} rebuild, "
                        f"{len(change_set.metadata_only_doc_ids)} metadata-only, "
                        f"{len(change_set.unchanged_doc_ids)} unchanged, "
                        f"{len(change_set.removed_doc_ids)} removed"
                    )

            # Phase 1: Extract documents (unified full/incremental)
            logger.info("[1/6] Extracting documents from %d inputs...", len(inputs))
            docs, all_heading_rows, all_nodes, canonical_texts = _extract_documents(
                inputs, pdf_fallback, ir_jsonl, tmp, chunking_config, chunk_size_value, overlap_value,
                change_set=change_set if is_incremental else None,
                target=target if is_incremental else None,
                workers=workers,
            )
            logger.info("[1/6] Done: %d docs, %d nodes", len(docs), len(all_nodes))

            # Phase 2: Write indexes and assets
            logger.info("[2/6] Writing indexes and assets for %s...", skill_name)
            kbtool_sha = _write_indexes_and_assets(tmp, skill_name, title, docs, all_heading_rows, package_kbtool)
            logger.info("[2/6] Done: indexes and assets written")

            # Phase 3: Merge build history
            logger.info("[3/6] Merging build history...")
            merged_docs, merged_nodes, merged_edges, merged_aliases = _merge_build_history(
                target, force, docs, all_nodes, tmp, incremental=is_incremental
            )
            logger.info("[3/6] Done: %d docs, %d nodes, %d edges, %d aliases",
                        len(merged_docs), len(merged_nodes), len(merged_edges), len(merged_aliases))

            # Phase 4: Write database and graph
            logger.info("[4/6] Writing database and graph...")
            if is_incremental and change_set is not None:
                edges_for_artifacts = _write_database_and_graph_incremental(
                    tmp, target, change_set, merged_docs, merged_nodes, merged_edges, merged_aliases, enable_graph_edges
                )
            else:
                edges_for_artifacts = _write_database_and_graph(
                    tmp, merged_docs, merged_nodes, merged_edges, merged_aliases, enable_graph_edges
                )
            logger.info("[4/6] Done: %d edges", len(edges_for_artifacts))

            # Phase 5: Write final artifacts
            logger.info("[5/6] Writing final artifacts...")
            _write_final_artifacts(
                tmp, target, force or is_incremental, title, merged_docs, merged_nodes, edges_for_artifacts, merged_aliases, canonical_texts
            )
            logger.info("[5/6] Done: final artifacts written")

            # Phase 6: Atomic replace
            logger.info("[6/6] Atomic replace %s -> %s...", tmp, target)
            result = _atomic_replace(tmp, target)
            summary = _build_summary(out_dir / skill_name)
            logger.info("Build summary: %s", summary)
            print(f"Build complete: {summary}", file=sys.stderr)
            return result
