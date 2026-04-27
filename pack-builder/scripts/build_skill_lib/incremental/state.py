from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .. import templates_dir
from ..utils.contract import BUILD_STATE_FILENAME, stable_payload, empty_build_state, index_binding_payload, export_sha_by_doc
from ..extract import spans_from_markdown
from ..fingerprint.utils import alias_fingerprint, edge_fingerprint, node_fingerprint, sha256_text, source_fingerprint
from ..utils.registry import canonical_model_registry_json
from ..types import AliasRecord, EdgeRecord, InputDoc, NodeRecord


ARTIFACT_VERSION = "kbtool.artifact.v2"
DEFAULT_MODEL_REGISTRY_SHA256 = sha256_text(canonical_model_registry_json())


def write_build_state(path: Path, state: Mapping[str, Any]) -> None:
    """原子写入 build_state：先写临时文件再 replace，避免半写入状态。"""
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    tmp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    from ..utils.contract import read_json
    return read_json(path)


def _manifest_rows_by_doc(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payload = _read_json(root / "corpus_manifest.json")
    rows = payload.get("docs")
    if not isinstance(rows, list):
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        doc_id = str(row.get("doc_id") or "").strip()
        source_version = str(row.get("source_version") or "current").strip() or "current"
        if not doc_id:
            continue
        out[(doc_id, source_version)] = row
    return out


def _export_sha_by_doc(root: Path) -> dict[tuple[str, str], str]:
    payload = _read_json(root / "phase_a_artifact.json")
    return export_sha_by_doc(payload)


def _infer_active_parser(nodes: Sequence[NodeRecord]) -> str:
    kinds = {node.kind for node in nodes if node.is_active}
    if {"clause", "table", "figure"} & kinds:
        return "outline"
    if "chapter" in kinds or "section" in kinds:
        return "markdown_headings"
    if "block" in kinds:
        return "block_fallback"
    return ""


def build_state_from_artifact(
    *,
    root: Path,
    docs: Sequence[InputDoc],
    nodes: Sequence[NodeRecord],
    edges: Sequence[EdgeRecord],
    aliases: Sequence[AliasRecord],
    canonical_texts: Mapping[tuple[str, str], str],
) -> dict[str, Any]:
    state = empty_build_state()
    manifest_path = root / "corpus_manifest.json"
    manifest_rows = _manifest_rows_by_doc(root)
    export_sha_by_doc_map = _export_sha_by_doc(root)

    state["artifact_version"] = ARTIFACT_VERSION
    state["created_at"] = datetime.now(timezone.utc).isoformat()
    state["corpus_manifest_sha256"] = (
        sha256_text(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else ""
    )
    state["model_registry_sha256"] = DEFAULT_MODEL_REGISTRY_SHA256

    active_docs = [doc for doc in docs if doc.is_active]

    # Pre-group by (doc_id, source_version) — one pass over each collection instead of
    # O(d·n) filtering inside the document loop.
    nodes_by_doc: dict[tuple[str, str], list[NodeRecord]] = {}
    for node in nodes:
        if node.is_active:
            nodes_by_doc.setdefault((node.doc_id, node.source_version), []).append(node)
    aliases_by_doc: dict[tuple[str, str], list[AliasRecord]] = {}
    for alias in aliases:
        if alias.is_active:
            aliases_by_doc.setdefault((alias.doc_id, alias.source_version), []).append(alias)
    edges_by_doc: dict[tuple[str, str], list[EdgeRecord]] = {}
    for edge in edges:
        if edge.is_active:
            edges_by_doc.setdefault((edge.doc_id, edge.source_version), []).append(edge)

    # documents_state 使用嵌套结构：doc_id -> source_version -> state
    # 避免同 doc_id 不同 source_version 相互覆盖
    documents_state: dict[str, dict[str, dict[str, Any]]] = {}
    for doc in sorted(active_docs, key=lambda item: (item.doc_id, item.source_version)):
        key = (doc.doc_id, doc.source_version)
        canonical_text = str(canonical_texts.get(key) or "")
        doc_nodes = nodes_by_doc.get(key, [])
        doc_aliases = aliases_by_doc.get(key, [])
        doc_edges = edges_by_doc.get(key, [])
        span_fingerprints = {}
        for span in spans_from_markdown(canonical_text, doc_id=doc.doc_id):
            span_fingerprints[span.span_id] = sha256_text(canonical_text[span.char_start : span.char_end])
        node_fingerprints = {node.node_id: node_fingerprint(node) for node in doc_nodes}
        alias_fingerprints = {
            f"{alias.normalized_alias}|{alias.target_node_id}|{alias.alias_level}": alias_fingerprint(alias)
            for alias in doc_aliases
        }
        edge_fingerprints = {
            f"{edge.edge_type}|{edge.from_node_id}|{edge.to_node_id}": edge_fingerprint(edge) for edge in doc_edges
        }
        doc_versions = documents_state.setdefault(doc.doc_id, {})
        doc_versions[doc.source_version] = {
            "source_path": str(doc.path),
            "source_fingerprint": source_fingerprint(doc.path, doc.doc_hash),
            "extracted_text_fingerprint": sha256_text(canonical_text),
            "span_fingerprints": span_fingerprints,
            "node_fingerprints": node_fingerprints,
            "alias_fingerprints": alias_fingerprints,
            "edge_fingerprints": edge_fingerprints,
            "active_parser": str(getattr(doc, "active_parser", "") or "") or _infer_active_parser(doc_nodes),
            "export_sha256": export_sha_by_doc_map.get(key, ""),
            "doc_title": doc.title,
            "doc_hash": doc.doc_hash,
        }
    state["documents"] = documents_state

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
        "sqlite": index_binding_payload("sqlite", active_node_rows),
        "fts": index_binding_payload("fts", active_leaf_rows),
        "aliases": index_binding_payload("aliases", active_alias_rows),
        "edges": index_binding_payload("edges", active_edge_rows),
    }

    # Toolchain checksum: SHA256 of key build configuration files + build code itself.
    # Allows runtime to detect if the build configuration or code has changed since last build.
    state["build_toolchain_checksum"] = compute_toolchain_checksum(root)

    return state


def compute_toolchain_checksum(root: Path) -> str:
    """Compute SHA256 of key build configuration files and source code for integrity verification."""
    h = hashlib.sha256()
    key_files = [
        "chunking.json",
    ]
    config_hash = hashlib.sha256()
    for fname in key_files:
        fpath = root / fname
        if fpath.exists():
            # Exclude unstable timestamp fields so identical configurations produce identical checksums.
            try:
                payload = json.loads(fpath.read_text(encoding="utf-8"))
                payload.pop("generated_at", None)
                data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
                config_hash.update(data)
            except Exception:
                config_hash.update(fpath.read_bytes())
    h.update(config_hash.digest())

    # Include build source code to detect logic changes that invalidate prior builds.
    builder_hash = hashlib.sha256()
    try:
        import build_skill_lib
        builder_dir = Path(build_skill_lib.__file__).resolve().parent
        for pyfile in sorted(builder_dir.rglob("*.py")):
            if pyfile.name.startswith("test_"):
                continue
            with pyfile.open("rb") as f:
                while chunk := f.read(65536):
                    builder_hash.update(chunk)
    except Exception:
        pass
    h.update(builder_hash.digest())

    # Include runtime template code so that kbtool template changes trigger full rebuild.
    tmpl_hash = hashlib.sha256()
    try:
        tmpl_dir = templates_dir() / "kbtool_lib"
        if tmpl_dir.exists():
            for pyfile in sorted(tmpl_dir.rglob("*.py")):
                with pyfile.open("rb") as f:
                    while chunk := f.read(65536):
                        tmpl_hash.update(chunk)
    except Exception:
        pass
    h.update(tmpl_hash.digest())

    return h.hexdigest()


