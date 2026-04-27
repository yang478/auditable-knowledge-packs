from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence


PHASE_A_ARTIFACT_EXPORT = "phase_a_artifact.json"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def manifest_rows_from_root(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payload = read_json(root / "corpus_manifest.json")
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


def _doc_title(doc: Any) -> str:
    return str(getattr(doc, "title", "") or getattr(doc, "doc_title", "") or "")


def _doc_source_file(doc: Any) -> str:
    path = getattr(doc, "path", None)
    if path:
        return Path(path).name
    return str(getattr(doc, "source_file", "") or "")


def _doc_source_path(doc: Any) -> str:
    path = getattr(doc, "path", None)
    if path:
        return str(path)
    return str(getattr(doc, "source_path", "") or "")


def _doc_row(doc: Any, manifest_row: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "doc_id": str(getattr(doc, "doc_id")),
        "doc_title": _doc_title(doc),
        "source_file": _doc_source_file(doc),
        "source_path": _doc_source_path(doc),
        "doc_hash": str(getattr(doc, "doc_hash", "") or ""),
        "source_version": str(getattr(doc, "source_version", "current") or "current"),
        "is_active": bool(getattr(doc, "is_active", True)),
        "canonical_text_path": str((manifest_row or {}).get("canonical_text_path") or ""),
        "canonical_text_sha256": str((manifest_row or {}).get("canonical_text_sha256") or ""),
    }


def _node_row(node: Any, doc_row: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "node_id": str(getattr(node, "node_id")),
        "doc_id": str(getattr(node, "doc_id")),
        "doc_title": str(getattr(node, "doc_title", "") or (doc_row or {}).get("doc_title") or ""),
        "source_file": str((doc_row or {}).get("source_file") or ""),
        "source_path": str((doc_row or {}).get("source_path") or ""),
        "source_version": str(getattr(node, "source_version", "current") or "current"),
        "kind": str(getattr(node, "kind", "") or ""),
        "label": str(getattr(node, "label", "") or ""),
        "title": str(getattr(node, "title", "") or ""),
        "parent_id": getattr(node, "parent_id", None),
        "prev_id": getattr(node, "prev_id", None),
        "next_id": getattr(node, "next_id", None),
        "ordinal": int(getattr(node, "ordinal", 0) or 0),
        "ref_path": str(getattr(node, "ref_path", "") or ""),
        "is_leaf": bool(getattr(node, "is_leaf", False)),
        "raw_span_start": int(getattr(node, "raw_span_start", 0) or 0),
        "raw_span_end": int(getattr(node, "raw_span_end", 0) or 0),
        "is_active": bool(getattr(node, "is_active", True)),
    }


def _edge_row(edge: Any) -> dict[str, Any]:
    return {
        "doc_id": str(getattr(edge, "doc_id")),
        "edge_type": str(getattr(edge, "edge_type", "") or ""),
        "from_node_id": str(getattr(edge, "from_node_id", "") or ""),
        "to_node_id": str(getattr(edge, "to_node_id", "") or ""),
        "source_version": str(getattr(edge, "source_version", "current") or "current"),
        "is_active": bool(getattr(edge, "is_active", True)),
        "confidence": float(getattr(edge, "confidence", 1.0) or 0.0),
    }


def _alias_row(alias: Any) -> dict[str, Any]:
    return {
        "doc_id": str(getattr(alias, "doc_id")),
        "alias": str(getattr(alias, "alias", "") or ""),
        "normalized_alias": str(getattr(alias, "normalized_alias", "") or ""),
        "target_node_id": str(getattr(alias, "target_node_id", "") or ""),
        "alias_level": str(getattr(alias, "alias_level", "") or ""),
        "confidence": float(getattr(alias, "confidence", 1.0) or 0.0),
        "source": str(getattr(alias, "source", "") or ""),
        "source_version": str(getattr(alias, "source_version", "current") or "current"),
        "is_active": bool(getattr(alias, "is_active", True)),
    }


def export_for_phase_a(
    *,
    docs: Sequence[Any],
    nodes: Sequence[Any],
    edges: Sequence[Any],
    aliases: Sequence[Any],
    manifest_rows: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
) -> dict[str, object]:
    manifest_lookup = dict(manifest_rows or {})
    document_rows = [
        _doc_row(doc, manifest_lookup.get((str(getattr(doc, "doc_id")), str(getattr(doc, "source_version", "current")))))
        for doc in sorted(docs, key=lambda item: (str(getattr(item, "doc_id")), str(getattr(item, "source_version", "current"))))
    ]
    doc_rows_by_key = {(row["doc_id"], row["source_version"]): row for row in document_rows}

    node_rows = [
        _node_row(
            node,
            doc_rows_by_key.get((str(getattr(node, "doc_id")), str(getattr(node, "source_version", "current")))),
        )
        for node in sorted(
            nodes,
            key=lambda item: (
                str(getattr(item, "doc_id")),
                str(getattr(item, "source_version", "current")),
                int(getattr(item, "ordinal", 0) or 0),
                str(getattr(item, "node_id")),
            ),
        )
    ]

    edge_rows = [
        _edge_row(edge)
        for edge in sorted(
            edges,
            key=lambda item: (
                str(getattr(item, "doc_id")),
                str(getattr(item, "source_version", "current")),
                str(getattr(item, "edge_type", "")),
                str(getattr(item, "from_node_id", "")),
                str(getattr(item, "to_node_id", "")),
            ),
        )
    ]

    alias_rows = [
        _alias_row(alias)
        for alias in sorted(
            aliases,
            key=lambda item: (
                str(getattr(item, "doc_id")),
                str(getattr(item, "source_version", "current")),
                str(getattr(item, "normalized_alias", "")),
                str(getattr(item, "target_node_id", "")),
                str(getattr(item, "alias_level", "")),
            ),
        )
    ]

    locator_rows = [
        {
            "node_id": row["node_id"],
            "doc_id": row["doc_id"],
            "source_version": row["source_version"],
            "ref_path": row["ref_path"],
            "raw_span_start": row["raw_span_start"],
            "raw_span_end": row["raw_span_end"],
            "canonical_text_path": str(
                doc_rows_by_key.get((str(row["doc_id"]), str(row["source_version"])), {}).get("canonical_text_path") or ""
            ),
            "canonical_text_sha256": str(
                doc_rows_by_key.get((str(row["doc_id"]), str(row["source_version"])), {}).get("canonical_text_sha256")
                or ""
            ),
        }
        for row in node_rows
    ]

    return {
        "documents": document_rows,
        "nodes": node_rows,
        "edges": edge_rows,
        "aliases": alias_rows,
        "locators": locator_rows,
    }


def write_phase_a_artifact_export(
    root: Path,
    *,
    docs: Sequence[Any],
    nodes: Sequence[Any],
    edges: Sequence[Any],
    aliases: Sequence[Any],
    manifest_rows: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
) -> Path:
    payload = export_for_phase_a(
        docs=docs,
        nodes=nodes,
        edges=edges,
        aliases=aliases,
        manifest_rows=manifest_rows or manifest_rows_from_root(root),
    )
    out_path = root / PHASE_A_ARTIFACT_EXPORT
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return out_path
