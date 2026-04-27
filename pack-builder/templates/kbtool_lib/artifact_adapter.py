from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .runtime import open_db

PHASE_A_ARTIFACT_EXPORT = "phase_a_artifact.json"


@dataclass(frozen=True)
class ArtifactDocument:
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
    doc_hash: str
    source_version: str
    canonical_text_path: str
    canonical_text_sha256: str


@dataclass(frozen=True)
class ArtifactNode:
    node_id: str
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
    source_version: str
    kind: str
    label: str
    title: str
    parent_id: str | None
    prev_id: str | None
    next_id: str | None
    ordinal: int
    ref_path: str
    is_leaf: bool
    raw_span_start: int
    raw_span_end: int


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


def _is_active_row(row: dict[str, Any]) -> bool:
    value = row.get("is_active", row.get("active_version", True))
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no"}
    return bool(value)


def _manifest_rows_by_key(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    payload = _read_json(path)
    rows = payload.get("docs")
    if not isinstance(rows, list):
        return {}
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        doc_id = row.get("doc_id")
        source_version = row.get("source_version")
        if not isinstance(doc_id, str) or not isinstance(source_version, str):
            continue
        by_key[(doc_id, source_version)] = row
    return by_key


def _document_from_row(row: sqlite3.Row, manifest_row: dict[str, Any] | None) -> ArtifactDocument:
    return ArtifactDocument(
        doc_id=str(row["doc_id"]),
        doc_title=str(row["doc_title"]),
        source_file=str(row["source_file"]),
        source_path=str(row["source_path"]),
        doc_hash=str(row["doc_hash"]),
        source_version=str(row["source_version"]),
        canonical_text_path=str((manifest_row or {}).get("canonical_text_path") or ""),
        canonical_text_sha256=str((manifest_row or {}).get("canonical_text_sha256") or ""),
    )


def _document_from_export_row(row: dict[str, Any]) -> ArtifactDocument:
    return ArtifactDocument(
        doc_id=str(row["doc_id"]),
        doc_title=str(row.get("doc_title") or ""),
        source_file=str(row.get("source_file") or ""),
        source_path=str(row.get("source_path") or ""),
        doc_hash=str(row.get("doc_hash") or ""),
        source_version=str(row.get("source_version") or "current"),
        canonical_text_path=str(row.get("canonical_text_path") or ""),
        canonical_text_sha256=str(row.get("canonical_text_sha256") or ""),
    )


def _node_from_row(row: sqlite3.Row) -> ArtifactNode:
    return ArtifactNode(
        node_id=str(row["node_id"]),
        doc_id=str(row["doc_id"]),
        doc_title=str(row["doc_title"]),
        source_file=str(row["source_file"]),
        source_path=str(row["source_path"]),
        source_version=str(row["source_version"]),
        kind=str(row["kind"]),
        label=str(row["label"]),
        title=str(row["title"]),
        parent_id=str(row["parent_id"]) if row["parent_id"] else None,
        prev_id=str(row["prev_id"]) if row["prev_id"] else None,
        next_id=str(row["next_id"]) if row["next_id"] else None,
        ordinal=int(row["ordinal"]),
        ref_path=str(row["ref_path"]),
        is_leaf=bool(row["is_leaf"]),
        raw_span_start=int(row["raw_span_start"]),
        raw_span_end=int(row["raw_span_end"]),
    )


def _node_from_export_row(row: dict[str, Any]) -> ArtifactNode:
    return ArtifactNode(
        node_id=str(row["node_id"]),
        doc_id=str(row["doc_id"]),
        doc_title=str(row.get("doc_title") or ""),
        source_file=str(row.get("source_file") or ""),
        source_path=str(row.get("source_path") or ""),
        source_version=str(row.get("source_version") or "current"),
        kind=str(row.get("kind") or ""),
        label=str(row.get("label") or ""),
        title=str(row.get("title") or ""),
        parent_id=str(row["parent_id"]) if row.get("parent_id") else None,
        prev_id=str(row["prev_id"]) if row.get("prev_id") else None,
        next_id=str(row["next_id"]) if row.get("next_id") else None,
        ordinal=int(row.get("ordinal") or 0),
        ref_path=str(row.get("ref_path") or ""),
        is_leaf=bool(row.get("is_leaf")),
        raw_span_start=int(row.get("raw_span_start") or 0),
        raw_span_end=int(row.get("raw_span_end") or 0),
    )


def _choose_latest_rows(rows: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(key_name) or "")
        if not key:
            continue
        current = selected.get(key)
        if current is None:
            selected[key] = row
            continue
        candidate_priority = (_is_active_row(row), str(row.get("source_version") or ""))
        current_priority = (_is_active_row(current), str(current.get("source_version") or ""))
        if candidate_priority > current_priority:
            selected[key] = row
    return selected


def _load_export_indexes(
    root: Path,
) -> tuple[dict[str, ArtifactDocument], dict[str, ArtifactNode], dict[str, list[ArtifactNode]]] | None:
    payload = _read_json(root / PHASE_A_ARTIFACT_EXPORT)
    document_rows = payload.get("documents")
    node_rows = payload.get("nodes")
    if not isinstance(document_rows, list) or not isinstance(node_rows, list):
        return None

    document_dict_rows = [row for row in document_rows if isinstance(row, dict)]
    node_dict_rows = [row for row in node_rows if isinstance(row, dict)]
    if not document_dict_rows and not node_dict_rows:
        return None

    selected_docs = _choose_latest_rows(document_dict_rows, "doc_id")
    selected_nodes = _choose_latest_rows(node_dict_rows, "node_id")
    documents_by_id = {doc_id: _document_from_export_row(row) for doc_id, row in selected_docs.items()}
    nodes_by_id = {node_id: _node_from_export_row(row) for node_id, row in selected_nodes.items()}

    children_by_parent: dict[str, list[ArtifactNode]] = {}
    for row in node_dict_rows:
        if not _is_active_row(row):
            continue
        parent_id = row.get("parent_id")
        if not parent_id:
            continue
        node_id = str(row.get("node_id") or "")
        node = nodes_by_id.get(node_id)
        if node is None:
            continue
        children_by_parent.setdefault(str(parent_id), []).append(node)
    for parent_id in list(children_by_parent.keys()):
        children_by_parent[parent_id] = sorted(
            children_by_parent[parent_id],
            key=lambda node: (node.ordinal, node.node_id),
        )

    return documents_by_id, nodes_by_id, children_by_parent


class ArtifactAdapter:
    def __init__(
        self,
        *,
        root: Path,
        conn: sqlite3.Connection | None,
        manifest_rows: dict[tuple[str, str], dict[str, Any]],
        documents_by_id: dict[str, ArtifactDocument] | None = None,
        nodes_by_id: dict[str, ArtifactNode] | None = None,
        children_by_parent: dict[str, list[ArtifactNode]] | None = None,
    ) -> None:
        self.root = root
        self.conn = conn
        self._manifest_rows = manifest_rows
        self._documents_by_id = documents_by_id or {}
        self._nodes_by_id = nodes_by_id or {}
        self._children_by_parent = children_by_parent or {}

    @classmethod
    def from_root(cls, root: Path) -> "ArtifactAdapter":
        root_path = Path(root).resolve()
        export_indexes = _load_export_indexes(root_path)
        if export_indexes is not None:
            documents_by_id, nodes_by_id, children_by_parent = export_indexes
            return cls(
                root=root_path,
                conn=None,
                manifest_rows={},
                documents_by_id=documents_by_id,
                nodes_by_id=nodes_by_id,
                children_by_parent=children_by_parent,
            )
        return cls(
            root=root_path,
            conn=open_db(root_path / "kb.sqlite"),
            manifest_rows=_manifest_rows_by_key(root_path / "corpus_manifest.json"),
        )

    def get_document(self, doc_id: str) -> ArtifactDocument:
        if self._documents_by_id:
            document = self._documents_by_id.get(doc_id)
            if document is None:
                raise KeyError(f"Unknown document: {doc_id}")
            return document
        assert self.conn is not None
        row = self.conn.execute(
            """
            SELECT doc_id, doc_title, source_file, source_path, doc_hash, source_version
            FROM docs
            WHERE doc_id = ? AND is_active = 1
            ORDER BY source_version DESC
            LIMIT 1
            """,
            (doc_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown document: {doc_id}")
        manifest_row = self._manifest_rows.get((str(row["doc_id"]), str(row["source_version"])))
        return _document_from_row(row, manifest_row)

    def get_node(self, node_id: str) -> ArtifactNode:
        if self._nodes_by_id:
            node = self._nodes_by_id.get(node_id)
            if node is None:
                raise KeyError(f"Unknown node: {node_id}")
            return node
        assert self.conn is not None
        row = self.conn.execute(
            """
            SELECT
              n.node_id, n.doc_id, d.doc_title, d.source_file, d.source_path, n.source_version,
              n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id,
              n.ordinal, n.ref_path, n.is_leaf, n.raw_span_start, n.raw_span_end
            FROM nodes n
            JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
            WHERE n.node_id = ? AND n.is_active = 1 AND d.is_active = 1
            ORDER BY n.source_version DESC
            LIMIT 1
            """,
            (node_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown node: {node_id}")
        return _node_from_row(row)

    def iter_children(self, node_id: str) -> list[ArtifactNode]:
        if self._children_by_parent:
            return list(self._children_by_parent.get(node_id, []))
        assert self.conn is not None
        rows = self.conn.execute(
            """
            SELECT
              n.node_id, n.doc_id, d.doc_title, d.source_file, d.source_path, n.source_version,
              n.kind, n.label, n.title, n.parent_id, n.prev_id, n.next_id,
              n.ordinal, n.ref_path, n.is_leaf, n.raw_span_start, n.raw_span_end
            FROM nodes n
            JOIN docs d ON d.doc_id = n.doc_id AND d.source_version = n.source_version
            WHERE n.parent_id = ? AND n.is_active = 1 AND d.is_active = 1
            ORDER BY n.ordinal ASC, n.node_id ASC
            """,
            (node_id,),
        ).fetchall()
        return [_node_from_row(row) for row in rows]
