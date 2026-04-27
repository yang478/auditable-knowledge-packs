from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


BUILD_STATE_FILENAME = "build_state.json"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def stable_payload(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _empty_index_binding() -> dict[str, str]:
    return {
        "version": "",
        "binding_sha256": "",
    }


def empty_build_state() -> dict[str, Any]:
    return {
        "schema": "kbtool.build_state.v1",
        "artifact_version": "",
        "created_at": "",
        "corpus_manifest_sha256": "",
        "phase_contracts": {
            "phase_a_bundle_schema": "kbtool.bundle",
            "phase_b_export_schema": "kbtool.ir_export",
        },
        "documents": {},
        "indexes": {
            "sqlite": _empty_index_binding(),
            "fts": _empty_index_binding(),
            "aliases": _empty_index_binding(),
            "edges": _empty_index_binding(),
        },
        "model_registry_sha256": "",
    }


def index_binding_payload(name: str, rows: Sequence[object]) -> dict[str, str]:
    return {
        "version": f"{name}.v1",
        "binding_sha256": sha256_text(stable_payload(list(rows))),
    }


def export_sha_by_doc(payload: Mapping[str, Any]) -> dict[tuple[str, str], str]:
    grouped: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    for section in ("documents", "nodes", "edges", "aliases", "locators"):
        rows = payload.get(section)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("doc_id") or "").strip()
            source_version = str(row.get("source_version") or "current").strip() or "current"
            if not doc_id:
                continue
            grouped.setdefault(
                (doc_id, source_version),
                {"documents": [], "nodes": [], "edges": [], "aliases": [], "locators": []},
            )[section].append(row)
    return {key: sha256_text(stable_payload(value)) for key, value in grouped.items()}
