from __future__ import annotations

from types import ModuleType
from typing import Any, Mapping, Sequence

from .. import templates_dir
from .registry import load_template_module

_MODULE_CACHE: dict[str, ModuleType] = {}


def _load_module(name: str, rel_path: str) -> ModuleType:
    if name not in _MODULE_CACHE:
        module_path = templates_dir() / "kbtool_lib" / rel_path
        _MODULE_CACHE[name] = load_template_module(name, module_path)
    return _MODULE_CACHE[name]


def _load_artifact_contract_module() -> ModuleType:
    return _load_module("pack_builder_kbtool_artifact_contract", "artifact_contract.py")


def _load_state_contract_module() -> ModuleType:
    return _load_module("pack_builder_kbtool_state_contract", "state_contract.py")


# Artifact contract exports
PHASE_A_ARTIFACT_EXPORT = "phase_a_artifact.json"


def read_json(path: str) -> dict[str, Any]:
    result = _load_artifact_contract_module().read_json(path)
    if not isinstance(result, dict):
        raise TypeError(f"read_json expected dict, got {type(result).__name__}")
    return result


def manifest_rows_from_root(root):
    return dict(_load_artifact_contract_module().manifest_rows_from_root(root))


def export_for_phase_a(
    *,
    docs: Sequence[Any],
    nodes: Sequence[Any],
    edges: Sequence[Any],
    aliases: Sequence[Any],
    manifest_rows: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
) -> dict[str, object]:
    return dict(
        _load_artifact_contract_module().export_for_phase_a(
            docs=docs,
            nodes=nodes,
            edges=edges,
            aliases=aliases,
            manifest_rows=manifest_rows,
        )
    )


def write_phase_a_artifact_export(
    root,
    *,
    docs: Sequence[Any],
    nodes: Sequence[Any],
    edges: Sequence[Any],
    aliases: Sequence[Any],
    manifest_rows: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
):
    return _load_artifact_contract_module().write_phase_a_artifact_export(
        root,
        docs=docs,
        nodes=nodes,
        edges=edges,
        aliases=aliases,
        manifest_rows=manifest_rows,
    )


# State contract exports
BUILD_STATE_FILENAME = "build_state.json"


def stable_payload(value: object) -> str:
    return str(_load_state_contract_module().stable_payload(value))


def empty_build_state() -> dict[str, Any]:
    return dict(_load_state_contract_module().empty_build_state())


def index_binding_payload(name: str, rows: Sequence[object]) -> dict[str, str]:
    return dict(_load_state_contract_module().index_binding_payload(name, rows))


def export_sha_by_doc(payload: Mapping[str, Any]) -> dict[tuple[str, str], str]:
    return dict(_load_state_contract_module().export_sha_by_doc(payload))
