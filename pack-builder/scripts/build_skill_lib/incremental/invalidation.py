from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ChangeSet:
    changed_doc_ids: set[str] = field(default_factory=set)
    unchanged_doc_ids: set[str] = field(default_factory=set)
    metadata_only_doc_ids: set[str] = field(default_factory=set)
    rebuild_doc_ids: set[str] = field(default_factory=set)
    removed_doc_ids: set[str] = field(default_factory=set)


def _find_previous_doc_state(previous_docs: dict[str, Any], doc_id: str, source_version: str) -> dict[str, Any] | None:
    """从新/旧两种 documents_state 结构中查找指定文档状态。"""
    if not isinstance(previous_docs, dict):
        return None
    doc_entry = previous_docs.get(doc_id)
    if not isinstance(doc_entry, dict):
        return None

    # 新结构：doc_id -> source_version -> state
    if source_version in doc_entry and isinstance(doc_entry[source_version], dict):
        state = doc_entry[source_version]
        if "source_fingerprint" in state:
            return state

    # 旧结构：doc_id -> state（单版本，source_version 被忽略）
    if "source_fingerprint" in doc_entry:
        return doc_entry

    # 另一种新结构兼容：doc_id -> {"source_versions": {source_version: state}}
    versions = doc_entry.get("source_versions")
    if isinstance(versions, dict):
        state = versions.get(source_version)
        if isinstance(state, dict):
            return state

    return None


def _diff_fingerprint_map(previous: Any, current: Any) -> set[str]:
    previous_map = previous if isinstance(previous, dict) else {}
    current_map = current if isinstance(current, dict) else {}
    changed: set[str] = set()
    for key in set(previous_map.keys()) | set(current_map.keys()):
        if previous_map.get(key) != current_map.get(key):
            changed.add(str(key))
    return changed



