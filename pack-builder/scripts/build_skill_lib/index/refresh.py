from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..incremental.invalidation import ChangeSet


@dataclass(frozen=True)
class RefreshResult:
    dirty_doc_ids: tuple[str, ...]
    refreshed_indexes: tuple[str, ...]
    rewritten_rows: int
    full_rewrite_rows: int
    uses_atomic_activation: bool = True


def _document_row_footprint(doc_state: dict[str, Any]) -> int:
    span_count = len(doc_state.get("span_fingerprints", {})) if isinstance(doc_state.get("span_fingerprints"), dict) else 0
    node_count = len(doc_state.get("node_fingerprints", {})) if isinstance(doc_state.get("node_fingerprints"), dict) else 0
    return max(1, 1 + span_count + node_count)


def _refreshed_indexes(change_set: ChangeSet) -> tuple[str, ...]:
    refreshed: list[str] = []
    if change_set.rebuild_doc_ids or change_set.removed_doc_ids:
        refreshed.extend(["sqlite", "fts", "aliases", "edges"])
    elif change_set.metadata_only_doc_ids:
        refreshed.extend(["aliases", "edges"])
    return tuple(refreshed)


def incremental_reindex(previous_state: dict[str, Any], change_set: ChangeSet) -> RefreshResult:
    documents = previous_state.get("documents")
    document_map = documents if isinstance(documents, dict) else {}

    full_rewrite_rows = sum(_document_row_footprint(doc_state) for doc_state in document_map.values())
    full_rewrite_rows = max(1, full_rewrite_rows)

    dirty_doc_ids = sorted(change_set.rebuild_doc_ids or change_set.changed_doc_ids or change_set.removed_doc_ids)
    rewritten_rows = sum(_document_row_footprint(document_map.get(doc_id, {})) for doc_id in dirty_doc_ids)
    rewritten_rows = max(1, rewritten_rows) if dirty_doc_ids else 0

    return RefreshResult(
        dirty_doc_ids=tuple(dirty_doc_ids),
        refreshed_indexes=_refreshed_indexes(change_set),
        rewritten_rows=rewritten_rows,
        full_rewrite_rows=full_rewrite_rows,
        uses_atomic_activation=True,
    )


def should_refresh_indexes(previous_state: dict[str, Any], change_set: ChangeSet) -> bool:
    """判断是否需要刷新任何索引。"""
    return bool(
        change_set.rebuild_doc_ids
        or change_set.removed_doc_ids
        or change_set.metadata_only_doc_ids
        or _toolchain_changed(previous_state)
    )


def _toolchain_changed(previous_state: dict[str, Any]) -> bool:
    """检测 toolchain 是否发生变化（需要外部传入 current_checksum 做比较时扩展）。"""
    # 占位：实际比较在 build.py 中完成
    return False


def log_refresh_plan(result: RefreshResult) -> None:
    """打印增量刷新计划摘要。"""
    if not result.dirty_doc_ids:
        print("[incremental] No changes detected. Skipping rebuild.")
        return
    print(
        f"[incremental] {len(result.dirty_doc_ids)} changed document(s), "
        f"rewritten_rows={result.rewritten_rows}, full_rewrite_rows={result.full_rewrite_rows}, "
        f"indexes={result.refreshed_indexes}"
    )
