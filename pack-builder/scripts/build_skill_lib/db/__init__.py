from __future__ import annotations

import dataclasses
from dataclasses import replace
from typing import Callable, List, Sequence, TypeVar

from .crud import (
    write_kb_sqlite_db,
    incremental_update_kb_sqlite_db,
    read_existing_docs,
    read_existing_nodes,
    read_existing_edges,
    read_existing_aliases,
)
from .graph_io import append_graph_edges_to_db

T = TypeVar("T")


def merge_history(
    current_records: Sequence[T],
    rebuilt_records: Sequence[T],
    *,
    key_fn: Callable[[T], object],
    sort_key: Callable[[T], object],
) -> List[T]:
    """Merge rebuilt records into existing records with soft-delete semantics.

    Incremental build policy:
      - All rebuilt_records are emitted with is_active=True (new/current data).
      - Any current_records whose key_fn is NOT present in rebuilt_records
        are emitted with is_active=False (soft-deleted — e.g., a document
        that was removed from the input set between builds).
      - Any current_records whose key_fn IS present in rebuilt_records are
        fully replaced (not merged) — the rebuilt version wins.
      - Result is sorted by sort_key for deterministic output.

    This means:
      - Removing a file from --inputs marks its records inactive (not deleted).
      - Adding a new file adds its records as active.
      - Re-running with the same files is idempotent.
      - Historical data is preserved for audit trails, just deactivated.
    """
    if not rebuilt_records:
        return sorted([replace(r, is_active=False) for r in current_records], key=sort_key)
    sample = rebuilt_records[0]
    if not dataclasses.is_dataclass(sample):
        raise TypeError(f"merge_history requires dataclass instances, got {type(sample).__name__}")
    if not hasattr(sample, 'is_active'):
        raise TypeError(f"merge_history requires records with 'is_active' field")
    rebuilt_keys = {key_fn(record) for record in rebuilt_records}
    merged: List[T] = [replace(record, is_active=True) for record in rebuilt_records]  # type: ignore[arg-type]
    for record in current_records:
        if key_fn(record) in rebuilt_keys:
            continue
        merged.append(replace(record, is_active=False))  # type: ignore[arg-type]
    return sorted(merged, key=sort_key)


__all__ = [
    "write_kb_sqlite_db",
    "incremental_update_kb_sqlite_db",
    "read_existing_docs",
    "read_existing_nodes",
    "read_existing_edges",
    "read_existing_aliases",
    "merge_history",
    "append_graph_edges_to_db",
]
