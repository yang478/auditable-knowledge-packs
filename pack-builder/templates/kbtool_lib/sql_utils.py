"""Small SQL utilities shared across kbtool_lib modules.

Placed in a separate module to avoid circular imports
(retrieval <-> memory).
"""
from __future__ import annotations

from typing import Sequence


def build_in_placeholders(items: Sequence[object]) -> str:
    """Build comma-separated '?' placeholders for SQL IN clauses.

    Prevents the repeated inline pattern ``",".join("?" for _ in items)``
    and centralizes empty-list guard logic.
    """
    return ",".join("?" for _ in items)
