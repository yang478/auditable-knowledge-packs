from __future__ import annotations

import json
import math
from typing import Any


def _round_floats(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return round(value, 6)
        return value
    if isinstance(value, list):
        return [_round_floats(item) for item in value]
    if isinstance(value, tuple):
        return [_round_floats(item) for item in value]
    if isinstance(value, dict):
        return {key: _round_floats(item) for key, item in value.items()}
    return value


def to_canonical_json(payload: object) -> str:
    return json.dumps(
        _round_floats(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
