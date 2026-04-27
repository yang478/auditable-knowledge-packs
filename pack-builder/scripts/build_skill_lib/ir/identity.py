from __future__ import annotations

import hashlib
import json


def _identity_digest(parts: list[object]) -> str:
    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def derive_span_id(doc_id: str, char_start: int, char_end: int) -> str:
    return _identity_digest([str(doc_id), int(char_start), int(char_end)])


def derive_node_id(doc_id: str, structural_role: str, ordinal: int) -> str:
    suffix = _identity_digest([str(doc_id), str(structural_role), int(ordinal)])
    return f"{doc_id}:{structural_role}:{suffix}"
