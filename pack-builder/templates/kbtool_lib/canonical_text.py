from __future__ import annotations

import hashlib
import unicodedata
from typing import Sequence


def normalize_canonical_text(text: str) -> str:
    value = unicodedata.normalize("NFKC", text).replace("\r\n", "\n").replace("\r", "\n")
    if not value.endswith("\n"):
        value += "\n"
    return value


def canonical_text_from_markdown(text: str) -> str:
    return normalize_canonical_text(text)


def compose_canonical_document(title: str, sections: Sequence[str]) -> str:
    parts = []
    heading = str(title or "").strip()
    if heading:
        parts.append(f"# {heading}")
    for section in sections:
        block = str(section or "").strip()
        if block:
            parts.append(block)
    return normalize_canonical_text("\n\n".join(parts))


def canonical_text_sha256(text: str) -> str:
    return hashlib.sha256(normalize_canonical_text(text).encode("utf-8")).hexdigest()
