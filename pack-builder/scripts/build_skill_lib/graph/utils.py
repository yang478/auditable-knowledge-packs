from __future__ import annotations

import re

from ..tokenizer_core import _ASCII_WORD_RE


def ascii_alias_in_text(alias: str, text: str) -> bool:
    """Return True if *alias* appears as a whole word in *text* (original text)."""
    if not alias or not text:
        return False
    pattern = r"(?<![A-Za-z0-9_])" + re.escape(alias) + r"(?![A-Za-z0-9_])"
    return bool(re.search(pattern, text, re.IGNORECASE))
