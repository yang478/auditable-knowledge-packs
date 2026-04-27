"""
Build-time text utilities — thin proxy over the shared tokenizer_core module.

All tokenization functions delegate to tokenizer_core.py, which is the SINGLE
source of truth for both build-time (indexing) and runtime (query) behavior.
The same tokenizer_core.py is copied into generated skills during build.

See also: tokenizer_core.py for the actual implementations.
"""

from __future__ import annotations

from typing import Dict, List, Sequence

from ..tokenizer_core import (
    # CJK
    is_cjk,
    tokenize_cjk_2gram,
    # FTS5 — import list version, wrap to string below
    fts_tokens as _fts_tokens_list,
    build_match_query,
    build_match_all,
    build_match_expression,
    query_terms,
    # Text utilities
    count_occurrences,
    extract_window,
    # Markdown
    markdown_to_plain,
    parse_frontmatter,
    strip_frontmatter,
    # Hashing & identifiers
    stable_hash,
    node_key,
    derive_source_version,
    # Normalization
    normalize_article_ref,
    normalize_alias_text,
    core_alias_title,
    # Regex
    build_punctuation_tolerant_regex,
    # Keywords
    extract_keywords,
)


def fts_tokens(text: str) -> str:
    """Build-time wrapper: returns space-joined FTS tokens (string for SQLite).

    Runtime kbtool uses ``_fts_tokens_list`` directly (list form).
    Build-time callers (crud.py, cooccurrence.py) expect a string.
    """
    tokens = _fts_tokens_list(text)
    return " ".join(tokens)

# ---------------------------------------------------------------------------
# Canonical text helpers — these live in canonical_text_core but are proxied
# through here for backward compatibility
# ---------------------------------------------------------------------------

from types import ModuleType
from .. import templates_dir
from .registry import load_template_module

_CANONICAL_TEXT_MODULE: ModuleType | None = None


def _load_canonical_text_module() -> ModuleType:
    global _CANONICAL_TEXT_MODULE
    if _CANONICAL_TEXT_MODULE is not None:
        return _CANONICAL_TEXT_MODULE
    module_path = templates_dir() / "kbtool_lib" / "canonical_text.py"
    _CANONICAL_TEXT_MODULE = load_template_module("pack_builder_kbtool_canonical_text", module_path)
    return _CANONICAL_TEXT_MODULE


def normalize_canonical_text(text: str) -> str:
    return str(_load_canonical_text_module().normalize_canonical_text(text))


def canonical_text_from_markdown(text: str) -> str:
    return str(_load_canonical_text_module().canonical_text_from_markdown(text))


def compose_canonical_document(title: str, sections: list[str] | tuple[str, ...]) -> str:
    return str(_load_canonical_text_module().compose_canonical_document(title, sections))


def canonical_text_sha256(text: str) -> str:
    return str(_load_canonical_text_module().canonical_text_sha256(text))
