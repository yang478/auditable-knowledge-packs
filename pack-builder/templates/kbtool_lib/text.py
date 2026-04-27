"""
Runtime text utilities — re-exports from the shared tokenizer_core module.

tokenizer_core.py is the SINGLE source of truth for tokenization. It is
copied from the build tree during pack-builder generation. This module
re-exports everything so existing imports from kbtool_lib.text continue
to work unchanged.
"""

from __future__ import annotations

from .tokenizer_core import (
    # CJK
    is_cjk,
    tokenize_cjk_2gram,
    # FTS5
    fts_tokens,
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
