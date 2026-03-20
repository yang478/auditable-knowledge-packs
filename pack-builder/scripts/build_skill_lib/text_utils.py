from __future__ import annotations

import importlib.util
from types import ModuleType
from typing import Dict, List, Sequence

from . import templates_dir
from .fs_utils import die


_TEXT_MODULE: ModuleType | None = None


def _load_kbtool_text() -> ModuleType:
    """
    Single source of truth for tokenization/normalization.

    We reuse the runtime implementation from `templates/kbtool_lib/text.py` so that:
    - build-time indexing (FTS tokens)
    - build-time alias/reference extraction
    - runtime search query construction
    stay aligned and deterministic.
    """

    global _TEXT_MODULE
    if _TEXT_MODULE is not None:
        return _TEXT_MODULE

    text_path = templates_dir() / "kbtool_lib" / "text.py"
    if not text_path.exists() or not text_path.is_file():
        die(f"Missing template: {text_path} (pack-builder installation is incomplete)")

    spec = importlib.util.spec_from_file_location("pack_builder_kbtool_text", str(text_path))
    if spec is None or spec.loader is None:
        die(f"Failed to load template module: {text_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    _TEXT_MODULE = module
    return module


def is_cjk(ch: str) -> bool:
    return bool(_load_kbtool_text().is_cjk(ch))


def tokenize_cjk_2gram(text: str) -> List[str]:
    return list(_load_kbtool_text().tokenize_cjk_2gram(text))


def fts_tokens(text: str) -> str:
    value = _load_kbtool_text().fts_tokens(text)
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(value)
    return " ".join(str(v) for v in value)


def normalize_article_ref(label: str) -> str:
    return str(_load_kbtool_text().normalize_article_ref(label))


def normalize_alias_text(text: str) -> str:
    return str(_load_kbtool_text().normalize_alias_text(text))


def core_alias_title(title: str) -> str:
    return str(_load_kbtool_text().core_alias_title(title))


def markdown_to_plain(md: str) -> str:
    return str(_load_kbtool_text().markdown_to_plain(md))


def stable_hash(text: str) -> str:
    return str(_load_kbtool_text().stable_hash(text))


def node_key(node_id: str, source_version: str) -> str:
    return str(_load_kbtool_text().node_key(node_id, source_version))


def derive_source_version(name: str, title: str) -> str:
    return str(_load_kbtool_text().derive_source_version(name, title))


def build_match_query(tokens: Sequence[str], *, max_tokens: int = 64) -> str:
    return str(_load_kbtool_text().build_match_query(tokens, max_tokens=max_tokens))


def build_match_all(tokens: Sequence[str], *, max_tokens: int = 16) -> str:
    return str(_load_kbtool_text().build_match_all(tokens, max_tokens=max_tokens))


def query_terms(raw_query: str) -> List[str]:
    return list(_load_kbtool_text().query_terms(raw_query))


def build_match_expression(
    raw_query: str,
    *,
    query_mode: str,
    must_terms: Sequence[str],
    max_tokens: int = 64,
) -> str:
    return str(
        _load_kbtool_text().build_match_expression(
            raw_query,
            query_mode=query_mode,
            must_terms=must_terms,
            max_tokens=max_tokens,
        )
    )


def count_occurrences(haystack: str, needle: str) -> int:
    return int(_load_kbtool_text().count_occurrences(haystack, needle))


def extract_window(text: str, terms: Sequence[str], max_chars: int) -> str:
    return str(_load_kbtool_text().extract_window(text, terms, max_chars))


def parse_frontmatter(md: str) -> Dict[str, str]:
    return dict(_load_kbtool_text().parse_frontmatter(md))


def strip_frontmatter(md: str) -> str:
    return str(_load_kbtool_text().strip_frontmatter(md))

