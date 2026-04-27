from __future__ import annotations

import re
import unicodedata

from .types import NormalizedQuery


_CN_NUMERAL_MAP = {"零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
_CN_UNIT_MAP = {"十": 10, "百": 100, "千": 1000}
_PUNCTUATION_MAP = str.maketrans(
    {
        "？": "?",
        "，": ",",
        "。": ".",
        "：": ":",
        "；": ";",
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
    }
)


def _cn_numeral_to_int(text: str) -> int:
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    total = 0
    current = 0
    for ch in text:
        if ch in _CN_NUMERAL_MAP:
            current = _CN_NUMERAL_MAP[ch]
            continue
        unit = _CN_UNIT_MAP.get(ch)
        if unit is None:
            continue
        total += (current or 1) * unit
        current = 0
    return total + current


def _normalize_punctuation(text: str) -> str:
    normalized = text.translate(_PUNCTUATION_MAP)
    normalized = re.sub(r"\s+([?!,:;.%])", r"\1", normalized)
    normalized = re.sub(r"([([{])\s+", r"\1", normalized)
    normalized = re.sub(r"\s+([)\]}])", r"\1", normalized)
    return normalized


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text).strip()
    normalized = "".join(ch.lower() if ord(ch) < 128 else ch for ch in normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return _normalize_punctuation(normalized)


def _query_terms(text: str) -> list[str]:
    if not text:
        return []
    return [part for part in re.split(r"\s+", text) if part]


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def normalize_query(raw_query: str) -> NormalizedQuery:
    query_raw = str(raw_query)
    query_normalized = _normalize_text(query_raw)

    article_terms: list[str] = []
    for match in re.finditer(r"第\s*([0-9一二三四五六七八九十百千]+)\s*[条條]", query_normalized):
        article_num = _cn_numeral_to_int(match.group(1))
        if article_num > 0:
            article_terms.append(f"第{article_num}条")

    title_fragment = re.sub(r"第\s*[0-9一二三四五六七八九十百千]+\s*[条條]", " ", query_normalized)
    title_fragment = title_fragment.replace("是什么", " ").replace("什么", " ")
    title_fragment = re.sub(r"[的与和]\s*", " ", title_fragment).strip()
    title_terms = _query_terms(title_fragment) if title_fragment else []
    if not title_terms and query_normalized:
        title_terms = [query_normalized]

    alias_terms = _unique([query_normalized, *title_terms])
    return NormalizedQuery(
        query_raw=query_raw,
        query_normalized=query_normalized,
        article_terms=_unique(article_terms),
        title_terms=title_terms,
        alias_terms=alias_terms,
    )
