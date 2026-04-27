from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, TypeAlias


# ---------- 类型别名 ----------
NodeId: TypeAlias = str
DocId: TypeAlias = str
AliasText: TypeAlias = str
HeadingRow: TypeAlias = Tuple[str, str, str, str, str, str]


@dataclass(frozen=True)
class NormalizedQuery:
    query_raw: str
    query_normalized: str
    article_terms: list[str]
    title_terms: list[str]
    alias_terms: list[str]


@dataclass(frozen=True)
class SearchHit:
    node_key: str
    node_id: NodeId
    doc_id: DocId
    title: str
    ref_path: str
    score: float
    snippet: str = ""
    source_version: str = "current"


@dataclass
class SearchResult:
    hits: list[SearchHit]
    total: int
    query: str
    elapsed_ms: float = 0.0
    query_mode: str = ""
