from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, Optional, Tuple, TypeAlias

from .tokenizer_core import KNOWN_NODE_KINDS
from .utils.text import node_key, stable_hash

# ---------- 类型别名 ----------
NodeId: TypeAlias = str
DocId: TypeAlias = str
AliasText: TypeAlias = str


class HeadingRow(NamedTuple):
    title: str
    doc_id: str
    doc_title: str
    kind: str
    node_id: str
    ref_path: str


@dataclass(frozen=True)
class InputDoc:
    path: Path
    doc_id: DocId
    title: str
    source_version: str = "current"
    doc_hash: str = ""
    active_parser: str = ""
    is_active: bool = True


@dataclass
class NodeRecord:
    node_id: NodeId
    doc_id: DocId
    doc_title: str
    kind: str
    label: str
    title: str
    parent_id: Optional[NodeId]
    prev_id: Optional[NodeId]
    next_id: Optional[NodeId]
    ordinal: int
    ref_path: str
    is_leaf: bool
    body_md: str
    body_plain: str
    source_version: str = "current"
    is_active: bool = True
    aliases: Tuple[AliasText, ...] = ()
    raw_span_start: int = 0
    raw_span_end: int = 0
    node_hash: str = ""
    confidence: float = 1.0
    heading_path: str = ""

    def __post_init__(self) -> None:
        if self.kind not in KNOWN_NODE_KINDS:
            warnings.warn(
                f"NodeRecord kind={self.kind!r} not in KNOWN_NODE_KINDS. "
                f"Valid kinds: {sorted(KNOWN_NODE_KINDS)}",
                UserWarning,
                stacklevel=2,
            )
        if self.raw_span_end == 0:
            self.raw_span_end = len(self.body_md)
        if not self.node_hash:
            self.node_hash = stable_hash(self.body_md)

    @property
    def node_key(self) -> str:
        return node_key(self.node_id, self.source_version)


@dataclass(frozen=True)
class EdgeRecord:
    doc_id: DocId
    edge_type: str
    from_node_id: NodeId
    to_node_id: NodeId
    source_version: str
    is_active: bool = True
    confidence: float = 1.0


@dataclass(frozen=True)
class AliasRecord:
    doc_id: DocId
    alias: AliasText
    normalized_alias: AliasText
    target_node_id: NodeId
    alias_level: str
    confidence: float
    source: str
    source_version: str
    is_active: bool = True


@dataclass
class Heading:
    level: int
    title: str
    line_index: int


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


@dataclass(frozen=True)
class AtomicSpan:
    doc_id: str
    span_id: str
    char_start: int
    char_end: int
    reading_order: int
