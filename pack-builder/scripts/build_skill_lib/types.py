from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from .text_utils import node_key, stable_hash


@dataclass(frozen=True)
class InputDoc:
    path: Path
    doc_id: str
    title: str
    source_version: str = "current"
    doc_hash: str = ""
    is_active: bool = True


@dataclass
class NodeRecord:
    node_id: str
    doc_id: str
    doc_title: str
    kind: str
    label: str
    title: str
    parent_id: Optional[str]
    prev_id: Optional[str]
    next_id: Optional[str]
    ordinal: int
    ref_path: str
    is_leaf: bool
    body_md: str
    body_plain: str
    source_version: str = "current"
    is_active: bool = True
    aliases: Tuple[str, ...] = ()
    raw_span_start: int = 0
    raw_span_end: int = 0
    node_hash: str = ""
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not self.raw_span_end:
            self.raw_span_end = max(1, len(self.body_md))
        if not self.node_hash:
            self.node_hash = stable_hash(self.body_md)

    @property
    def node_key(self) -> str:
        return node_key(self.node_id, self.source_version)


@dataclass(frozen=True)
class EdgeRecord:
    doc_id: str
    edge_type: str
    from_node_id: str
    to_node_id: str
    source_version: str
    is_active: bool = True
    confidence: float = 1.0


@dataclass(frozen=True)
class AliasRecord:
    doc_id: str
    alias: str
    normalized_alias: str
    target_node_id: str
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

