from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Sequence, Set

from ..utils.node_io import leaf_haystack_plain
from ..utils.text import normalize_alias_text, normalize_article_ref
from ..types import EdgeRecord, NodeRecord


_REFERENCE_PATTERNS = (
    re.compile(r"参见第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
    re.compile(r"依据第\s*([0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?)\s*[条條]"),
)


def extract_reference_edges(nodes: Sequence[NodeRecord], *, base_dir: Optional[Path] = None) -> List[EdgeRecord]:
    article_targets = {
        (node.doc_id, normalize_article_ref(node.label)): node.node_id
        for node in nodes
        if node.kind == "article"
    }
    edges: Set[EdgeRecord] = set()
    for node in nodes:
        if not node.is_leaf:
            continue
        haystack = leaf_haystack_plain(base_dir, node)
        if not haystack:
            continue
        for pattern in _REFERENCE_PATTERNS:
            for match in pattern.finditer(haystack):
                label = normalize_article_ref(f"第{match.group(1)}条")
                target_node_id = article_targets.get((node.doc_id, label))
                if not target_node_id or target_node_id == node.node_id:
                    continue
                edges.add(
                    EdgeRecord(
                        doc_id=node.doc_id,
                        edge_type="references",
                        from_node_id=node.node_id,
                        to_node_id=target_node_id,
                        source_version=node.source_version,
                        is_active=node.is_active,
                        confidence=1.0,
                    )
                )
    return sorted(edges, key=lambda row: (row.doc_id, row.source_version, row.edge_type, row.from_node_id, row.to_node_id))


def relation_edge_to_edge_record(
    edge,
    *,
    source_version: str = "current",
    is_active: bool = True,
    confidence: float = 1.0,
) -> EdgeRecord:
    return EdgeRecord(
        doc_id=str(edge.doc_id),
        edge_type=str(edge.edge_type),
        from_node_id=str(edge.from_node_id),
        to_node_id=str(edge.to_node_id),
        source_version=str(source_version),
        is_active=bool(is_active),
        confidence=float(confidence),
    )
