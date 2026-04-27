from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from typing import List

from ..tokenizer_core import fts_tokens as fts_tokens_list
from ..types import EdgeRecord

logger = logging.getLogger(__name__)


def _feature_tokens(text: str, *, top_k: int) -> set[str]:
    tokens = fts_tokens_list(text)
    counts = Counter(token for token in tokens if len(token) >= 2 and not token.isdigit())
    return {token for token, _count in counts.most_common(top_k)}


def build_cooccurrence_edges(
    conn: sqlite3.Connection,
    *,
    top_k: int = 12,
    min_shared: int = 3,
    max_nodes_per_doc: int = 200,
    max_edges_per_doc: int = 5000,
    chunks: list[sqlite3.Row] | None = None,
) -> list[EdgeRecord]:
    rows = chunks if chunks is not None else conn.execute(
        """
        SELECT n.node_id, n.doc_id, n.source_version, t.body_plain
        FROM nodes n
        JOIN node_text t ON t.node_key = n.node_key
        WHERE n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk'
        ORDER BY n.doc_id, n.source_version, n.ordinal, n.node_id
        """
    ).fetchall()
    groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in rows:
        groups.setdefault((str(row["doc_id"]), str(row["source_version"])), []).append(row)

    edges: list[EdgeRecord] = []
    for (doc_id, source_version), group_rows in groups.items():
        limited_rows = group_rows[:max_nodes_per_doc]
        group_size = len(limited_rows)
        if group_size > 50:
            logger.info("Co-occurrence: processing %d chunks for doc %s", group_size, doc_id)

        # Pre-filter: only include nodes with enough tokens to possibly form an edge.
        raw_signatures = {
            str(row["node_id"]): _feature_tokens(str(row["body_plain"] or ""), top_k=top_k)
            for row in limited_rows
        }
        signatures = {
            nid: sig for nid, sig in raw_signatures.items()
            if len(sig) >= min_shared
        }
        node_ids = list(signatures.keys())

        # Build inverted index: token → list of node_ids that contain it
        inverted: dict[str, list[str]] = {}
        for nid in node_ids:
            for token in signatures.get(nid, set()):
                inverted.setdefault(token, []).append(nid)

        # Count shared tokens between pairs (via inverted index)
        shared_counts: dict[tuple[str, str], int] = {}
        for _token, nids in inverted.items():
            for i, nid_a in enumerate(nids):
                for nid_b in nids[i + 1 :]:
                    pair = (nid_a, nid_b) if nid_a < nid_b else (nid_b, nid_a)
                    shared_counts[pair] = shared_counts.get(pair, 0) + 1

        # Create bidirectional edges
        doc_edges: list[EdgeRecord] = []
        for (nid_a, nid_b), shared in shared_counts.items():
            if shared < min_shared:
                continue
            sig_a = signatures.get(nid_a, set())
            sig_b = signatures.get(nid_b, set())
            confidence = round(shared / max(len(sig_a), len(sig_b)), 4)
            doc_edges.append(
                EdgeRecord(
                    doc_id=doc_id,
                    edge_type="co_occurrence",
                    from_node_id=nid_a,
                    to_node_id=nid_b,
                    source_version=source_version,
                    is_active=True,
                    confidence=confidence,
                )
            )
            doc_edges.append(
                EdgeRecord(
                    doc_id=doc_id,
                    edge_type="co_occurrence",
                    from_node_id=nid_b,
                    to_node_id=nid_a,
                    source_version=source_version,
                    is_active=True,
                    confidence=confidence,
                )
            )

        # Cap edges per doc to avoid runaway output for very large documents.
        if len(doc_edges) > max_edges_per_doc:
            original_count = len(doc_edges)
            doc_edges.sort(key=lambda e: (-e.confidence, e.from_node_id, e.to_node_id))
            doc_edges = doc_edges[:max_edges_per_doc]
            logger.info(
                "Co-occurrence: capped %d edges to %d for doc %s",
                original_count, max_edges_per_doc, doc_id,
            )

        edges.extend(doc_edges)
    return edges
