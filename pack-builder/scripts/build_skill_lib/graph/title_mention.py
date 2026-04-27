from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from typing import List

from ..utils.text import core_alias_title, normalize_alias_text
from ..types import EdgeRecord
from .utils import _ASCII_WORD_RE, ascii_alias_in_text


def build_title_mention_edges(conn: sqlite3.Connection, *, chunks: list[sqlite3.Row] | None = None) -> list[EdgeRecord]:
    targets = conn.execute(
        """
        SELECT node_id, title, doc_id
        FROM nodes
        WHERE is_active = 1 AND is_leaf = 1 AND kind <> 'chunk'
        ORDER BY doc_id, source_version, ordinal, node_id
        """
    ).fetchall()
    title_targets = []
    for row in targets:
        title = core_alias_title(str(row["title"] or ""))
        title_norm = normalize_alias_text(title)
        if len(title_norm) >= 2:
            title_targets.append((str(row["node_id"]), title, str(row["doc_id"]), title_norm))
    if not title_targets:
        return []

    # Group title targets by doc_id so each chunk only scans targets from the same doc.
    targets_by_doc: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for node_id, title, doc_id, title_norm in title_targets:
        targets_by_doc[doc_id].append((node_id, title, title_norm))

    if chunks is None:
        chunks = conn.execute(
            """
            SELECT n.node_id, n.doc_id, n.source_version, t.body_plain
            FROM nodes n
            JOIN node_text t ON t.node_key = n.node_key
            WHERE n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk'
            ORDER BY n.doc_id, n.source_version, n.ordinal, n.node_id
            """
        ).fetchall()

    edges: list[EdgeRecord] = []
    max_edges_per_doc = 2000
    doc_edge_counts: dict[str, int] = {}
    for chunk in chunks:
        body = str(chunk["body_plain"] or "")
        body_norm = normalize_alias_text(body)
        if not body_norm:
            continue
        chunk_id = str(chunk["node_id"])
        doc_id = str(chunk["doc_id"])
        if doc_edge_counts.get(doc_id, 0) >= max_edges_per_doc:
            continue
        for target_id, _title, title_norm in targets_by_doc.get(doc_id, []):
            if target_id == chunk_id:
                continue
            if title_norm not in body_norm:
                continue
            # K': ASCII-only titles need word-boundary check on original text.
            if _ASCII_WORD_RE.fullmatch(title_norm):
                if not ascii_alias_in_text(_title, body):
                    continue
            edges.append(
                EdgeRecord(
                    doc_id=doc_id,
                    edge_type="title_mention",
                    from_node_id=chunk_id,
                    to_node_id=target_id,
                    source_version=str(chunk["source_version"]),
                    is_active=True,
                    confidence=0.9,
                )
            )
            doc_edge_counts[doc_id] = doc_edge_counts.get(doc_id, 0) + 1
            if doc_edge_counts[doc_id] >= max_edges_per_doc:
                break
    return edges
