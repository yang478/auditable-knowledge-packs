from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from typing import List

from ..utils.text import normalize_alias_text
from ..types import EdgeRecord
from .utils import _ASCII_WORD_RE, ascii_alias_in_text


def build_alias_mention_edges(conn: sqlite3.Connection, *, chunks: list[sqlite3.Row] | None = None) -> list[EdgeRecord]:
    aliases = conn.execute(
        """
        SELECT doc_id, alias, normalized_alias, target_node_id, source_version, confidence
        FROM aliases
        WHERE is_active = 1
        ORDER BY doc_id, source_version, normalized_alias, target_node_id
        """
    ).fetchall()
    if not aliases:
        return []

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

    body_norms = {
        str(chunk["node_id"]): normalize_alias_text(str(chunk["body_plain"] or ""))
        for chunk in chunks
    }
    body_originals = {
        str(chunk["node_id"]): str(chunk["body_plain"] or "")
        for chunk in chunks
    }

    # Group aliases by doc_id so each chunk only scans aliases from the same doc.
    aliases_by_doc: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for alias in aliases:
        aliases_by_doc[str(alias["doc_id"])].append(alias)

    edges: list[EdgeRecord] = []
    max_edges_per_doc = 2000
    doc_edge_counts: dict[str, int] = {}
    for chunk in chunks:
        node_id = str(chunk["node_id"])
        body_norm = body_norms.get(node_id, "")
        if not body_norm:
            continue
        doc_id = str(chunk["doc_id"])
        if doc_edge_counts.get(doc_id, 0) >= max_edges_per_doc:
            continue
        for alias in aliases_by_doc.get(doc_id, []):
            target_id = str(alias["target_node_id"])
            normalized_alias = str(alias["normalized_alias"] or "")
            if target_id == node_id or len(normalized_alias) < 2:
                continue
            if normalized_alias not in body_norm:
                continue
            # K': ASCII-only aliases need word-boundary check on original text.
            if _ASCII_WORD_RE.fullmatch(normalized_alias):
                original_alias = str(alias["alias"] or "")
                if not ascii_alias_in_text(original_alias, body_originals.get(node_id, "")):
                    continue
            edges.append(
                EdgeRecord(
                    doc_id=doc_id,
                    edge_type="alias_mention",
                    from_node_id=node_id,
                    to_node_id=target_id,
                    source_version=str(chunk["source_version"]),
                    is_active=True,
                    confidence=float(alias["confidence"] or 1.0),
                )
            )
            doc_edge_counts[doc_id] = doc_edge_counts.get(doc_id, 0) + 1
            if doc_edge_counts[doc_id] >= max_edges_per_doc:
                break
    return edges
