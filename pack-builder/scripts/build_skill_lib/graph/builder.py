from __future__ import annotations

import sqlite3
from typing import Sequence

from .alias_mention import build_alias_mention_edges
from .cooccurrence import build_cooccurrence_edges
from .title_mention import build_title_mention_edges
from ..types import EdgeRecord, NodeRecord


def build_structure_edges(nodes: Sequence[NodeRecord]) -> list[EdgeRecord]:
    active_node_ids = {node.node_id for node in nodes if node.is_active}
    edges: list[EdgeRecord] = []
    for node in nodes:
        if not node.is_active:
            continue
        for edge_type, target_id in (
            ("parent", node.parent_id),
            ("prev", node.prev_id),
            ("next", node.next_id),
        ):
            if target_id and target_id in active_node_ids:
                edges.append(
                    EdgeRecord(
                        doc_id=node.doc_id,
                        edge_type=edge_type,
                        from_node_id=node.node_id,
                        to_node_id=target_id,
                        source_version=node.source_version,
                        is_active=True,
                        confidence=1.0,
                    )
                )
    return edges


def _active_chunk_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT n.node_id, n.doc_id, n.source_version, t.body_plain
        FROM nodes n
        JOIN node_text t ON t.node_key = n.node_key
        WHERE n.is_active = 1 AND n.is_leaf = 1 AND n.kind = 'chunk'
        ORDER BY n.doc_id, n.source_version, n.ordinal, n.node_id
        """
    ).fetchall()


def build_graph_edges(
    nodes: Sequence[NodeRecord],
    conn: sqlite3.Connection,
    *,
    include_cooccurrence: bool = True,
    cooccurrence_min_shared: int = 3,
    cooccurrence_max_nodes_per_doc: int = 200,
    rebuild_doc_ids: set[str] | None = None,
) -> list[EdgeRecord]:
    # When doing incremental rebuilds, only scan changed docs.
    if rebuild_doc_ids is not None:
        nodes = [n for n in nodes if n.doc_id in rebuild_doc_ids]

    # Fetch active chunks once, shared across all edge builders.
    chunks = _active_chunk_rows(conn)

    # Filter chunks by rebuild_doc_ids for incremental builds.
    if rebuild_doc_ids is not None:
        chunks = [c for c in chunks if str(c["doc_id"]) in rebuild_doc_ids]

    edges: list[EdgeRecord] = []
    edges.extend(build_structure_edges(nodes))
    edges.extend(build_alias_mention_edges(conn, chunks=chunks))
    edges.extend(build_title_mention_edges(conn, chunks=chunks))
    if include_cooccurrence:
        edges.extend(
            build_cooccurrence_edges(
                conn,
                min_shared=cooccurrence_min_shared,
                max_nodes_per_doc=cooccurrence_max_nodes_per_doc,
                chunks=chunks,
            )
        )
    return dedupe_edges(edges)


def dedupe_edges(edges: Sequence[EdgeRecord]) -> list[EdgeRecord]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[EdgeRecord] = []
    for edge in edges:
        key = (edge.edge_type, edge.from_node_id, edge.to_node_id, edge.source_version)
        if key in seen:
            continue
        seen.add(key)
        out.append(edge)
    return out
