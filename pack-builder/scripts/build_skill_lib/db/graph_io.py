from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Sequence

from ..graph.builder import build_graph_edges
from ..types import EdgeRecord, NodeRecord


def append_graph_edges_to_db(
    db_path: Path,
    nodes: Sequence[NodeRecord],
    *,
    include_cooccurrence: bool = True,
    cooccurrence_min_shared: int = 3,
    cooccurrence_max_nodes_per_doc: int = 200,
    rebuild_doc_ids: set[str] | None = None,
) -> tuple[int, float]:
    from ..utils.safe_sqlite import open_db_wal
    started = time.perf_counter()
    conn = open_db_wal(db_path)
    try:
        edges = build_graph_edges(
            nodes,
            conn,
            include_cooccurrence=include_cooccurrence,
            cooccurrence_min_shared=cooccurrence_min_shared,
            cooccurrence_max_nodes_per_doc=cooccurrence_max_nodes_per_doc,
            rebuild_doc_ids=rebuild_doc_ids,
        )
        conn.executemany(
            """
            INSERT INTO edges(
                doc_id, edge_type, from_node_id, to_node_id, source_version, is_active, confidence
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(edge_type, from_node_id, to_node_id, source_version)
            DO UPDATE SET
                is_active = excluded.is_active,
                confidence = excluded.confidence
            """,
            [
                (
                    edge.doc_id,
                    edge.edge_type,
                    edge.from_node_id,
                    edge.to_node_id,
                    edge.source_version,
                    1 if edge.is_active else 0,
                    edge.confidence,
                )
                for edge in edges
            ],
        )
        conn.commit()
        return len(edges), time.perf_counter() - started
    finally:
        conn.close()
