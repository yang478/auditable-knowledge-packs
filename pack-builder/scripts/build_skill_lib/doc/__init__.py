from __future__ import annotations

from .references import generate_doc, generate_doc_from_ir, HeadingRow
from .aliases import extract_alias_rows, ALIAS_EXACT, ALIAS_ABBREVIATION, ALIAS_SOFT
from .reference_edges import extract_reference_edges, relation_edge_to_edge_record

__all__ = [
    "generate_doc",
    "generate_doc_from_ir",
    "HeadingRow",
    "extract_alias_rows",
    "ALIAS_EXACT",
    "ALIAS_ABBREVIATION",
    "ALIAS_SOFT",
    "extract_reference_edges",
    "relation_edge_to_edge_record",
]
