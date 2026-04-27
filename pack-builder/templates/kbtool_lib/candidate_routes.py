from __future__ import annotations

from typing import Iterable


ROUTE_PRECEDENCE = [
    "structural_exact",
    "title_exact",
    "alias_exact",
    "alias_abbreviation",
    "body_text",
    "alias_soft",
    "edge_expanded",
]


def _extend_unique(out: list[str], seen: set[str], values: Iterable[str]) -> None:
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)


def merge_route_candidates(
    *,
    structural: Iterable[str] = (),
    title: Iterable[str] = (),
    alias: Iterable[str] = (),
    alias_abbreviation: Iterable[str] = (),
    body: Iterable[str] = (),
    alias_soft: Iterable[str] = (),
    edge_expanded: Iterable[str] = (),
) -> list[str]:
    ranked: list[str] = []
    seen: set[str] = set()
    route_inputs = {
        "structural_exact": structural,
        "title_exact": title,
        "alias_exact": alias,
        "alias_abbreviation": alias_abbreviation,
        "body_text": body,
        "alias_soft": alias_soft,
        "edge_expanded": edge_expanded,
    }
    for route_name in ROUTE_PRECEDENCE:
        _extend_unique(ranked, seen, route_inputs[route_name])
    return ranked
