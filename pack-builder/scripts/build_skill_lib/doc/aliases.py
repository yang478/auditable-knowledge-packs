from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Sequence

from ..utils.node_io import leaf_haystack_plain
from ..utils.text import core_alias_title, normalize_alias_text
from ..types import AliasRecord, NodeRecord


ALIAS_EXACT = "exact"
ALIAS_ABBREVIATION = "abbreviation"
ALIAS_SOFT = "soft"


def extract_alias_rows(nodes: Sequence[NodeRecord], *, base_dir: Optional[Path] = None) -> List[AliasRecord]:
    # Deduplicate by the SQLite PK to avoid IntegrityError when multiple sources generate the
    # same (normalized_alias, target_node_id, alias_level, source_version) tuple.
    rows_by_key: dict[tuple[str, str, str, str], AliasRecord] = {}

    source_priority = {
        "frontmatter_alias": 0,
        "title": 1,
        "body_alias": 2,
        "title_abbreviation": 3,
    }

    def upsert(row: AliasRecord) -> None:
        key = (row.normalized_alias, row.target_node_id, row.alias_level, row.source_version)
        cur = rows_by_key.get(key)
        if cur is None:
            rows_by_key[key] = row
            return
        if row.confidence > cur.confidence + 1e-9:
            rows_by_key[key] = row
        elif abs(row.confidence - cur.confidence) <= 1e-9:
            a = (source_priority.get(row.source, 99), len(row.alias), row.alias)
            b = (source_priority.get(cur.source, 99), len(cur.alias), cur.alias)
            if a < b:
                rows_by_key[key] = row

    for node in nodes:
        if not node.is_leaf:
            continue
        for alias in node.aliases:
            normalized_alias = normalize_alias_text(alias)
            if normalized_alias:
                upsert(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=alias,
                        normalized_alias=normalized_alias,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_EXACT,
                        confidence=1.0,
                        source="frontmatter_alias",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )
        if node.kind == "chunk":
            # Occam chunking: chunk titles are mechanical, but explicit frontmatter aliases above are useful.
            continue
        core_title = core_alias_title(node.title)
        if not core_title:
            continue
        normalized_title = normalize_alias_text(core_title)
        if normalized_title:
            upsert(
                AliasRecord(
                    doc_id=node.doc_id,
                    alias=core_title,
                    normalized_alias=normalized_title,
                    target_node_id=node.node_id,
                    alias_level=ALIAS_EXACT,
                    confidence=1.0,
                    source="title",
                    source_version=node.source_version,
                    is_active=node.is_active,
                )
            )

        if core_title.endswith("期限") and len(core_title) >= 6:
            abbreviation = core_title[0] + core_title[2] + core_title[-2]
            normalized_abbreviation = normalize_alias_text(abbreviation)
            if normalized_abbreviation:
                upsert(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=abbreviation,
                        normalized_alias=normalized_abbreviation,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_ABBREVIATION,
                        confidence=0.92,
                        source="title_abbreviation",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )

        haystack = leaf_haystack_plain(base_dir, node)
        if not haystack:
            continue
        for match in re.finditer(r'(?:简称|以下简称)[""]?([^""""、，。；;]{2,12})[""""]?', haystack):
            alias = match.group(1).strip()
            normalized_alias = normalize_alias_text(alias)
            if normalized_alias:
                upsert(
                    AliasRecord(
                        doc_id=node.doc_id,
                        alias=alias,
                        normalized_alias=normalized_alias,
                        target_node_id=node.node_id,
                        alias_level=ALIAS_EXACT,
                        confidence=0.98,
                        source="body_alias",
                        source_version=node.source_version,
                        is_active=node.is_active,
                    )
                )
    return sorted(
        rows_by_key.values(),
        key=lambda row: (row.doc_id, row.source_version, row.normalized_alias, row.target_node_id, row.alias_level),
    )
