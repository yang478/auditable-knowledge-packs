from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from ..types import AliasRecord, EdgeRecord, NodeRecord


def sha256_text(text: str) -> str:
    """对文本内容做 SHA-256 摘要（UTF-8 编码）。"""
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    """对原始字节做 SHA-256 摘要。"""
    return hashlib.sha256(data).hexdigest()


def fingerprint_summary(source_bytes: str, extracted_text: str, span_payload: str, node_payload: str) -> dict[str, Any]:
    return {
        "source_fingerprint": sha256_text(source_bytes),
        "extracted_text_fingerprint": sha256_text(extracted_text),
        "span_fingerprint": sha256_text(span_payload),
        "node_fingerprint": sha256_text(node_payload),
    }


def source_fingerprint_for_path(path: Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def extracted_text_fingerprint_for_path(path: Path, *, pdf_fallback: str = "none") -> str:
    from ..extract import extract_to_markdown
    from ..utils.text import canonical_text_from_markdown

    markdown = extract_to_markdown(Path(path), pdf_fallback=pdf_fallback)
    return sha256_text(canonical_text_from_markdown(markdown))


def source_fingerprint(path: Path, fallback: str) -> str:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError:
        return sha256_text(fallback)


def node_fingerprint(node: NodeRecord | Any) -> str:
    from ..utils.contract import stable_payload

    return sha256_text(
        stable_payload(
            {
                "node_id": node.node_id,
                "kind": node.kind,
                "label": node.label,
                "title": node.title,
                "parent_id": node.parent_id,
                "prev_id": node.prev_id,
                "next_id": node.next_id,
                "ordinal": node.ordinal,
                "ref_path": node.ref_path,
                "is_leaf": node.is_leaf,
                "aliases": list(node.aliases),
                "raw_span_start": node.raw_span_start,
                "raw_span_end": node.raw_span_end,
                "node_hash": node.node_hash,
            }
        )
    )


def alias_fingerprint(alias: AliasRecord | Any) -> str:
    from ..utils.contract import stable_payload

    return sha256_text(
        stable_payload(
            {
                "normalized_alias": alias.normalized_alias,
                "target_node_id": alias.target_node_id,
                "alias_level": alias.alias_level,
                "confidence": alias.confidence,
                "source": alias.source,
            }
        )
    )


def edge_fingerprint(edge: EdgeRecord | Any) -> str:
    from ..utils.contract import stable_payload

    return sha256_text(
        stable_payload(
            {
                "edge_type": edge.edge_type,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "confidence": edge.confidence,
            }
        )
    )
