"""kbtool docs — list documents from manifest.json."""
from __future__ import annotations

import argparse
import json
import logging
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

from .runtime import die, resolve_root, safe_output_path
from .text import derive_source_version

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DocRow:
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
    doc_hash: str = ""
    source_version: str = "current"
    is_active: bool = True


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFKC", text).lower().strip()


def render_docs_md(
    docs: Sequence[DocRow],
    *,
    query: str,
    limit: int,
) -> str:
    q = normalize_text(query) if query else ""
    filtered: List[DocRow] = []
    for d in docs:
        if q:
            hay = normalize_text(d.doc_title) + "\n" + normalize_text(d.source_file)
            if q not in hay:
                continue
        filtered.append(d)

    filtered.sort(key=lambda d: (d.doc_title, d.doc_id))
    if limit > 0:
        filtered = filtered[:limit]

    parts: List[str] = [
        "# Docs\n\n",
        f"- query: `{query}`\n" if query else "- query: `(none)`\n",
        f"- hits: {len(filtered)}\n\n",
    ]
    parts.append("| doc_id | 标题 | 源文件 | 目录 |\n|---|---|---|---|\n")
    for d in filtered:
        toc = f"references/{d.doc_id}/toc.md"
        parts.append(f"| `{d.doc_id}` | {d.doc_title} | `{d.source_file}` | `{toc}` |\n")
    return "".join(parts)


def cmd_docs(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    docs = list(load_manifest_docs(root).values())
    if not docs:
        die("No docs in manifest.json. Rebuild the skill or check --root.")
    content = render_docs_md(
        docs,
        query=args.query,
        limit=int(args.limit),
    )
    out_path = safe_output_path(root, args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8", newline="\n")
    logger.info("Wrote docs: %s", out_path)
    return 0


def load_manifest_docs(root: Path) -> Dict[str, DocRow]:
    manifest = root / "manifest.json"
    if not manifest.exists():
        return {}
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        die(f"Invalid manifest.json: {e}")
    out: Dict[str, DocRow] = {}
    for d in data.get("docs", []) if isinstance(data, dict) else []:
        if not isinstance(d, dict):
            continue
        doc_id = str(d.get("doc_id") or "").strip()
        if not doc_id:
            continue
        out[doc_id] = DocRow(
            doc_id=doc_id,
            doc_title=str(d.get("title") or doc_id),
            source_file=str(d.get("source_file") or "(unknown)"),
            source_path=str(d.get("source_path") or str(root / "references" / doc_id)),
            doc_hash=str(d.get("doc_hash") or ""),
            source_version=str(d.get("source_version") or derive_source_version(doc_id, str(d.get("title") or doc_id))),
            is_active=bool(d.get("active_version", d.get("is_active", True))),
        )
    return out
