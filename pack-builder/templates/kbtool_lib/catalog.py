from __future__ import annotations

import argparse
import json
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

from .runtime import die, resolve_root, safe_output_path
from .text import derive_source_version


@dataclass(frozen=True)
class DocRow:
    doc_id: str
    doc_title: str
    source_file: str
    source_path: str
    doc_hash: str = ""
    source_version: str = "current"
    is_active: bool = True


def doc_category(title: str, source_file: str) -> str:
    for text in (title or "", source_file or ""):
        s = str(text).strip()
        if not s:
            continue
        if " - " in s:
            left = s.split(" - ", 1)[0].strip()
            if left:
                return left
        if "—" in s:
            left = s.split("—", 1)[0].strip()
            if left:
                return left
    return "未分类"


def load_catalog_overrides(root: Path) -> Dict[str, str]:
    """
    Best-effort: read build-time LLM assignments if present.

    Expected files (written by pack-builder when using --catalog-assignments):
    - catalog/taxonomy.json
    - catalog/assignments.jsonl
    """
    catalog_dir = root / "catalog"
    taxonomy_path = catalog_dir / "taxonomy.json"
    assignments_path = catalog_dir / "assignments.jsonl"
    if not taxonomy_path.exists() or not assignments_path.exists():
        return {}

    try:
        tax = json.loads(taxonomy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(tax, dict):
        return {}
    cats = tax.get("categories")
    if not isinstance(cats, list):
        return {}
    labels: Dict[str, str] = {}
    for row in cats:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id") or "").strip()
        label = str(row.get("label") or "").strip()
        if cid and label:
            labels[cid] = label

    overrides: Dict[str, str] = {}
    for raw in assignments_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        doc_hash = str(row.get("doc_hash") or "").strip()
        primary_id = str(row.get("primary_category_id") or "").strip()
        if not doc_hash or not primary_id:
            continue
        overrides[doc_hash] = labels.get(primary_id) or primary_id
    return overrides


def doc_primary_category(d: DocRow, overrides: Dict[str, str] | None) -> str:
    if overrides and d.doc_hash:
        cat = overrides.get(d.doc_hash)
        if cat:
            return cat
    return doc_category(d.doc_title, d.source_file)


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFKC", text).lower().strip()


def render_categories_md(docs: Sequence[DocRow], *, overrides: Dict[str, str] | None = None) -> str:
    counts = Counter(doc_primary_category(d, overrides) for d in docs)
    parts: List[str] = ["# Categories\n\n", f"- total_docs: {len(docs)}\n\n", "## List\n\n"]
    parts.append("| 分类 | 数量 |\n|---|---:|\n")
    for cat, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        parts.append(f"| {cat} | {n} |\n")
    return "".join(parts)


def cmd_categories(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    docs = list(load_manifest_docs(root).values())
    if not docs:
        die("No docs in manifest.json. Rebuild the skill or check --root.")
    overrides = load_catalog_overrides(root)
    content = render_categories_md(docs, overrides=overrides or None)
    out_path = safe_output_path(root, args.out)
    out_path.write_text(content, encoding="utf-8", newline="\n")
    print("[OK] Wrote categories:", out_path)
    return 0


def render_docs_md(
    docs: Sequence[DocRow],
    *,
    category: str,
    query: str,
    limit: int,
    overrides: Dict[str, str] | None = None,
) -> str:
    q = normalize_text(query) if query else ""
    cat_filter = category.strip()
    filtered: List[DocRow] = []
    for d in docs:
        cat = doc_primary_category(d, overrides)
        if cat_filter and cat != cat_filter:
            continue
        if q:
            hay = normalize_text(d.doc_title) + "\n" + normalize_text(d.source_file)
            if q not in hay:
                continue
        filtered.append(d)

    filtered.sort(key=lambda d: (doc_primary_category(d, overrides), d.doc_title, d.doc_id))
    if limit > 0:
        filtered = filtered[:limit]

    parts: List[str] = [
        "# Docs\n\n",
        f"- category: `{cat_filter}`\n" if cat_filter else "- category: `(any)`\n",
        f"- query: `{query}`\n" if query else "- query: `(none)`\n",
        f"- hits: {len(filtered)}\n\n",
    ]
    parts.append("| doc_id | 分类 | 标题 | 源文件 | 目录 |\n|---|---|---|---|---|\n")
    for d in filtered:
        cat = doc_primary_category(d, overrides)
        toc = f"references/{d.doc_id}/toc.md"
        parts.append(f"| `{d.doc_id}` | {cat} | {d.doc_title} | `{d.source_file}` | `{toc}` |\n")
    return "".join(parts)


def cmd_docs(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)
    docs = list(load_manifest_docs(root).values())
    if not docs:
        die("No docs in manifest.json. Rebuild the skill or check --root.")
    overrides = load_catalog_overrides(root)
    content = render_docs_md(
        docs,
        category=args.category,
        query=args.query,
        limit=int(args.limit),
        overrides=overrides or None,
    )
    out_path = safe_output_path(root, args.out)
    out_path.write_text(content, encoding="utf-8", newline="\n")
    print("[OK] Wrote docs:", out_path)
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
