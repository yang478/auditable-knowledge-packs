from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Sequence

from .fs_utils import die, slugify_ascii, write_text
from .types import InputDoc


_WINDOWS_FORBIDDEN = '<>:"/\\\\|?*'


def safe_fs_component(text: str) -> str:
    s = unicodedata.normalize("NFKC", text).strip()
    if not s:
        return ""
    for ch in _WINDOWS_FORBIDDEN:
        s = s.replace(ch, "-")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.rstrip(". ").lstrip(" ")
    if len(s) > 80:
        s = s[:80].rstrip()
    return s


def doc_category(doc: InputDoc) -> str:
    text = (doc.title or doc.path.stem).strip()
    for sep in (" - ", "—"):
        if sep in text:
            left = text.split(sep, 1)[0].strip()
            if left:
                return left
    return "未分类"


def category_filename(category: str, used: set[str]) -> str:
    base = slugify_ascii(category)
    if not base:
        base = safe_fs_component(category) or "category"
    name = base
    if name in used:
        h = hashlib.sha1(category.encode("utf-8", errors="ignore")).hexdigest()[:8]
        name = f"{base}-{h}"
    i = 2
    while name in used:
        name = f"{base}-{i}"
        i += 1
    used.add(name)
    return f"{name}.md"


def write_catalog(out_skill_dir: Path, docs: Sequence[InputDoc], *, category_overrides: Dict[str, str] | None = None) -> None:
    catalog_dir = out_skill_dir / "catalog"
    cats_dir = catalog_dir / "categories"
    cats_dir.mkdir(parents=True, exist_ok=True)

    by_cat: Dict[str, List[InputDoc]] = {}
    for d in docs:
        cat = category_overrides.get(d.doc_hash) if category_overrides else ""
        by_cat.setdefault(cat or doc_category(d), []).append(d)

    used: set[str] = set()
    cat_files: Dict[str, str] = {}
    for cat in sorted(by_cat):
        fn = category_filename(cat, used)
        cat_files[cat] = fn

    # categories.md (library-style entry)
    lines: List[str] = ["# Catalog\n\n", f"- Total documents: {len(docs)}\n\n", "## Categories\n\n"]
    lines.append("| 分类 | 数量 | 文件 |\n|---|---:|---|\n")
    for cat in sorted(by_cat, key=lambda c: (-len(by_cat[c]), c)):
        fn = cat_files[cat]
        lines.append(f"| {cat} | {len(by_cat[cat])} | `catalog/categories/{fn}` |\n")
    write_text(catalog_dir / "categories.md", "".join(lines))

    # Per-category lists
    for cat, items in by_cat.items():
        fn = cat_files[cat]
        items_sorted = sorted(items, key=lambda d: (d.title, d.doc_id))
        parts: List[str] = [f"# {cat}\n\n", f"- Documents: {len(items_sorted)}\n\n"]
        parts.append("| doc_id | 标题 | 源文件 | 目录 |\n|---|---|---|---|\n")
        for d in items_sorted:
            toc = f"references/{d.doc_id}/toc.md"
            parts.append(f"| `{d.doc_id}` | {d.title} | `{d.path.name}` | `{toc}` |\n")
        write_text(cats_dir / fn, "".join(parts))


def load_taxonomy_labels(path: Path) -> Dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        die(f"Invalid taxonomy json: {path} ({e})")
    if not isinstance(data, dict):
        die(f"Invalid taxonomy json: {path} (expected object)")
    cats = data.get("categories")
    if not isinstance(cats, list):
        die(f"Invalid taxonomy json: {path} (missing categories list)")
    out: Dict[str, str] = {}
    for row in cats:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id") or "").strip()
        label = str(row.get("label") or "").strip()
        if cid and label:
            out[cid] = label
    if not out:
        die(f"Invalid taxonomy json: {path} (no usable categories)")
    return out


def load_catalog_category_overrides(assignments_path: Path, taxonomy_labels: Dict[str, str]) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    for i, raw in enumerate(assignments_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            die(f"Invalid assignments jsonl: {assignments_path} line {i} ({e})")
        if not isinstance(row, dict):
            continue
        doc_hash = str(row.get("doc_hash") or "").strip()
        primary_id = str(row.get("primary_category_id") or "").strip()
        if not doc_hash or not primary_id:
            continue
        if primary_id not in taxonomy_labels:
            die(f"Unknown primary_category_id in assignments: {primary_id} (line {i})")
        if doc_hash in overrides:
            die(f"Duplicate doc_hash in assignments: {doc_hash} (line {i})")
        label = taxonomy_labels[primary_id]
        overrides[doc_hash] = label
    if not overrides:
        die(f"Empty assignments: {assignments_path}")
    return overrides

