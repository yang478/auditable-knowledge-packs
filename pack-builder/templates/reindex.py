#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

_WINDOWS_INVALID_FILENAME_CHARS = set('<>:"/\\|?*')
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def write_tsv(path: Path, rows: Iterable[Tuple[str, ...]], header: Optional[Tuple[str, ...]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        if header:
            f.write("# " + "\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(row) + "\n")


def _shard_name_from_key(key: str) -> str:
    if not key:
        return "_EMPTY"
    if len(key) > 32:
        key = key[:32]
    if key.upper() in _WINDOWS_RESERVED_NAMES:
        return "U" + "-".join(f"{ord(c):04X}" for c in key)
    for ch in key:
        if ch in {".", " "} or ch in _WINDOWS_INVALID_FILENAME_CHARS or ord(ch) < 32:
            return "U" + "-".join(f"{ord(c):04X}" for c in key)
    return key


def _first_visible_prefix(text: str, n: int) -> str:
    s = text.strip()
    return s[: max(1, n)] if s else ""


def _shard_rows_by_prefix(
    rows: List[Tuple[str, ...]],
    primary_index: int,
    max_rows: int = 200,
    max_prefix_len: int = 4,
) -> Dict[str, List[Tuple[str, ...]]]:
    def group(n: int, chunk: List[Tuple[str, ...]]) -> Dict[str, List[Tuple[str, ...]]]:
        out: Dict[str, List[Tuple[str, ...]]] = {}
        for r in chunk:
            out.setdefault(_first_visible_prefix(r[primary_index], n), []).append(r)
        return out

    shards = group(1, rows)
    for n in range(1, max_prefix_len):
        oversize = [k for k, v in shards.items() if len(v) > max_rows]
        if not oversize:
            break
        for k in oversize:
            chunk = shards.pop(k)
            for sk, sv in group(n + 1, chunk).items():
                shards[sk] = sv
    return shards


def build_keywords_from_title(title: str) -> List[str]:
    raw = title.strip()
    parts = re.split(r"[\\s、/，,；;：:（）()《》“”\"'\\-]+|与|及|和|以及", raw)
    out: List[str] = []
    seen = set()
    for p in parts:
        p = p.strip()
        if len(p) < 2 or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def parse_frontmatter(md: str) -> Dict[str, str]:
    if not md.startswith("---"):
        return {}
    parts = md.split("---", 2)
    if len(parts) < 3:
        return {}
    fm = {}
    for line in parts[1].splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        fm[k.strip()] = v.strip().strip('"')
    return fm


def _write_sharded_index(root: Path, index_name: str, rows: List[Tuple[str, ...]], header: Tuple[str, ...]) -> None:
    idx_root = root / "indexes" / index_name
    idx_root.mkdir(parents=True, exist_ok=True)
    for old in idx_root.glob("*.tsv"):
        old.unlink()

    shards = _shard_rows_by_prefix(rows, primary_index=0)
    shard_map: List[Tuple[str, ...]] = []
    for key in sorted(shards.keys()):
        shard_file = _shard_name_from_key(key) + ".tsv"
        write_tsv(idx_root / shard_file, shards[key], header=header)
        shard_map.append((key, shard_file))
    write_tsv(idx_root / "_shards.tsv", shard_map, header=("key", "file"))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    refs = root / "references"

    heading_rows: List[Tuple[str, ...]] = []
    for path in refs.rglob("*.md"):
        rel = str(path.relative_to(root)).replace("\\", "/")
        md = path.read_text(encoding="utf-8", errors="replace")
        fm = parse_frontmatter(md)
        title = fm.get("section_title") or fm.get("chapter_title")
        if not title:
            continue
        doc_id = fm.get("doc_id", "")
        doc_title = fm.get("doc_title", "")
        if "section_title" in fm:
            kind = "section"
            item_id = f"{fm.get('chapter_id', '')}/{fm.get('section_id', '')}"
        else:
            kind = "chapter"
            item_id = fm.get("chapter_id", "")
        heading_rows.append((title, doc_id, doc_title, kind, item_id, rel))

    heading_rows.sort(key=lambda r: r[0])
    _write_sharded_index(root, "headings", heading_rows, ("title", "doc_id", "doc_title", "type", "id", "path"))

    kw_rows: List[Tuple[str, ...]] = []
    seen = set()
    for title, doc_id, doc_title, kind, item_id, rel in heading_rows:
        for kw in build_keywords_from_title(title):
            key = (kw, doc_id, kind, item_id, rel)
            if key in seen:
                continue
            seen.add(key)
            kw_rows.append((kw, doc_id, doc_title, kind, item_id, rel))
    kw_rows.sort(key=lambda r: r[0])
    _write_sharded_index(root, "kw", kw_rows, ("keyword", "doc_id", "doc_title", "type", "id", "path"))

    print("[OK] Rebuilt sharded indexes under", root / "indexes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

