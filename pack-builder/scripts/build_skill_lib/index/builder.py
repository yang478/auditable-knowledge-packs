from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from ..utils.fs import write_tsv


def build_keywords_from_title(title: str) -> List[str]:
    raw = title.strip()
    parts = re.split(r"[\s、/，,；;：:（）()《》""\'\-]+|与|及|和|以及", raw)
    keywords = [p.strip() for p in parts if len(p.strip()) >= 2]
    return list(dict.fromkeys(keywords))

_WINDOWS_INVALID_FILENAME_CHARS = set('<>:\"/\\|?*')
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def _shard_name_from_key(key: str) -> str:
    if not key:
        return "_EMPTY"
    if len(key) > 32:
        key = key[:32]
    if key.upper() in _WINDOWS_RESERVED_NAMES:
        return "U" + "-".join(f"{ord(c):04X}" for c in key)
    bad_chars = _WINDOWS_INVALID_FILENAME_CHARS | {".", " "}
    if any(ord(ch) < 32 or ch in bad_chars for ch in key):
        return "U" + "-".join(f"{ord(c):04X}" for c in key)
    return key


def _first_visible_prefix(text: str, n: int) -> str:
    s = text.strip()
    if not s:
        return ""
    return s[: max(1, n)]


def _shard_rows_by_prefix(
    rows: List[Tuple[str, ...]],
    *,
    primary_index: int,
    max_rows: int = 200,
    max_prefix_len: int = 4,
) -> Dict[str, List[Tuple[str, ...]]]:
    def group(n: int, chunk: List[Tuple[str, ...]]) -> Dict[str, List[Tuple[str, ...]]]:
        out: Dict[str, List[Tuple[str, ...]]] = {}
        for r in chunk:
            key = _first_visible_prefix(r[primary_index], n)
            out.setdefault(key, []).append(r)
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


def write_sharded_index(out_dir: Path, index_name: str, rows: List[Tuple[str, ...]], header: Tuple[str, ...]) -> None:
    idx_root = out_dir / "indexes" / index_name
    idx_root.mkdir(parents=True, exist_ok=True)

    shards = _shard_rows_by_prefix(rows, primary_index=0)
    shard_map_rows: List[Tuple[str, ...]] = []
    for key in sorted(shards.keys()):
        shard_file = _shard_name_from_key(key) + ".tsv"
        write_tsv(idx_root / shard_file, shards[key], header=header)
        shard_map_rows.append((key, shard_file))
    write_tsv(idx_root / "_shards.tsv", shard_map_rows, header=("key", "file"))
