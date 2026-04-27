from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator, Sequence


DEFAULT_SEPARATORS: tuple[str, ...] = (
    "。",     # 中文句号（语义边界，最可靠）
    "！",     # 中文感叹号
    "？",     # 中文问号
    ". ",     # 英文句号+空格
    "! ",     # 英文感叹号+空格
    "? ",     # 英文问号+空格
    "\n\n",   # 段落边界（优先于行边界）
    "\n",     # 行边界
    " ",
    "",
)

# 推荐默认值：基于 400-512 tokens ≈ 1500-2000 英文字符
DEFAULT_CHUNK_SIZE = 1800
DEFAULT_OVERLAP = 0


def validate_chunk_params(chunk_size: int, overlap: int) -> None:
    """Validate chunking parameters; raise ValueError on invalid input."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be > 0 (got {chunk_size})")
    if overlap < 0:
        raise ValueError(f"overlap must be >= 0 (got {overlap})")
    if overlap >= chunk_size:
        raise ValueError(f"overlap must be < chunk_size (got overlap={overlap} chunk_size={chunk_size})")


@dataclass(frozen=True)
class ChunkSpan:
    ordinal: int
    char_start: int
    char_end: int
    text: str


def _trim_span(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return start, end


# ---------------------------------------------------------------------------
# Atomic block detection
# ---------------------------------------------------------------------------

# 匹配三类原子块：
# 1. HTML table: <html><body><table>...</table></body></html>
# 2. Markdown table: >=2 consecutive lines starting with |
# 3. Fenced code block: ```...```
_ATOMIC_PATTERN = re.compile(
    r"<html><body><table>.*?</table></body></html>"
    r"|"
    r"(?:^\|[^\n]*\|$\n?){2,}"   # >=2 pipe-lines (Markdown table; [^\n] avoids DOTALL overmatch)
    r"|"
    r"(?:^```\w*\n[\s\S]*?^```$\n?)",  # fenced code block
    re.MULTILINE | re.DOTALL,
)


def _build_atomic_intervals(text: str) -> list[tuple[int, int]]:
    """Return sorted, non-overlapping (start, end) intervals of atomic blocks.

    Regex alternation branches may produce overlapping matches; we merge
    any overlaps here so that the downstream binary search is correct.
    """
    raw = sorted((m.start(), m.end()) for m in _ATOMIC_PATTERN.finditer(text))
    merged: list[tuple[int, int]] = []
    for s, e in raw:
        if merged and s <= merged[-1][1]:
            # Overlap detected — extend the previous interval.
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def _find_enclosing_interval(
    pos: int, idx: list[tuple[int, int]]
) -> tuple[int, int] | None:
    """If *pos* falls inside any atomic interval, return (start, end); else None."""
    lo, hi = 0, len(idx) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        s, e = idx[mid]
        if pos < s:
            hi = mid - 1
        elif pos >= e:
            lo = mid + 1
        else:
            return (s, e)
    return None


def _find_nearest_atomic_start(
    pos: int, idx: list[tuple[int, int]]
) -> int | None:
    """Return the start position of the nearest atomic block at or after *pos*."""
    lo, hi = 0, len(idx) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        s, e = idx[mid]
        if s >= pos:
            result = s
            hi = mid - 1
        else:
            lo = mid + 1
    return result


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def chunk_document(
    text: str,
    *,
    chunk_size: int,
    overlap: int,
    separators: Sequence[str] = DEFAULT_SEPARATORS,
) -> list[ChunkSpan]:
    """Eager wrapper for `iter_chunk_spans` (returns a list)."""

    return list(
        iter_chunk_spans(
            text,
            chunk_size=chunk_size,
            overlap=overlap,
            separators=separators,
        )
    )


def iter_chunk_spans(
    text: str,
    *,
    chunk_size: int,
    overlap: int,
    separators: Sequence[str] = DEFAULT_SEPARATORS,
) -> Iterator[ChunkSpan]:
    """Deterministic recursive-character chunking with atomic block protection.

    - `chunk_size` and `overlap` are measured in Python characters (code points).
    - Uses `separators` from coarse → fine, choosing the last separator occurrence
      before the size limit.
    - **Atomic blocks** (HTML tables, Markdown tables, code fences) are never split.
      If a split point would fall inside an atomic block, the split is moved to
      just before the block starts — keeping the block with its preceding context.
    - For atomic blocks that exceed ``chunk_size`` and have no preceding text to
      attach to, the block is emitted as a single oversized chunk.
    - Guarantees forward progress even when a chunk ends up shorter than `overlap`.

    This is a generator to avoid materializing all chunks in memory for very large documents.
    """

    validate_chunk_params(chunk_size, overlap)

    s = str(text or "")
    if not s:
        return

    # Pre-compute atomic block intervals for O(log n) lookup
    atomic_intervals = _build_atomic_intervals(s)
    atomic_idx = atomic_intervals

    start = 0
    ordinal = 0

    while start < len(s):
        target_end = min(start + chunk_size, len(s))
        end = target_end

        if target_end < len(s):
            for sep in separators:
                if sep == "":
                    break
                search_end = min(len(s), target_end + len(sep))
                pos = s.rfind(sep, start, search_end)
                if pos == -1:
                    continue
                candidate_end = pos + len(sep)
                if candidate_end <= start:
                    continue
                end = candidate_end
                break

        # --- Atomic block protection ---
        # Case 1: split point falls INSIDE an atomic block → push back to block start
        if end < len(s):
            enclosing = _find_enclosing_interval(end, atomic_idx)
            if enclosing is not None:
                block_start, block_end = enclosing
                if block_start > start:
                    end = block_start
                else:
                    end = block_end

        # Case 2: split point sits right at an atomic block boundary, and the
        # text from start to end is very short (just a heading/title).  Skip
        # this split so the short text gets included with the table in the
        # next iteration.
        if end < len(s) and end > start:
            next_atomic = _find_nearest_atomic_start(end, atomic_idx)
            if next_atomic is not None and next_atomic == end:
                preamble_len = len(s[start:end].strip())
                if preamble_len < chunk_size // 4 and preamble_len > 0:
                    # Preamble is short — extend to include the atomic block
                    enclosing = _find_enclosing_interval(next_atomic, atomic_idx)
                    if enclosing is not None:
                        end = enclosing[1]

        if end <= start:
            end = min(start + 1, len(s))

        trimmed_start, trimmed_end = _trim_span(s, start, end)
        if trimmed_end > trimmed_start:
            ordinal += 1
            yield ChunkSpan(
                ordinal=ordinal,
                char_start=trimmed_start,
                char_end=trimmed_end,
                text=s[trimmed_start:trimmed_end],
            )

        if end >= len(s):
            break

        chunk_len = max(1, end - start)
        effective_overlap = min(int(overlap), chunk_len - 1)
        next_start = end - effective_overlap
        if next_start <= start:
            next_start = end
        # Guard: disable overlap when atomic block shortens chunk to avoid
        # O(n) near-duplicate chunks (A' safe overlap).
        if chunk_len < int(overlap):
            next_start = end
        start = next_start


# ---------------------------------------------------------------------------
# Backward-compatible alias (used by references.py)
# ---------------------------------------------------------------------------

chunk_document_atomic = chunk_document
