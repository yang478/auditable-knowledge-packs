from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Sequence


_CJK_RANGES = (
    (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs
    (0xF900, 0xFAFF),  # CJK Compatibility Ideographs
)


def is_cjk(ch: str) -> bool:
    o = ord(ch)
    for lo, hi in _CJK_RANGES:
        if lo <= o <= hi:
            return True
    return False


_ASCII_WORD_RE = re.compile(r"[A-Za-z0-9_]{2,}")


def tokenize_cjk_2gram(text: str) -> List[str]:
    tokens: List[str] = []
    run: List[str] = []

    def flush() -> None:
        nonlocal run
        if len(run) >= 2:
            tokens.extend("".join(run[i : i + 2]) for i in range(len(run) - 1))
        elif len(run) == 1:
            tokens.append(run[0])
        run = []

    for ch in text:
        if is_cjk(ch):
            run.append(ch)
        else:
            flush()
    flush()
    return tokens


def fts_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    tokens.extend(tokenize_cjk_2gram(text))
    tokens.extend(m.group(0).lower() for m in _ASCII_WORD_RE.finditer(text))
    return tokens


def build_match_query(tokens: Sequence[str], *, max_tokens: int = 64) -> str:
    safe: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        t = t.replace('"', "")
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        safe.append(f'"{t}"')
        if len(safe) >= max_tokens:
            break
    if not safe:
        return ""
    return " OR ".join(safe)


def build_match_all(tokens: Sequence[str], *, max_tokens: int = 16) -> str:
    safe: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        t = t.replace('"', "")
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        safe.append(f'"{t}"')
        if len(safe) >= max_tokens:
            break
    if not safe:
        return ""
    return " AND ".join(safe)


def query_terms(raw_query: str) -> List[str]:
    q = raw_query.strip()
    if not q:
        return []
    parts = [p.strip() for p in re.split(r"\s+", q) if p.strip()]
    if parts:
        return parts
    return [q]


def build_match_expression(
    raw_query: str,
    *,
    query_mode: str,
    must_terms: Sequence[str],
    max_tokens: int = 64,
) -> str:
    must_clauses: List[str] = []
    for t in must_terms:
        clause = build_match_all(fts_tokens(t), max_tokens=16)
        if clause:
            must_clauses.append(f"({clause})" if " AND " in clause else clause)

    query_clause = ""
    if query_mode == "and":
        parts = query_terms(raw_query)
        q_clauses: List[str] = []
        for p in parts:
            clause = build_match_all(fts_tokens(p), max_tokens=16)
            if not clause:
                continue
            q_clauses.append(f"({clause})" if " AND " in clause else clause)
        query_clause = " AND ".join(q_clauses)
    else:
        # default: OR across all query tokens
        query_clause = build_match_query(fts_tokens(raw_query), max_tokens=max_tokens)

    clauses = [c for c in must_clauses + ([query_clause] if query_clause else []) if c]
    if not clauses:
        return ""
    if len(clauses) == 1:
        return clauses[0]
    return " AND ".join(f"({c})" if (" OR " in c or " AND " in c) else c for c in clauses)


def count_occurrences(haystack: str, needle: str) -> int:
    if not haystack or not needle:
        return 0
    return haystack.count(needle)


def extract_window(text: str, terms: Sequence[str], max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    s = text
    idx = -1
    hit = ""
    for t in terms:
        if not t:
            continue
        j = s.find(t)
        if j != -1 and (idx == -1 or j < idx):
            idx = j
            hit = t
    if idx == -1:
        return s[:max_chars]
    start = max(0, idx - max_chars // 3)
    end = min(len(s), start + max_chars)
    if end - start < max_chars:
        start = max(0, end - max_chars)
    snippet = s[start:end]
    if start > 0:
        snippet = "… " + snippet
    if end < len(s):
        snippet = snippet + " …"
    if hit and hit not in snippet:
        # best-effort: do nothing if windowing missed due to truncation
        pass
    return snippet


def parse_frontmatter(md: str) -> Dict[str, str]:
    if not md.startswith("---"):
        return {}
    parts = md.split("---", 2)
    if len(parts) < 3:
        return {}
    fm: Dict[str, str] = {}
    for line in parts[1].splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        fm[k.strip()] = v.strip().strip('"')
    return fm


def strip_frontmatter(md: str) -> str:
    if not md.startswith("---"):
        return md
    parts = md.split("---", 2)
    if len(parts) < 3:
        return md
    body = parts[2]
    return body.lstrip("\r\n")


def markdown_to_plain(md: str) -> str:
    out_lines: List[str] = []
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            out_lines.append("")
            continue
        line = re.sub(r"^#{1,6}\\s+", "", line)
        line = re.sub(r"`([^`]*)`", r"\\1", line)
        line = re.sub(r"\\[(.*?)\\]\\((.*?)\\)", r"\\1", line)
        line = line.replace("**", "").replace("__", "").replace("*", "")
        out_lines.append(line)
    return "\n".join(out_lines).strip() + "\n"


_VERSION_RE = re.compile(r"\bV(?P<num>\d+)\b", re.IGNORECASE)


def stable_hash(text: str) -> str:
    import hashlib

    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def node_key(node_id: str, source_version: str) -> str:
    return f"{node_id}@{source_version}"


def derive_source_version(name: str, title: str) -> str:
    match = _VERSION_RE.search(title) or _VERSION_RE.search(name)
    if match:
        return f"v{match.group('num')}"
    return "current"


def normalize_article_ref(label: str) -> str:
    return re.sub(r"\s+", "", label).replace("條", "条")


def normalize_alias_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).lower()
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", text)


def core_alias_title(title: str) -> str:
    return re.sub(
        r"^第\s*[0-9一二三四五六七八九十百千]+(?:\s*之\s*[0-9一二三四五六七八九十百千]+)?\s*[条條]\s*",
        "",
        title,
    ).strip()

