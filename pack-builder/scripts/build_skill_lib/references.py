from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .fs_utils import die, write_text
from .text_utils import derive_source_version, normalize_alias_text, stable_hash
from .types import Heading, InputDoc, NodeRecord


HEADING_RE = re.compile(r"^(?P<marks>#{1,6})\s+(?P<title>.+?)\s*$")

_CN_NUM_RE = r"[0-9一二三四五六七八九十百千]+"
ARTICLE_LINE_RE = re.compile(
    rf"^\s*第\s*(?P<num>{_CN_NUM_RE})(?:\s*之\s*(?P<zhi>{_CN_NUM_RE}))?\s*[条條]\s*(?P<rest>.*)$"
)
ITEM_LINE_RE = re.compile(r"^\s*[（(]\s*(?P<mark>[一二三四五六七八九十0-9]+)\s*[）)]\s*(?P<rest>.*)$")

TOC_DOT_LEADER_RE = re.compile(r"\.{3,}\s*\d+\s*$")

_OUTLINE_NUMSEG_RE = r"\d+[A-Za-z]?"
_OUTLINE_DECIMAL_ADDR_ANY_RE = rf"{_OUTLINE_NUMSEG_RE}(?:\.{_OUTLINE_NUMSEG_RE})*"
_OUTLINE_DECIMAL_ADDR_WITH_DOT_RE = rf"{_OUTLINE_NUMSEG_RE}(?:\.{_OUTLINE_NUMSEG_RE})+"
OUTLINE_CLAUSE_LINE_RE = re.compile(
    rf"^\s*(?P<addr>(?:NA|[A-Z]{{1,3}})\.{_OUTLINE_DECIMAL_ADDR_ANY_RE}|{_OUTLINE_DECIMAL_ADDR_WITH_DOT_RE})\s+(?P<title>.+?)\s*$"
)
OUTLINE_SECTION_LINE_RE = re.compile(r"^\s*(?P<num>\d+)\.\s+(?P<title>.+?)\s*$")
OUTLINE_SECTION_NO_DOT_LINE_RE = re.compile(r"^\s*(?P<num>\d+)\s{2,}(?P<title>.+?)\s*$")
_OUTLINE_CAPTION_SEP_RE = r"(?:\s*[:：\-–—]\s*|\s+)"
OUTLINE_TABLE_LINE_RE = re.compile(
    rf"^\s*(?P<prefix>table|表)\s*(?P<id>[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*){_OUTLINE_CAPTION_SEP_RE}(?P<title>.*)\s*$",
    re.IGNORECASE,
)
OUTLINE_FIGURE_LINE_RE = re.compile(
    rf"^\s*(?P<prefix>figure|fig\.?|图)\s*(?P<id>[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*){_OUTLINE_CAPTION_SEP_RE}(?P<title>.*)\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class OutlineHit:
    kind: str  # clause/table/figure
    ident: str
    title: str
    line_index: int
    raw_line: str


def parse_headings(md: str) -> List[Heading]:
    headings: List[Heading] = []
    for i, line in enumerate(md.splitlines()):
        m = HEADING_RE.match(line)
        if not m:
            continue
        headings.append(Heading(level=len(m.group("marks")), title=m.group("title").strip(), line_index=i))
    return headings


def _slice_lines(lines: List[str], start: int, end: int) -> List[str]:
    seg = lines[start:end]
    while seg and seg[0].strip() == "":
        seg.pop(0)
    while seg and seg[-1].strip() == "":
        seg.pop()
    return seg


def _strip_first_heading_line(lines: List[str]) -> List[str]:
    if not lines:
        return lines
    if HEADING_RE.match(lines[0].strip()):
        return lines[1:]
    return lines


def _strip_markdown_heading_prefix(line: str) -> str:
    s = line.strip()
    if not s.startswith("#"):
        return s
    return re.sub(r"^#{1,6}\s*", "", s).strip()


def _article_label(line: str) -> Optional[str]:
    m = ARTICLE_LINE_RE.match(_strip_markdown_heading_prefix(line))
    if not m:
        return None
    num = (m.group("num") or "").replace(" ", "")
    zhi = (m.group("zhi") or "").replace(" ", "")
    return f"第{num}{('之' + zhi) if zhi else ''}条"


def _item_label(line: str) -> Optional[str]:
    m = ITEM_LINE_RE.match(_strip_markdown_heading_prefix(line))
    if not m:
        return None
    mark = (m.group("mark") or "").strip()
    if not mark:
        return None
    return f"（{mark}）"


def _split_blocks_by_starts(lines: List[str], start_fn) -> List[List[str]]:
    blocks: List[List[str]] = []
    start: Optional[int] = None
    for i, line in enumerate(lines):
        if start_fn(line):
            if start is not None:
                blocks.append(lines[start:i])
            start = i
    if start is not None:
        blocks.append(lines[start:])
    return blocks


def _split_paragraphs(lines: List[str]) -> List[List[str]]:
    paragraphs: List[List[str]] = []
    cur: List[str] = []
    for line in lines:
        if line.strip() == "":
            if cur:
                paragraphs.append(cur)
                cur = []
            continue
        cur.append(line)
    if cur:
        paragraphs.append(cur)
    return paragraphs


def _pack_paragraphs_into_blocks(paragraphs: List[List[str]], *, max_chars: int) -> List[List[str]]:
    if max_chars <= 0:
        return []
    blocks: List[List[str]] = []

    def split_long_paragraph(p: List[str]) -> None:
        chunk: List[str] = []
        chunk_len = 0

        def push_chunk() -> None:
            nonlocal chunk, chunk_len
            if chunk:
                blocks.append(chunk)
            chunk = []
            chunk_len = 0

        for line in p:
            if len(line) > max_chars:
                push_chunk()
                for i in range(0, len(line), max_chars):
                    blocks.append([line[i : i + max_chars]])
                continue
            if not chunk:
                chunk = [line]
                chunk_len = len(line)
                continue
            candidate_len = chunk_len + 1 + len(line)
            if candidate_len > max_chars:
                push_chunk()
                chunk = [line]
                chunk_len = len(line)
            else:
                chunk.append(line)
                chunk_len = candidate_len
        push_chunk()

    cur: List[str] = []
    cur_len = 0

    def push_cur() -> None:
        nonlocal cur, cur_len
        if not cur:
            return
        while cur and cur[0].strip() == "":
            cur.pop(0)
        while cur and cur[-1].strip() == "":
            cur.pop()
        if cur:
            blocks.append(cur)
        cur = []
        cur_len = 0

    def add_line_to_cur(line: str) -> None:
        nonlocal cur, cur_len
        if not cur:
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += 1 + len(line)

    for p in paragraphs:
        if not p:
            continue
        para = list(p)
        para_len = len(para[0]) + sum(1 + len(line) for line in para[1:]) if para else 0
        if para_len > max_chars:
            push_cur()
            split_long_paragraph(para)
            continue

        if not cur:
            for line in para:
                add_line_to_cur(line)
            continue

        # Add a blank line between paragraphs to preserve structure.
        candidate_len = cur_len + 1 + 0 + sum(1 + len(line) for line in para)
        if candidate_len > max_chars:
            push_cur()
            for line in para:
                add_line_to_cur(line)
            continue

        add_line_to_cur("")
        for line in para:
            add_line_to_cur(line)

    push_cur()
    return blocks


def _frontmatter_kb_node(
    doc: InputDoc,
    *,
    node_id: str,
    kind: str,
    label: str,
    title: str,
    parent_id: str,
    ref_path: str,
) -> str:
    base = [
        "---",
        f'doc_id: "{doc.doc_id}"',
        f'doc_title: "{doc.title}"',
        f'source_file: "{doc.path.name}"',
        f'node_id: "{node_id}"',
        f'kind: "{kind}"',
        f'label: "{label}"',
        f'title: "{title}"',
        f'parent_id: "{parent_id}"',
        f'ref_path: "{ref_path}"',
        "---\n",
    ]
    return "\n".join(base) + "\n"


def _parse_ir_aliases(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    items: List[str] = []
    if isinstance(value, list):
        items = [str(v).strip() for v in value]
    elif isinstance(value, str):
        raw = value.strip()
        if raw:
            items = [raw]
    else:
        return ()

    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = normalize_alias_text(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(item)
    return tuple(out)


def _ir_node_file_index(node: NodeRecord) -> int:
    if node.ordinal > 0:
        return int(node.ordinal)
    m = re.search(r"(\d+)$", node.node_id)
    if m:
        return int(m.group(1))
    return 0


def _render_kb_node_frontmatter(doc: InputDoc, node: NodeRecord) -> str:
    base = [
        "---",
        f'doc_id: "{doc.doc_id}"',
        f'doc_title: "{doc.title}"',
        f'source_file: "{doc.path.name}"',
        f'node_id: "{node.node_id}"',
        f'kind: "{node.kind}"',
        f'label: "{node.label}"',
        f'title: "{node.title}"',
        f'parent_id: "{node.parent_id or ""}"',
        f'ref_path: "{node.ref_path}"',
    ]
    if node.aliases:
        base.append("aliases: " + json.dumps(list(node.aliases), ensure_ascii=False))
    base.append("---\n")
    return "\n".join(base) + "\n"


def read_ir_jsonl(path: Path) -> Tuple[List[InputDoc], List[NodeRecord]]:
    doc_rows: Dict[str, Dict[str, Any]] = {}
    node_rows: List[Dict[str, Any]] = []

    for i, raw in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            die(f"Invalid IR jsonl: {path} line {i} ({e})")
        if not isinstance(obj, dict):
            continue
        row_type = str(obj.get("type") or "node").strip().lower()
        if row_type == "doc":
            doc_id = str(obj.get("doc_id") or "").strip()
            if not doc_id:
                die(f"Invalid IR doc row: missing doc_id (line {i})")
            doc_rows[doc_id] = obj
        elif row_type == "node":
            node_rows.append(obj)
        else:
            die(f"Invalid IR row type: {row_type!r} (line {i})")

    doc_ids: set[str] = set()
    for row in node_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if not doc_id:
            die("Invalid IR node row: missing doc_id")
        doc_ids.add(doc_id)
        if doc_id not in doc_rows:
            doc_rows[doc_id] = {"doc_id": doc_id, "title": doc_id, "source_file": f"{doc_id}.ir"}

    docs_by_id: Dict[str, InputDoc] = {}
    for doc_id in sorted(doc_ids):
        row = doc_rows.get(doc_id) or {}
        title = str(row.get("title") or doc_id).strip() or doc_id
        source_file = str(row.get("source_file") or f"{doc_id}.ir").strip() or f"{doc_id}.ir"
        source_path = str(row.get("source_path") or source_file).strip() or source_file
        source_version = str(row.get("source_version") or "").strip() or derive_source_version(source_file, title)
        doc_hash = str(row.get("doc_hash") or "").strip()
        docs_by_id[doc_id] = InputDoc(
            path=Path(source_path),
            doc_id=doc_id,
            title=title,
            source_version=source_version,
            doc_hash=doc_hash,
        )

    nodes: List[NodeRecord] = []
    for row in node_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        node_id = str(row.get("node_id") or "").strip()
        kind = str(row.get("kind") or "").strip()
        if not doc_id or doc_id not in docs_by_id:
            die(f"Invalid IR node row: unknown doc_id {doc_id!r}")
        if not node_id:
            die("Invalid IR node row: missing node_id")
        if not kind:
            die(f"Invalid IR node row: missing kind for node_id={node_id}")

        title = str(row.get("title") or "").strip()
        label = str(row.get("label") or "").strip()
        if not title:
            title = label or node_id
        if not label:
            label = title

        parent_id = str(row.get("parent_id") or "").strip() or None
        prev_id = str(row.get("prev_id") or "").strip() or None
        next_id = str(row.get("next_id") or "").strip() or None
        try:
            ordinal = int(row.get("ordinal") or 0)
        except (TypeError, ValueError):
            ordinal = 0
        body_md = str(row.get("body_md") or row.get("body") or "").rstrip() + "\n"
        source_version = str(row.get("source_version") or "").strip() or docs_by_id[doc_id].source_version
        is_leaf = bool(row.get("is_leaf", True))
        is_active = bool(row.get("is_active", True))
        confidence = float(row.get("confidence") or 1.0)
        aliases = _parse_ir_aliases(row.get("aliases"))
        nodes.append(
            NodeRecord(
                node_id=node_id,
                doc_id=doc_id,
                doc_title=docs_by_id[doc_id].title,
                kind=kind,
                label=label,
                title=title,
                parent_id=parent_id,
                prev_id=prev_id,
                next_id=next_id,
                ordinal=ordinal,
                ref_path=str(row.get("ref_path") or "").strip(),
                is_leaf=is_leaf,
                body_md=body_md,
                body_plain="",
                source_version=source_version,
                is_active=is_active,
                aliases=aliases,
                confidence=confidence,
            )
        )

    by_group: Dict[Tuple[str, Optional[str], str], List[NodeRecord]] = {}
    for n in nodes:
        by_group.setdefault((n.doc_id, n.parent_id, n.kind), []).append(n)
    for siblings in by_group.values():
        siblings.sort(key=lambda x: (x.ordinal, x.node_id))
        for idx, cur in enumerate(siblings):
            if cur.prev_id is None and idx > 0:
                cur.prev_id = siblings[idx - 1].node_id
            if cur.next_id is None and idx + 1 < len(siblings):
                cur.next_id = siblings[idx + 1].node_id

    docs: List[InputDoc] = []
    for doc_id in sorted(docs_by_id):
        doc = docs_by_id[doc_id]
        if doc.doc_hash:
            docs.append(doc)
            continue
        parts: List[str] = []
        for n in sorted([x for x in nodes if x.doc_id == doc_id], key=lambda x: x.node_id):
            parts.append(n.node_id)
            parts.append(n.title)
            parts.append(n.body_md)
        docs.append(replace(doc, doc_hash=stable_hash("\n".join(parts))))

    return docs, nodes


def split_by_heading_level(md: str, *, level: int) -> List[Tuple[str, List[str]]]:
    lines = md.splitlines()
    headings = [h for h in parse_headings(md) if h.level == level]
    if not headings:
        return []
    blocks: List[Tuple[str, List[str]]] = []
    for idx, h in enumerate(headings):
        start = h.line_index
        end = headings[idx + 1].line_index if idx + 1 < len(headings) else len(lines)
        seg = _slice_lines(lines, start, end)
        blocks.append((h.title, seg))
    return blocks


def _frontmatter(
    doc: InputDoc,
    *,
    chapter_id: str,
    chapter_title: str,
    section_id: Optional[str] = None,
    section_title: Optional[str] = None,
) -> str:
    base = [
        "---",
        f'doc_id: "{doc.doc_id}"',
        f'doc_title: "{doc.title}"',
        f'source_file: "{doc.path.name}"',
        f'chapter_id: "{chapter_id}"',
        f'chapter_title: "{chapter_title}"',
    ]
    if section_id and section_title:
        base.append(f'section_id: "{section_id}"')
        base.append(f'section_title: "{section_title}"')
    base.append("---\n")
    return "\n".join(base) + "\n"


def _is_paragraph_start(lines: Sequence[str], index: int) -> bool:
    if index <= 0:
        return True
    return lines[index - 1].strip() == ""


def _normalize_outline_ident(value: str) -> str:
    s = unicodedata.normalize("NFKC", str(value or "")).strip()
    s = re.sub(r"\s+", "", s)
    out: List[str] = []
    for ch in s:
        out.append(ch.upper() if ch.isalpha() else ch)
    return "".join(out)


def _safe_outline_slug(value: str) -> str:
    s = unicodedata.normalize("NFKC", str(value or "")).strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9A-Za-z._-]+", "_", s)
    s = s.strip("._-")
    return s or "x"


def _outline_parent_addrs(addr: str) -> List[str]:
    parts = [p for p in addr.split(".") if p]
    if len(parts) <= 1:
        return []
    out: List[str] = []
    for i in range(1, len(parts)):
        out.append(".".join(parts[:i]))
    return out


def detect_structured_outline(md: str) -> Dict[str, Any]:
    lines = md.splitlines()

    clause_hits: List[OutlineHit] = []
    table_hits: List[OutlineHit] = []
    figure_hits: List[OutlineHit] = []

    ignored_toc_lines: List[Dict[str, Any]] = []
    ignored_dot_leader = 0
    ignored_outline_list = 0
    ignored_no_body = 0

    raw_clause_total = 0
    raw_clause_dupes = 0
    seen_clause: set[str] = set()

    def next_non_empty_cand(index: int) -> str:
        j = index + 1
        while j < len(lines):
            s = lines[j].strip()
            if not s:
                j += 1
                continue
            return _strip_markdown_heading_prefix(s)
        return ""

    def is_outline_marker(cand: str) -> bool:
        if not cand:
            return False
        if TOC_DOT_LEADER_RE.search(cand):
            return True
        if OUTLINE_CLAUSE_LINE_RE.match(cand):
            return True
        if OUTLINE_SECTION_LINE_RE.match(cand):
            return True
        if OUTLINE_SECTION_NO_DOT_LINE_RE.match(cand):
            return True
        if OUTLINE_TABLE_LINE_RE.match(cand):
            return True
        if OUTLINE_FIGURE_LINE_RE.match(cand):
            return True
        return False

    def looks_like_outline_list_entry(index: int) -> bool:
        nxt = next_non_empty_cand(index)
        return bool(nxt and is_outline_marker(nxt))

    def looks_like_body_line(cand: str) -> bool:
        s = str(cand or "").strip()
        if not s:
            return False
        if TOC_DOT_LEADER_RE.search(s):
            return False
        if (
            OUTLINE_CLAUSE_LINE_RE.match(s)
            or OUTLINE_SECTION_LINE_RE.match(s)
            or OUTLINE_SECTION_NO_DOT_LINE_RE.match(s)
            or OUTLINE_TABLE_LINE_RE.match(s)
            or OUTLINE_FIGURE_LINE_RE.match(s)
        ):
            return False
        if HEADING_RE.match(s):
            return False
        if re.fullmatch(r"\d+", s):
            return False
        s_lower = s.lower()
        if "copyright european committee for standardization" in s_lower:
            return False
        if "no reproduction or networking permitted" in s_lower:
            return False
        if "licensed copy:" in s_lower:
            return False

        # Ignore lines that start with a long run of non-content characters (common in extracted TOC pages).
        lead = 0
        for ch in s:
            if ch.isalnum() or ("\u3400" <= ch <= "\u9fff"):
                break
            lead += 1
        if lead >= 12:
            return False

        # Ignore lines that start with mostly punctuation garbage.
        prefix = s[:40]
        if re.match(r"^[\\-–—_,，。、\\s]{2,}", prefix):
            # If the line starts with a long punctuation run and contains no real words early on,
            # it's almost certainly extraction noise from a contents page.
            if not re.search(r"[A-Za-z]", prefix) and not re.search(r"[\u3400-\u9fff]", prefix):
                return False

        if re.match(r"^[（(]\\s*\\d+\\s*[)）]", s):
            return True
        if re.match(r"^(note|NOTE)\\b", s):
            return True

        has_alpha = bool(re.search(r"[A-Za-z]", s))
        has_cjk = bool(re.search(r"[\u3400-\u9fff]", s))
        has_lower = bool(re.search(r"[a-z]", s))

        # Headers/TOC noise often contains only uppercase letters + numbers.
        if not has_lower and not has_cjk and len(s) <= 40:
            return False

        if len(s) >= 30 and (has_alpha or has_cjk):
            return True
        if len(s) >= 20 and has_alpha and " " in s:
            return True
        # Some structured standards have very short clause bodies (e.g. "Short text.").
        # Accept short mixed-case/CJK lines as body, while still rejecting common headers
        # (filtered above by the uppercase-only heuristic).
        if (has_alpha or has_cjk) and len(s) >= 8:
            return True
        return False

    def clause_has_body(index: int) -> bool:
        scanned = 0
        for j in range(index + 1, min(len(lines), index + 160)):
            cand = _strip_markdown_heading_prefix(lines[j].strip())
            if not cand:
                continue
            if looks_like_body_line(cand):
                return True
            scanned += 1
            if scanned >= 10:
                break
        return False

    prev_blank = True
    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line:
            prev_blank = True
            continue

        is_heading = bool(HEADING_RE.match(line))
        is_start = prev_blank or is_heading
        prev_blank = False
        if not is_start:
            continue

        cand = _strip_markdown_heading_prefix(line)
        if not cand:
            continue

        if TOC_DOT_LEADER_RE.search(cand):
            ignored_toc_lines.append({"line_index": i, "raw_line": cand})
            ignored_dot_leader += 1
            continue

        m_table = OUTLINE_TABLE_LINE_RE.match(cand)
        if m_table:
            if looks_like_outline_list_entry(i):
                ignored_toc_lines.append({"line_index": i, "raw_line": cand})
                ignored_outline_list += 1
                continue
            ident = _normalize_outline_ident(m_table.group("id") or "")
            title = re.sub(r"\s+", " ", str(m_table.group("title") or "").strip())
            table_hits.append(
                OutlineHit(
                    kind="table",
                    ident=ident,
                    title=title or f"Table {ident}",
                    line_index=i,
                    raw_line=cand,
                )
            )
            continue

        m_figure = OUTLINE_FIGURE_LINE_RE.match(cand)
        if m_figure:
            if looks_like_outline_list_entry(i):
                ignored_toc_lines.append({"line_index": i, "raw_line": cand})
                ignored_outline_list += 1
                continue
            ident = _normalize_outline_ident(m_figure.group("id") or "")
            title = re.sub(r"\s+", " ", str(m_figure.group("title") or "").strip())
            figure_hits.append(
                OutlineHit(
                    kind="figure",
                    ident=ident,
                    title=title or f"Figure {ident}",
                    line_index=i,
                    raw_line=cand,
                )
            )
            continue

        m_clause = OUTLINE_CLAUSE_LINE_RE.match(cand)
        if m_clause:
            if looks_like_outline_list_entry(i):
                ignored_toc_lines.append({"line_index": i, "raw_line": cand})
                ignored_outline_list += 1
                continue
            if not clause_has_body(i):
                ignored_toc_lines.append({"line_index": i, "raw_line": cand})
                ignored_no_body += 1
                continue
            raw_clause_total += 1
            addr = _normalize_outline_ident(m_clause.group("addr") or "")
            title = re.sub(r"\s+", " ", str(m_clause.group("title") or "").strip())
            if addr in seen_clause:
                raw_clause_dupes += 1
                continue
            seen_clause.add(addr)
            clause_hits.append(
                OutlineHit(
                    kind="clause",
                    ident=addr,
                    title=title,
                    line_index=i,
                    raw_line=cand,
                )
            )
            continue

    unique_clause_addrs = len({h.ident for h in clause_hits})
    duplicate_rate = float(raw_clause_dupes) / float(max(1, raw_clause_total))

    suspicious_rate = 0.0
    suspicious_hits: List[Dict[str, Any]] = []

    should_outline = unique_clause_addrs >= 12 and duplicate_rate <= 0.15 and suspicious_rate <= 0.15
    mode = "outline" if should_outline else "fallback"
    reason = ""
    if not should_outline:
        reason = (
            "failed_thresholds:"
            + f" unique_clause_addrs={unique_clause_addrs} (>=12),"
            + f" duplicate_rate={duplicate_rate:.3f} (<=0.150),"
            + f" suspicious_rate={suspicious_rate:.3f} (<=0.150)"
        )

    return {
        "mode": mode,
        "reason": reason,
        "metrics": {
            "unique_clause_addrs": unique_clause_addrs,
            "raw_clause_total": raw_clause_total,
            "raw_clause_dupes": raw_clause_dupes,
            "duplicate_rate": duplicate_rate,
            "suspicious_rate": suspicious_rate,
            "ignored_toc_lines": len(ignored_toc_lines),
            "ignored_dot_leader": ignored_dot_leader,
            "ignored_outline_list": ignored_outline_list,
            "ignored_no_body": ignored_no_body,
        },
        "samples": {
            "clauses": [h.__dict__ for h in clause_hits[:12]],
            "tables": [h.__dict__ for h in table_hits[:12]],
            "figures": [h.__dict__ for h in figure_hits[:12]],
            "ignored_toc": ignored_toc_lines[:12],
            "suspicious": suspicious_hits[:12],
        },
        "hits": {
            "clauses": clause_hits,
            "tables": table_hits,
            "figures": figure_hits,
        },
    }


def generate_doc_outline(
    doc: InputDoc,
    md: str,
    out_skill_dir: Path,
    *,
    outline: Dict[str, Any],
) -> Tuple[List[Tuple[str, str, str, str, str, str]], List[NodeRecord]]:
    doc_dir = out_skill_dir / "references" / doc.doc_id
    outline_dir = doc_dir / "outline"
    clauses_dir = outline_dir / "clauses"
    tables_dir = outline_dir / "tables"
    figures_dir = outline_dir / "figures"

    clauses_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    write_text(
        doc_dir / "metadata.md",
        (
            f"# {doc.title}\n\n"
            f"- 源文件：`{doc.path.name}`\n"
            f"- 版本：`{doc.source_version}`\n"
            f"- 文档哈希：`{doc.doc_hash}`\n"
        ),
    )

    # Audit: structure detection report
    report_path = doc_dir / "structure_report.json"
    report_obj = {
        "doc_id": doc.doc_id,
        "doc_title": doc.title,
        "source_file": doc.path.name,
        "source_version": doc.source_version,
        "outline": {
            "mode": outline.get("mode"),
            "reason": outline.get("reason"),
            "metrics": outline.get("metrics", {}),
            "samples": outline.get("samples", {}),
        },
    }
    write_text(report_path, json.dumps(report_obj, ensure_ascii=False, indent=2) + "\n")

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    lines = md.splitlines()
    clause_hits: List[OutlineHit] = list(outline.get("hits", {}).get("clauses", []))
    table_hits: List[OutlineHit] = list(outline.get("hits", {}).get("tables", []))
    figure_hits: List[OutlineHit] = list(outline.get("hits", {}).get("figures", []))

    clause_hits.sort(key=lambda h: h.line_index)
    table_hits.sort(key=lambda h: h.line_index)
    figure_hits.sort(key=lambda h: h.line_index)

    nodes: List[NodeRecord] = []
    nodes_by_id: Dict[str, NodeRecord] = {}

    def ensure_clause_node(addr: str, *, ordinal: int) -> NodeRecord:
        node_id = f"{doc.doc_id}:clause:{addr}"
        existing = nodes_by_id.get(node_id)
        if existing is not None:
            # Keep the earliest ordinal for stable ordering.
            existing.ordinal = min(existing.ordinal, ordinal)
            return existing

        safe = _safe_outline_slug(addr)
        path = clauses_dir / f"clause-{safe}.md"
        ref_path = rel(path)
        title = addr
        parent_addrs = _outline_parent_addrs(addr)
        parent_id = f"{doc.doc_id}:clause:{parent_addrs[-1]}" if parent_addrs else None

        node = NodeRecord(
            node_id=node_id,
            doc_id=doc.doc_id,
            doc_title=doc.title,
            kind="clause",
            label=addr,
            title=title,
            parent_id=parent_id,
            prev_id=None,
            next_id=None,
            ordinal=int(ordinal),
            ref_path=ref_path,
            is_leaf=False,
            body_md="",
            body_plain="",
            source_version=doc.source_version,
            node_hash=stable_hash(""),
            raw_span_end=1,
        )
        nodes_by_id[node_id] = node
        nodes.append(node)
        write_text(
            path,
            _frontmatter_kb_node(
                doc,
                node_id=node_id,
                kind="clause",
                label=addr,
                title=title,
                parent_id=parent_id or "",
                ref_path=ref_path,
            )
            + f"{title}\n",
        )
        return node

    def write_leaf_node(
        *,
        node: NodeRecord,
        path: Path,
        body_md: str,
        aliases: Tuple[str, ...] = (),
    ) -> None:
        node.is_leaf = True
        node.aliases = aliases
        node.node_hash = stable_hash(body_md)
        node.raw_span_start = 0
        node.raw_span_end = max(1, len(body_md))
        write_text(
            path,
            _frontmatter_kb_node(
                doc,
                node_id=node.node_id,
                kind=node.kind,
                label=node.label,
                title=node.title,
                parent_id=node.parent_id or "",
                ref_path=node.ref_path,
            )
            + body_md,
        )

    # Preface block (content before first clause)
    if clause_hits:
        first_start = clause_hits[0].line_index
        preface_lines = _slice_lines(lines, 0, first_start)
        if preface_lines:
            preface_body_md = "\n".join(preface_lines).strip() + "\n"
            preface_node_id = f"{doc.doc_id}:block:preface"
            preface_path = outline_dir / "preface.md"
            preface_rel = rel(preface_path)
            preface_node = NodeRecord(
                node_id=preface_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="block",
                label="preface",
                title="Preface",
                parent_id=None,
                prev_id=None,
                next_id=None,
                ordinal=0,
                ref_path=preface_rel,
                is_leaf=True,
                body_md="",
                body_plain="",
                source_version=doc.source_version,
                node_hash=stable_hash(preface_body_md),
                raw_span_end=max(1, len(preface_body_md)),
            )
            preface_node.aliases = ()
            nodes.append(preface_node)
            nodes_by_id[preface_node_id] = preface_node
            write_text(
                preface_path,
                _frontmatter_kb_node(
                    doc,
                    node_id=preface_node_id,
                    kind="block",
                    label="preface",
                    title="Preface",
                    parent_id="",
                    ref_path=preface_rel,
                )
                + preface_body_md,
            )

    # Build clause nodes and extract child tables/figures.
    for idx, hit in enumerate(clause_hits):
        addr = _normalize_outline_ident(hit.ident)
        for p in _outline_parent_addrs(addr):
            ensure_clause_node(p, ordinal=hit.line_index)

        clause_node = ensure_clause_node(addr, ordinal=hit.line_index)

        start = hit.line_index
        end = clause_hits[idx + 1].line_index if idx + 1 < len(clause_hits) else len(lines)

        # Gather inner table/figure hits within this clause span
        inner: List[OutlineHit] = []
        inner.extend([t for t in table_hits if start <= t.line_index < end])
        inner.extend([f for f in figure_hits if start <= f.line_index < end])
        inner.sort(key=lambda h: h.line_index)

        # Extract table/figure bodies and replace in clause body with placeholders.
        cursor = start
        clause_body_parts: List[str] = []
        for inner_hit in inner:
            inner_start = inner_hit.line_index
            next_start = end
            for h2 in inner:
                if h2.line_index > inner_start:
                    next_start = min(next_start, h2.line_index)
                    break

            inner_end = next_start
            for j in range(inner_start + 1, next_start):
                if not _is_paragraph_start(lines, j):
                    continue
                if re.match(r"^\s*\(\s*\d+\s*\)", _strip_markdown_heading_prefix(lines[j].strip())):
                    inner_end = j
                    break

            clause_body_parts.extend(lines[cursor:inner_start])
            placeholder = f"[Extracted {inner_hit.kind} {inner_hit.ident}]"
            clause_body_parts.append(placeholder)
            cursor = inner_end

            inner_body = "\n".join(_slice_lines(lines, inner_start, inner_end)).strip() + "\n"
            if not inner_body.strip():
                continue

            if inner_hit.kind == "table":
                ident = _normalize_outline_ident(inner_hit.ident)
                node_id = f"{doc.doc_id}:table:{ident}"
                safe = _safe_outline_slug(ident)
                path = tables_dir / f"table-{safe}.md"
                ref_path = rel(path)
                title = f"Table {ident}"
                if inner_hit.title and inner_hit.title not in {ident, title}:
                    title = f"Table {ident}: {inner_hit.title}"

                node = nodes_by_id.get(node_id)
                if node is None:
                    node = NodeRecord(
                        node_id=node_id,
                        doc_id=doc.doc_id,
                        doc_title=doc.title,
                        kind="table",
                        label=f"Table {ident}",
                        title=title,
                        parent_id=clause_node.node_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=int(inner_start),
                        ref_path=ref_path,
                        is_leaf=True,
                        body_md="",
                        body_plain="",
                        source_version=doc.source_version,
                        node_hash=stable_hash(inner_body),
                        raw_span_end=max(1, len(inner_body)),
                        aliases=(ident, f"Table {ident}"),
                    )
                    nodes_by_id[node_id] = node
                    nodes.append(node)
                write_text(
                    path,
                    _frontmatter_kb_node(
                        doc,
                        node_id=node_id,
                        kind="table",
                        label=node.label,
                        title=title,
                        parent_id=clause_node.node_id,
                        ref_path=ref_path,
                    )
                    + inner_body,
                )

            if inner_hit.kind == "figure":
                ident = _normalize_outline_ident(inner_hit.ident)
                node_id = f"{doc.doc_id}:figure:{ident}"
                safe = _safe_outline_slug(ident)
                path = figures_dir / f"figure-{safe}.md"
                ref_path = rel(path)
                title = f"Figure {ident}"
                if inner_hit.title and inner_hit.title not in {ident, title}:
                    title = f"Figure {ident}: {inner_hit.title}"

                node = nodes_by_id.get(node_id)
                if node is None:
                    node = NodeRecord(
                        node_id=node_id,
                        doc_id=doc.doc_id,
                        doc_title=doc.title,
                        kind="figure",
                        label=f"Figure {ident}",
                        title=title,
                        parent_id=clause_node.node_id,
                        prev_id=None,
                        next_id=None,
                        ordinal=int(inner_start),
                        ref_path=ref_path,
                        is_leaf=True,
                        body_md="",
                        body_plain="",
                        source_version=doc.source_version,
                        node_hash=stable_hash(inner_body),
                        raw_span_end=max(1, len(inner_body)),
                        aliases=(ident, f"Figure {ident}"),
                    )
                    nodes_by_id[node_id] = node
                    nodes.append(node)
                write_text(
                    path,
                    _frontmatter_kb_node(
                        doc,
                        node_id=node_id,
                        kind="figure",
                        label=node.label,
                        title=title,
                        parent_id=clause_node.node_id,
                        ref_path=ref_path,
                    )
                    + inner_body,
                )

        clause_body_parts.extend(lines[cursor:end])
        clause_body = "\n".join(_slice_lines(clause_body_parts, 0, len(clause_body_parts))).strip() + "\n"

        # Update clause node with real title + body.
        clause_title = f"{addr} {hit.title}".strip()
        clause_node.title = clause_title
        clause_node.label = addr
        clause_node.kind = "clause"
        clause_node.is_leaf = True
        clause_node.aliases = (addr,)
        clause_node.node_hash = stable_hash(clause_body)
        clause_node.raw_span_start = 0
        clause_node.raw_span_end = max(1, len(clause_body))
        clause_path = out_skill_dir / clause_node.ref_path
        write_text(
            clause_path,
            _frontmatter_kb_node(
                doc,
                node_id=clause_node.node_id,
                kind="clause",
                label=addr,
                title=clause_title,
                parent_id=clause_node.parent_id or "",
                ref_path=clause_node.ref_path,
            )
            + clause_body,
        )

    # Link siblings (across kinds) deterministically by ordinal then node_id.
    by_parent: Dict[Tuple[str, Optional[str]], List[NodeRecord]] = {}
    for n in nodes:
        by_parent.setdefault((n.doc_id, n.parent_id), []).append(n)
    for siblings in by_parent.values():
        siblings.sort(key=lambda x: (x.ordinal, x.node_id))
        for s_idx, cur in enumerate(siblings):
            cur.prev_id = siblings[s_idx - 1].node_id if s_idx > 0 else None
            cur.next_id = siblings[s_idx + 1].node_id if s_idx + 1 < len(siblings) else None

    toc_lines: List[str] = [
        f"# {doc.title} 目录\n\n",
        "## Outline\n\n",
        f"- mode: `{outline.get('mode')}`\n",
        f"- clauses: {len([n for n in nodes if n.kind == 'clause'])}\n",
        f"- tables: {len([n for n in nodes if n.kind == 'table'])}\n",
        f"- figures: {len([n for n in nodes if n.kind == 'figure'])}\n\n",
        "## Nodes\n\n",
        "| kind | node_id | title | file |\n|---|---|---|---|\n",
    ]
    for n in sorted(nodes, key=lambda r: (r.kind, r.node_id)):
        toc_lines.append(f"| `{n.kind}` | `{n.node_id}` | {n.title} | `{n.ref_path}` |\n")
    write_text(doc_dir / "toc.md", "".join(toc_lines))

    heading_rows: List[Tuple[str, str, str, str, str, str]] = [
        (n.title, doc.doc_id, doc.title, n.kind, n.node_id, n.ref_path) for n in nodes if n.ref_path
    ]

    return heading_rows, nodes


def generate_doc(
    doc: InputDoc,
    md: str,
    out_skill_dir: Path,
) -> Tuple[List[Tuple[str, str, str, str, str, str]], List[NodeRecord]]:
    outline = detect_structured_outline(md)
    if str(outline.get("mode")) == "outline":
        return generate_doc_outline(doc, md, out_skill_dir, outline=outline)

    doc_dir = out_skill_dir / "references" / doc.doc_id
    chapters_dir = doc_dir / "chapters"
    sections_root = doc_dir / "sections"
    articles_dir = doc_dir / "articles"
    items_root = doc_dir / "items"
    blocks_dir = doc_dir / "blocks"

    chapters_dir.mkdir(parents=True, exist_ok=True)
    sections_root.mkdir(parents=True, exist_ok=True)
    articles_dir.mkdir(parents=True, exist_ok=True)
    items_root.mkdir(parents=True, exist_ok=True)
    blocks_dir.mkdir(parents=True, exist_ok=True)

    write_text(
        doc_dir / "metadata.md",
        (
            f"# {doc.title}\n\n"
            f"- 源文件：`{doc.path.name}`\n"
            f"- 版本：`{doc.source_version}`\n"
            f"- 文档哈希：`{doc.doc_hash}`\n"
        ),
    )

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    # Determine "chapter level" from headings (prefer smallest heading level present after the title H1).
    headings = parse_headings(md)
    levels = sorted({h.level for h in headings})
    chapter_level = 2 if 2 in levels else (levels[0] if levels else 0)
    if chapter_level < 1:
        chapter_level = 2

    lines = md.splitlines()
    chapter_blocks = split_by_heading_level(md, level=chapter_level)

    toc_rows: List[Dict[str, str]] = []
    heading_rows: List[Tuple[str, str, str, str, str, str]] = []
    nodes: List[NodeRecord] = []
    prev_chapter: Optional[NodeRecord] = None
    article_counter = 0
    block_counter = 0

    def write_articles_and_items(parent_node: NodeRecord, content_lines: List[str]) -> int:
        nonlocal article_counter

        body_lines = _strip_first_heading_line(content_lines)
        body_lines = _slice_lines(body_lines, 0, len(body_lines))
        if not body_lines:
            return 0

        article_blocks = _split_blocks_by_starts(body_lines, _article_label)
        if not article_blocks:
            return 0

        prev_article: Optional[NodeRecord] = None
        wrote = 0

        for a_lines in article_blocks:
            first_raw = a_lines[0] if a_lines else ""
            first = _strip_markdown_heading_prefix(first_raw)
            label = _article_label(first_raw) or "条"
            title = first or label

            article_counter += 1
            article_id = f"article-{article_counter:04d}"
            article_path = articles_dir / f"{article_id}.md"
            article_rel = rel(article_path)

            article_node_id = f"{doc.doc_id}:article:{article_counter:04d}"
            article_body_md = "\n".join(a_lines).strip() + "\n"
            article_node_hash = stable_hash(article_body_md)
            article_span_end = max(1, len(article_body_md))

            article_node = NodeRecord(
                node_id=article_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="article",
                label=label,
                title=title,
                parent_id=parent_node.node_id,
                prev_id=prev_article.node_id if prev_article else None,
                next_id=None,
                ordinal=article_counter,
                ref_path=article_rel,
                is_leaf=True,
                body_md="",
                body_plain="",
                source_version=doc.source_version,
                raw_span_end=article_span_end,
                node_hash=article_node_hash,
            )
            if prev_article:
                prev_article.next_id = article_node.node_id
            prev_article = article_node
            nodes.append(article_node)
            heading_rows.append((title, doc.doc_id, doc.title, "article", article_node_id, article_rel))

            write_text(
                article_path,
                _frontmatter_kb_node(
                    doc,
                    node_id=article_node_id,
                    kind="article",
                    label=label,
                    title=title,
                    parent_id=parent_node.node_id,
                    ref_path=article_rel,
                )
                + article_body_md,
            )

            item_blocks = _split_blocks_by_starts(a_lines[1:], _item_label)
            if item_blocks:
                prev_item: Optional[NodeRecord] = None
                for item_idx, i_lines in enumerate(item_blocks, start=1):
                    i_first_raw = i_lines[0] if i_lines else ""
                    i_first = _strip_markdown_heading_prefix(i_first_raw)
                    i_label = _item_label(i_first_raw) or f"（{item_idx}）"
                    i_title = i_first or i_label
                    item_node_id = f"{doc.doc_id}:item:{article_counter:04d}:{item_idx:02d}"

                    item_dir = items_root / article_id
                    item_path = item_dir / f"item-{item_idx:02d}.md"
                    item_rel = rel(item_path)
                    item_body_md = "\n".join(i_lines).strip() + "\n"
                    item_node_hash = stable_hash(item_body_md)
                    item_span_end = max(1, len(item_body_md))

                    item_node = NodeRecord(
                        node_id=item_node_id,
                        doc_id=doc.doc_id,
                        doc_title=doc.title,
                        kind="item",
                        label=i_label,
                        title=i_title,
                        parent_id=article_node_id,
                        prev_id=prev_item.node_id if prev_item else None,
                        next_id=None,
                        ordinal=item_idx,
                        ref_path=item_rel,
                        is_leaf=True,
                        body_md="",
                        body_plain="",
                        source_version=doc.source_version,
                        raw_span_end=item_span_end,
                        node_hash=item_node_hash,
                    )
                    if prev_item:
                        prev_item.next_id = item_node.node_id
                    prev_item = item_node
                    nodes.append(item_node)
                    heading_rows.append((i_title, doc.doc_id, doc.title, "item", item_node_id, item_rel))

                    write_text(
                        item_path,
                        _frontmatter_kb_node(
                            doc,
                            node_id=item_node_id,
                            kind="item",
                            label=i_label,
                            title=i_title,
                            parent_id=article_node_id,
                            ref_path=item_rel,
                        )
                        + item_body_md,
                    )

            wrote += 1

        return wrote

    def write_blocks(parent_node: NodeRecord, content_lines: List[str]) -> int:
        nonlocal block_counter

        body_lines = _slice_lines(content_lines, 0, len(content_lines))
        if not body_lines:
            return 0

        paragraphs = _split_paragraphs(body_lines)
        if not paragraphs:
            return 0
        blocks = _pack_paragraphs_into_blocks(paragraphs, max_chars=6000)
        if not blocks:
            return 0

        prev_block: Optional[NodeRecord] = None
        wrote = 0

        for b_lines in blocks:
            block_counter += 1
            block_id = f"block-{block_counter:04d}"
            block_node_id = f"{doc.doc_id}:block:{block_counter:04d}"
            block_path = blocks_dir / f"{block_id}.md"
            block_rel = rel(block_path)
            block_body_md = "\n".join(b_lines).strip() + "\n"
            block_node_hash = stable_hash(block_body_md)
            block_span_end = max(1, len(block_body_md))

            title = block_id
            for raw in b_lines:
                s = raw.strip()
                if not s:
                    continue
                s = re.sub(r"^#{1,6}\s+", "", s)
                s = s.replace('"', "").strip()
                if len(s) > 80:
                    s = (s[:80].rstrip() + "…").strip()
                title = f"{block_id} {s}" if s else block_id
                break

            block_node = NodeRecord(
                node_id=block_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="block",
                label=block_id,
                title=title,
                parent_id=parent_node.node_id,
                prev_id=prev_block.node_id if prev_block else None,
                next_id=None,
                ordinal=block_counter,
                ref_path=block_rel,
                is_leaf=True,
                body_md="",
                body_plain="",
                source_version=doc.source_version,
                raw_span_end=block_span_end,
                node_hash=block_node_hash,
            )
            if prev_block:
                prev_block.next_id = block_node.node_id
            prev_block = block_node
            nodes.append(block_node)

            write_text(
                block_path,
                _frontmatter_kb_node(
                    doc,
                    node_id=block_node_id,
                    kind="block",
                    label=block_id,
                    title=title,
                    parent_id=parent_node.node_id,
                    ref_path=block_rel,
                )
                + block_body_md,
            )
            wrote += 1
        return wrote

    def write_chapter(chapter_index: int, title: str, content_lines: List[str]) -> Tuple[str, int]:
        nonlocal prev_chapter
        chapter_id = f"chapter{chapter_index:02d}"
        chapter_path = chapters_dir / f"{chapter_id}.md"
        body_md = "\n".join(content_lines).strip() + "\n"
        write_text(chapter_path, _frontmatter(doc, chapter_id=chapter_id, chapter_title=title) + body_md)
        chapter_rel = rel(chapter_path)
        heading_rows.append((title, doc.doc_id, doc.title, "chapter", chapter_id, chapter_rel))

        section_level = min(6, chapter_level + 1)
        chapter_md = "\n".join(content_lines) + "\n"
        section_blocks = split_by_heading_level(chapter_md, level=section_level)
        section_count = 0

        chapter_node_id = f"{doc.doc_id}:chapter:{chapter_id}"
        chapter_node = NodeRecord(
            node_id=chapter_node_id,
            doc_id=doc.doc_id,
            doc_title=doc.title,
            kind="chapter",
            label=chapter_id,
            title=title,
            parent_id=None,
            prev_id=prev_chapter.node_id if prev_chapter else None,
            next_id=None,
            ordinal=chapter_index,
            ref_path=chapter_rel,
            is_leaf=False,
            body_md="",
            body_plain="",
            source_version=doc.source_version,
        )
        if prev_chapter:
            prev_chapter.next_id = chapter_node.node_id
        prev_chapter = chapter_node
        nodes.append(chapter_node)

        if section_blocks:
            prev_section: Optional[NodeRecord] = None
            for sec_idx, (sec_title, sec_lines) in enumerate(section_blocks, start=1):
                section_id = f"section-{chapter_index:02d}-{sec_idx:02d}"
                section_path = sections_root / chapter_id / f"{section_id}.md"
                sec_body_md = "\n".join(sec_lines).strip() + "\n"
                section_node_hash = stable_hash(sec_body_md)
                section_span_end = max(1, len(sec_body_md))
                write_text(
                    section_path,
                    _frontmatter(
                        doc,
                        chapter_id=chapter_id,
                        chapter_title=title,
                        section_id=section_id,
                        section_title=sec_title,
                    )
                    + sec_body_md,
                )
                section_rel = rel(section_path)
                heading_rows.append((sec_title, doc.doc_id, doc.title, "section", f"{chapter_id}/{section_id}", section_rel))

                section_node_id = f"{doc.doc_id}:section:{chapter_id}/{section_id}"
                section_node = NodeRecord(
                    node_id=section_node_id,
                    doc_id=doc.doc_id,
                    doc_title=doc.title,
                    kind="section",
                    label=section_id,
                    title=sec_title,
                    parent_id=chapter_node_id,
                    prev_id=prev_section.node_id if prev_section else None,
                    next_id=None,
                    ordinal=sec_idx,
                    ref_path=section_rel,
                    is_leaf=True,
                    body_md="",
                    body_plain="",
                    source_version=doc.source_version,
                    raw_span_end=section_span_end,
                    node_hash=section_node_hash,
                )
                if prev_section:
                    prev_section.next_id = section_node.node_id
                prev_section = section_node
                nodes.append(section_node)

                wrote_articles = write_articles_and_items(section_node, sec_lines)
                if wrote_articles:
                    section_node.is_leaf = False
                    section_node.body_md = ""
                    section_node.body_plain = ""
                    section_node.raw_span_start = 0
                    section_node.raw_span_end = 1
                    section_node.node_hash = stable_hash("")
                else:
                    wrote_blocks = write_blocks(section_node, sec_lines)
                    if wrote_blocks:
                        section_node.is_leaf = False
                        section_node.body_md = ""
                        section_node.body_plain = ""
                        section_node.raw_span_start = 0
                        section_node.raw_span_end = 1
                        section_node.node_hash = stable_hash("")
                section_count += 1
        else:
            section_id = f"section-{chapter_index:02d}-01"
            section_path = sections_root / chapter_id / f"{section_id}.md"
            sec_body_md = "\n".join(content_lines).strip() + "\n"
            section_node_hash = stable_hash(sec_body_md)
            section_span_end = max(1, len(sec_body_md))
            write_text(
                section_path,
                _frontmatter(
                    doc,
                    chapter_id=chapter_id,
                    chapter_title=title,
                    section_id=section_id,
                    section_title=title,
                )
                + sec_body_md,
            )
            section_rel = rel(section_path)
            heading_rows.append((title, doc.doc_id, doc.title, "section", f"{chapter_id}/{section_id}", section_rel))

            section_node_id = f"{doc.doc_id}:section:{chapter_id}/{section_id}"
            section_node = NodeRecord(
                node_id=section_node_id,
                doc_id=doc.doc_id,
                doc_title=doc.title,
                kind="section",
                label=section_id,
                title=title,
                parent_id=chapter_node_id,
                prev_id=None,
                next_id=None,
                ordinal=1,
                ref_path=section_rel,
                is_leaf=True,
                body_md="",
                body_plain="",
                source_version=doc.source_version,
                raw_span_end=section_span_end,
                node_hash=section_node_hash,
            )
            nodes.append(section_node)

            wrote_articles = write_articles_and_items(section_node, content_lines)
            if wrote_articles:
                section_node.is_leaf = False
                section_node.body_md = ""
                section_node.body_plain = ""
                section_node.raw_span_start = 0
                section_node.raw_span_end = 1
                section_node.node_hash = stable_hash("")
            else:
                wrote_blocks = write_blocks(section_node, content_lines)
                if wrote_blocks:
                    section_node.is_leaf = False
                    section_node.body_md = ""
                    section_node.body_plain = ""
                    section_node.raw_span_start = 0
                    section_node.raw_span_end = 1
                    section_node.node_hash = stable_hash("")
            section_count = 1
        return chapter_id, section_count

    if not chapter_blocks:
        all_lines = _slice_lines(lines, 0, len(lines))
        chapter_id, section_count = write_chapter(1, "正文", all_lines)
        chapter_node_id = f"{doc.doc_id}:chapter:{chapter_id}"
        chapter_node = next((n for n in reversed(nodes) if n.node_id == chapter_node_id), None)
        if chapter_node and chapter_node.is_leaf:
            wrote_blocks = write_blocks(chapter_node, all_lines)
            if wrote_blocks:
                chapter_node.is_leaf = False
        toc_rows.append(
            {
                "chapter_id": chapter_id,
                "chapter_title": "正文",
                "path": f"references/{doc.doc_id}/chapters/{chapter_id}.md",
                "sections": str(section_count),
            }
        )
    else:
        first_ch_line = None
        for h in parse_headings(md):
            if h.level == chapter_level:
                first_ch_line = h.line_index
                break
        if first_ch_line and first_ch_line > 0:
            preface_lines = _slice_lines(lines, 0, first_ch_line)
            if preface_lines:
                preface_id, section_count = write_chapter(0, "前置内容", preface_lines)
                toc_rows.append(
                    {
                        "chapter_id": preface_id,
                        "chapter_title": "前置内容",
                        "path": f"references/{doc.doc_id}/chapters/{preface_id}.md",
                        "sections": str(section_count),
                    }
                )

        for idx, (title, content_lines) in enumerate(chapter_blocks, start=1):
            chapter_id, section_count = write_chapter(idx, title, content_lines)
            toc_rows.append(
                {
                    "chapter_id": chapter_id,
                    "chapter_title": title,
                    "path": f"references/{doc.doc_id}/chapters/{chapter_id}.md",
                    "sections": str(section_count),
                }
            )

    toc_md = [f"# {doc.title} 目录\n\n", "## 章节列表\n\n", "| 章节 | 标题 | 文件 | 小节数 |\n|---|---|---|---|\n"]
    for row in toc_rows:
        toc_md.append(f"| `{row['chapter_id']}` | {row['chapter_title']} | `{row['path']}` | {row['sections']} |\n")
    write_text(doc_dir / "toc.md", "".join(toc_md))

    return heading_rows, nodes


def generate_doc_from_ir(
    doc: InputDoc,
    nodes: List[NodeRecord],
    out_skill_dir: Path,
) -> List[Tuple[str, str, str, str, str, str]]:
    doc_dir = out_skill_dir / "references" / doc.doc_id
    articles_dir = doc_dir / "articles"
    items_root = doc_dir / "items"
    blocks_dir = doc_dir / "blocks"

    write_text(
        doc_dir / "metadata.md",
        (
            f"# {doc.title}\n\n"
            f"- 源文件：`{doc.path.name}`\n"
            f"- 版本：`{doc.source_version}`\n"
            f"- 文档哈希：`{doc.doc_hash}`\n"
        ),
    )

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    heading_rows: List[Tuple[str, str, str, str, str, str]] = []

    for node in [n for n in nodes if n.doc_id == doc.doc_id]:
        kind = node.kind
        index = _ir_node_file_index(node)
        used_item: Dict[str, set[int]] = {}

        if kind == "block":
            blocks_dir.mkdir(parents=True, exist_ok=True)
            block_id = f"block-{index:04d}" if index > 0 else "block-0000"
            path = blocks_dir / f"{block_id}.md"
        elif kind == "article":
            articles_dir.mkdir(parents=True, exist_ok=True)
            article_id = f"article-{index:04d}" if index > 0 else "article-0000"
            path = articles_dir / f"{article_id}.md"
        elif kind == "item":
            items_root.mkdir(parents=True, exist_ok=True)
            parent = node.parent_id or ""
            m = re.search(r":article:(\d+)$", parent)
            article_num = int(m.group(1)) if m else 0
            folder = f"article-{article_num:04d}" if article_num else "article-0000"
            used = used_item.setdefault(folder, set())
            item_num = index if index > 0 and index not in used else 1
            while item_num in used:
                item_num += 1
            used.add(item_num)
            path = items_root / folder / f"item-{item_num:02d}.md"
        else:
            die(f"Unsupported IR node kind: {kind} (node_id={node.node_id})")

        node.ref_path = rel(path)
        body_md = node.body_md.rstrip() + "\n"
        node.body_plain = ""

        write_text(path, _render_kb_node_frontmatter(doc, node) + body_md)
        heading_rows.append((node.title, doc.doc_id, doc.title, kind, node.node_id, node.ref_path))

        if node.is_leaf:
            node.body_md = ""
        else:
            node.body_md = body_md

    toc_lines: List[str] = [
        f"# {doc.title} 目录\n\n",
        "## Nodes\n\n",
        "| kind | node_id | title | file |\n|---|---|---|---|\n",
    ]
    for kind, node_id, title, ref_path in sorted(
        [(n.kind, n.node_id, n.title, n.ref_path) for n in nodes if n.doc_id == doc.doc_id],
        key=lambda r: (r[0], r[1]),
    ):
        toc_lines.append(f"| `{kind}` | `{node_id}` | {title} | `{ref_path}` |\n")
    write_text(doc_dir / "toc.md", "".join(toc_lines))

    return heading_rows
