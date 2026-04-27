from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..utils.text import canonical_text_from_markdown
from ..chunking import DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP, DEFAULT_SEPARATORS, chunk_document_atomic, validate_chunk_params
from ..utils.fs import ConfigError, BuildError, write_text
from ..ir.io import _ir_node_file_index, read_ir_jsonl
from ..render.node import frontmatter_kb_node, render_kb_node_frontmatter, write_doc_metadata, write_structure_report
from ..utils.text import stable_hash
from ..types import HeadingRow, InputDoc, NodeRecord


# ---------------------------------------------------------------------------
# Heading extraction: 从 canonical_md 中提取标题及字符位置范围
# ---------------------------------------------------------------------------

@dataclass
class _HeadingEntry:
    title: str
    level: int
    char_start: int
    char_end: int


def _extract_heading_entries(text: str) -> list[_HeadingEntry]:
    """从 markdown 文本中提取所有标题及其字符位置。

    返回按 char_start 排序的列表。标题行的 char_end 指向行尾。
    """
    entries: list[_HeadingEntry] = []
    cursor = 0
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        m = re.match(r'^(#{1,6})\s+(.+)', stripped)
        if m:
            level = len(m.group(1))
            title = m.group(2).strip()
            entries.append(_HeadingEntry(
                title=title,
                level=level,
                char_start=cursor,
                char_end=cursor + len(line),
            ))
        cursor += len(line)
    return entries


def _heading_stack_at(char_pos: int, headings: list[_HeadingEntry]) -> list[str]:
    """返回字符位置 char_pos 处有效的标题栈（层级路径）。

    算法：按顺序遍历 headings，维护一个栈。
    """
    stack: list[tuple[int, str]] = []  # (level, title)
    for h in headings:
        if h.char_start > char_pos:
            break
        # 弹出栈中 level >= 当前 level 的条目
        while stack and stack[-1][0] >= h.level:
            stack.pop()
        stack.append((h.level, h.title))
    return [title for _, title in stack]


def _chunk_title(chunk_id: str, chunk_text: str) -> str:
    title = chunk_id
    for raw in str(chunk_text or "").splitlines():
        s = raw.strip()
        if not s:
            continue
        s = re.sub(r"^#{1,6}\s+", "", s).replace('"', "").strip()
        if len(s) > 80:
            s = (s[:80].rstrip() + "…").strip()
        if s:
            return f"{chunk_id} {s}"
        break
    return title


def _build_chapter_toc(
    doc_title: str,
    chunk_headings: List[Tuple[str, List[str]]],
) -> str:
    """根据每个 chunk 的 heading_stack 生成章节目录。

    chunk_headings: [(chunk_id, heading_stack), ...] 按 ordinal 排序。
    按最深层非文档标题分组，生成章节目录表格。
    """
    lines: List[str] = [f"# {doc_title} 章节目录\n\n"]

    # 确定分组级别：如果 stack[0] 全部相同（文档标题），则按 stack[1] 分组
    top_headings = set()
    for _, h_stack in chunk_headings:
        if h_stack:
            top_headings.add(h_stack[0])

    # 选择分组键索引：如果只有一种顶级标题（通常是文档名），则用下一级
    group_index = 1 if len(top_headings) <= 1 else 0

    # 按选中级别标题分组
    chapters: dict[str, list[str]] = {}
    order: list[str] = []
    for chunk_id, h_stack in chunk_headings:
        key = h_stack[group_index] if len(h_stack) > group_index else h_stack[-1] if h_stack else "(未分类)"
        if key not in chapters:
            order.append(key)
        chapters.setdefault(key, []).append(chunk_id)

    if not chapters:
        lines.append("> 无章节信息。\n")
        return "".join(lines)

    lines.append("| 章节 | Chunk 范围 |\n|---|---|\n")
    for ch_title in order:
        chunk_ids = chapters[ch_title]
        lines.append(f"| {ch_title} | {chunk_ids[0]} ~ {chunk_ids[-1]} |\n")

    total_chunks = sum(len(v) for v in chapters.values())
    lines.append(f"\n共 {len(chapters)} 章，{total_chunks} 个 chunks。\n")
    return "".join(lines)


def generate_doc(
    doc: InputDoc,
    md: str,
    out_skill_dir: Path,
    *,
    chunking_config: Dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overlap: int = DEFAULT_OVERLAP,
    canonical_md: str | None = None,
) -> Tuple[List[HeadingRow], List[NodeRecord]]:
    chunk_size_value = int(chunk_size)
    overlap_value = int(overlap)
    try:
        validate_chunk_params(chunk_size_value, overlap_value)
    except ValueError as exc:
        raise ConfigError(str(exc))

    doc_dir = out_skill_dir / "references" / doc.doc_id
    chunks_dir = doc_dir / "chunks"
    doc_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    write_doc_metadata(doc, doc_dir, active_parser="occam_chunking")

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    canonical_md = str(canonical_md) if canonical_md is not None else canonical_text_from_markdown(md)
    separators = chunking_config.get("separators") if isinstance(chunking_config, dict) else None
    if not isinstance(separators, list) or not all(isinstance(v, str) for v in separators):
        separators = list(DEFAULT_SEPARATORS)

    # 预提取所有标题及位置，用于 heading_stack 注入和目录生成
    heading_entries = _extract_heading_entries(canonical_md)

    heading_rows: List[HeadingRow] = []
    nodes: List[NodeRecord] = []

    doc_node_id = f"{doc.doc_id}:doc"
    doc_path = doc_dir / "doc.md"
    doc_rel = rel(doc_path)
    nodes.append(
        NodeRecord(
            node_id=doc_node_id,
            doc_id=doc.doc_id,
            doc_title=doc.title,
            kind="doc",
            label=doc.doc_id,
            title=doc.title,
            parent_id=None,
            prev_id=None,
            next_id=None,
            ordinal=0,
            ref_path=doc_rel,
            is_leaf=False,
            body_md="",
            body_plain="",
            source_version=doc.source_version,
        )
    )
    heading_rows.append((doc.title, doc.doc_id, doc.title, "doc", doc_node_id, doc_rel))

    preview_limit = 200
    toc_preview_rows: List[Tuple[str, str, str]] = []
    chunk_headings: List[Tuple[str, List[str]]] = []  # (chunk_id, heading_stack)

    chunk_count = 0
    total_chunk_chars = 0
    min_chunk_chars: int | None = None
    max_chunk_chars = 0

    prev_chunk: Optional[NodeRecord] = None
    for span in chunk_document_atomic(
        canonical_md,
        chunk_size=chunk_size_value,
        overlap=overlap_value,
        separators=separators,
    ):
        ordinal = int(span.ordinal)
        chunk_count += 1

        chunk_id = f"chunk-{ordinal:06d}"
        chunk_node_id = f"{doc.doc_id}:chunk:{ordinal:06d}"
        chunk_path = chunks_dir / f"{chunk_id}.md"
        chunk_rel = rel(chunk_path)

        chunk_body_md = str(span.text or "").rstrip() + "\n"
        chunk_chars = len(span.text or "")
        total_chunk_chars += chunk_chars
        if min_chunk_chars is None or chunk_chars < min_chunk_chars:
            min_chunk_chars = chunk_chars
        if chunk_chars > max_chunk_chars:
            max_chunk_chars = chunk_chars
        if len(toc_preview_rows) < preview_limit:
            toc_preview_rows.append((chunk_id, chunk_node_id, f"references/{doc.doc_id}/chunks/{chunk_id}.md"))

        title = _chunk_title(chunk_id, chunk_body_md)

        # 根据 chunk 起始位置计算标题栈
        h_stack = _heading_stack_at(int(span.char_start), heading_entries)
        heading_path = " > ".join(h_stack) if h_stack else ""

        # 收集 chunk 的 heading 信息用于章节目录
        chunk_headings.append((chunk_id, h_stack))

        node = NodeRecord(
            node_id=chunk_node_id,
            doc_id=doc.doc_id,
            doc_title=doc.title,
            kind="chunk",
            label=f"{doc.doc_id}#{ordinal}",
            title=title,
            parent_id=doc_node_id,
            prev_id=prev_chunk.node_id if prev_chunk else None,
            next_id=None,
            ordinal=ordinal,
            ref_path=chunk_rel,
            is_leaf=True,
            body_md="",
            body_plain="",
            source_version=doc.source_version,
            raw_span_start=int(span.char_start),
            raw_span_end=max(int(span.char_end), int(span.char_start) + 1),
            node_hash=stable_hash(chunk_body_md),
            heading_path=heading_path,
        )
        if prev_chunk is not None:
            prev_chunk.next_id = node.node_id
        prev_chunk = node
        nodes.append(node)
        heading_rows.append((title, doc.doc_id, doc.title, "chunk", chunk_node_id, chunk_rel))
        write_text(
            chunk_path,
            frontmatter_kb_node(
                doc,
                node_id=chunk_node_id,
                kind="chunk",
                label=node.label,
                title=title,
                parent_id=doc_node_id,
                ref_path=chunk_rel,
                heading_stack=h_stack,
            )
            + chunk_body_md,
        )

    stats = {
        "canonical_char_len": len(canonical_md),
        "chunk_count": chunk_count,
        "avg_chunk_chars": (total_chunk_chars / chunk_count) if chunk_count else 0.0,
        "min_chunk_chars": int(min_chunk_chars or 0),
        "max_chunk_chars": int(max_chunk_chars or 0),
    }
    write_structure_report(
        doc,
        doc_dir,
        selected_parser="occam_chunking",
        runner_ups=(),
        selected_report={
            "chunking": dict(chunking_config or {}),
            "stats": stats,
        },
    )

    # 生成 doc.md：frontmatter + 标题 + 目录 + 统计
    doc_body_lines = [
        f"# {doc.title}\n",
        "",
        f"- chunks: {chunk_count}",
        f"- chunk_size_chars: {chunk_size_value}",
        f"- overlap_chars: {overlap_value}",
        "",
    ]

    # 从 heading_entries 生成文档目录
    if heading_entries:
        doc_body_lines.append("## 目录\n")
        for h in heading_entries:
            indent = "  " * (h.level - 1)
            doc_body_lines.append(f"{indent}- {h.title}")
        doc_body_lines.append("")

    write_text(
        doc_path,
        frontmatter_kb_node(
            doc,
            node_id=doc_node_id,
            kind="doc",
            label=doc.doc_id,
            title=doc.title,
            parent_id="",
            ref_path=doc_rel,
        )
        + "\n".join(doc_body_lines) + "\n",
    )

    write_text(doc_dir / "toc.md", _build_chapter_toc(doc.title, chunk_headings))

    return heading_rows, nodes


def generate_doc_from_ir(
    doc: InputDoc,
    nodes: List[NodeRecord],
    out_skill_dir: Path,
) -> List[HeadingRow]:
    doc_dir = out_skill_dir / "references" / doc.doc_id
    chunks_dir = doc_dir / "chunks"
    doc_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    write_doc_metadata(doc, doc_dir, active_parser=doc.active_parser or "occam_chunking")

    def rel(path: Path) -> str:
        return str(path.relative_to(out_skill_dir)).replace("\\", "/")

    heading_rows: List[HeadingRow] = []

    for node in [n for n in nodes if n.doc_id == doc.doc_id]:
        kind = node.kind
        index = _ir_node_file_index(node)

        if kind == "doc":
            path = doc_dir / "doc.md"
        elif kind == "chunk":
            chunk_id = f"chunk-{index:06d}" if index > 0 else "chunk-000000"
            path = chunks_dir / f"{chunk_id}.md"
        else:
            raise BuildError(f"Unsupported IR node kind for Occam chunking: {kind} (node_id={node.node_id})")

        node.ref_path = rel(path)
        body_md = node.body_md.rstrip() + "\n"
        node.body_plain = ""

        # 从 body_md 中提取标题以构建 heading_path（IR 导入时可能未设置）
        if kind == "chunk":
            headings = _extract_heading_entries(body_md)
            h_stack = _heading_stack_at(0, headings)
            node.heading_path = " > ".join(h_stack) if h_stack else ""

        write_text(path, render_kb_node_frontmatter(doc, node) + body_md)
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
