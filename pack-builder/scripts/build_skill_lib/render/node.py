"""节点渲染：frontmatter 生成、文档元数据和结构报告输出。

从 references.py 拆分出来，负责将节点信息序列化为 Markdown frontmatter
格式以及生成文档级元数据文件。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Sequence

from ..utils.fs import write_text
from ..types import InputDoc, NodeRecord


def frontmatter_kb_node(
    doc: InputDoc,
    *,
    node_id: str,
    kind: str,
    label: str,
    title: str,
    parent_id: str,
    ref_path: str,
    heading_stack: Sequence[str] | None = None,
    aliases: Sequence[str] = (),
) -> str:
    """生成 kb-node 格式的 YAML frontmatter。"""
    base = [
        "---",
        f"doc_id: {json.dumps(doc.doc_id, ensure_ascii=False)}",
        f"doc_title: {json.dumps(doc.title, ensure_ascii=False)}",
        f"source_file: {json.dumps(doc.path.name, ensure_ascii=False)}",
        f"node_id: {json.dumps(node_id, ensure_ascii=False)}",
        f"kind: {json.dumps(kind, ensure_ascii=False)}",
        f"label: {json.dumps(label, ensure_ascii=False)}",
        f"title: {json.dumps(title, ensure_ascii=False)}",
        f"parent_id: {json.dumps(parent_id, ensure_ascii=False)}",
        f"ref_path: {json.dumps(ref_path, ensure_ascii=False)}",
    ]
    if aliases:
        base.append("aliases: " + json.dumps(list(aliases), ensure_ascii=False))
    if heading_stack:
        base.append("heading_stack: " + json.dumps(list(heading_stack), ensure_ascii=False))
    base.append("---\n")
    return "\n".join(base) + "\n"


def render_kb_node_frontmatter(doc: InputDoc, node: NodeRecord) -> str:
    """生成包含 aliases 的 kb-node YAML frontmatter。"""
    return frontmatter_kb_node(
        doc,
        node_id=node.node_id,
        kind=node.kind,
        label=node.label,
        title=node.title,
        parent_id=node.parent_id or "",
        ref_path=node.ref_path,
        aliases=node.aliases,
    )


def frontmatter_chapter_section(
    doc: InputDoc,
    *,
    chapter_id: str,
    chapter_title: str,
    section_id: str | None = None,
    section_title: str | None = None,
) -> str:
    """生成 chapter/section 格式的 YAML frontmatter。"""
    base = [
        "---",
        f"doc_id: {json.dumps(doc.doc_id, ensure_ascii=False)}",
        f"doc_title: {json.dumps(doc.title, ensure_ascii=False)}",
        f"source_file: {json.dumps(doc.path.name, ensure_ascii=False)}",
        f"chapter_id: {json.dumps(chapter_id, ensure_ascii=False)}",
        f"chapter_title: {json.dumps(chapter_title, ensure_ascii=False)}",
    ]
    if section_id and section_title:
        base.append(f"section_id: {json.dumps(section_id, ensure_ascii=False)}")
        base.append(f"section_title: {json.dumps(section_title, ensure_ascii=False)}")
    base.append("---\n")
    return "\n".join(base) + "\n"


def write_doc_metadata(doc: InputDoc, doc_dir: Path, *, active_parser: str = "") -> None:
    """写入文档 metadata.md。"""
    lines = [
        f"# {doc.title}",
        "",
        f"- 源文件：`{doc.path.name}`",
        f"- 源路径：`{doc.path}`",
        f"- 版本：`{doc.source_version}`",
        f"- 文档哈希：`{doc.doc_hash}`",
    ]
    parser_value = str(active_parser or doc.active_parser or "").strip()
    if parser_value:
        lines.append(f"- 解析器：`{parser_value}`")
    write_text(doc_dir / "metadata.md", "\n".join(lines) + "\n")


def write_structure_report(
    doc: InputDoc,
    doc_dir: Path,
    *,
    selected_parser: str = "",
    runner_ups: Sequence[str] = (),
    selected_report: Dict[str, Any] | None = None,
    outline: Dict[str, Any] | None = None,
) -> None:
    """写入结构检测报告 structure_report.json。"""
    report_obj = {
        "doc_id": doc.doc_id,
        "doc_title": doc.title,
        "source_file": doc.path.name,
        "source_path": str(doc.path),
        "source_version": doc.source_version,
        "selected_parser": str(selected_parser or doc.active_parser or ""),
        "runner_ups": [str(name) for name in runner_ups],
        "selected_report": dict(selected_report or {}),
    }
    if outline is not None:
        report_obj["outline"] = {
            "mode": outline.get("mode"),
            "reason": outline.get("reason"),
            "metrics": outline.get("metrics", {}),
            "samples": outline.get("samples", {}),
        }
    write_text(doc_dir / "structure_report.json", json.dumps(report_obj, ensure_ascii=False, indent=2) + "\n")
