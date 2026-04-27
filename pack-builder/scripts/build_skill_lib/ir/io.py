"""IR JSONL 读写与校验。

从 references.py 拆分出来，负责 Intermediate Representation 的
序列化/反序列化及基础校验。
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..utils.fs import BuildError
from ..utils.text import derive_source_version, normalize_alias_text, stable_hash
from ..types import InputDoc, NodeRecord


def _parse_bool(value, default: bool = True) -> bool:
    """安全解析布尔值，避免 bool("false") == True 的陷阱。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return default


def _parse_ir_aliases(value: Any) -> Tuple[str, ...]:
    """解析 IR 行中的 aliases 字段。"""
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
    """从 NodeRecord 推断文件编号。"""
    if node.ordinal > 0:
        return int(node.ordinal)
    m = re.search(r"(\d+)$", node.node_id)
    if m:
        return int(m.group(1))
    return 0


def read_ir_jsonl(path: Path) -> Tuple[List[InputDoc], List[NodeRecord]]:
    """从 IR JSONL 文件读取文档和节点记录。

    Returns:
        (docs, nodes) 元组。
    """
    doc_rows: Dict[str, Dict[str, Any]] = {}
    node_rows: List[Dict[str, Any]] = []

    with path.open(encoding="utf-8", errors="replace") as f:
        for i, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise BuildError(f"Invalid IR jsonl: {path} line {i} ({e})")
            if not isinstance(obj, dict):
                continue
            row_type = str(obj.get("type") or "node").strip().lower()
            if row_type == "doc":
                doc_id = str(obj.get("doc_id") or "").strip()
                if not doc_id:
                    raise BuildError(f"Invalid IR doc row: missing doc_id (line {i})")
                doc_rows[doc_id] = obj
            elif row_type == "node":
                node_rows.append(obj)
            else:
                raise BuildError(f"Invalid IR row type: {row_type!r} (line {i})")

    doc_ids: set[str] = set()
    for row in node_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if not doc_id:
            raise BuildError("Invalid IR node row: missing doc_id")
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
        # 注：str(0)="0" 非空，数字 0 不会被误判为缺失；逻辑正确无需修改
        doc_hash = str(row.get("doc_hash", "")).strip()
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
            raise BuildError(f"Invalid IR node row: unknown doc_id {doc_id!r}")
        if not node_id:
            raise BuildError("Invalid IR node row: missing node_id")
        if not kind:
            raise BuildError(f"Invalid IR node row: missing kind for node_id={node_id}")

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
        # Bug-fix: body_md 为空字符串 "" 时不应回退到 body 字段
        raw_md = row.get("body_md")
        if raw_md is None:
            raw_md = row.get("body")
        body_md = (str(raw_md).rstrip() + "\n") if raw_md is not None else ""
        source_version = str(row.get("source_version") or "").strip() or docs_by_id[doc_id].source_version
        # Bug-fix: 使用 _parse_bool 替代 bool()，避免 bool("false") == True
        is_leaf = _parse_bool(row.get("is_leaf", True), True)
        is_active = _parse_bool(row.get("is_active", True), True)
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

    # 预分组 nodes by doc_id，避免 O(D*N) 线性扫描
    nodes_by_doc: Dict[str, List[NodeRecord]] = {}
    for n in nodes:
        nodes_by_doc.setdefault(n.doc_id, []).append(n)

    docs: List[InputDoc] = []
    for doc_id in sorted(docs_by_id):
        doc = docs_by_id[doc_id]
        if doc.doc_hash:
            docs.append(doc)
            continue
        parts: List[str] = []
        for n in sorted(nodes_by_doc.get(doc_id, []), key=lambda x: x.node_id):
            parts.append(n.node_id)
            parts.append(n.title)
            parts.append(n.body_md)
        docs.append(replace(doc, doc_hash=stable_hash("\n".join(parts))))

    return docs, nodes
