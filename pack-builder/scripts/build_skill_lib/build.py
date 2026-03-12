from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .catalog import load_catalog_category_overrides, load_taxonomy_labels, write_catalog
from .extract import extract_to_markdown
from .fs_utils import derive_doc_id, derive_doc_title, die, write_text
from .indexes import build_keywords_from_title, write_sharded_index
from .kbtool_assets import (
    maybe_package_kbtool_pyinstaller,
    write_kbtool_script,
    write_kbtool_sha1,
    write_reindex_script,
    write_root_kbtool_entrypoints,
)
from .references import generate_doc, generate_doc_from_ir, read_ir_jsonl
from .render import render_generated_skill_md
from .sqlite_db import (
    extract_alias_rows,
    extract_reference_edges,
    merge_history,
    read_existing_aliases,
    read_existing_docs,
    read_existing_edges,
    read_existing_nodes,
    write_kb_sqlite_db,
)
from .text_utils import derive_source_version, stable_hash
from .types import InputDoc, NodeRecord


HeadingRow = Tuple[str, str, str, str, str, str]


def _write_manifest(out_skill_dir: Path, *, skill_name: str, title: str, docs: Sequence[InputDoc]) -> None:
    payload = {
        "skill_name": skill_name,
        "title": title,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "docs": [
            {
                "doc_id": d.doc_id,
                "title": d.title,
                "source_file": d.path.name,
                "source_path": str(d.path),
                "doc_hash": d.doc_hash,
                "source_version": d.source_version,
                "active_version": bool(d.is_active),
            }
            for d in docs
        ],
    }
    write_text(out_skill_dir / "manifest.json", json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def build_skill(
    skill_name: str,
    title: str,
    inputs: Sequence[Path],
    out_dir: Path,
    force: bool,
    *,
    ir_jsonl: Optional[Path] = None,
    catalog_taxonomy: Optional[Path] = None,
    catalog_assignments: Optional[Path] = None,
    package_kbtool: bool = False,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / skill_name
    tmp = out_dir / f".{skill_name}.tmp-{os.getpid()}"
    if tmp.exists():
        shutil.rmtree(tmp)
    if target.exists() and not force:
        die(f"Output already exists: {target} (use --force to overwrite)")
    tmp.mkdir(parents=True, exist_ok=True)

    # Preserve user-managed files when rebuilding (e.g., multiple platform binaries).
    if target.exists() and force:
        for keep in ("bin", "hooks"):
            src = target / keep
            if not src.exists():
                continue
            shutil.copytree(src, tmp / keep, dirs_exist_ok=True)

    docs: List[InputDoc] = []
    all_heading_rows: List[HeadingRow] = []
    all_nodes: List[NodeRecord] = []

    if ir_jsonl is not None:
        docs, all_nodes = read_ir_jsonl(ir_jsonl)
        for doc in docs:
            all_heading_rows.extend(generate_doc_from_ir(doc, all_nodes, tmp))
    else:
        used_doc_ids: set[str] = set()
        for p in inputs:
            md = extract_to_markdown(p)
            title_for_doc = derive_doc_title(p, md)
            doc = InputDoc(
                path=p,
                doc_id=derive_doc_id(p, used_doc_ids),
                title=title_for_doc,
                source_version=derive_source_version(p.stem, title_for_doc),
                doc_hash=stable_hash(md),
            )
            docs.append(doc)
            headings, nodes = generate_doc(doc, md, tmp)
            all_heading_rows.extend(headings)
            all_nodes.extend(nodes)

    _write_manifest(tmp, skill_name=skill_name, title=title, docs=docs)

    # Global indexes
    all_heading_rows.sort(key=lambda r: r[0])
    headings_rows = [
        (t, doc_id, doc_title, kind, item_id, path) for (t, doc_id, doc_title, kind, item_id, path) in all_heading_rows
    ]
    write_sharded_index(tmp, "headings", headings_rows, header=("title", "doc_id", "doc_title", "type", "id", "path"))

    kw_rows: List[Tuple[str, ...]] = []
    seen = set()
    for (t, doc_id, doc_title, kind, item_id, path) in all_heading_rows:
        for kw in build_keywords_from_title(t):
            key = (kw, doc_id, kind, item_id, path)
            if key in seen:
                continue
            seen.add(key)
            kw_rows.append((kw, doc_id, doc_title, kind, item_id, path))
    kw_rows.sort(key=lambda r: r[0])
    write_sharded_index(tmp, "kw", kw_rows, header=("keyword", "doc_id", "doc_title", "type", "id", "path"))

    category_overrides: Dict[str, str] | None = None
    if catalog_assignments:
        if not catalog_taxonomy:
            die("--catalog-assignments requires --catalog-taxonomy")
        taxonomy_labels = load_taxonomy_labels(catalog_taxonomy)
        category_overrides = load_catalog_category_overrides(catalog_assignments, taxonomy_labels)

        # Audit trail
        (tmp / "catalog").mkdir(parents=True, exist_ok=True)
        write_text(tmp / "catalog" / "taxonomy.json", catalog_taxonomy.read_text(encoding="utf-8"))
        write_text(tmp / "catalog" / "assignments.jsonl", catalog_assignments.read_text(encoding="utf-8"))

    write_catalog(tmp, docs, category_overrides=category_overrides)

    # Generated skill SKILL.md
    write_text(tmp / "SKILL.md", render_generated_skill_md(skill_name, title, docs, category_overrides=category_overrides))
    write_reindex_script(tmp)
    write_kbtool_script(tmp)
    kbtool_sha = write_kbtool_sha1(tmp)
    write_root_kbtool_entrypoints(tmp)
    if package_kbtool:
        exe = maybe_package_kbtool_pyinstaller(tmp)
        if exe is not None:
            write_text(exe.parent / "kbtool.sha1", kbtool_sha + "\n")

    existing_db = target / "kb.sqlite"
    current_docs = read_existing_docs(existing_db) if target.exists() and force else []
    current_nodes = read_existing_nodes(existing_db) if target.exists() and force else []
    current_edges = read_existing_edges(existing_db) if target.exists() and force else []
    current_aliases = read_existing_aliases(existing_db) if target.exists() and force else []

    rebuilt_edges = extract_reference_edges(all_nodes)
    rebuilt_aliases = extract_alias_rows(all_nodes)
    merged_docs = merge_history(
        current_docs,
        docs,
        key_fn=lambda record: (record.doc_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, 0 if record.is_active else 1),
    )
    merged_nodes = merge_history(
        current_nodes,
        all_nodes,
        key_fn=lambda record: (record.node_id, record.source_version),
        sort_key=lambda record: (record.doc_id, record.source_version, record.node_id, 0 if record.is_active else 1),
    )
    merged_edges = merge_history(
        current_edges,
        rebuilt_edges,
        key_fn=lambda record: (record.edge_type, record.from_node_id, record.to_node_id, record.source_version),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.edge_type,
            record.from_node_id,
            record.to_node_id,
            0 if record.is_active else 1,
        ),
    )
    merged_aliases = merge_history(
        current_aliases,
        rebuilt_aliases,
        key_fn=lambda record: (record.normalized_alias, record.target_node_id, record.alias_level, record.source_version),
        sort_key=lambda record: (
            record.doc_id,
            record.source_version,
            record.normalized_alias,
            record.target_node_id,
            record.alias_level,
            0 if record.is_active else 1,
        ),
    )

    write_kb_sqlite_db(tmp / "kb.sqlite", merged_docs, merged_nodes, merged_edges, merged_aliases)

    if target.exists():
        shutil.rmtree(target)
    tmp.rename(target)
    return target
