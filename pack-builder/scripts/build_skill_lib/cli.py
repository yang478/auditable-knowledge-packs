from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, List, Optional

from .fs_utils import die, safe_skill_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a monitor-style knowledge base skill from documents.")
    parser.add_argument("--skill-name", required=True, help="Output skill folder name (lowercase letters/digits/hyphens).")
    parser.add_argument(
        "--out-dir",
        default=".claude/skills",
        help="Directory to write the generated skill into (default: .claude/skills).",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--inputs", nargs="+", help="Input documents (.md .txt .docx .pdf).")
    src.add_argument("--ir-jsonl", default="", help="JSONL IR input (type=doc/node rows).")
    parser.add_argument("--title", default="Document Knowledge Base", help="Human-friendly title for the generated skill.")
    parser.add_argument("--force", action="store_true", help="Overwrite output folder if it already exists.")
    parser.add_argument(
        "--package-kbtool",
        action="store_true",
        help="(Optional) Package scripts/kbtool.py into bin/<platform>/kbtool using PyInstaller if available.",
    )
    parser.add_argument("--catalog-taxonomy", default="", help="(Optional) Taxonomy JSON for catalog (categories list with id/label).")
    parser.add_argument("--catalog-assignments", default="", help="(Optional) Assignments JSONL mapping doc_hash -> primary_category_id.")
    return parser


def main(
    argv: Optional[List[str]] = None,
    *,
    build_skill_fn: Callable[..., Path] | None = None,
) -> int:
    if build_skill_fn is None:
        from .build import build_skill as build_skill_fn

    parser = build_parser()
    args = parser.parse_args(argv)

    skill_name = safe_skill_name(args.skill_name)
    out_dir = Path(args.out_dir)
    inputs: List[Path] = []
    ir_jsonl: Path | None = None
    if args.ir_jsonl:
        ir_jsonl = Path(str(args.ir_jsonl)).resolve()
        if not ir_jsonl.exists() or not ir_jsonl.is_file():
            die(f"Missing --ir-jsonl file: {ir_jsonl}")
    else:
        inputs = [Path(p) for p in (args.inputs or [])]
        for p in inputs:
            if not p.exists() or not p.is_file():
                die(f"Missing input file: {p}")

    catalog_taxonomy = Path(args.catalog_taxonomy) if str(args.catalog_taxonomy).strip() else None
    catalog_assignments = Path(args.catalog_assignments) if str(args.catalog_assignments).strip() else None
    if catalog_taxonomy and not catalog_taxonomy.exists():
        die(f"Missing --catalog-taxonomy file: {catalog_taxonomy}")
    if catalog_assignments and not catalog_assignments.exists():
        die(f"Missing --catalog-assignments file: {catalog_assignments}")

    build_skill_fn(
        skill_name=skill_name,
        title=args.title,
        inputs=inputs,
        out_dir=out_dir,
        force=args.force,
        ir_jsonl=ir_jsonl,
        catalog_taxonomy=catalog_taxonomy,
        catalog_assignments=catalog_assignments,
        package_kbtool=bool(args.package_kbtool),
    )
    print(f"[OK] Generated skill: {out_dir / skill_name}")
    return 0
