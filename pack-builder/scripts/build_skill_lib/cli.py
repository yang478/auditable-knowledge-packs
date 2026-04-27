from __future__ import annotations

import argparse
import json as json_mod
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

from .chunking import DEFAULT_CHUNK_SIZE, DEFAULT_OVERLAP
from .utils.fs import die, safe_skill_name, PackBuilderError

logger = logging.getLogger(__name__)


def _configure_cli_logging(*, log_format: str = "text") -> None:
    if log_format == "json":
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                payload = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": record.levelname,
                    "msg": record.getMessage(),
                    "module": record.module,
                }
                return json_mod.dumps(payload, ensure_ascii=False)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logging.basicConfig(level=logging.INFO, handlers=[handler])
    else:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a monitor-style knowledge base skill from documents.")
    parser.add_argument(
        "--skill-name", required=True, help="Output skill folder name (lowercase letters/digits/hyphens)."
    )
    parser.add_argument(
        "--out-dir",
        default=".claude/skills",
        help="Directory to write the generated skill into (default: .claude/skills).",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--inputs", nargs="+", help="Input documents (.md .txt .docx .pdf).")
    src.add_argument("--ir-jsonl", default="", help="JSONL IR input (type=doc/node rows).")
    parser.add_argument(
        "--title", default="Document Knowledge Base", help="Human-friendly title for the generated skill."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite output folder if it already exists. Also enables incremental merge of user-managed files (bin/, hooks/)."
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incrementally update existing output folder (only changed documents are reprocessed).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Occam chunk size (characters, default: {DEFAULT_CHUNK_SIZE} ≈ 450 tokens).",
    )
    parser.add_argument(
        "--overlap",
        type=int,
        default=DEFAULT_OVERLAP,
        help=f"Occam chunk overlap (characters, default: {DEFAULT_OVERLAP}). Must be < --chunk-size.",
    )
    parser.add_argument(
        "--pdf-fallback",
        choices=["none", "pypdf"],
        default="none",
        help="When `pdftotext` is unavailable, optionally fall back to pure-Python PDF extraction via `pypdf` (best-effort).",
    )
    parser.add_argument(
        "--package-kbtool",
        action="store_true",
        help="(Optional) Package scripts/kbtool.py into bin/<platform>/kbtool using PyInstaller if available.",
    )
    graph_group = parser.add_mutually_exclusive_group()
    graph_group.add_argument(
        "--enable-graph-edges",
        dest="enable_graph_edges",
        action="store_true",
        default=True,
        help="Build lightweight navigation graph edges (default).",
    )
    graph_group.add_argument(
        "--disable-graph-edges",
        dest="enable_graph_edges",
        action="store_false",
        help="Disable graph edge generation and keep only direct references.",
    )
    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Logging output format (default: text).",
    )
    parser.add_argument("--workers", type=int, default=None,
                        help="并行提取文档的线程数 (默认: min(8, 文档数))")
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

    # Apply log format once, after parsing.
    log_format = str(getattr(args, "log_format", "text") or "text")
    _configure_cli_logging(log_format=log_format)

    chunk_size_value = int(getattr(args, "chunk_size"))
    overlap_value = int(getattr(args, "overlap"))
    argv_effective = argv if argv is not None else sys.argv[1:]
    explicit = " (default)"
    if argv_effective:
        has_chunk_size = any(a == "--chunk-size" or a.startswith("--chunk-size=") for a in argv_effective)
        has_overlap = any(a == "--overlap" or a.startswith("--overlap=") for a in argv_effective)
        if has_chunk_size or has_overlap:
            explicit = " (explicit)"
    logger.info("Chunking: chunk_size=%s overlap=%s%s", chunk_size_value, overlap_value, explicit)

    try:
        skill_name = safe_skill_name(args.skill_name)
    except PackBuilderError as exc:
        die(str(exc))

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

    if args.force and args.incremental:
        die("--force and --incremental are mutually exclusive.")

    def _die_with_error(exc: Exception, *, unexpected: bool = False) -> None:
        header = "Build failed due to unexpected error." if unexpected else "Build failed."
        lines = [
            header,
            f"{type(exc).__name__}: {exc}",
            "Hint: set PACK_BUILDER_TRACEBACK=1 for a full stack trace.",
        ]
        if os.environ.get("PACK_BUILDER_TRACEBACK") or os.environ.get("PACK_BUILDER_DEBUG"):
            lines.append("")
            lines.append(traceback.format_exc())
        die("\n".join(lines))

    try:
        build_skill_fn(
            skill_name=skill_name,
            title=args.title,
            inputs=inputs,
            out_dir=out_dir,
            force=args.force,
            incremental=args.incremental,
            chunk_size=chunk_size_value,
            overlap=overlap_value,
            pdf_fallback=str(getattr(args, "pdf_fallback", "none") or "none"),
            ir_jsonl=ir_jsonl,
            package_kbtool=bool(args.package_kbtool),
            enable_graph_edges=bool(getattr(args, "enable_graph_edges", True)),
            workers=args.workers,
        )
    except PackBuilderError as exc:
        _die_with_error(exc)
    except SystemExit:
        raise
    except Exception as exc:
        _die_with_error(exc, unexpected=True)
    logger.info("Generated skill: %s", out_dir / skill_name)
    return 0
