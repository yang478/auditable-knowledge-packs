---
name: pack-builder
description: Use when generating an auditable, deterministic knowledge pack from one or more documents (txt/md/docx/readable-pdf), producing `references/` + `kb.sqlite` (FTS5, no embeddings) + a `kbtool` CLI (root wrapper + python scripts + optional per-platform binary) for deterministic search→context bundling and citations.
---

# Auditable Knowledge Pack Builder

Generate a `monitor`-style knowledge base skill from one or more documents:

- Progressive disclosure layout: `references/<doc_id>/{metadata.md,toc.md,chapters/,sections/}`
- SQLite index for fast non-vector search: `kb.sqlite` (CJK 2-gram + ASCII word tokens in FTS5)
- Deterministic bundle command (recommended): `./kbtool bundle` → outputs a single `bundle.md` with forced sources
- Optional sharded TSV indexes: `indexes/headings/*.tsv`, `indexes/kw/*.tsv` (fallback only)
  - `scripts/kbtool.py` and `scripts/kbtool_lib/*.py` are the python implementation
  - `bin/<platform>/kbtool(.exe)` is optional (PyInstaller); root `kbtool` wrapper prefers a fresh matching binary

## Controlled Retrieval V1

- 在线主流程固定为：标题/正文/术语 三路召回 → 确定性融合排序 → 最多一轮补查 → `bundle.md`
- 一轮补查只允许 `definition`、`references`、`version_metadata` 这类受控动作
- `./kbtool reindex`（或 `python3 scripts/kbtool.py reindex`）使用 shadow rebuild + 原子重建（atomic switch）来激活新版本，并保留旧版本文档行为非激活记录

## Quick Start

1. Choose an output skill name (lowercase letters/digits/hyphens only), e.g. `my-books`.
2. Run:
   - From this repo: `python3 pack-builder/scripts/build_skill.py --skill-name my-books --inputs /path/to/book1.pdf /path/to/book2.docx`
   - If installed under `.claude/skills`: `python3 .claude/skills/pack-builder/scripts/build_skill.py --skill-name my-books --inputs ...`
3. Use the generated skill at `.claude/skills/my-books/`.

## Command Reference

- Show help: `python3 .claude/skills/pack-builder/scripts/build_skill.py --help`
- Write to a specific directory: `--out-dir .claude/skills`
- Overwrite an existing output folder: `--force`

## What You Provide

- One file or many files via `--inputs` (supports `.md`, `.txt`, `.docx`, readable `.pdf`)
- Optional `--title` for the generated skill’s human-friendly heading

## Output Layout (Generated Skill)

```
.claude/skills/<skill-name>/
  SKILL.md
  kbtool                  # recommended entrypoint (POSIX wrapper)
  kbtool.cmd              # Windows wrapper
  kbtool.sha1             # stable hash of python sources
  kb.sqlite
  bin/
    <platform>/           # optional per-platform binary build (PyInstaller)
      kbtool(.exe)
      kbtool.sha1         # copy of kbtool.sha1 for freshness check
  scripts/
    kbtool.py             # python entrypoint (deterministic)
    kbtool_lib/           # implementation modules (db/search/bundle/hooks/skill-json…)
    reindex.py            # TSV-only reindex helper (fallback)
  catalog/
    categories.md
    categories/
  indexes/
    headings/            # sharded TSV title→path
    kw/                  # sharded TSV keyword→path (fallback only)
  references/
    <doc_id>/
      metadata.md
      toc.md
      chapters/
      sections/
```

## Robustness Rules (Do Not Skip)

- Prefer “structure-first”: if the document has headings, preserve them; don’t “chunk by size” unless necessary.
- Prefer deterministic search→bundle: run `./kbtool bundle --query "..." --out bundle.md`, then answer from `bundle.md`.
- Prefer the controlled retrieval path: let `bundle` perform 标题/正文/术语 三路召回 and inspect `## 补查记录` when a one-round expansion was triggered.
- Prefer “path-direct” only when user specifies chapter/section: open `references/<doc_id>/toc.md`, then open the target file.
- Treat `indexes/*` as fallback only; never load a whole large index file if a smaller shard or TOC suffices.

## Dependency Model (Cross-Platform)

- **Required:** `python3`
- **PDF (readable)**: prefers `pdftotext` (poppler-utils). If unavailable, the build fails with actionable instructions.
  - Optional fallback: pass `--pdf-fallback pypdf` (best-effort; requires `pypdf` installed).
- **DOCX:** uses a built-in OOXML extractor (no third-party Python deps); if extraction fails, instruct user to convert DOCX → MD/TXT.

## Pressure Scenarios (Self-Test)

- Missing dependencies: build from PDF on a machine without `pdftotext` (should fail with actionable instructions unless `--pdf-fallback pypdf` is enabled).
- Mixed inputs: build from `.md` + `.txt` + `.docx` in one run (should succeed).
- Rebuild safety: output skill folder already exists (should refuse unless `--force` is set).
- Version roll-forward: editing `references/` and running `python3 scripts/kbtool.py reindex` should create a shadow DB, validate it, then atomically switch.

## Common Mistakes

- Feeding a scanned PDF: this tool only supports *readable* PDFs; OCR first, or convert to TXT/MD.
- Assuming indexes are “the knowledge”: answers must cite `references/` files actually read; indexes are lookup only.
- Letting the model load huge files: always start from path-direct or per-doc `toc.md`, not `indexes/*` shards.

## Red Flags (Stop and Fix)

- “I’ll just open the whole index, it’s easier” → split the question and use TOC/shards.
- “PDF import failed, so I’ll guess” → stop; convert PDF to text, install `pdftotext`, or try `--pdf-fallback pypdf` for readable PDFs.
