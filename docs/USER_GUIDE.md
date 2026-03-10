# User Guide

This guide explains how to generate a knowledge-base skill from documents, and how to use the generated deterministic retrieval (`search`/`bundle`) workflow.

## Concepts

- **Generated skill**: an output folder (e.g. `.claude/skills/my-books/`) containing:
  - `references/`: Markdown files you can open and audit
  - `kb.sqlite`: SQLite + FTS5 index used for retrieval
  - `scripts/kbtool.py`: deterministic CLI used at query time
- **Deterministic bundle**: instead of letting an LLM assemble context itself, you run `kbtool.py bundle` to produce a single `bundle.md` that includes evidence plus forced provenance.

## Requirements

- Python 3.10+
- Optional: `pdftotext` (Poppler) for **readable** PDFs

If your PDF is scanned (image-only), OCR or convert it to text/Markdown first.

## Generate a skill

From this repo:

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "My Document KB"
```

Notes:
- `--skill-name` must match `^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?$`.
- Use `--force` to overwrite an existing output folder.
- Use `--out-dir` to place the generated skill anywhere (not tied to `.claude/skills`).

## Search (debug / inspection)

`search` writes a ranked list of leaf-node hits with snippets.

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py search --query "质量保证期限" --out search.md
```

## Bundle (recommended path)

`bundle` performs **search → expand → budgeted rendering** and writes a single evidence file.

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py bundle --query "适用范围是什么？" --out bundle.md
```

Common options:
- `--neighbors 1`: include previous/next leaf nodes under the same parent.
- `--max-chars 40000`: total output budget.
- `--per-node-max-chars 6000`: truncate a single node if it’s too long.
- `--query-mode and|or`: compose the FTS query more strictly/loosely.
- `--must TERM` (repeatable): terms that must appear (used as additional constraints).
- `--debug-triggers`: emit diagnostics and one-hop reference expansion.

## Answering with provenance

Open `bundle.md` and answer **only** based on its contents.

At the bottom, the tool appends a references section (paths into `references/`). Keep it when you quote or summarize content.

## Editing references and reindexing

If you manually edit files under `references/`, rebuild the SQLite index:

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py reindex
```

The reindex path uses a “shadow rebuild + atomic switch” approach to reduce the chance of ending up with a partially-built database.

## Troubleshooting

- PDF import fails: ensure `pdftotext` exists; otherwise convert PDF → TXT/MD first.
- Too much output: lower `--max-chars` or `--per-node-max-chars`.
- No results: try `search` first, simplify query terms, or use `--query-mode and` with `--must` constraints.
