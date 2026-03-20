# User Guide

This guide explains how to generate a knowledge-base skill from documents, and how to use the generated deterministic retrieval (`search`/`bundle`) workflow.

## Concepts

- **Generated skill**: an output folder (e.g. `.claude/skills/my-books/`) containing:
  - `references/`: Markdown files you can open and audit
  - `kb.sqlite`: SQLite + FTS5 index used for retrieval
  - `kbtool` / `kbtool.cmd`: recommended entrypoints (prefer a fresh matching binary, fall back to Python)
  - `scripts/kbtool.py` + `scripts/kbtool_lib/`: deterministic CLI implementation used at query time
  - (Optional) `bin/<platform>/kbtool(.exe)`: PyInstaller packaged single-file executable (no Python required)
- **Deterministic bundle**: instead of letting an LLM assemble context itself, you run `kbtool bundle` to produce a single `bundle.md` that includes evidence plus forced provenance.

## Requirements

- Python 3.10+
- Optional: `pdftotext` (Poppler) for **readable** PDFs
- Optional: PyInstaller (for `--package-kbtool` to build a native executable)

If your PDF is scanned (image-only), OCR or convert it to text/Markdown first.

## Generate a skill

From this repo:

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "My Document KB" \
  --package-kbtool
```

Notes:
- `--skill-name` must match `^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?$`.
- Use `--force` to overwrite an existing output folder.
- Use `--out-dir` to place the generated skill anywhere (not tied to `.claude/skills`).
- `--package-kbtool` builds a binary only for the **current platform**. To ship both Windows+Linux binaries in one skill folder, run packaging on both platforms and keep the same output directory (a `--force` rebuild tries to preserve existing `bin/`).

### (Optional) Build from IR (JSONL)

If you already have a structured node tree (e.g. from a crawler / database / offline preprocessing), you can build from a JSONL IR instead of raw documents:

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-kb \
  --out-dir .claude/skills \
  --ir-jsonl /path/to/ir.jsonl \
  --title "My KB (IR)"
```

IR v1 supports `type=doc` and `type=node` rows. Recommended node kinds: `article` / `block` / `item`.

## Search (debug / inspection)

`search` writes a ranked list of leaf-node hits with snippets.

```bash
cd .claude/skills/my-books
./kbtool search --query "质量保证期限" --out search.md
# Or (python): python3 scripts/kbtool.py search --query "质量保证期限" --out search.md
# Or (binary): bin/<platform>/kbtool search --query "质量保证期限" --out search.md
```

## Bundle (recommended path)

`bundle` performs **deterministic iterative search (≤5 rounds) → expand → budgeted rendering** and writes a single evidence file.

```bash
cd .claude/skills/my-books
./kbtool bundle --query "适用范围是什么？" --out bundle.md
# Or (python): python3 scripts/kbtool.py bundle --query "适用范围是什么？" --out bundle.md
# Or (binary): bin/<platform>/kbtool bundle --query "适用范围是什么？" --out bundle.md
```

Audit note:
- `bundle.md` includes a `## 检索轨迹` section that logs each retrieval round (tighten/relax actions) and the selected round.
- No LLM calls are involved; only `query_mode` and `--must` constraints may be adjusted during the rounds to focus results to a few articles.
- For safety, `--out` must point to a file path **within the skill root** (path traversal / absolute paths outside root are refused).

Common options:
- `--neighbors 1`: include previous/next leaf nodes under the same parent.
- `--max-chars 40000`: total output budget.
- `--per-node-max-chars 6000`: truncate a single node if it’s too long.
- `--query-mode and|or`: compose the FTS query more strictly/loosely.
- `--must TERM` (repeatable): terms that must appear (used as additional constraints).
- `--timeout-ms 2000`: abort SQLite queries if they exceed this duration (0 = disabled).
- Iterative retrieval knobs:
  - `--iter-max-rounds 3`: cap iterative refinement rounds (1 = single-pass).
  - `--iter-focus-max-articles 2`: try to converge to <= N articles.
  - `--iter-mass-top3-threshold 0.9`: stricter convergence threshold.
  - `--no-iter`: disable iterative refinement.
- `--debug-triggers`: emit diagnostics and one-hop reference expansion.
- `--enable-hooks`: enable runtime hooks from `hooks/` (see below).

## Atomic commands (JSON output)

The generated tool also exposes atomic subcommands (each does one deterministic thing and prints JSON), useful for LLM tool-chaining:

```bash
cd .claude/skills/my-books
./kbtool --skill
./kbtool get-node "standard-v1:article:0003"
./kbtool follow-references "standard-v1:article:0003" --direction out
```

## Runtime hooks (optional, off by default)

Create `hooks/` under the generated skill root and add any of:

- `hooks/pre_search.py`
- `hooks/post_search.py`
- `hooks/pre_expand.py`
- `hooks/pre_render.py`

Each file must export `run(payload: dict) -> dict`. Hooks only run when you pass `--enable-hooks` to `search`/`bundle`.
If `hooks/allowlist.sha1` exists, kbtool will only execute hooks whose sha1 is listed (one per line).

## Answering with provenance

Open `bundle.md` and answer **only** based on its contents.

At the bottom, the tool appends a references section (paths into `references/`). Keep it when you quote or summarize content.

## Editing references and reindexing

If you manually edit files under `references/`, rebuild the SQLite index:

```bash
cd .claude/skills/my-books
./kbtool reindex
# Or (python): python3 scripts/kbtool.py reindex
# Or (binary): bin/<platform>/kbtool reindex
```

The reindex path uses a “shadow rebuild + atomic switch” approach to reduce the chance of ending up with a partially-built database.

## Troubleshooting

- PDF import fails: ensure `pdftotext` exists; otherwise convert PDF → TXT/MD first.
- Too much output: lower `--max-chars` or `--per-node-max-chars`.
- No results: try `search` first, simplify query terms, or use `--query-mode and` with `--must` constraints.
