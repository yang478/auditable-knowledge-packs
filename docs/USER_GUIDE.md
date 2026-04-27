# User Guide

This guide explains how to generate a knowledge-base skill from documents, and how to use the generated deterministic retrieval (`search`/`research`) workflow.

## Concepts

- **Generated skill**: an output folder (e.g. `.claude/skills/my-books/`) containing:
  - `references/`: Markdown files you can open and audit
  - `kb.sqlite`: SQLite + FTS5 index used for retrieval
  - `kbtool` / `kbtool.cmd`: recommended entrypoints (prefer a fresh matching binary, fall back to Python)
  - `scripts/kbtool.py` + `scripts/kbtool_lib/`: deterministic CLI implementation used at query time
  - (Optional) `bin/<platform>/kbtool(.exe)`: PyInstaller packaged single-file executable (no Python required)
- **Deterministic Phase A research run**: instead of letting an LLM assemble context itself, you run `kbtool research` to produce:
  - `run_dir/bundle.json` (machine-readable retrieval state)
  - `run_dir/bundle.md` (human-readable evidence for answering)
  - stdout JSON with `run_dir`, `paths.bundle_json`, and `paths.bundle_md`

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
./kbtool search --pattern "质量保证期限" --out search.md
# (Optional) Literal match: ./kbtool search --pattern "a.b" --fixed
# Or (python): python3 scripts/kbtool.py search --pattern "质量保证期限" --out search.md
# Or (binary): bin/<platform>/kbtool search --pattern "质量保证期限" --out search.md
```

## Research (recommended path)

`research` performs **deterministic iterative search (≤5 rounds) → expand → budgeted rendering** and writes one bounded Phase A result.

```bash
cd .claude/skills/my-books
./kbtool research \
  --query "适用范围是什么？" \
  --run-dir research_runs/case-001
# Or (python): python3 scripts/kbtool.py research --query "..." --run-dir research_runs/case-001
# Or (binary): bin/<platform>/kbtool research --query "..." --run-dir research_runs/case-001
```

Audit note:
- `bundle.md` includes `## Search Goal`, `## Coverage Assessment`, `## Answerability Assessment`, `## Probe Trace`, `## Round Decision`, `## Evidence`, and `## References`.
- `bundle.json` is the LLM-facing v2 payload rooted in `search_goal`, `coverage_assessment`, `answerability_assessment`, `probe_trace`, `evidence_items`, and `round_decision`.
- `trace.roundNN.json` and `verify.roundNN.json` are written alongside the bundle for round-level audit details and machine checks.
- No LLM calls are involved in retrieval; the LLM only answers from the emitted bundle.
- `--planner-json` is accepted for compatibility, but the authoritative Phase A audit artifacts are `trace.roundNN.json` and `verify.roundNN.json` rather than a separate planner payload.
- For safety, `--run-dir` must be within the skill root (path traversal / absolute paths outside root are refused).

Common options:
- `--run-dir research_runs/case-001`: where round artifacts are written.
- `--round 0`: reserved for compatibility and ignored by the Phase A contract.
- `--note "..."`: reserved for compatibility with older wrappers.
- `--neighbors 1`: include previous/next leaf nodes under the same parent.
- `--max-chars 40000`: total output budget.
- `--per-node-max-chars 6000`: truncate a single node if it’s too long.
- `--query-mode and|or`: compose the FTS query more strictly/loosely.
- `--focus-doc DOC` (repeatable): first-class document focus control by `doc_id` or title substring.
- `--require-term TERM` (repeatable): lexical term that must appear in matched text.
- `--exclude-term TERM` (repeatable): lexical term that must not appear in matched text.
- `--doc-scope DOC` (repeatable): operational/manual document restriction by `doc_id` or title substring.
- `--timeout-ms 2000`: abort SQLite queries if they exceed this duration (0 = disabled).
- Iterative retrieval knobs:
  - `--iter-max-rounds 3`: cap iterative refinement rounds (1 = single-pass).
  - `--iter-focus-max-articles 2`: try to converge to <= N articles.
  - `--iter-mass-top3-threshold 0.9`: stricter convergence threshold.
  - `--no-iter`: disable iterative refinement.
- `--debug-triggers`: emit diagnostics and one-hop reference expansion.
- `--enable-hooks`: enable runtime hooks from `hooks/` (see below).

If you rerun `research` with the same `--run-dir`, `bundle.json`, `bundle.md`, and the current round's `trace.roundNN.json` / `verify.roundNN.json` are overwritten. Use a new `--run-dir` when you want to keep multiple attempts side by side.

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

Each file must export `run(payload: dict) -> dict`. Hooks only run when you pass `--enable-hooks` to `search`/`research`.
If `hooks/allowlist.sha1` exists, kbtool will only execute hooks whose sha1 is listed (one per line).
`pre_search` may rewrite `query`, `query_mode`, `doc_scope`, `require_terms`, `exclude_terms`, `doc_hints`, and `query_variants`.

## Answering with provenance

Open `bundle.md` and answer **only** based on its contents.

At the bottom, the tool appends a `## References` section (paths into `references/`). Keep it when you quote or summarize content.

When deciding whether to stop searching, do not treat coverage alone as success:
- `coverage_assessment` tells you whether the right topic/facets were covered.
- `answerability_assessment` tells you whether the current evidence can directly answer the requested question shape.
- `probe_trace` tells you whether runtime had to keep searching after a navigation/pattern hit.

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
- No results: try `search` first, simplify query terms, or narrow with `--focus-doc`, `--require-term`, `--exclude-term`, and `--query-mode and`.
