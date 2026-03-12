# Architecture

This repository contains a generator (`pack-builder`) that turns documents into a deterministic, non-vector retrieval bundle workflow.

The core idea is: **LLMs should not be responsible for retrieval assembly**. Instead, scripts produce a single evidence file (`bundle.md`) with forced provenance.

## High-level flow

```text
build_skill.py (offline build)
  ├─ parses inputs (md/txt/docx/(readable)pdf) or imports IR (jsonl)
  ├─ writes references/ tree (human-auditable)
  ├─ writes kb.sqlite (nodes + FTS index)
  └─ writes scripts/kbtool.py (query-time CLI)
      └─ (optional) writes bin/<platform>/kbtool(.exe) (PyInstaller one-file executable)

kbtool.py bundle (online query)
  ├─ search: FTS candidates
  ├─ rerank: deterministic scoring rules
  ├─ expand: neighbors / parent chain / controlled one-hop expansions
  └─ render: budgeted bundle.md + reference list

kbtool.py (atomic toolbox)
  ├─ --skill: prints a JSON usage “manual” for LLMs
  └─ get-node / get-children / get-parent / get-siblings / follow-references: atomic JSON subcommands
```

## Design principles

- **Structure-first**: preserve chapter/section/article structure when possible.
- **Forced provenance**: every bundled excerpt keeps enough metadata to point back to a file in `references/`.
- **Deterministic pipeline**: `bundle` is designed to be repeatable across models and prompts.
- **No embeddings**: retrieval relies on SQLite FTS5 with pre-tokenized CJK 2-gram + ASCII word tokens.

## Flex levers (opt-in)

- **Data IR (JSONL)**: `build_skill.py --ir-jsonl ...` imports a pre-built node tree (from any upstream pipeline) and builds `references/` + `kb.sqlite`.
- **Runtime hooks**: `kbtool.py search|bundle --enable-hooks` can execute `hooks/*.py` at a few stages (query rewrite, candidate filtering, expansion control, render-time redaction). Default behavior remains unchanged when hooks are disabled.

## Storage model (conceptual)

The generated SQLite database (`kb.sqlite`) stores:
- documents (`docs`)
- nodes (tree structure + stable identifiers)
- node text (`node_text`)
- an FTS table for retrieval (`node_fts`)
- optional edges/aliases (for controlled expansion and alias matching)

## Reindexing safety

`kbtool.py reindex` rebuilds the database from `references/` using a shadow DB and then switches atomically, reducing the risk of partial activation.
