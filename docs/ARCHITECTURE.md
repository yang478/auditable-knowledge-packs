# Architecture

This repository contains a generator (`pack-builder`) that turns documents into a deterministic, non-vector retrieval + evidence-bundling workflow.

The core idea is: **LLMs should not be responsible for retrieval assembly**. Instead, scripts produce a small set of auditable artifacts: `bundle.json` for the LLM-facing dual-signal contract, `bundle.md` for answer-time evidence with forced provenance, and round-level `trace.roundNN.json` / `verify.roundNN.json` for machine-auditable diagnostics.

## High-level flow

```text
build_skill.py (offline build)
  ├─ parses inputs (md/txt/docx/(readable)pdf) or imports IR (jsonl)
  ├─ writes references/ tree (human-auditable)
  ├─ writes kb.sqlite (nodes + FTS index)
  ├─ writes phase_a_artifact.json (document/node/export contract for runtime adapters)
  ├─ writes build_state.json (fingerprints, parser/export checksums, index bindings)
  └─ writes scripts/kbtool.py (query-time CLI)
      └─ (optional) writes bin/<platform>/kbtool(.exe) (PyInstaller one-file executable)

kbtool.py research (online query, one bounded iterative Phase A run)
  ├─ (optional) hook: pre_search
  ├─ search: deterministic iterative retrieval (≤5 rounds, focuses to 2–3 articles)
  ├─ rerank: deterministic scoring rules
  ├─ (optional) hook: post_search / pre_expand
  ├─ expand: neighbors / parent chain / controlled one-hop expansions
  ├─ render: verify.roundNN.json (machine checks + v2 control state)
  ├─ render: trace.roundNN.json (round input/effective/retrieval + v2 round objects)
  ├─ render: budgeted bundle.json (`search_goal`, `coverage_assessment`, `answerability_assessment`, `probe_trace`, `evidence_items`, `round_decision`)
  └─ render: bundle.md (Search Goal + Coverage Assessment + Answerability Assessment + Probe Trace + Round Decision + Evidence + References)

kbtool.py (atomic toolbox)
  ├─ --skill: prints a JSON usage “manual” for LLMs
  └─ get-node / get-children / get-parent / get-siblings / follow-references: atomic JSON subcommands
```

## Design principles

- **Structure-first**: preserve chapter/section/article structure when possible.
- **Forced provenance**: every bundled excerpt keeps enough metadata to point back to a file in `references/`.
- **Deterministic pipeline**: `research` is an iterative but bounded rules-only loop designed to stay repeatable across models and prompts.
- **Dual-signal contract**: one round is expressed through shared search/coverage objects plus answerability/probe state: `search_goal`, `coverage_assessment`, `answerability_assessment`, `probe_trace`, `evidence_items`, and `round_decision`.
- **Auditable trace**: `bundle.md` exposes the answer-time summary, while `trace.roundNN.json` and `verify.roundNN.json` preserve the machine-readable round state and checks.
- **No embeddings**: retrieval relies on SQLite FTS5 with pre-tokenized CJK 2-gram + ASCII word tokens.

## Flex levers (opt-in)

- **Data IR (JSONL)**: `build_skill.py --ir-jsonl ...` imports a pre-built node tree (from any upstream pipeline) and builds `references/` + `kb.sqlite`.
- **IR export bridge**: the build also emits `phase_a_artifact.json`, a parser-agnostic contract that keeps document metadata, topology, aliases, edges, and locators available without querying SQLite.
- **Incremental state**: `build_state.json` freezes source/text/span/node fingerprints plus index binding checksums so reindex can reason about document-local drift without re-planning the whole corpus from scratch.
- **Runtime hooks**: `kbtool.py search|research --enable-hooks` can execute `hooks/*.py` at a few stages (query rewrite, candidate filtering, expansion control, render-time redaction). Default behavior remains unchanged when hooks are disabled.

## Storage model (conceptual)

The generated SQLite database (`kb.sqlite`) stores:
- documents (`docs`)
- nodes (tree structure + stable identifiers)
- node text (`node_text`)
- an FTS table for retrieval (`node_fts`)
- optional edges/aliases (for controlled expansion and alias matching)

The generated root also carries `phase_a_artifact.json` and `build_state.json`. Phase A runtime adapters read the export first for document/node topology and locator metadata, while `build_state.json` tracks deterministic fingerprints and index bindings for incremental refresh decisions.

## Reindexing safety

`kbtool.py reindex` rebuilds the database from `references/` using a shadow DB and then switches atomically, reducing the risk of partial activation. After the atomic switch it also refreshes `corpus_manifest.json`, `phase_a_artifact.json`, and `build_state.json`, then emits an incremental refresh summary showing dirty document count, rewritten row footprint, and affected index families.
