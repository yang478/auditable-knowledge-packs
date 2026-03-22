# Auditable Knowledge Packs

[English](README.md) | [简体中文](README.zh-CN.md)

Auditable, deterministic citations for regulations, policies, SOPs, and manuals — **no embeddings**.

Generate a deterministic, non-vector “knowledge-base skill” from one or more documents (Markdown/TXT/DOCX/readable PDF). Instead of letting an LLM browse files and assemble citations, you run a deterministic CLI that produces auditable evidence bundles (`bundle.roundNN.md`) with forced provenance, plus machine-readable trace/verify artifacts.

## What you get

- `references/` — a human-auditable tree of Markdown files (chapters/sections/articles/blocks)
- `kb.sqlite` — SQLite + FTS5 index (CJK 2-gram + ASCII word tokens; no vector DB)
- `kbtool` / `kbtool.cmd` — root wrappers that prefer a fresh matching binary and fall back to Python
- `scripts/kbtool.py` + `scripts/kbtool_lib/` — deterministic `search`/`research`/`reindex` commands. `research` runs **one auditable round** and writes:
  - `run_dir/bundle.roundNN.md` (evidence + citations + answering constraints)
  - `run_dir/trace.roundNN.json` + `run_dir/verify.roundNN.json` + append-only `run_dir/trace.jsonl`
- `bin/<platform>/kbtool(.exe)` — optional PyInstaller onefile binary (built per-platform)

## Why this design (audit-first)

When answers must be reviewable and repeatable (regulations, policies, internal SOPs), the failure mode isn’t “slightly lower recall” — it’s “can’t prove where this came from”.

If you let an LLM “search → open files → assemble context → cite sources” on its own, results vary by model and it’s easy to lose provenance.
This project makes the retrieval path **scripted and repeatable**, so the model only needs to read `bundle.roundNN.md` to answer consistently (and must not invent anything not present in the bundle).

It’s optimized for:

- **Repeatability**: same inputs + same query → same ranked hits and bundle
- **Auditability**: `references/` is the authoritative, reviewable intermediate layer
- **Change control**: edit `references/`, then `reindex` (shadow rebuild + atomic switch)
- **Portability**: everything lives in a folder; runtime retrieval is just Python + SQLite

## Compared to common RAG setups

- **Embedding-first stacks** (LangChain/LlamaIndex/RAGFlow, etc.) are great for semantic recall, but require an embedding model + vector store, and citation stability can drift as models, chunking, or embeddings change.
- **Agentic “LLM reads files and cites”** is flexible, but hard to reproduce; provenance can be inconsistent across prompts/models.
- **Plain keyword search** (grep/Elasticsearch/SQLite FTS) is good for finding strings, but you still need a deterministic, structure-aware context assembler; `kbtool research` runs one deterministic retrieval+render round and produces a bundle with an auditable trace/verify report.

If you need paraphrase-heavy semantic matching, hybrid (keyword + vectors) may be a better fit. If you need stable evidence bundles with traceable sources, this project is designed for that.

## Not a fit (non-goals)

- Open-domain Q&A without authoritative source documents
- Queries that rely on paraphrase-only matching (where key terms don’t appear in the text)
- Free-form, exploratory research where you want the model to browse and decide what to read on the fly

## 30-second mental model

```text
documents ──(build_skill.py)──> references/ + kb.sqlite
query     ──(kbtool research)──> run_dir/bundle.roundNN.md  (+ trace/verify)
```

## Quick Start

### Requirements

- Python **3.10+**
- Optional (for readable PDFs): `pdftotext` (Poppler). For scanned PDFs, OCR or convert to text first.

### Build a skill from documents

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "My Document KB"
```

Output: `.claude/skills/my-books/`

### Generate a deterministic evidence bundle

```bash
cd .claude/skills/my-books
./kbtool research \
  --query "适用范围是什么？" \
  --run-dir research_runs/case-001 \
  --planner-json '{"model":"gpt-5","temperature":0.2,"prompt_sha256":"..."}'
```

Then open `research_runs/case-001/bundle.round01.md` and answer based on it (copy the generated `## 参考依据` at the bottom).
If `verify.round01.json` fails (`ok=false`), revise query/params and run the next round in the same `--run-dir` (round auto-increments).

## Docs

- User guide: `docs/USER_GUIDE.md`
- Architecture/design: `docs/ARCHITECTURE.md`
- Development: `docs/DEVELOPMENT.md`

## Repo Layout

```
pack-builder/
  scripts/build_skill.py        # generator CLI
  templates/                    # generated skill templates (kbtool.py, kbtool_lib/, reindex.py)
docs/
  USER_GUIDE.md                 # how to generate/use research bundles
  ARCHITECTURE.md               # design overview
  DEVELOPMENT.md                # dev/test workflow
```

## Development

Run tests:

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## License

Apache-2.0. See `LICENSE`.
