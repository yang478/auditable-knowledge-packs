---
name: pack-builder
description: Use when generating an auditable, deterministic knowledge pack from one or more documents (txt/md/docx/readable-pdf), producing `references/` + `kb.sqlite` (FTS5, no embeddings) + a `kbtool` CLI (root wrapper + python scripts + optional per-platform binary) for deterministic search→context bundling and citations.
---

# Auditable Knowledge Pack Builder

Generate a `monitor`-style knowledge base skill from one or more documents:

- Progressive disclosure layout: `references/<doc_id>/{metadata.md,toc.md,chunks/}`
- SQLite index for fast non-vector search: `kb.sqlite` (CJK 2-gram + ASCII word tokens in FTS5)
- Deterministic bundle command (recommended): `./kbtool bundle` → writes a single `bundle.md` (and prints JSON to stdout)
- Auditable chunk config: `chunking.json` (build-time chunk_size/overlap/separators)
- Optional sharded TSV indexes: `indexes/headings/*.tsv`, `indexes/kw/*.tsv` (fallback only)
  - `scripts/kbtool.py` and `scripts/kbtool_lib/*.py` are the python implementation
  - `bin/<platform>/kbtool(.exe)` is optional (PyInstaller); root `kbtool` wrapper prefers a fresh matching binary

## Three-Tier Search Strategy

生成的 skill 提供三层互补检索，并额外提供一个“并行快捷入口”，覆盖模糊到精确的全部场景：

| 命令 | 工具 | 擅长 | 典型场景 |
|------|------|------|----------|
| `kbtool triage --query "..." --out runs/triage.md` | bundle + rg + fd | 并行检索（推荐默认） | 同时需要模糊证据包 + 精确定位线索 |
| `kbtool bundle --preset quick --query "..."` | BM25 (FTS5) | 语义模糊匹配 | 用户用自然语言提问（小上下文起步） |
| `kbtool search --pattern "..."` | ripgrep (rg) | 精确内容搜索 | 函数名、变量名、特定字符串 |
| `kbtool files --pattern "..."` | fd | 精确文件定位 | 按文件名/路径查找文档 |

## 最少参数（LLM 友好）

- **默认最快**：`kbtool triage --query "..." --out runs/triage.md`（triage 默认 `--preset quick` 小输出）
- **精确验证一句话是否出现**：`kbtool triage --pattern "原句" --fixed --out runs/triage.md`（或 `kbtool search --pattern "原句" --fixed`）
- **需要更大上下文**：用 `kbtool bundle --preset standard --query "..."`，或在 `bundle` 中显式调大 `--neighbors/--per-node-max-chars/--limit`

**决策指南（由 AI Agent 根据上下文判断）：**

- **默认** → `triage`（一次并行跑完，最快）
- **用户提问涉及概念/定义/流程** → `bundle`（BM25 语义匹配）
- **用户提及精确术语/代码标识** → `search`（rg 精确正则搜索）
- **用户要找特定文件/文档** → `files`（fd 文件名匹配）
- **复杂问题** → 先 `bundle` 获取语义上下文，再用 `search` 补漏精确术语
- **BM25 无命中** → 尝试 `search` 做精确兜底

**rg/fd 二进制来源：** 构建时自动复制到生成 skill 的 `bin/` 目录；运行时优先 `bin/` 内嵌二进制，fallback 到系统 PATH。

## Occam Chunking (Only)

- 唯一分块策略：递归字符分割（`\n\n` → `\n` → `。` `！` `？` `. ` `! ` `? ` → 空格 → 字符），以句子/段落为最小语义单位
- 每个 chunk 建立 `prev/next` 双向链表关系；检索命中后用 `--neighbors` 做邻居扩展
- 分块参数作为**构建时参数**提供，并写入产物 `chunking.json` 供审计/复现

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
- Chunk tuning: `--chunk-size` (chars, default 1800 ≈ 450 tokens), `--overlap` (chars, default 0)

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
  chunking.json           # auditable chunking config (chars + overlap + separators)
  bin/
    <platform>/           # optional per-platform binary build (PyInstaller)
      kbtool(.exe)
      kbtool.sha1         # copy of kbtool.sha1 for freshness check
  scripts/
    kbtool.py             # python entrypoint (deterministic)
    kbtool_lib/           # implementation modules (db/search/bundle/hooks/skill-json…)
    reindex.py            # TSV-only reindex helper (fallback)
  indexes/
    headings/            # sharded TSV title→path
    kw/                  # sharded TSV keyword→path (fallback only)
  references/
    <doc_id>/
      metadata.md
      toc.md
      chunks/
```

## Robustness Rules (Do Not Skip)

- Prefer deterministic search→bundle: run `./kbtool bundle --query "..." --neighbors 1 --out bundle.md`, then answer from `bundle.md`.
- Use neighbor expansion instead of structure inference: set `--neighbors 2` to include prev/next chunk context.
- If output is too large, reduce `--neighbors/--limit` or use `--body snippet`.
- Treat `indexes/*` as fallback only; never load a whole large index file if a smaller shard or `toc.md` suffices.

## Dependency Model (Cross-Platform)

- **Required:** `python3`
- **PDF (readable)**: prefers `pdftotext` (poppler-utils). If unavailable, the build fails with actionable instructions.
  - Optional fallback: pass `--pdf-fallback pypdf` (best-effort; requires `pypdf` installed).
- **DOCX:** uses a built-in OOXML extractor (no third-party Python deps); if extraction fails, instruct user to convert DOCX → MD/TXT.

## Pressure Scenarios (Self-Test)

- Missing dependencies: build from PDF on a machine without `pdftotext` (should fail with actionable instructions unless `--pdf-fallback pypdf` is enabled).
- Mixed inputs: build from `.md` + `.txt` + `.docx` in one run (should succeed).
- Rebuild safety: output skill folder already exists (should refuse unless `--force` is set).
- Version roll-forward: adjust `--chunk-size/--overlap` or inputs, then rerun the generator (use `--force` to overwrite).

## Common Mistakes

- Feeding a scanned PDF: this tool only supports *readable* PDFs; OCR first, or convert to TXT/MD.
- Assuming indexes are “the knowledge”: answers must cite `references/` files actually read; indexes are lookup only.
- Letting the model load huge files: always start from path-direct or per-doc `toc.md`, not `indexes/*` shards.

## Red Flags (Stop and Fix)

- “I’ll just open the whole index, it’s easier” → split the question and use TOC/shards.
- “PDF import failed, so I’ll guess” → stop; convert PDF to text, install `pdftotext`, or try `--pdf-fallback pypdf` for readable PDFs.
