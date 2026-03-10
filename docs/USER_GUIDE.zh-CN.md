# 使用手册

本文介绍如何从文档生成知识库 skill，以及如何使用生成后的确定性检索流程（`search`/`bundle`）。

## 核心概念

- **生成后的 skill**：一个输出目录（例如 `.claude/skills/my-books/`），包含：
  - `references/`：可打开、可审计的 Markdown 文件树
  - `kb.sqlite`：用于检索的 SQLite + FTS5 索引
  - `scripts/kbtool.py`：在线查询时使用的确定性 CLI
- **确定性 bundle**：不让 LLM 自己“找资料+拼上下文”，而是通过 `kbtool.py bundle` 生成单一证据文件 `bundle.md`，并强制携带来源信息。

## 依赖

- Python 3.10+
- 可选：`pdftotext`（Poppler），用于处理**可读** PDF

扫描版（图片型）PDF 请先 OCR 或转成文本/Markdown。

## 生成 skill

在本仓库中运行：

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "我的文档知识库"
```

说明：
- `--skill-name` 必须匹配 `^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?$`。
- 如需覆盖已有输出目录，使用 `--force`。
- `--out-dir` 可输出到任意位置，并不强依赖 `.claude/skills`。

## Search（用于调试/检查）

`search` 会输出叶子节点的命中列表与片段，便于观察召回质量。

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py search --query "质量保证期限" --out search.md
```

## Bundle（推荐主路径）

`bundle` 会执行 **search → expand → 按预算渲染**，输出单一证据文件。

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py bundle --query "适用范围是什么？" --out bundle.md
```

常用参数：
- `--neighbors 1`：扩展同一父节点下相邻的前/后叶子节点。
- `--max-chars 40000`：bundle 总字符预算。
- `--per-node-max-chars 6000`：单节点过长时截断。
- `--query-mode and|or`：更严格/更宽松地组合 FTS query。
- `--must TERM`（可重复）：必须出现的约束项。
- `--debug-triggers`：输出诊断信息，并启用一跳补查相关行为。

## 带来源回答

打开 `bundle.md`，**仅基于其中内容**进行回答。

文件末尾会自动生成来源清单（指向 `references/` 的路径），引用/复述时建议保留该清单。

## 手工修改 references 后重建索引

如果你手工修改了 `references/` 下的文件，需要重建 SQLite 索引：

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py reindex
```

`reindex` 采用 “shadow rebuild + atomic switch” 思路，尽量避免半成品索引被激活。

## 常见问题排查

- PDF 导入失败：确认安装 `pdftotext`；否则先把 PDF 转成 TXT/MD。
- 输出太长：调小 `--max-chars` 或 `--per-node-max-chars`。
- 命中为空：先跑 `search` 看召回情况，尝试简化 query，或配合 `--query-mode and` / `--must` 约束。
