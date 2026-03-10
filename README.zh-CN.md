# Auditable Knowledge Packs（可审计知识包）

[English](README.md) | [简体中文](README.zh-CN.md)

面向法规/制度/合规与 SOP/手册：生成**可审计、可复现、强出处**的证据包（**不使用向量/embedding**）。

把一份或多份文档（Markdown/TXT/DOCX/可读 PDF）离线生成一个**确定性、非向量**的“知识库 Skill”。在线查询时，不让 LLM 自己“检索 → 打开文件 → 拼上下文 → 写引用”，而是通过确定性 CLI 输出单一证据文件 `bundle.md`（强制携带来源清单）。

## 产出内容

- `references/`：可人工审计的 Markdown 文件树（章/节/条/块）
- `kb.sqlite`：SQLite + FTS5 索引（中文 CJK 2-gram + 英文/数字词；无需向量库）
- `scripts/kbtool.py`：确定性的 `search`/`bundle`/`reindex` 命令，把 query 变成一个 `bundle.md`（并附带指向 `references/` 的来源清单）

## 为什么要这样设计（审计优先）

在合规/制度/SOP 场景里，关键风险往往不是“召回差一点”，而是“出处说不清 / 复现不一致 / 证据链不稳定”。

如果让 LLM 自己去做“检索 → 打开文件 → 拼上下文 → 写引用”，不同模型/不同提示词的结果差异很大，而且很容易丢来源。
本项目把关键路径脚本化，保证**可重复、可追溯**：模型只需要阅读 `bundle.md` 就能稳定回答。

它主要优化：

- **可复现**：同一份输入 + 同一个 query → 命中排序与 bundle 一致
- **可审计**：`references/` 是权威、可回查、可抽检的中间层
- **可控更新**：手改 `references/` 后 `reindex`（shadow rebuild + 原子切换）
- **可移植**：一个目录即一个“知识包”；在线检索只依赖 Python + SQLite

## 与常见 RAG/检索方案对比

- **向量/embedding-first 框架**（LangChain/LlamaIndex/RAGFlow 等）擅长语义召回，但需要 embedding 模型 + 向量库；而且当模型、切分策略或 embedding 变化时，引用稳定性更容易漂移。
- **让模型自己翻文件并写引用**更灵活，但很难复现；不同提示词/不同模型下的证据链一致性也难保证。
- **纯关键词检索**（grep/Elasticsearch/SQLite FTS）擅长“找字符串”，但仍缺少一个确定性、结构感知的上下文拼装器；`kbtool.py bundle` 通过 rerank + 扩展 + 按预算渲染，把结果稳定收敛到单一证据文件。

如果你更需要“强语义改写/同义转述”的匹配，混合检索（关键词 + 向量）通常更合适；如果你更需要稳定证据包与可追溯来源，本项目就是为此设计的。

## 不适合 / 非目标

- 开放域问答（缺少明确的权威文档来源）
- 主要依赖同义改写/转述的语义匹配（原文里不出现关键字）
- 需要模型自由探索式“边搜边看边试”的研究型任务

## 30 秒理解

```text
文档输入 ──(build_skill.py)──> references/ + kb.sqlite
提出问题 ──(kbtool.py bundle)─> bundle.md（证据 + 引用）
```

## 快速开始

### 依赖

- Python **3.10+**
- 可选（处理可读 PDF）：`pdftotext`（Poppler）。扫描版 PDF 请先 OCR 或转成文本。

### 从文档生成 skill

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "我的文档知识库"
```

输出目录：`.claude/skills/my-books/`

### 生成确定性的证据包（bundle）

```bash
cd .claude/skills/my-books
python3 scripts/kbtool.py bundle --query "适用范围是什么？" --out bundle.md
```

打开 `bundle.md`，基于其中内容回答（并复制底部自动生成的“参考依据/来源清单”）。

## 文档

- 使用手册：`docs/USER_GUIDE.zh-CN.md`
- 架构/设计：`docs/ARCHITECTURE.zh-CN.md`
- 开发指南：`docs/DEVELOPMENT.zh-CN.md`

## 仓库结构

```
pack-builder/
  scripts/build_skill.py        # 生成器 CLI
  templates/                    # 生成 skill 的模板（kbtool.py, reindex.py）
docs/
  USER_GUIDE.zh-CN.md           # 使用手册
  ARCHITECTURE.zh-CN.md         # 架构/设计
  DEVELOPMENT.zh-CN.md          # 开发指南
```

## 开发与测试

运行单元测试：

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## License

Apache-2.0，见 `LICENSE`。
