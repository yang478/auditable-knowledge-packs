# 架构与设计

本仓库的核心是一个生成器（`pack-builder`），用于把文档转换成“确定性、非向量”的检索与证据打包工作流。

核心思想：**不要让 LLM 承担检索拼装**。相反，通过脚本生成单一证据文件（`bundle.md`），并强制携带来源信息。

## 总体流程

```text
build_skill.py（离线构建）
  ├─ 解析输入（md/txt/docx/(可读)pdf）或导入 IR（jsonl）
  ├─ 产出 references/ 文件树（可人工审计）
  ├─ 产出 kb.sqlite（节点树 + FTS 索引）
  └─ 产出 scripts/kbtool.py（在线查询 CLI）
      └─ （可选）产出 bin/<platform>/kbtool(.exe)（PyInstaller 单文件可执行）

kbtool.py bundle（在线查询）
  ├─ （可选）hook：pre_search
  ├─ search：确定性迭代检索（≤5 轮，尽量收敛到 2–3 条款/父节点）
  ├─ rerank：确定性打分与融合
  ├─ （可选）hook：post_search / pre_expand
  ├─ expand：邻接扩展 / 父链导航 / 受控的一跳补查
  └─ render：按预算渲染 bundle.md + 来源清单 + 检索轨迹

kbtool.py（原子化工具箱）
  ├─ --skill：输出 JSON “说明书”（便于 LLM 自主调用）
  └─ get-node / get-children / get-parent / get-siblings / follow-references：原子命令（JSON 输出）
```

## 两个“灵活性杠杆点”

在不改变默认确定性主路径的前提下，本项目提供两类可选扩展点：

1) **Data IR（JSONL）输入**：`build_skill.py --ir-jsonl ...` 允许把外部数据源（爬虫/数据库/人工整理/离线加工）产出的节点树直接导入，跳过文档解析，只做 references + SQLite 索引构建。

2) **运行时 Hooks（显式启用）**：`kbtool.py search|bundle --enable-hooks` 会在若干阶段尝试执行 `hooks/*.py`（如 `pre_search`/`post_search`/`pre_expand`/`pre_render`），用于局部改写查询、过滤候选、控制扩展或脱敏渲染。默认关闭以保持确定性与可审计的基线行为。

## 关键设计原则

- **结构优先**：尽量保留章/节/条等结构，便于人工抽查与导航。
- **来源强制**：bundle 中每段内容都保留可回跳到 `references/` 的信息。
- **脚本确定性**：`bundle` 目标是跨模型、跨提示词依然可重复。
- **可审计轨迹**：`bundle.md` 会包含 `## 检索轨迹`，记录每轮检索的收敛/放宽决策。
- **不使用向量**：基于 SQLite FTS5 + 预分词（中文 CJK 2-gram + 英文/数字词）。

## 存储模型（概念）

生成的 `kb.sqlite` 一般包含：
- 文档表（`docs`）
- 节点表（树结构 + 稳定 ID）
- 文本表（`node_text`）
- 检索用的 FTS 表（`node_fts`）
- 可选的 edges/aliases（用于受控扩展与别名匹配）

## 重建索引的安全性

`kbtool.py reindex` 会从 `references/` 重建数据库，并使用 shadow DB + 原子切换，尽量避免半成品索引被激活。
