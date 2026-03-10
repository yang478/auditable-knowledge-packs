# 架构与设计

本仓库的核心是一个生成器（`pack-builder`），用于把文档转换成“确定性、非向量”的检索与证据打包工作流。

核心思想：**不要让 LLM 承担检索拼装**。相反，通过脚本生成单一证据文件（`bundle.md`），并强制携带来源信息。

## 总体流程

```text
build_skill.py（离线构建）
  ├─ 解析输入（md/txt/docx/(可读)pdf）
  ├─ 产出 references/ 文件树（可人工审计）
  ├─ 产出 kb.sqlite（节点树 + FTS 索引）
  └─ 产出 scripts/kbtool.py（在线查询 CLI）

kbtool.py bundle（在线查询）
  ├─ search：FTS 初召回
  ├─ rerank：确定性打分与融合
  ├─ expand：邻接扩展 / 父链导航 / 受控的一跳补查
  └─ render：按预算渲染 bundle.md + 来源清单
```

## 关键设计原则

- **结构优先**：尽量保留章/节/条等结构，便于人工抽查与导航。
- **来源强制**：bundle 中每段内容都保留可回跳到 `references/` 的信息。
- **脚本确定性**：`bundle` 目标是跨模型、跨提示词依然可重复。
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
