# 架构与设计

本仓库的核心是一个生成器（`pack-builder`），用于把文档转换成“确定性、非向量”的检索与证据打包工作流。

核心思想：**不要让 LLM 承担检索拼装**。相反，通过脚本生成一组可审计产物：面向 LLM 的 `bundle.json`、回答时使用的 `bundle.md`，以及 round 级机器审计文件 `trace.roundNN.json` / `verify.roundNN.json`，并且都强制携带来源信息。

## 总体流程

```text
build_skill.py（离线构建）
  ├─ 解析输入（md/txt/docx/(可读)pdf）或导入 IR（jsonl）
  ├─ 产出 references/ 文件树（可人工审计）
  ├─ 产出 kb.sqlite（节点树 + FTS 索引）
  ├─ 产出 phase_a_artifact.json（供 runtime adapter 直接读取的导出契约）
  ├─ 产出 build_state.json（指纹、导出校验值、索引 binding 状态）
  └─ 产出 scripts/kbtool.py（在线查询 CLI）
      └─ （可选）产出 bin/<platform>/kbtool(.exe)（PyInstaller 单文件可执行）

kbtool.py research（在线查询，一次受限的迭代式 Phase A 运行）
  ├─ （可选）hook：pre_search
  ├─ search：确定性迭代检索（≤5 轮，尽量收敛到 2–3 条款/父节点）
  ├─ rerank：确定性打分与融合
  ├─ （可选）hook：post_search / pre_expand
  ├─ expand：邻接扩展 / 父链导航 / 受控的一跳补查
  ├─ render：写出 verify.roundNN.json（机器校验 + v2 control state）
  ├─ render：写出 trace.roundNN.json（round 的 input/effective/retrieval 与 v2 round objects）
  ├─ render：按预算渲染 bundle.json（`search_goal`、`coverage_assessment`、`answerability_assessment`、`probe_trace`、`evidence_items`、`round_decision`）
  └─ render：写出 bundle.md（Search Goal + Coverage Assessment + Answerability Assessment + Probe Trace + Round Decision + Evidence + References）

kbtool.py（原子化工具箱）
  ├─ --skill：输出 JSON “说明书”（便于 LLM 自主调用）
  └─ get-node / get-children / get-parent / get-siblings / follow-references：原子命令（JSON 输出）
```

## 两个“灵活性杠杆点”

在不改变默认确定性主路径的前提下，本项目提供两类可选扩展点：

1) **Data IR（JSONL）输入**：`build_skill.py --ir-jsonl ...` 允许把外部数据源（爬虫/数据库/人工整理/离线加工）产出的节点树直接导入，跳过文档解析，只做 references + SQLite 索引构建。

2) **IR 导出桥接**：构建产物还会生成 `phase_a_artifact.json`，把文档元数据、节点拓扑、aliases、edges 和 locators 以 parser-agnostic 的方式导出给 runtime adapter，避免运行时再去依赖 parser-specific 分支。

3) **增量状态**：`build_state.json` 会冻结 source/text/span/node 指纹，以及各索引族的 binding checksum，用于判断哪些文档和索引片段真的脏了，哪些只需要 provenance 级刷新。

4) **运行时 Hooks（显式启用）**：`kbtool.py search|research --enable-hooks` 会在若干阶段尝试执行 `hooks/*.py`（如 `pre_search`/`post_search`/`pre_expand`/`pre_render`），用于局部改写查询、过滤候选、控制扩展或脱敏渲染。默认关闭以保持确定性与可审计的基线行为。

## 关键设计原则

- **结构优先**：尽量保留章/节/条等结构，便于人工抽查与导航。
- **来源强制**：bundle 中每段内容都保留可回跳到 `references/` 的信息。
- **脚本确定性**：`research` 是有界的 rules-only 迭代主链，目标是在跨模型、跨提示词时依然保持可重复。
- **双信号契约**：每个 round 统一由检索/覆盖对象与 answerability/probe 状态共同表达：`search_goal`、`coverage_assessment`、`answerability_assessment`、`probe_trace`、`evidence_items`、`round_decision`。
- **可审计轨迹**：`bundle.md` 面向回答阶段，`trace.roundNN.json` 与 `verify.roundNN.json` 保留机器可读的 round 状态与校验结果。
- **不使用向量**：基于 SQLite FTS5 + 预分词（中文 CJK 2-gram + 英文/数字词）。

## 存储模型（概念）

生成的 `kb.sqlite` 一般包含：
- 文档表（`docs`）
- 节点表（树结构 + 稳定 ID）
- 文本表（`node_text`）
- 检索用的 FTS 表（`node_fts`）
- 可选的 edges/aliases（用于受控扩展与别名匹配）

除此之外，artifact 根目录还会生成 `phase_a_artifact.json` 和 `build_state.json`。Phase A runtime adapter 会优先读取前者来获取文档/节点拓扑与 locator 元数据，而后者负责记录可审计的增量状态与索引 binding。

## 重建索引的安全性

`kbtool.py reindex` 会从 `references/` 重建数据库，并使用 shadow DB + 原子切换，尽量避免半成品索引被激活。原子切换完成后还会同步刷新 `corpus_manifest.json`、`phase_a_artifact.json` 和 `build_state.json`，并输出一条 incremental refresh 摘要，说明脏文档数、重写行数与受影响索引族。
