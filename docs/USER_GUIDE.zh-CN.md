# 使用手册

本文介绍如何从文档生成知识库 skill，以及如何使用生成后的确定性检索流程（`search`/`research`）。

## 核心概念

- **生成后的 skill**：一个输出目录（例如 `.claude/skills/my-books/`），包含：
  - `references/`：可打开、可审计的 Markdown 文件树
  - `kb.sqlite`：用于检索的 SQLite + FTS5 索引
  - `kbtool` / `kbtool.cmd`：推荐入口（优先匹配当前平台 fresh binary，回退到 Python）
  - `scripts/kbtool.py` + `scripts/kbtool_lib/`：在线查询时使用的确定性 CLI 实现
  - （可选）`bin/<platform>/kbtool(.exe)`：PyInstaller 打包后的单文件可执行工具（无 Python 依赖）
- **确定性 research（单轮）**：不让 LLM 自己“找资料+拼上下文”，而是通过 `kbtool research` 生成一轮可审计产物：
  - `run_dir/bundle.roundNN.md`（证据 + 引用 + 回答约束）
  - `run_dir/trace.roundNN.json` + `run_dir/verify.roundNN.json`，并追加 `run_dir/trace.jsonl`
  - stdout JSON，供 LLM 决定是否进入下一轮以及如何调参

## 依赖

- Python 3.10+
- 可选：`pdftotext`（Poppler），用于处理**可读** PDF
- 可选：PyInstaller（用于 `--package-kbtool` 生成无 Python 的可执行文件）

扫描版（图片型）PDF 请先 OCR 或转成文本/Markdown。

## 生成 skill

在本仓库中运行：

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-books \
  --out-dir .claude/skills \
  --inputs /path/to/book1.pdf /path/to/book2.docx /path/to/notes.md \
  --title "我的文档知识库" \
  --package-kbtool
```

说明：
- `--skill-name` 必须匹配 `^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?$`。
- 如需覆盖已有输出目录，使用 `--force`。
- `--out-dir` 可输出到任意位置，并不强依赖 `.claude/skills`。
- `--package-kbtool` 只会为**当前平台**打包二进制；如需同一 skill 同时包含 Windows+Linux 两套二进制，请分别在两端运行生成/打包，并保留同一个输出目录（`--force` 重建会尽量保留已有 `bin/`）。

### （可选）从 IR（JSONL）生成 skill

当你的“知识”不是来自文件（或你想先做类似 SEO 的离线加工：提炼标题/别名/分层/摘要等），可以把数据整理成 JSON Lines（每行一个 JSON），再用 `--ir-jsonl` 构建：

```bash
python3 pack-builder/scripts/build_skill.py \
  --skill-name my-kb \
  --out-dir .claude/skills \
  --ir-jsonl /path/to/ir.jsonl \
  --title "我的知识库（IR）"
```

IR v1 支持两类行：

- `type=doc`：声明文档元信息（`doc_id`/`title`/`source_file`/`source_version`/`doc_hash` 可选）
- `type=node`：声明节点（至少包含 `doc_id`/`node_id`/`kind`/`title`/`body_md`；可选 `parent_id`/`ordinal`/`aliases`/`confidence`）

当前 `kind` 主要用于生成 `references/` 与索引，推荐使用：`article` / `block` / `item`。

## Search（用于调试/检查）

`search` 会输出叶子节点的命中列表与片段，便于观察召回质量。

```bash
cd .claude/skills/my-books
./kbtool search --query "质量保证期限" --out search.md
# 或（python）：python3 scripts/kbtool.py search --query "质量保证期限" --out search.md
# 或（binary）：bin/<platform>/kbtool search --query "质量保证期限" --out search.md
```

## Research（推荐主路径）

`research` 会执行 **确定性迭代检索（≤5 轮）→ expand → 按预算渲染 → verify**，并写出一轮可审计产物。

```bash
cd .claude/skills/my-books
./kbtool research \
  --query "适用范围是什么？" \
  --run-dir research_runs/case-001 \
  --planner-json '{"model":"gpt-5","temperature":0.2,"prompt_sha256":"..."}'
# 或（python）：python3 scripts/kbtool.py research --query "..." --run-dir research_runs/case-001 --planner-json '{...}'
# 或（binary）：bin/<platform>/kbtool research --query "..." --run-dir research_runs/case-001 --planner-json '{...}'
```

审计说明：
- `bundle.roundNN.md` 会包含 `## 检索轨迹`（并在顶部写明 Evidence-only 的回答约束）。
- 检索阶段全程不调用任何 LLM；LLM 只负责决定是否进入下一轮以及传入什么参数。
- `--planner-json` 属于审计链路的一部分，建议至少包含：`{model, temperature, prompt_sha256}`（或用 `prompt_path`）。
  - 若在 `planner-json` 里提供 `prompt_path`（文件）或 `prompt`（字符串），kbtool 会把 prompt 复制到 `run_dir/` 并计算 `prompt_sha256`。
- 出于安全考虑，`--run-dir` 必须位于 **skill 根目录内**（拒绝路径穿越与 root 外绝对路径）。

常用参数：
- `--run-dir research_runs/case-001`：产物输出目录。
- `--round 0`：0 = 自动递增（推荐）；也可以手动指定轮次编号。
- `--note "..."`：把备注写入 trace（自由文本）。
- `--neighbors 1`：扩展同一父节点下相邻的前/后叶子节点。
- `--max-chars 40000`：bundle 总字符预算。
- `--per-node-max-chars 6000`：单节点过长时截断。
- `--query-mode and|or`：更严格/更宽松地组合 FTS query。
- `--must TERM`（可重复）：必须出现的约束项。若 TERM 像文档号且能匹配到 `doc_id`（例如 `1993-1-1`），会被当作 doc hint 用于优先/限定文档，而不是要求其出现在正文里。
- `--timeout-ms 2000`：SQLite 查询超时保护（0 = 关闭）。
- 迭代检索参数（可选）：
  - `--iter-max-rounds 3`：最多迭代轮数（1 = 退化为单轮检索）。
  - `--iter-focus-max-articles 2`：尝试收敛到不超过 N 个条款/父节点。
  - `--iter-mass-top3-threshold 0.9`：更严格的收敛阈值。
  - `--no-iter`：关闭迭代收敛（单轮检索）。
- `--debug-triggers`：输出诊断信息，并启用一跳补查相关行为。
- `--enable-hooks`：启用运行时 hooks（见下节）。

若 verify 未通过（stdout JSON 里 `ok=false`），修改 query/参数后继续在同一 `--run-dir` 跑下一轮（省略 `--round` 让它自动递增）。`verify.roundNN.json` 会给出 `suggested_next_params` 帮助自动调参。

## 原子化命令（JSON 输出，便于 LLM 自主组合）

生成后的 `kbtool.py` 还提供一组“原子命令”，每条命令只做一件确定的事，直接输出 JSON：

```bash
cd .claude/skills/my-books
./kbtool --skill
./kbtool get-node "standard-v1:article:0003"
./kbtool follow-references "standard-v1:article:0003" --direction out
```

## 运行时 Hooks（可选，默认关闭）

在 skill 根目录创建 `hooks/`，放入下列文件即可在运行时“插一脚”（只有在传入 `--enable-hooks` 时才会执行）：

- `hooks/pre_search.py`：改写 query / must / query_mode
- `hooks/post_search.py`：过滤/重排搜索候选（返回 `hits`）
- `hooks/pre_expand.py`：在补查前调整节点集合（返回 `hits`）
- `hooks/pre_render.py`：渲染前修改节点展示（如脱敏/替换正文）

每个 hook 文件需提供函数：`run(payload: dict) -> dict`。
如果存在 `hooks/allowlist.sha1`，kbtool 只会执行 sha1 在 allowlist 中的 hook（每行一个 sha1）。

审计性说明：
- hooks 会执行本地 Python 代码，请仅在信任该 skill 内容时启用。
- `bundle.roundNN.md` 的 `## 补查记录` 会记录已启用 hook 的 `sha1`（例如 `hook: pre_search sha1=...`），便于追溯当次输出使用了哪个 hook 版本。

## 带来源回答

打开 `bundle.roundNN.md`，**仅基于其中内容**进行回答。

文件末尾会自动生成来源清单（指向 `references/` 的路径），引用/复述时建议保留该清单。

## 手工修改 references 后重建索引

如果你手工修改了 `references/` 下的文件，需要重建 SQLite 索引：

```bash
cd .claude/skills/my-books
./kbtool reindex
# 或（python）：python3 scripts/kbtool.py reindex
# 或（binary）：bin/<platform>/kbtool reindex
```

`reindex` 采用 “shadow rebuild + atomic switch” 思路，尽量避免半成品索引被激活。

## 常见问题排查

- PDF 导入失败：确认安装 `pdftotext`；否则先把 PDF 转成 TXT/MD。
- 输出太长：调小 `--max-chars` 或 `--per-node-max-chars`。
- 命中为空：先跑 `search` 看召回情况，尝试简化 query，或配合 `--query-mode and` / `--must` 约束。
