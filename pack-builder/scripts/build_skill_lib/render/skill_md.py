from __future__ import annotations

from typing import List

from ..utils.fs import normalize_title_whitespace
from ..types import InputDoc

# Unified audit output directory for all kbtool commands.
RUNS_DIR = "runs/"


def _clean_title(value: str) -> str:
    return normalize_title_whitespace(value)


def render_generated_skill_md(
    skill_name: str,
    title: str,
    docs: List[InputDoc],
) -> str:
    doc_list = ", ".join(_clean_title(d.title) for d in docs[:5])
    if len(docs) > 5:
        doc_list += "..."

    desc = (
        f"当用户询问以下文档相关内容时使用：{doc_list}。"
        "入口：进入 skill 目录，优先运行 ./kbtool triage 检索证据并引用 references/...。"
        "触发条件：用户提问涉及这些文档中的概念、定义、流程、数据。"
        "不触发：与文档无关的通用知识问题。"
    )
    frontmatter = f'---\nname: {skill_name}\ndescription: "{desc}"\n---\n\n'

    lines: List[str] = [frontmatter]
    lines.append(f"# {title}\n\n")
    lines.append("> **核心原则**：查询是查询，生成是生成。所有结论必须来自 `runs/` 或 `references/` 的文本，不要凭记忆补全。\n\n")
    lines.append("## 默认流程\n\n")
    lines.append("```bash\n")
    lines.append("cd <本 skill 目录>\n")
    lines.append(f'./kbtool triage --query "问题" --out {RUNS_DIR}r1-triage.md\n')
    lines.append("```\n\n")
    lines.append("- 先读再决定：每轮只读生成的 `runs/*.md`，再决定是否补查。如需精读 chunk 原文，**必须使用** `./kbtool get-node <node_id> --format body`。\n")
    lines.append("- **停止规则**：如果连续 **2 轮** triage/search 都无命中，立即停止搜索，向用户报告\"未找到相关证据\"，不要无限换关键词尝试。\n")
    lines.append("- 最多 3 轮：R1 用默认 `triage`；R2 换关键词或精确 `search`；R3 只精读少量原文。\n")
    lines.append("- 默认已启用 1 跳证据导航图，边类型为 `prev next references alias_mention title_mention`，不默认走高噪声 `co_occurrence`。\n")
    lines.append("- 如果图带来噪声或上下文变大，下一轮加 `--graph-depth 0`；不要通过盲目增大 `--limit` 堆上下文。\n")
    lines.append("- 回答必须引用 `references/...`，未找到证据就明确说未找到。\n\n")

    lines.append("## 推理链查询（关键）\n\n")
    lines.append("当问题包含多跳因果关系（A->B->C->...->答案）时，**禁止一次性查询所有关键词**。必须分轮迭代：\n\n")
    lines.append("1. **从起点开始**：每轮只查询 1-2 个环节的关键词。\n")
    lines.append("2. **用关键词辅助发现线索**：开启 `--show-keywords` 后，每个 chunk 后面会列出高频关键词。\n")
    lines.append("   - 基础用法：直接看 chunk 的 keywords，从中挑选下一跳的人名/物品/事件作为下一轮查询词。\n")
    lines.append("   - 进阶用法：调高 `--keyword-count`（如 12）来发现更多线索，或结合 `--preset standard` 获取更大上下文。\n")
    lines.append("3. **用新线索推进**：将提取到的新关键词作为下一轮查询词。\n")
    lines.append("4. **保留审计轨迹**：每轮使用新的 `--out` 文件名（r1, r2, r3...）。\n")
    lines.append("5. **不跳过环节**：如果某一轮没有命中，调整关键词重新查询，直到确认这一跳。\n\n")
    lines.append("**分轮迭代示例（每轮加 --show-keywords）**：\n")
    lines.append(f"- R1: `./kbtool triage --query '角色A' --show-keywords --out {RUNS_DIR}r1.md` -> 看到 keywords 有 [角色B, 物品X]，确认交好物件\n")
    lines.append(f"- R2: `./kbtool triage --query '角色B 物品X' --show-keywords --out {RUNS_DIR}r2.md` -> 看到 keywords 有 [第三方C]，确认上门者\n")
    lines.append(f"- R3: `./kbtool triage --query '第三方C' --show-keywords --out {RUNS_DIR}r3.md` -> 看到 keywords 有 [角色D, 受罚]，确认受罚者\n")
    lines.append(f"- 加速收敛：`--preset standard --show-keywords --keyword-count 10` 一次获取更多线索\n\n")

    lines.append("## 常用命令\n\n")
    lines.append(f"- 快速证据包：`./kbtool triage --query \"问题\" --out {RUNS_DIR}r1-triage.md`\n")
    lines.append(f"- 关键词辅助（推理链推荐）：`./kbtool triage --query \"关键词\" --show-keywords --out {RUNS_DIR}r1-triage.md`\n")
    lines.append(f"- 更多关键词：`./kbtool triage --query \"关键词\" --show-keywords --keyword-count 10 --out {RUNS_DIR}r1-triage.md`\n")
    lines.append(f"- BM25 小包：`./kbtool bundle --query \"关键词\" --out {RUNS_DIR}r1-bundle.md`\n")
    lines.append(f"- 关闭图：`./kbtool triage --query \"关键词\" --graph-depth 0 --out {RUNS_DIR}r2-no-graph.md`\n")
    lines.append(f"- 精确搜索：`./kbtool search --pattern \"原句或术语\" --fixed --out {RUNS_DIR}r2-search.md`\n")
    lines.append(f"- 查文件名：`./kbtool files --pattern \"文件名\" --out {RUNS_DIR}files.md`\n")
    lines.append("- 定点精读：`./kbtool get-node <node_id> --format body`（唯一方式）。\n\n")
    lines.append("## 精读规范（必须遵守）\n\n")
    lines.append("1. **精读 chunk 必须使用 `get-node`**：从 triage 结果中提取 `node_id`（如 `doc:chunk:0012`），运行 `./kbtool get-node doc:chunk:0012 --format body`。\n")
    lines.append("2. **禁止直接 `read` 参考文献**：不要直接读取 `references/.../chunks/*.md` 文件。直接读取会绕过审计日志，导致轨迹断裂。\n")
    lines.append("3. **优先读 `runs/*.md`**：triage/bundle 的输出文件已经聚合了证据，优先读这些审计文件，只在需要细节时才用 get-node 精读单个 chunk。\n\n")
    lines.append("### ✅ 正确 vs ❌ 错误示例\n\n")
    lines.append("**✅ 正确流程**：\n")
    lines.append("1. `./kbtool triage --query \"关键词\" --out runs/r1.md`\n")
    lines.append("2. 读 `runs/r1.md`，看到命中 chunk 的 `node_id`\n")
    lines.append("3. `./kbtool get-node doc:chunk:0012 --format body` 精读原文\n\n")
    lines.append("**❌ 错误流程**：\n")
    lines.append("1. `./kbtool triage --query \"关键词\"`\n")
    lines.append("2. 直接 `read references/.../chunks/chunk-0012.md` ← 跳过 get-node，审计断裂\n\n")

    lines.append("## 调参原则\n\n")
    lines.append("- 默认参数就是推荐起步：`triage` 默认 quick、小 `search-limit`、1 跳低噪图。\n")
    lines.append("- 推理链查询必开 `--show-keywords`：让工具帮你提取关键词，不用自己从长文本中找。\n")
    lines.append("- 加速收敛：发现线索密集时，用 `--preset standard --show-keywords --keyword-count 10` 一次获取更多线索。\n")
    lines.append('- 需要覆盖"所有/几次/列举"时，用 `search --pattern "别称1|别称2"` 做穷举，再精读命中片段。\n')
    lines.append("- 只有确认证据不足时，再小幅增加 `--limit`、`--per-node-max-chars` 或改用 `--preset standard`。\n\n")

    lines.append("## 输出位置\n\n")
    lines.append(f"- `{RUNS_DIR}`：检索审计文件。\n")
    lines.append("- `references/<doc_id>/chunks/*.md`：可引用原文 chunk。\n")
    lines.append("- `references/<doc_id>/toc.md`：文档/目录预览。\n")
    lines.append("- `kb.sqlite`：FTS5 与图边索引。\n\n")

    # -- Document List --
    lines.append("## 文档列表\n\n")
    lines.append("| doc_id | 标题 | 目录 |\n|---|---|---|\n")
    for d in sorted(docs, key=lambda x: (x.doc_id, x.source_version)):
        toc = f"references/{d.doc_id}/toc.md"
        lines.append(f"| `{d.doc_id}` | {_clean_title(d.title)} | `{toc}` |\n")

    return "".join(lines)
