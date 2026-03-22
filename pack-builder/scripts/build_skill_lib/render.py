from __future__ import annotations

import os
from collections import Counter
from typing import Dict, List

from .catalog import doc_category
from .types import InputDoc


def skill_md_docs_limit() -> int:
    raw = os.environ.get("PACK_BUILDER_SKILL_MD_DOCS_LIMIT", "").strip()
    if not raw:
        return 200
    try:
        limit = int(raw)
    except ValueError:
        return 200
    if limit < 0:
        return 200
    return limit


def render_generated_skill_md(
    skill_name: str,
    title: str,
    docs: List[InputDoc],
    *,
    category_overrides: Dict[str, str] | None = None,
) -> str:
    doc_list = ", ".join(d.title for d in docs[:5])
    if len(docs) > 5:
        doc_list += "…"
    desc = (
        f"Use when querying the generated documents knowledge base ({doc_list}) and needing "
        "deterministic search→context bundling with citations (no embeddings)."
    )
    frontmatter = f"---\nname: {skill_name}\ndescription: {desc}\n---\n\n"

    lines: List[str] = [frontmatter]
    lines.append(f"# {title}\n\n")

    lines.append("## TL;DR (LLM/Agent)\n\n")
    lines.append("最短路径（不要翻目录）：\n\n")
    lines.append("1. 获取机器可读说明书（JSON）：\n")
    lines.append("   - macOS/Linux: `./kbtool --skill`\n")
    lines.append("   - Windows: `kbtool.cmd --skill`\n\n")
    lines.append("2. （推荐）LLM-in-the-loop 单轮 research（bundle+verify+trace；LLM 决定是否下一轮）：\n")
    lines.append(
        "   - `./kbtool research --query \"...\" --run-dir research_runs/case-001 --planner-json '{\"model\":\"...\",\"temperature\":0.2,\"prompt_sha256\":\"...\"}'`\n"
    )
    lines.append("   - 会写出：`bundle.roundNN.md` / `trace.roundNN.json` / `verify.roundNN.json`，并追加 `trace.jsonl`；stdout 返回 JSON。\n")
    lines.append("   - 审计要求：`--planner-json` 至少包含 `model` / `temperature` / `prompt_sha256`（或 `prompt_path`）。\n")
    lines.append("   - 若需要更小的上下文：加 `--body snippet --neighbors 0 --per-node-max-chars 800 --max-chars 12000`\n\n")
    lines.append("3. （可选）预览命中（不产出审计/verify）：\n")
    lines.append("   - `./kbtool search --query \"...\" --out search.md`\n")
    lines.append(
        "   - 需要更严格时：`--must \"...\"`（可重复）或 `--query-mode and`。（注意：`--must` 通常是“文本必须出现”；但像 `1993-1-1` 这类“文档号/规范号”若能匹配到 doc_id，会被当作 doc hint 用于优先/限定文档，并不会要求它出现在正文里。）\n\n"
    )
    lines.append("规则：\n")
    lines.append("- 始终使用根目录入口：不要直接调用 `scripts/` 或 `bin/`。\n")
    lines.append("- Hooks 默认关闭；仅在你信任本地代码时使用 `--enable-hooks`。\n\n")

    docs_limit = skill_md_docs_limit()
    lines.append(f"## Documents ({len(docs)})\n\n")
    if len(docs) <= docs_limit:
        for d in docs:
            lines.append(f"- {d.doc_id}: `{d.path.name}`（标题：{d.title}）\n")
    else:
        lines.append(
            f"- 本技能包含 {len(docs)} 份文档，已超过 SKILL.md 列表上限（{docs_limit}），不在此展开。\n"
        )
        lines.append("- 按“图书馆目录/分类”浏览：打开 `catalog/categories.md`。\n\n")

        def cat_for_doc(d: InputDoc) -> str:
            if category_overrides:
                return category_overrides.get(d.doc_hash) or doc_category(d)
            return doc_category(d)

        cat_counts = Counter(cat_for_doc(d) for d in docs)
        lines.append("### Categories (Top)\n\n")
        for cat, n in cat_counts.most_common(20):
            lines.append(f"- {cat}（{n}）\n")
        if len(cat_counts) > 20:
            lines.append(f"- …（另有 {len(cat_counts) - 20} 个分类）\n")

    lines.append("\n## Recommended Workflow (Deterministic)\n\n")
    lines.append("- 运行路径：标题/正文/术语 三路召回 → 确定性融合排序 → 最多一轮补查 → `bundle.roundNN.md`。\n")
    lines.append("- 推荐用 `kbtool research`：每一轮都会落盘 `bundle.roundNN.md` + `trace.roundNN.json` + `verify.roundNN.json`，并追加 `trace.jsonl`。\n")
    lines.append("- `verify.roundNN.json` 会给出 blocking_issues / suggestions / suggested_next_params，供 LLM 决定是否进入下一轮。\n")
    lines.append("\n### Tool Entry\n\n")
    lines.append("- 推荐入口（自动选择 binary 或 python）：\n")
    lines.append("  - macOS/Linux: `./kbtool --skill`\n")
    lines.append("  - Windows: `kbtool.cmd --skill`\n")
    lines.append("- 直接入口（调试/指定路径时用）：\n")
    lines.append("  - Python: `python3 scripts/kbtool.py --skill`\n")
    lines.append("  - Binary: `bin/<platform>/kbtool --skill`\n")
    lines.append("- 原子化查询（JSON 输出）：`get-node` / `get-children` / `get-parent` / `get-siblings` / `follow-references`\n")
    lines.append("\n1. Run ONE research round (recommended):\n\n")
    lines.append(
        "   - Recommended: `./kbtool research --query \"...\" --run-dir research_runs/case-001 --planner-json '{\"model\":\"...\",\"temperature\":0.2,\"prompt_sha256\":\"...\"}'`\n"
    )
    lines.append(
        "   - Python: `python3 scripts/kbtool.py research --query \"...\" --run-dir research_runs/case-001 --planner-json '{\"model\":\"...\",\"temperature\":0.2,\"prompt_sha256\":\"...\"}'`\n"
    )
    lines.append(
        "   - Binary: `bin/<platform>/kbtool research --query \"...\" --run-dir research_runs/case-001 --planner-json '{\"model\":\"...\",\"temperature\":0.2,\"prompt_sha256\":\"...\"}'`\n\n"
    )
    lines.append("   Tips:\n")
    lines.append("   - Reduce context: add `--body snippet --neighbors 0 --per-node-max-chars 800 --max-chars 12000`.\n")
    lines.append("   - For noisy queries: add `--must \"...\"` (repeatable) or `--query-mode and`.\n")
    lines.append("   - For timeline questions: add `--order chronological`.\n")
    lines.append("   - If the query suggests definitions / scope / exceptions, inspect `## 补查记录` (one-round expansion).\n\n")
    lines.append(
        "2. Open the generated `bundle.roundNN.md`, then answer using only its contents (DO NOT invent formulas that do not appear in the bundle).\n"
    )
    lines.append("   - If an equation is missing/blank, say so and cite only its number (e.g. `(6.xx)`), do not reconstruct it from memory.\n")
    lines.append("3. Copy/paste the auto-generated `## 参考依据` section from that bundle at the end of your answer.\n")
    lines.append("4. If verify fails, revise query/params and run the NEXT research round (same `--run-dir`, omit `--round` so it auto-increments).\n")
    lines.append("5. (Optional) Preview ranked hits:\n\n")
    lines.append("   - Recommended: `./kbtool search --query \"...\" --out search.md`\n")
    lines.append("   - Python: `python3 scripts/kbtool.py search --query \"...\" --out search.md`\n")
    lines.append("   - Binary: `bin/<platform>/kbtool search --query \"...\" --out search.md`\n")
    lines.append("\n## Direct Lookup (Fallback)\n\n")
    lines.append("- If the user specifies a document/chapter/section, open `references/<doc_id>/toc.md`, then open the referenced `references/` file directly.\n")
    lines.append("\n## Rebuild\n\n")
    lines.append("- If you edit `references/`, rebuild `kb.sqlite`:\n")
    lines.append("  - Recommended: `./kbtool reindex`\n")
    lines.append("  - Python: `python3 scripts/kbtool.py reindex`\n")
    lines.append("  - Binary: `bin/<platform>/kbtool reindex`\n")
    lines.append("- `reindex` uses shadow rebuild + 原子重建 / atomic switch, and keeps older document versions as inactive rows when the version changes.\n")
    lines.append("- If you add/remove input documents, rerun the generator.\n")
    lines.append("- Optional (TSV only): `python3 scripts/reindex.py`\n")
    return "".join(lines)
