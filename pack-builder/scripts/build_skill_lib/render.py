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
    lines.append("2. 生成证据包并回答（推荐）：\n")
    lines.append("   - `./kbtool bundle --query \"...\" --out bundle.md`\n")
    lines.append("   - 若需要更小的上下文：加 `--body snippet --neighbors 0 --per-node-max-chars 800 --max-chars 12000`\n")
    lines.append("   - 仅基于 `bundle.md` 作答，并把其中的 `## 参考依据` 原样附在答案末尾。\n\n")
    lines.append("3. 没命中时先搜索再 bundle：\n")
    lines.append("   - `./kbtool search --query \"...\" --out search.md`\n")
    lines.append("   - 需要更严格时：`--must \"...\"`（可重复）或 `--query-mode and`。\n\n")
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
    lines.append("- 运行路径：标题/正文/术语 三路召回 → 确定性融合排序 → 最多一轮补查 → `bundle.md`。\n")
    lines.append("- 补查只做一轮，动作限定为 definition / references / version_metadata。\n")
    lines.append("\n### Tool Entry\n\n")
    lines.append("- 推荐入口（自动选择 binary 或 python）：\n")
    lines.append("  - macOS/Linux: `./kbtool --skill`\n")
    lines.append("  - Windows: `kbtool.cmd --skill`\n")
    lines.append("- 直接入口（调试/指定路径时用）：\n")
    lines.append("  - Python: `python3 scripts/kbtool.py --skill`\n")
    lines.append("  - Binary: `bin/<platform>/kbtool --skill`\n")
    lines.append("- 原子化查询（JSON 输出）：`get-node` / `get-children` / `get-parent` / `get-siblings` / `follow-references`\n")
    lines.append("\n1. (Optional) Preview ranked hits:\n\n")
    lines.append("   - Recommended: `./kbtool search --query \"...\" --out search.md`\n")
    lines.append("   - Python: `python3 scripts/kbtool.py search --query \"...\" --out search.md`\n")
    lines.append("   - Binary: `bin/<platform>/kbtool search --query \"...\" --out search.md`\n\n")
    lines.append("2. Generate a single evidence bundle:\n\n")
    lines.append("   - Recommended: `./kbtool bundle --query \"...\" --out bundle.md`\n")
    lines.append("   - Python: `python3 scripts/kbtool.py bundle --query \"...\" --out bundle.md`\n")
    lines.append("   - Binary: `bin/<platform>/kbtool bundle --query \"...\" --out bundle.md`\n\n")
    lines.append("   Tips:\n")
    lines.append("   - For noisy queries: add `--must \"...\"` (repeatable) or `--query-mode and`.\n")
    lines.append("   - For timeline questions: add `--order chronological`.\n")
    lines.append("   - If the query suggests definitions / scope / exceptions, inspect `## 补查记录` for the one-round expansion.\n\n")
    lines.append("3. Open `bundle.md`, then answer using only its contents.\n")
    lines.append("4. Copy/paste the auto-generated `## 参考依据` section from `bundle.md` at the end of your answer.\n")
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

