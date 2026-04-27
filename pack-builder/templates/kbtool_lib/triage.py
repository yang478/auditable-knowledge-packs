from __future__ import annotations

import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from . import memory
from .retrieval import (
    _BundleResult,
    _execute_bundle_core,
)
from .runtime import SqliteTimeout, die, escape_markdown_inline, open_db, print_json, resolve_db_path, resolve_root, safe_output_path
from .safe_subprocess import run_subprocess_safe
from .signals import install_signal_handlers, is_shutdown_requested, shutdown_executor_now
from .text import build_punctuation_tolerant_regex
from .tools import parse_rg_output, resolve_tool_binary

# Defense-in-depth: truncate excessively long patterns to avoid resource exhaustion.
_MAX_SUBPROCESS_PATTERN_LEN = 8192


def _run_bundle_task(root: Path, args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    conn = open_db(root / str(getattr(args, "db", "kb.sqlite") or "kb.sqlite"))
    try:
        timeout_ms = int(getattr(args, "timeout_ms", 0) or 0)
        with SqliteTimeout(conn, timeout_ms):
            result = _execute_bundle_core(conn, args)

            if not result.hits:
                if getattr(args, "cmd", "") in ("bundle", "triage"):
                    memory.log_query(
                        conn,
                        query_text=result.raw_query,
                        cmd="bundle",
                        preset=str(getattr(args, "preset", "") or ""),
                        hit_ids=[],
                        neighbors=result.neighbors,
                    )
                return result.payload, ""

            if getattr(args, "cmd", "") in ("bundle", "triage"):
                memory.log_query(
                    conn,
                    query_text=result.raw_query,
                    cmd="bundle",
                    preset=str(getattr(args, "preset", "") or ""),
                    hit_ids=result.expanded,
                    neighbors=result.neighbors,
                )
            return result.payload, result.content
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 级联辅助函数
# ---------------------------------------------------------------------------

def _build_candidate_file_list_from_paths(root: Path, ref_paths: list[str]) -> list[Path]:
    """从 ref_path 列表提取存在的 chunk 文件路径，去重。"""
    seen: set[str] = set()
    result: list[Path] = []
    for ref_path in ref_paths:
        ref_path = ref_path.strip()
        if not ref_path or ref_path in seen:
            continue
        full = root / ref_path
        if full.exists():
            seen.add(ref_path)
            result.append(full)
    return result


def _build_candidate_file_list(root: Path, rendered: list[dict[str, Any]]) -> list[Path]:
    """从 BM25 命中的 rendered 列表提取 chunk 文件路径，去重。"""
    ref_paths = [str(node.get("ref_path") or "").strip() for node in rendered]
    return _build_candidate_file_list_from_paths(root, ref_paths)


def _rerank_by_rg_hits(
    rendered: list[dict[str, Any]],
    rg_result: dict[str, Any],
) -> list[dict[str, Any]]:
    """将 rg 命中的 chunk 提到 rendered 前面（双重命中优先）。"""
    if not rg_result.get("matches"):
        return rendered

    # 收集 rg 命中的文件路径集合
    rg_files: set[str] = set()
    for m in rg_result.get("matches", []):
        if isinstance(m, dict):
            f = str(m.get("file") or "")
            if f:
                rg_files.add(f)

    if not rg_files:
        return rendered

    # 分为：rg 命中的 + rg 未命中的，保持各自原序
    hit: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for node in rendered:
        ref = str(node.get("ref_path") or "")
        (hit if ref in rg_files else rest).append(node)

    return hit + rest


# ---------------------------------------------------------------------------
# rg / fd 子任务
# ---------------------------------------------------------------------------

def _run_rg_task(
    root: Path,
    *,
    pattern: str,
    fixed: bool,
    limit: int,
    candidate_files: list[Path] | None = None,
) -> dict[str, Any]:
    pattern_value = str(pattern or "").strip()
    if len(pattern_value) > _MAX_SUBPROCESS_PATTERN_LEN:
        pattern_value = pattern_value[:_MAX_SUBPROCESS_PATTERN_LEN]

    # 无 pattern 且无候选文件 → 跳过
    if not pattern_value and not candidate_files:
        return {"tool": "kbtool", "cmd": "search", "pattern": "", "fixed": bool(fixed), "matches": []}

    rg_path = resolve_tool_binary("rg", search_paths=[root / "bin"])
    if rg_path is None:
        return {
            "tool": "kbtool",
            "cmd": "search",
            "pattern": pattern_value,
            "fixed": bool(fixed),
            "error": "ripgrep (rg) not found",
            "matches": [],
        }

    # 有候选文件时只在候选范围内搜；否则搜整个 references/
    if candidate_files:
        search_targets = [str(f) for f in candidate_files if f.exists()]
    else:
        search_targets = [str(root / "references")]

    rg_args = [
        str(rg_path),
        "--line-number",
        "--no-heading",
        "--with-filename",
        "--color",
        "never",
        "--fixed-strings" if fixed else "",
        "--max-count",
        str(max(1, int(limit))),
        "--",
        pattern_value,
    ] + search_targets
    rg_args = [a for a in rg_args if a]

    try:
        proc = run_subprocess_safe(
            rg_args,
            timeout=60,
            check=False,
            text=True,
        )
    except subprocess.TimeoutExpired:
        return {
            "tool": "kbtool",
            "cmd": "search",
            "pattern": pattern_value,
            "fixed": bool(fixed),
            "error": f"rg timed out after 60s for pattern: {pattern_value}",
            "matches": [],
        }
    except OSError as exc:
        return {
            "tool": "kbtool",
            "cmd": "search",
            "pattern": pattern_value,
            "fixed": bool(fixed),
            "error": f"Failed to run rg: {exc}",
            "matches": [],
        }

    matches = parse_rg_output(proc.stdout if proc.returncode == 0 else "", root, limit)
    used_punct_fallback = False
    pattern_regex = ""

    # When fixed-string search finds no hits, try a punctuation-tolerant regex fallback.
    if fixed and not matches:
        regex = build_punctuation_tolerant_regex(pattern_value)
        if regex and regex != pattern_value:
            rg_args2 = [
                str(rg_path),
                "--line-number",
                "--no-heading",
                "--with-filename",
                "--color",
                "never",
                "--max-count",
                str(max(1, int(limit))),
                "--",
                regex,
            ] + search_targets
            try:
                proc2 = run_subprocess_safe(
                    rg_args2,
                    timeout=60,
                    check=False,
                    text=True,
                )
                matches2 = parse_rg_output(proc2.stdout if proc2.returncode == 0 else "", root, limit)
                if matches2:
                    matches = matches2
                    used_punct_fallback = True
                    pattern_regex = regex
            except subprocess.TimeoutExpired:
                pass

    return {
        "tool": "kbtool",
        "cmd": "search",
        "pattern": pattern_value,
        "fixed": bool(fixed),
        "pattern_regex": pattern_regex,
        "punct_fallback": bool(used_punct_fallback),
        "matches": matches,
    }


def _extract_first_token(text: str) -> str:
    """从文本中提取第一个有意义的 token（级联兜底用）。"""
    for part in text.replace("/", " ").replace("-", " ").split():
        if len(part) >= 2:
            return part
    return text[:4] if text else ""


def _split_query_tokens(query: str) -> list[str]:
    """将 query 拆为 rg 搜索用的 token 列表。

    策略：
    - 英文/数字：按空格拆分
    - CJK 连续串：按 2-gram 滑动窗口拆分（匹配 SQLite FTS CJK 分词行为）
    - 每个 token 长度 >= 2
    """
    import re
    tokens: list[str] = []

    # 按空格拆出 ASCII/数字段和 CJK 段
    parts = query.replace("/", " ").replace("-", " ").split()
    for part in parts:
        if len(part) >= 2:
            tokens.append(part)

    # 如果空格拆分后只有一个 token 且全是 CJK（如 "林黛玉葬花"），
    # 则用 2-gram 滑动窗口
    if len(tokens) <= 1 and query:
        has_cjk = any("\u4e00" <= c <= "\u9fff" for c in query)
        if has_cjk:
            cjk_str = "".join(c for c in query if "\u4e00" <= c <= "\u9fff")
            for i in range(len(cjk_str) - 1):
                bigram = cjk_str[i : i + 2]
                tokens.append(bigram)

    return tokens[:6]  # 上限 6 个 token


def _cascade_rg_search(
    root: Path,
    query: str,
    candidate_files: list[Path] | None,
    rg_limit: int,
) -> dict[str, Any]:
    """级联 rg 搜索：用 query 的 tokens 并行搜索候选文件，合并去重。"""
    tokens = _split_query_tokens(query)
    if not tokens:
        return {"tool": "kbtool", "cmd": "search", "pattern": query, "fixed": True, "matches": []}

    # 最多用前 6 个 token 搜索
    tokens = tokens[:6]

    total_limit = max(1, int(rg_limit))

    # 并行启动所有 token 搜索（rg 本身很快，串行累积延迟明显）
    install_signal_handlers()
    pool = ThreadPoolExecutor(max_workers=min(len(tokens), 4))
    try:
        futures = [
            pool.submit(_run_rg_task, root, pattern=token, fixed=True, limit=total_limit, candidate_files=candidate_files)
            for token in tokens
        ]
        results = [f.result() for f in futures]
    finally:
        if is_shutdown_requested():
            shutdown_executor_now(pool)
        pool.shutdown(wait=False)

    # 按 token 原始顺序合并去重，保持优先级
    all_matches: list[dict[str, object]] = []
    seen_lines: set[str] = set()
    for result in results:
        for m in result.get("matches") or []:
            key = f"{m.get('file')}:{m.get('line_number')}"
            if key not in seen_lines:
                seen_lines.add(key)
                all_matches.append(m)
                if len(all_matches) >= total_limit:
                    break
        if len(all_matches) >= total_limit:
            break

    return {
        "tool": "kbtool",
        "cmd": "search",
        "pattern": query,
        "fixed": True,
        "matches": all_matches,
    }


def _merge_dedup(primary: dict[str, Any], secondary: dict[str, Any], *, limit: int = 0) -> dict[str, Any]:
    """合并两个 rg 结果集，按 file:line_number 去重，primary 优先。"""
    seen: set[str] = set()
    merged: list[dict[str, object]] = []

    for m in primary.get("matches") or []:
        if limit > 0 and len(merged) >= limit:
            break
        key = f"{m.get('file')}:{m.get('line_number')}"
        if key not in seen:
            seen.add(key)
            merged.append(m)

    for m in secondary.get("matches") or []:
        if limit > 0 and len(merged) >= limit:
            break
        key = f"{m.get('file')}:{m.get('line_number')}"
        if key not in seen:
            seen.add(key)
            merged.append(m)

    return {
        "tool": "kbtool",
        "cmd": "search",
        "pattern": primary.get("pattern", ""),
        "fixed": primary.get("fixed", False),
        "matches": merged,
    }


def _run_fd_task(root: Path, *, pattern: str, limit: int) -> dict[str, Any]:
    pattern_value = str(pattern or "").strip()
    if len(pattern_value) > _MAX_SUBPROCESS_PATTERN_LEN:
        pattern_value = pattern_value[:_MAX_SUBPROCESS_PATTERN_LEN]
    if not pattern_value:
        return {"tool": "kbtool", "cmd": "files", "pattern": "", "files": []}

    fd_path = resolve_tool_binary("fd", search_paths=[root / "bin"])
    if fd_path is None:
        return {
            "tool": "kbtool",
            "cmd": "files",
            "pattern": pattern_value,
            "error": "fd not found",
            "files": [],
        }

    search_dir = root / "references"
    fd_args = [
        str(fd_path),
        "--type",
        "f",
        "--max-results",
        str(max(1, int(limit))),
        "--color",
        "never",
        "--absolute-path",
        pattern_value,
        str(search_dir),
    ]

    try:
        proc = run_subprocess_safe(
            fd_args,
            timeout=60,
            check=False,
            text=True,
        )
    except subprocess.TimeoutExpired:
        return {
            "tool": "kbtool",
            "cmd": "files",
            "pattern": pattern_value,
            "error": f"fd timed out after 60s for pattern: {pattern_value}",
            "files": [],
        }
    except OSError as exc:
        return {
            "tool": "kbtool",
            "cmd": "files",
            "pattern": pattern_value,
            "error": f"Failed to run fd: {exc}",
            "files": [],
        }

    files: list[str] = []
    if proc.returncode == 0 and proc.stdout:
        for line in proc.stdout.splitlines():
            value = line.strip()
            if not value:
                continue
            try:
                rel_path = str(Path(value).resolve().relative_to(root.resolve())).replace("\\", "/")
            except ValueError:
                rel_path = value.replace("\\", "/")
            files.append(rel_path)
    files = sorted(set(files))

    return {
        "tool": "kbtool",
        "cmd": "files",
        "pattern": pattern_value,
        "files": files,
    }


def _render_search_md(search: dict[str, Any], *, label: str) -> str:
    pattern = str(search.get("pattern") or "")
    pattern_regex = str(search.get("pattern_regex") or "")
    fixed = bool(search.get("fixed", False))
    punct_fallback = bool(search.get("punct_fallback", False))
    matches = search.get("matches") or []
    lines: list[str] = [
        "## Exact Search (rg)\n\n",
        f"- pattern: `{pattern}`\n",
        f"- fixed: `{fixed}`\n",
    ]
    if pattern_regex:
        lines.append(f"- pattern_regex: `{pattern_regex}`\n")
    lines.append(f"- punct_fallback: `{punct_fallback}`\n")
    if not isinstance(matches, list) or not matches:
        lines.append(f"- hits: 0\n\n")
        if not pattern:
            lines.append(f"- note: `{label}`\n\n")
        return "".join(lines)

    lines.append(f"- hits: {len(matches)}\n\n")
    for m in matches:
        if not isinstance(m, dict):
            continue
        file = str(m.get("file") or "")
        line_number = m.get("line_number")
        text = str(m.get("line_text") or "")
        lines.append(f"- `{file}:{line_number}`: {escape_markdown_inline(text)}\n")
    lines.append("\n")
    return "".join(lines)


def _render_files_md(files: dict[str, Any], *, label: str) -> str:
    pattern = str(files.get("pattern") or "")
    hits = files.get("files") or []
    lines: list[str] = [f"## File Hits (fd)\n\n", f"- pattern: `{pattern}`\n"]
    if not isinstance(hits, list) or not hits:
        lines.append("- hits: 0\n\n")
        if not pattern:
            lines.append(f"- note: `{label}`\n\n")
        return "".join(lines)
    lines.append(f"- hits: {len(hits)}\n\n")
    for f in hits:
        value = str(f or "").strip()
        if not value:
            continue
        lines.append(f"- `{value}`\n")
    lines.append("\n")
    return "".join(lines)


def _inject_before_references(bundle_md: str, insert_md: str) -> str:
    marker = "\n## References\n\n"
    idx = bundle_md.rfind(marker)
    if idx < 0:
        return bundle_md.rstrip() + "\n\n" + insert_md
    return bundle_md[:idx] + "\n" + insert_md + "\n" + bundle_md[idx:]


def _rrf_fusion(*ranked_lists: list[str], k: int = 60) -> list[str]:
    """Reciprocal Rank Fusion: 多个排序列表的数学融合。
    
    用于 triage 的三路结果：BM25 chunk IDs、rg file hits、fd file paths。
    返回按 RRF 分数降序排列的统一列表。
    """
    scores: dict[str, float] = {}
    for lst in ranked_lists:
        if not lst:
            continue
        for rank, item in enumerate(lst):
            scores[item] = scores.get(item, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores, key=lambda x: scores[x], reverse=True)


# ---------------------------------------------------------------------------
# cmd_triage — 级联模式
# ---------------------------------------------------------------------------

def cmd_triage(args: argparse.Namespace) -> int:
    root = resolve_root(args.root)

    pattern = str(getattr(args, "pattern", "") or "").strip()
    file_pattern = str(getattr(args, "file_pattern", "") or "").strip()
    fixed = bool(getattr(args, "fixed", False))
    query = str(getattr(args, "query", "") or "").strip()
    if not query:
        if pattern:
            query = pattern
        elif file_pattern:
            query = file_pattern
        else:
            die("Missing --query (or provide --pattern/--file-pattern).")
    if fixed and not pattern:
        die("--fixed requires --pattern.")
    setattr(args, "query", query)

    rg_limit = int(getattr(args, "search_limit", 12) or 12)
    files_limit = int(getattr(args, "files_limit", 20) or 20)

    # ── Parallel execution ──
    install_signal_handlers()
    executor = ThreadPoolExecutor(max_workers=3)
    try:
        bundle_future = executor.submit(_run_bundle_task, root, args)
        files_future = (
            executor.submit(_run_fd_task, root, pattern=file_pattern, limit=files_limit)
            if file_pattern
            else None
        )

        if pattern:
            # Explicit exact search is independent → run in parallel with bundle.
            search_future = executor.submit(_run_rg_task, root, pattern=pattern, fixed=fixed, limit=rg_limit)
            global_future = None
        else:
            # No explicit pattern: start a global cascade search in parallel; a candidate-limited pass
            # (if BM25 hits) will run after bundle completes.
            global_future = executor.submit(_cascade_rg_search, root, query, None, rg_limit)
            search_future = None

        bundle_payload, bundle_md = bundle_future.result()

        if pattern:
            search_payload = search_future.result() if search_future else {"tool": "kbtool", "cmd": "search", "pattern": pattern, "fixed": bool(fixed), "matches": []}
        else:
            global_payload = global_future.result() if global_future else {"tool": "kbtool", "cmd": "search", "pattern": query, "fixed": True, "matches": []}
            rendered = bundle_payload.get("rendered") or []
            if rendered:
                # triage JSON 输出已隐藏 ref_path（避免 Agent 直接读取文件），
                # 需从 kb.sqlite 查询 ref_path 以保持 rg 级联搜索优化。
                db_path = resolve_db_path(root, str(getattr(args, "db", "kb.sqlite") or "kb.sqlite"))
                conn = open_db(db_path)
                try:
                    node_ids = [str(n.get("node_id") or "").strip() for n in rendered if n.get("node_id")]
                    if node_ids:
                        placeholders = ",".join("?" * len(node_ids))
                        rows = conn.execute(
                            f"SELECT ref_path FROM nodes WHERE node_id IN ({placeholders}) AND ref_path != ''",
                            node_ids,
                        ).fetchall()
                        ref_paths = [str(row["ref_path"]) for row in rows]
                        candidate_files: list[Path] | None = _build_candidate_file_list_from_paths(root, ref_paths) if ref_paths else None
                    else:
                        candidate_files = None
                finally:
                    conn.close()
            else:
                candidate_files = None
            if candidate_files:
                # Reuse global search hits to avoid redundant rg invocations:
                # only search candidate files that were NOT already covered by the global pass.
                global_hit_files = {str(m.get("file", "")) for m in (global_payload.get("matches") or []) if isinstance(m, dict)}
                uncovered_candidates = [f for f in candidate_files if str(f) not in global_hit_files]
                if uncovered_candidates:
                    cascade_payload = _cascade_rg_search(root, query, uncovered_candidates, rg_limit)
                    search_payload = _merge_dedup(cascade_payload, global_payload, limit=rg_limit)
                else:
                    search_payload = global_payload
            else:
                search_payload = global_payload

        files_payload = files_future.result() if files_future else _run_fd_task(root, pattern="", limit=1)
    finally:
        # Fast shutdown on signal: don't wait for workers.
        if is_shutdown_requested():
            shutdown_executor_now(executor)
        executor.shutdown(wait=False)

    search_md = _render_search_md(search_payload, label="(skipped: no --pattern)")
    files_md = _render_files_md(files_payload, label="(skipped: no --file-pattern)")

    # ── RRF 多路融合 ──
    # 从各结果中提取排序列表
    bundle_ids = [str(n.get("node_id", "")) for n in (bundle_payload.get("rendered") or []) if n.get("node_id")]
    # search hits → 文件路径列表
    search_files = []
    for m in (search_payload.get("matches") or []):
        if isinstance(m, dict):
            f = str(m.get("file", ""))
            if f:
                search_files.append(f)
    # files hits → 文件路径列表
    fd_paths = [str(f) for f in (files_payload.get("files") or [])]

    # RRF 融合
    fused = _rrf_fusion(bundle_ids, search_files, fd_paths)

    # 组装 markdown
    combined_md = bundle_md
    if not combined_md:
        combined_md = "# Bundle\n\n" + f"- query: `{query}`\n\n"
    combined_md = _inject_before_references(combined_md, search_md + files_md)

    # 输出路径
    out_path = safe_output_path(root, str(getattr(args, "out", "runs/triage.md") or "runs/triage.md"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(combined_md, encoding="utf-8", newline="\n")
    out_rel = str(out_path.relative_to(root)).replace("\\", "/")

    # 将融合结果附加到 JSON payload 中
    if "out" not in bundle_payload:
        bundle_payload["out"] = out_rel
    bundle_payload["rrf_fusion"] = {
        "fused_order": fused[:30],
        "sources": {"bundle": len(bundle_ids), "search": len(search_files), "files": len(fd_paths)},
    }

    result = {
        "tool": "kbtool",
        "cmd": "triage",
        "query": query,
        "pattern": pattern,
        "file_pattern": file_pattern,
        "out": out_rel,
        "bundle": bundle_payload,
        "search": search_payload,
        "files": files_payload,
    }
    print_json(result)

    # NOTE: logging is already done inside _run_bundle_task() via memory.log_query().
    # Do NOT log again here to avoid double-counting weights.

    has_any_hits = False
    if isinstance(bundle_payload, dict):
        has_any_hits = has_any_hits or bool(bundle_payload.get("hits"))
        has_any_hits = has_any_hits or bool(bundle_payload.get("rendered"))
    if isinstance(search_payload, dict):
        has_any_hits = has_any_hits or bool(search_payload.get("matches"))
    if isinstance(files_payload, dict):
        has_any_hits = has_any_hits or bool(files_payload.get("files"))
    return 0 if has_any_hits else 1
