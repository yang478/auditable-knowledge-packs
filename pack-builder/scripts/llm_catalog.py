#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def die(msg: str, *, code: int = 2) -> "NoReturn":
    print(f"[ERR] {msg}", file=sys.stderr)
    raise SystemExit(code)


@dataclass(frozen=True)
class LlmConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 120


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        die(f"Missing file: {path}")
    except json.JSONDecodeError as e:
        die(f"Invalid json: {path} ({e})")


def load_llm_config(path: Path) -> LlmConfig:
    data = load_json(path)
    if isinstance(data, dict) and "llm" in data and isinstance(data["llm"], dict):
        data = data["llm"]
    if not isinstance(data, dict):
        die(f"Invalid llm config: {path} (expected object)")
    base_url = str(data.get("base_url") or data.get("baseUrl") or "").strip()
    model = str(data.get("model") or "").strip()
    api_key = str(data.get("api_key") or data.get("apiKey") or "").strip()
    timeout = int(data.get("timeout_seconds") or data.get("timeoutSeconds") or 120)
    if not base_url or not model:
        die(f"Invalid llm config: {path} (require base_url + model)")
    return LlmConfig(base_url=base_url, api_key=api_key, model=model, timeout_seconds=max(1, timeout))


def openai_chat_completion(llm: LlmConfig, messages: List[Dict[str, str]], *, temperature: float = 0.0) -> str:
    base = llm.base_url.rstrip("/")
    url = f"{base}/chat/completions"
    req = urllib.request.Request(url, method="POST")
    req.add_header("content-type", "application/json")
    if llm.api_key:
        req.add_header("authorization", f"Bearer {llm.api_key}")
    payload = json.dumps(
        {
            "model": llm.model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    try:
        with urllib.request.urlopen(req, data=payload, timeout=llm.timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise RuntimeError(f"LLM HTTP {e.code}: {text or e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"LLM connection error: {e}") from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"LLM response not json: {raw[:200]}") from e
    content = None
    if isinstance(data, dict):
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            c0 = choices[0] if isinstance(choices[0], dict) else {}
            msg = c0.get("message") if isinstance(c0, dict) else {}
            if isinstance(msg, dict):
                content = msg.get("content")
            if content is None and isinstance(c0, dict):
                content = c0.get("text")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("LLM response missing choices[0].message.content")
    return content.strip()


def _extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    s = text.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        pass
    start = s.find("{")
    end = s.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    try:
        obj = json.loads(s[start : end + 1])
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def load_taxonomy(path: Path) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    data = load_json(path)
    if not isinstance(data, dict):
        die(f"Invalid taxonomy: {path} (expected object)")
    cats = data.get("categories")
    if not isinstance(cats, list) or not cats:
        die(f"Invalid taxonomy: {path} (missing categories)")
    out: Dict[str, Dict[str, str]] = {}
    order: List[str] = []
    for row in cats:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id") or "").strip()
        label = str(row.get("label") or "").strip()
        if not cid or not label:
            continue
        out[cid] = {
            "id": cid,
            "label": label,
            "definition": str(row.get("definition") or "").strip(),
        }
        order.append(cid)
    if not out:
        die(f"Invalid taxonomy: {path} (no usable categories)")
    return out, order


def render_taxonomy_for_prompt(tax: Dict[str, Dict[str, str]], order: List[str]) -> str:
    lines: List[str] = []
    for cid in order:
        row = tax.get(cid) or {}
        label = row.get("label") or cid
        definition = row.get("definition") or ""
        if definition:
            lines.append(f"- {cid}: {label}（{definition}）")
        else:
            lines.append(f"- {cid}: {label}")
    return "\n".join(lines)


def catalog_prompt_messages(*, taxonomy_text: str, doc_title: str, doc_meta: str, doc_excerpt: str) -> List[Dict[str, str]]:
    system = (
        "你是一位严谨的图书馆编目专家。"
        "请把给定文档分到【且仅分到】一个主类目(primary_category_id)，必须从税onomies列表中选择。"
        "输出必须是 JSON 对象，且只输出 JSON，不要输出其它文本。"
        "JSON 字段：primary_category_id(必填), confidence(0-1), evidence(<=80字), rationale(一句话)."
        "如果无法可靠判断，请选择 other 并降低 confidence。"
    )
    user = (
        "【Taxonomy】\n"
        f"{taxonomy_text}\n\n"
        "【Document】\n"
        f"Title: {doc_title}\n"
        f"{doc_meta}\n\n"
        "Excerpt:\n"
        f"{doc_excerpt}\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def normalize_confidence(value: Any) -> float:
    try:
        f = float(value)
    except Exception:
        return 0.0
    if f != f:  # NaN
        return 0.0
    return max(0.0, min(1.0, f))


def classify_text(
    llm: LlmConfig,
    *,
    taxonomy: Dict[str, Dict[str, str]],
    order: List[str],
    doc_title: str,
    doc_meta: str,
    doc_excerpt: str,
) -> Dict[str, Any]:
    taxonomy_text = render_taxonomy_for_prompt(taxonomy, order)
    messages = catalog_prompt_messages(
        taxonomy_text=taxonomy_text,
        doc_title=doc_title,
        doc_meta=doc_meta,
        doc_excerpt=doc_excerpt,
    )
    content = openai_chat_completion(llm, messages, temperature=0.0)
    obj = _extract_first_json_object(content) or {}
    primary = str(obj.get("primary_category_id") or "").strip()
    conf = normalize_confidence(obj.get("confidence"))
    evidence = str(obj.get("evidence") or "").strip()
    rationale = str(obj.get("rationale") or "").strip()
    if primary not in taxonomy:
        if "other" in taxonomy:
            primary = "other"
            conf = min(conf, 0.4) if conf else 0.2
        else:
            primary = order[0]
            conf = min(conf, 0.4) if conf else 0.2
    return {
        "primary_category_id": primary,
        "confidence": conf,
        "evidence": evidence[:80],
        "rationale": rationale[:200],
        "raw": obj,
    }


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def load_existing_doc_hashes(assignments_path: Path) -> set[str]:
    seen: set[str] = set()
    for row in read_jsonl(assignments_path):
        h = str(row.get("doc_hash") or "").strip()
        if h:
            seen.add(h)
    return seen


def iter_jsonl_with_lines(path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    for i, raw in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            die(f"Invalid assignments jsonl: {path} line {i} ({e})")
        if isinstance(row, dict):
            yield i, row


def iter_gold_rows(path: Path) -> Iterable[Tuple[str, str, str]]:
    """
    Returns tuples of (id, text, expected_primary_category_id).
    """
    for i, row in enumerate(read_jsonl(path), start=1):
        gid = str(row.get("id") or row.get("name") or f"row-{i}").strip()
        text = str(row.get("text") or row.get("input") or "").strip()
        expected = str(row.get("expected_primary_category_id") or row.get("expected") or "").strip()
        if not text or not expected:
            continue
        yield gid, text, expected


def cmd_calibrate(args: argparse.Namespace) -> int:
    llm = load_llm_config(Path(args.llm_config))
    taxonomy, order = load_taxonomy(Path(args.taxonomy))
    gold_path = Path(args.gold)
    rows = list(iter_gold_rows(gold_path))
    if not rows:
        die(f"Empty gold file: {gold_path}")

    total = 0
    correct = 0
    low_conf = 0
    details: List[Dict[str, Any]] = []

    for gid, text, expected in rows:
        total += 1
        r = classify_text(llm, taxonomy=taxonomy, order=order, doc_title=gid, doc_meta="", doc_excerpt=text[:2000])
        pred = r["primary_category_id"]
        conf = float(r["confidence"])
        if conf < float(args.low_confidence_threshold):
            low_conf += 1
        ok = pred == expected
        if ok:
            correct += 1
        details.append(
            {
                "id": gid,
                "expected_primary_category_id": expected,
                "pred_primary_category_id": pred,
                "confidence": conf,
                "ok": ok,
            }
        )

    accuracy = (correct / total) if total else 0.0
    low_conf_ratio = (low_conf / total) if total else 0.0
    passed = accuracy >= float(args.min_accuracy)

    report = {
        "passed": passed,
        "accuracy": accuracy,
        "total": total,
        "correct": correct,
        "low_confidence_ratio": low_conf_ratio,
        "llm": {"base_url": llm.base_url, "model": llm.model},
        "taxonomy_path": str(Path(args.taxonomy)),
        "gold_path": str(gold_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "details": details,
    }

    out_path = Path(args.out) if args.out else None
    if out_path:
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("[OK] Wrote calibration report:", out_path)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))

    return 0 if passed else 3


def load_manifest_docs(skill_root: Path) -> List[Dict[str, Any]]:
    manifest = skill_root / "manifest.json"
    if not manifest.exists():
        die(f"Missing manifest.json under --skill-root: {skill_root}")
    data = load_json(manifest)
    if not isinstance(data, dict):
        die(f"Invalid manifest.json: {manifest}")
    docs = data.get("docs")
    if not isinstance(docs, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in docs:
        if not isinstance(row, dict):
            continue
        doc_id = str(row.get("doc_id") or "").strip()
        title = str(row.get("title") or doc_id).strip()
        doc_hash = str(row.get("doc_hash") or "").strip()
        source_file = str(row.get("source_file") or "").strip()
        source_path = str(row.get("source_path") or "").strip()
        if not doc_id or not doc_hash:
            continue
        out.append(
            {
                "doc_id": doc_id,
                "title": title,
                "doc_hash": doc_hash,
                "source_file": source_file,
                "source_path": source_path,
            }
        )
    return out


def read_doc_excerpt_from_skill(skill_root: Path, doc_id: str, *, max_chars: int) -> str:
    toc = skill_root / "references" / doc_id / "toc.md"
    if toc.exists():
        return toc.read_text(encoding="utf-8", errors="replace")[:max_chars]
    return ""


def _stable_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def cmd_classify(args: argparse.Namespace) -> int:
    llm = load_llm_config(Path(args.llm_config))
    taxonomy, order = load_taxonomy(Path(args.taxonomy))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    seen = load_existing_doc_hashes(out_path)

    docs: List[Dict[str, Any]] = []
    if args.skill_root:
        docs = load_manifest_docs(Path(args.skill_root))
    elif args.inputs:
        try:
            from build_skill_lib.extract import extract_to_markdown
            from build_skill_lib.text_utils import stable_hash
        except Exception as e:
            die(f"Failed to import build_skill.py for --inputs mode: {e}")
        for raw in args.inputs:
            p = Path(raw)
            md = extract_to_markdown(p)
            h = stable_hash(md)
            docs.append({"doc_id": p.stem, "title": p.stem, "doc_hash": h, "source_file": p.name, "source_path": str(p)})
    else:
        die("classify requires --skill-root or --inputs")

    max_items = int(args.limit) if str(args.limit).strip() else 0
    wrote = 0
    skipped = 0
    for d in docs:
        if max_items > 0 and wrote >= max_items:
            break
        doc_hash = str(d.get("doc_hash") or "")
        if doc_hash in seen:
            skipped += 1
            continue

        title = str(d.get("title") or d.get("doc_id") or "").strip()
        meta = ""
        if d.get("source_file"):
            meta += f"source_file: {d.get('source_file')}\n"
        if d.get("source_path"):
            meta += f"source_path: {d.get('source_path')}\n"

        excerpt = ""
        if args.skill_root:
            excerpt = read_doc_excerpt_from_skill(Path(args.skill_root), str(d.get("doc_id")), max_chars=int(args.excerpt_chars))
        if not excerpt:
            excerpt = title

        r = classify_text(llm, taxonomy=taxonomy, order=order, doc_title=title, doc_meta=meta, doc_excerpt=excerpt)
        row = {
            "doc_hash": doc_hash,
            "primary_category_id": r["primary_category_id"],
            "confidence": r["confidence"],
            "evidence": r["evidence"],
            "rationale": r["rationale"],
            "doc_title": title,
            "source_file": d.get("source_file") or "",
            "source_path": d.get("source_path") or "",
            "model": llm.model,
            "prompt_version": str(args.prompt_version or "v1"),
            "classified_at": _stable_now_iso(),
        }
        with out_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        seen.add(doc_hash)
        wrote += 1
        if int(args.sleep_ms) > 0:
            time.sleep(int(args.sleep_ms) / 1000.0)

    print(f"[OK] classify done. wrote={wrote} skipped={skipped} out={out_path}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    taxonomy, _order = load_taxonomy(Path(args.taxonomy))
    taxonomy_ids = set(taxonomy.keys())
    assignments_path = Path(args.assignments)
    if not assignments_path.exists():
        die(f"Missing assignments file: {assignments_path}")

    mapping: Dict[str, str] = {}
    errors: List[Dict[str, Any]] = []

    for lineno, row in iter_jsonl_with_lines(assignments_path):
        doc_hash = str(row.get("doc_hash") or "").strip()
        primary_id = str(row.get("primary_category_id") or "").strip()
        if not doc_hash or not primary_id:
            errors.append({"type": "missing_fields", "line": lineno})
            continue
        if primary_id not in taxonomy_ids:
            errors.append(
                {
                    "type": "unknown_category_id",
                    "line": lineno,
                    "doc_hash": doc_hash,
                    "primary_category_id": primary_id,
                }
            )
            continue
        if doc_hash in mapping:
            errors.append(
                {
                    "type": "duplicate_doc_hash",
                    "line": lineno,
                    "doc_hash": doc_hash,
                    "primary_category_id": primary_id,
                    "previous_primary_category_id": mapping.get(doc_hash) or "",
                }
            )
            continue
        mapping[doc_hash] = primary_id

    docs = load_manifest_docs(Path(args.skill_root))
    if not docs:
        die(f"Empty manifest docs under --skill-root: {args.skill_root}")

    manifest_hashes = [str(d.get("doc_hash") or "") for d in docs]
    manifest_set = set(manifest_hashes)
    covered = sum(1 for h in manifest_hashes if h in mapping)
    total = len(manifest_hashes)
    coverage = (covered / total) if total else 0.0

    missing_docs = [d for d in docs if str(d.get("doc_hash") or "") not in mapping]
    stale_hashes = sorted(h for h in mapping.keys() if h not in manifest_set)

    passed = True
    min_cov = float(args.min_coverage)
    if errors:
        passed = False
    if coverage < min_cov:
        passed = False
    if stale_hashes and bool(args.fail_on_stale):
        passed = False

    report = {
        "passed": passed,
        "coverage": coverage,
        "min_coverage": min_cov,
        "manifest_docs": total,
        "assignments": len(mapping),
        "missing_mappings": len(missing_docs),
        "stale_mappings": len(stale_hashes),
        "errors": errors,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fix": {
            "missing_mappings": f"python3 {Path(__file__).name} classify --llm-config <llm.json> --taxonomy {Path(args.taxonomy).name} --skill-root {args.skill_root} --out {args.assignments}",
        },
    }

    out_path = Path(args.out) if str(args.out).strip() else None
    if out_path:
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("[OK] Wrote validate report:", out_path)

    # Human summary
    status = "PASS" if passed else "FAIL"
    print(f"[VALIDATE] {status} coverage={coverage:.4f} min_coverage={min_cov:.4f} (covered={covered}/{total})")
    if errors:
        print(f"[VALIDATE] errors={len(errors)}")
        for e in errors[: int(args.max_errors)]:
            if e.get("type") == "unknown_category_id":
                print(f"- unknown_category_id line={e.get('line')} primary_category_id={e.get('primary_category_id')}")
            elif e.get("type") == "duplicate_doc_hash":
                print(f"- duplicate_doc_hash line={e.get('line')} doc_hash={e.get('doc_hash')}")
            else:
                print(f"- {e.get('type')} line={e.get('line')}")
        if len(errors) > int(args.max_errors):
            print(f"- ... (more errors: {len(errors) - int(args.max_errors)})")

    if coverage < min_cov:
        print(f"[VALIDATE] coverage below threshold: coverage={coverage:.4f} < min_coverage={min_cov:.4f}")
        if missing_docs:
            sample = ", ".join(str(d.get("doc_id")) for d in missing_docs[:10])
            print(f"[VALIDATE] missing_mappings={len(missing_docs)} sample_doc_ids=[{sample}]")
            print(f"[FIX] rerun classify to append missing mappings: python3 {Path(__file__).name} classify --skill-root {args.skill_root} --out {args.assignments}")

    if stale_hashes:
        sample = ", ".join(stale_hashes[:10])
        print(f"[VALIDATE] stale_mappings={len(stale_hashes)} sample_doc_hashes=[{sample}]")
        if bool(args.fail_on_stale):
            print("[VALIDATE] fail_on_stale enabled")

    return 0 if passed else 3


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="llm_catalog.py", description="LLM offline cataloger for pack-builder (auditable assignments).")
    sub = p.add_subparsers(dest="cmd", required=True)

    cal = sub.add_parser("calibrate", help="Run a calibration gate on a gold set.")
    cal.add_argument("--llm-config", required=True, help="Path to llm config json (OpenAI-compatible).")
    cal.add_argument("--taxonomy", required=True, help="Path to taxonomy json (categories list).")
    cal.add_argument("--gold", required=True, help="Gold jsonl with {text, expected_primary_category_id}.")
    cal.add_argument("--min-accuracy", default="0.85", help="Fail if accuracy below this threshold.")
    cal.add_argument("--low-confidence-threshold", default="0.5", help="Count confidence below this as low-confidence.")
    cal.add_argument("--out", default="", help="(Optional) Write report json to this path.")
    cal.set_defaults(func=cmd_calibrate)

    clf = sub.add_parser("classify", help="Classify docs and write assignments.jsonl.")
    clf.add_argument("--llm-config", required=True, help="Path to llm config json (OpenAI-compatible).")
    clf.add_argument("--taxonomy", required=True, help="Path to taxonomy json (categories list).")
    clf.add_argument("--skill-root", default="", help="(Recommended) Existing skill root containing manifest.json + references/.")
    clf.add_argument("--inputs", nargs="*", default=[], help="(Optional) Raw input files (slow; will extract markdown for hash).")
    clf.add_argument("--out", required=True, help="Output assignments jsonl path.")
    clf.add_argument("--limit", default="0", help="(Optional) Max docs to classify.")
    clf.add_argument("--excerpt-chars", default="4000", help="Max chars of excerpt to send to LLM (skill-root mode).")
    clf.add_argument("--prompt-version", default="v1", help="Recorded in output for audit.")
    clf.add_argument("--sleep-ms", default="0", help="Sleep between requests (rate limiting).")
    clf.set_defaults(func=cmd_classify)

    val = sub.add_parser("validate", help="Validate taxonomy+assignments against a built skill (gate).")
    val.add_argument("--taxonomy", required=True, help="Path to taxonomy json (categories list).")
    val.add_argument("--assignments", required=True, help="Assignments jsonl path (doc_hash -> primary_category_id).")
    val.add_argument("--skill-root", required=True, help="Built skill root containing manifest.json.")
    val.add_argument("--min-coverage", default="0.95", help="Fail if coverage below this ratio.")
    val.add_argument("--fail-on-stale", action="store_true", help="Also fail if assignments contains stale doc_hash.")
    val.add_argument("--max-errors", default="20", help="Print at most N error lines.")
    val.add_argument("--out", default="", help="(Optional) Write report json to this path.")
    val.set_defaults(func=cmd_validate)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
