from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Tuple


def die(message: str, code: int = 2) -> "NoReturn":  # type: ignore[name-defined]
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(code)


def print_json(obj: object) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")


def infer_skill_root() -> Path:
    if bool(getattr(sys, "frozen", False)):
        exe = Path(sys.executable).resolve()
        try:
            if exe.parent.parent.name == "bin":
                return exe.parents[2]
        except Exception:
            pass
        return exe.parent

    here = Path(__file__).resolve()
    for parent in here.parents:
        if parent.name == "scripts":
            return parent.parent
    return here.parent


def resolve_root(root_arg: str) -> Path:
    if root_arg:
        return Path(root_arg).resolve()
    return infer_skill_root()


def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        die(f"Missing kb.sqlite: {db_path} (run build or reindex first)")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def sha1_file(path: Path) -> str:
    import hashlib

    return hashlib.sha1(path.read_bytes()).hexdigest()


def run_hook(root: Path, hook_name: str, payload: Dict[str, object]) -> Tuple[Dict[str, object], str]:
    hook_path = root / "hooks" / f"{hook_name}.py"
    if not hook_path.exists():
        return {}, ""
    digest = sha1_file(hook_path)
    module_name = f"kbtool_hook_{hook_name}_{digest[:12]}"
    spec = importlib.util.spec_from_file_location(module_name, str(hook_path))
    if spec is None or spec.loader is None:
        die(f"Invalid hook module: {hook_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    fn = getattr(module, "run", None)
    if not callable(fn):
        die(f"Hook missing run(payload) function: {hook_path}")
    try:
        out = fn(payload)
    except Exception as e:
        die(f"Hook failed: {hook_name} ({e})")
    if out is None:
        return {}, digest
    if isinstance(out, dict):
        return out, digest
    die(f"Hook must return dict or None: {hook_path}")

