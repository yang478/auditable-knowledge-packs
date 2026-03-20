from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, Optional, Tuple


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
        except IndexError:
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


class SqliteTimeout:
    def __init__(self, conn: sqlite3.Connection, timeout_ms: int) -> None:
        self._conn = conn
        self._timeout_ms = int(timeout_ms)
        self.timed_out = False
        self._deadline = 0.0

    def __enter__(self) -> "SqliteTimeout":
        if self._timeout_ms <= 0:
            return self
        self._deadline = time.monotonic() + (float(self._timeout_ms) / 1000.0)

        def handler() -> int:
            if time.monotonic() >= self._deadline:
                self.timed_out = True
                return 1
            return 0

        # Called every N VM steps during long-running queries.
        self._conn.set_progress_handler(handler, 10_000)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        if self._timeout_ms > 0:
            self._conn.set_progress_handler(None, 0)
        return False


def sha1_file(path: Path) -> str:
    import hashlib

    return hashlib.sha1(path.read_bytes()).hexdigest()


def safe_output_path(root: Path, out_arg: str) -> Path:
    root_resolved = root.resolve()
    out_path = (root / str(out_arg)).resolve()
    try:
        out_path.relative_to(root_resolved)
    except ValueError:
        die(f"Refusing to write outside skill root: --out {out_arg!r}")
    if out_path == root_resolved:
        die(f"Invalid --out (points to skill root directory): --out {out_arg!r}")
    return out_path


def _load_hook_allowlist(hooks_dir: Path) -> Optional[set[str]]:
    allow_path = hooks_dir / "allowlist.sha1"
    if not allow_path.exists():
        return None
    if allow_path.is_symlink():
        die(f"Refusing to read symlink allowlist: {allow_path}")
    if not allow_path.is_file():
        die(f"Invalid hook allowlist (not a file): {allow_path}")
    allowed: set[str] = set()
    for raw in allow_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("sha1="):
            line = line.removeprefix("sha1=").strip()
        if len(line) < 8:
            continue
        allowed.add(line)
    return allowed


def run_hook(root: Path, hook_name: str, payload: Dict[str, object]) -> Tuple[Dict[str, object], str]:
    hooks_dir = root / "hooks"
    hook_path = hooks_dir / f"{hook_name}.py"
    if not hook_path.exists():
        return {}, ""
    if hook_path.is_symlink():
        die(f"Refusing to execute symlink hook: {hook_path}")
    if not hook_path.is_file():
        die(f"Invalid hook path (not a file): {hook_path}")
    digest = sha1_file(hook_path)
    allowlist = _load_hook_allowlist(hooks_dir)
    if allowlist is not None and digest not in allowlist:
        die(
            "\n".join(
                [
                    f"Hook not allowlisted: {hook_path}",
                    f"sha1={digest}",
                    "To allow this hook, add its sha1 to hooks/allowlist.sha1 (one per line).",
                ]
            )
        )
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
        detail = f"{type(e).__name__}: {e}"
        if os.environ.get("KBTOOL_TRACEBACK") or os.environ.get("KBTOOL_DEBUG"):
            detail += "\n" + traceback.format_exc()
        die(f"Hook failed: {hook_name} path={hook_path} sha1={digest} ({detail})")
    if out is None:
        return {}, digest
    if isinstance(out, dict):
        return out, digest
    die(f"Hook must return dict or None: {hook_path}")
