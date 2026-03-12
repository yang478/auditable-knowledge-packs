from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from . import templates_dir
from .fs_utils import die, platform_tag, write_text
from .text_utils import stable_hash


def write_reindex_script(out_skill_dir: Path) -> None:
    script_path = out_skill_dir / "scripts" / "reindex.py"
    template = templates_dir() / "reindex.py"
    if not template.exists():
        die("Missing template: templates/reindex.py (pack-builder installation is incomplete)")
    write_text(script_path, template.read_text(encoding="utf-8"))
    script_path.chmod(0o755)


def write_kbtool_script(out_skill_dir: Path) -> None:
    script_path = out_skill_dir / "scripts" / "kbtool.py"
    template = templates_dir() / "kbtool.py"
    if not template.exists():
        die("Missing template: templates/kbtool.py (pack-builder installation is incomplete)")
    write_text(script_path, template.read_text(encoding="utf-8"))
    script_path.chmod(0o755)

    lib_src = templates_dir() / "kbtool_lib"
    if not lib_src.exists():
        die("Missing template: templates/kbtool_lib/ (pack-builder installation is incomplete)")
    lib_dst = out_skill_dir / "scripts" / "kbtool_lib"
    if lib_dst.exists():
        shutil.rmtree(lib_dst)
    shutil.copytree(
        lib_src,
        lib_dst,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )


def write_kbtool_sha1(out_skill_dir: Path) -> str:
    scripts_dir = out_skill_dir / "scripts"
    script_path = scripts_dir / "kbtool.py"
    lib_dir = scripts_dir / "kbtool_lib"
    if not script_path.exists():
        die(f"Missing kbtool.py for hashing: {script_path}")
    if not lib_dir.exists():
        die(f"Missing kbtool_lib/ for hashing: {lib_dir}")

    sources: List[Path] = [script_path]
    sources.extend(sorted((p for p in lib_dir.rglob("*.py") if p.is_file()), key=lambda p: p.as_posix()))
    if not sources:
        die(f"Empty kbtool sources for hashing under: {scripts_dir}")

    # Cross-platform stable hash: hash normalized text per file (universal newlines), then hash the path+hash list.
    h = hashlib.sha1()
    for path in sources:
        rel = str(path.relative_to(out_skill_dir)).replace("\\", "/")
        file_hash = stable_hash(path.read_text(encoding="utf-8", errors="ignore"))
        h.update(rel.encode("utf-8", errors="ignore"))
        h.update(b"\n")
        h.update(file_hash.encode("ascii"))
        h.update(b"\n")
    sha = h.hexdigest()
    write_text(out_skill_dir / "kbtool.sha1", sha + "\n")
    return sha


def write_root_kbtool_entrypoints(out_skill_dir: Path) -> None:
    """
    Write a single, human/LLM-friendly entrypoint at skill root:
    - kbtool (POSIX shell)
    - kbtool.cmd (Windows)

    The wrapper prefers a matching binary in bin/<platform>/, and falls back to python scripts/kbtool.py.
    """
    sh_path = out_skill_dir / "kbtool"
    cmd_path = out_skill_dir / "kbtool.cmd"

    sh = """#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$ROOT"

uname_s="$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo '')"
uname_m="$(uname -m 2>/dev/null || echo '')"

case "$uname_s" in
  linux*) os_tag="linux" ;;
  darwin*) os_tag="macos" ;;
  msys*|mingw*|cygwin*) os_tag="windows" ;;
  *) os_tag="${uname_s:-unknown}" ;;
esac

case "$uname_m" in
  x86_64|amd64) arch_tag="x86_64" ;;
  aarch64|arm64) arch_tag="arm64" ;;
  *) arch_tag="${uname_m:-unknown}" ;;
esac

plat="$os_tag-$arch_tag"
bin="$ROOT/bin/$plat/kbtool"
bin_sha="$ROOT/bin/$plat/kbtool.sha1"
root_sha="$ROOT/kbtool.sha1"
py_script="$ROOT/scripts/kbtool.py"

strip_sha() {
  tr -d '\r\n ' < "$1" 2>/dev/null || true
}

if [ -x "$bin" ] && [ -f "$bin_sha" ] && [ -f "$root_sha" ]; then
  if [ "$(strip_sha "$bin_sha")" = "$(strip_sha "$root_sha")" ] && [ -n "$(strip_sha "$root_sha")" ]; then
    exec "$bin" "$@"
  fi
fi

if [ -x "$bin" ] && [ -f "$root_sha" ]; then
  if [ -f "$bin_sha" ] && [ "$(strip_sha "$bin_sha")" != "$(strip_sha "$root_sha")" ] && [ -n "$(strip_sha "$root_sha")" ]; then
    echo "[WARN] Found stale kbtool binary for $plat. Falling back to python script. Rebuild with --package-kbtool on this platform to update bin/$plat." >&2
  fi
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$py_script" "$@"
fi
if command -v python >/dev/null 2>&1; then
  exec python "$py_script" "$@"
fi

if [ -x "$bin" ]; then
  echo "[WARN] Python not found; running kbtool binary for $plat even if stale." >&2
  exec "$bin" "$@"
fi

echo "[ERROR] No usable kbtool entry found. Missing scripts/kbtool.py and bin/$plat/kbtool." >&2
exit 2
"""

    cmd = r"""@echo off
setlocal enabledelayedexpansion
set "ROOT=%~dp0"
cd /d "%ROOT%"

set "OS_TAG=windows"
set "ARCH_TAG=%PROCESSOR_ARCHITECTURE%"
if /I "%ARCH_TAG%"=="AMD64" set "ARCH_TAG=x86_64"
if /I "%ARCH_TAG%"=="ARM64" set "ARCH_TAG=arm64"
if /I "%ARCH_TAG%"=="x86" set "ARCH_TAG=x86"
if "%ARCH_TAG%"=="" set "ARCH_TAG=unknown"

set "PLAT=%OS_TAG%-%ARCH_TAG%"
set "BIN=%ROOT%bin\%PLAT%\kbtool.exe"
set "BIN_SHA=%ROOT%bin\%PLAT%\kbtool.sha1"
set "ROOT_SHA=%ROOT%kbtool.sha1"
set "PY_SCRIPT=%ROOT%scripts\kbtool.py"

set "USEBIN=0"
if exist "%BIN%" if exist "%BIN_SHA%" if exist "%ROOT_SHA%" (
  for /f "usebackq delims=" %%a in ("%BIN_SHA%") do set "BINSHA=%%a"
  for /f "usebackq delims=" %%a in ("%ROOT_SHA%") do set "ROOTSHA=%%a"
  if /I "!BINSHA!"=="!ROOTSHA!" if not "!ROOTSHA!"=="" set "USEBIN=1"
)

if "%USEBIN%"=="1" (
  "%BIN%" %*
  exit /b %errorlevel%
)

if exist "%BIN%" if exist "%ROOT_SHA%" (
  if exist "%BIN_SHA%" (
    for /f "usebackq delims=" %%a in ("%BIN_SHA%") do set "BINSHA=%%a"
    for /f "usebackq delims=" %%a in ("%ROOT_SHA%") do set "ROOTSHA=%%a"
    if /I not "!BINSHA!"=="!ROOTSHA!" if not "!ROOTSHA!"=="" (
      echo [WARN] Found stale kbtool binary for %PLAT%. Falling back to python script. Rebuild with --package-kbtool on this platform to update bin\%PLAT%. 1>&2
    )
  )
)

where python3 >nul 2>nul
if "%errorlevel%"=="0" (
  python3 "%PY_SCRIPT%" %*
  exit /b %errorlevel%
)
where python >nul 2>nul
if "%errorlevel%"=="0" (
  python "%PY_SCRIPT%" %*
  exit /b %errorlevel%
)

if exist "%BIN%" (
  echo [WARN] Python not found; running kbtool binary for %PLAT% even if stale. 1>&2
  "%BIN%" %*
  exit /b %errorlevel%
)

echo [ERROR] No usable kbtool entry found. Missing scripts\kbtool.py and bin\%PLAT%\kbtool.exe. 1>&2
exit /b 2
"""

    write_text(sh_path, sh)
    sh_path.chmod(0o755)
    write_text(cmd_path, cmd)


def maybe_package_kbtool_pyinstaller(out_skill_dir: Path) -> Optional[Path]:
    pyinstaller = shutil.which("pyinstaller")
    if not pyinstaller:
        print("[WARN] PyInstaller not found on PATH; skipping --package-kbtool.", file=sys.stderr)
        return None

    script_path = out_skill_dir / "scripts" / "kbtool.py"
    if not script_path.exists():
        die(f"Missing kbtool.py for packaging: {script_path}")

    tag = platform_tag()
    dist_dir = out_skill_dir / "bin" / tag
    dist_dir.mkdir(parents=True, exist_ok=True)

    name = "kbtool"
    with tempfile.TemporaryDirectory(prefix="pack_builder_pyinstaller_") as tmp:
        tmp_path = Path(tmp)
        work_path = tmp_path / "work"
        spec_path = tmp_path / "spec"
        work_path.mkdir(parents=True, exist_ok=True)
        spec_path.mkdir(parents=True, exist_ok=True)
        proc = subprocess.run(
            [
                pyinstaller,
                "--onefile",
                "--noconfirm",
                "--clean",
                "--name",
                name,
                "--distpath",
                str(dist_dir),
                "--workpath",
                str(work_path),
                "--specpath",
                str(spec_path),
                str(script_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            die(f"PyInstaller failed:\n{proc.stdout}")

    exe = dist_dir / (name + (".exe" if tag.startswith("windows-") else ""))
    if exe.exists():
        return exe
    candidates = sorted(dist_dir.glob(name + "*"), key=lambda p: p.name)
    return candidates[0] if candidates else None

