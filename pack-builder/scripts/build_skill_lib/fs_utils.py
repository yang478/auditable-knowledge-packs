from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import sys
import unicodedata
from pathlib import Path
from typing import Iterable, Optional, Tuple


def die(message: str, code: int = 2) -> "NoReturn":  # type: ignore[name-defined]
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(code)


def safe_skill_name(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?", name):
        die("Invalid --skill-name. Use lowercase letters/digits/hyphens only (e.g. my-books).")
    if name.startswith("-") or name.endswith("-") or "--" in name:
        die("Invalid --skill-name. Avoid leading/trailing hyphens and consecutive '--'.")
    return name


def slugify_ascii(text: str) -> str:
    s = unicodedata.normalize("NFKC", text)
    s = s.lower()
    s = re.sub(r"(?<![a-z0-9])v(?=\d+\b)", "versionkeep_", s)
    s = re.sub(r"([a-z])(\d)", r"\1-\2", s)
    s = re.sub(r"(\d)([a-z])", r"\1-\2", s)
    s = s.replace("versionkeep_", "v")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def derive_doc_id(path: Path, used: set[str]) -> str:
    base = slugify_ascii(path.stem)
    if not base:
        base = "doc"
    if len(base) > 48:
        base = base[:48].strip("-") or "doc"

    doc_id = base
    if doc_id in used:
        h = hashlib.sha1(str(path).encode("utf-8", errors="ignore")).hexdigest()[:8]
        doc_id = f"{base}-{h}"
    i = 2
    while doc_id in used:
        doc_id = f"{base}-{i}"
        i += 1
    used.add(doc_id)
    return doc_id


def derive_doc_title(path: Path, md: str) -> str:
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            title = re.sub(r"^#{1,6}\s+", "", line).strip()
            return title or path.stem
        break
    return path.stem


def read_text(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "gb18030", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def which(cmd: str) -> Optional[str]:
    if cmd == "pdftotext" and os.environ.get("BOOK_SKILL_GENERATOR_NO_PDFTOTEXT"):
        return None
    return shutil.which(cmd)


def platform_tag() -> str:
    import platform

    system = (platform.system() or "").lower()
    if system.startswith("windows"):
        system = "windows"
    elif system.startswith("linux"):
        system = "linux"
    elif system.startswith("darwin"):
        system = "macos"
    elif not system:
        system = "unknown"

    machine = (platform.machine() or "").lower()
    if machine in {"amd64", "x64"}:
        machine = "x86_64"
    if machine in {"aarch64"}:
        machine = "arm64"
    if not machine:
        machine = "unknown"

    return f"{system}-{machine}"


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def write_tsv(path: Path, rows: Iterable[Tuple[str, ...]], header: Optional[Tuple[str, ...]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        if header:
            f.write("# " + "\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(row) + "\n")

