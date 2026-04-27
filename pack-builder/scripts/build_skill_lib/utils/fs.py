from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import shutil
import sys
import unicodedata
from pathlib import Path
from typing import Iterable, NoReturn, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception hierarchy for build-time errors
# ---------------------------------------------------------------------------

class PackBuilderError(Exception):
    """Base exception for all pack-builder errors."""
    pass


class ConfigError(PackBuilderError):
    """Invalid configuration or CLI arguments (e.g. bad --skill-name)."""
    pass


class BuildError(PackBuilderError):
    """Build process failure (DB write, file I/O, extraction)."""
    pass


class DataIntegrityError(PackBuilderError):
    """Data inconsistency (missing body for leaf node, hash mismatch)."""
    pass


def die(message: str, code: int = 2) -> "NoReturn":  # type: ignore[name-defined]
    logger.error(message)
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(code)


_CJK_CHAR_RE = r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]"


def normalize_title_whitespace(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if not value:
        return value
    return re.sub(rf"(?<={_CJK_CHAR_RE}) (?={_CJK_CHAR_RE})", "", value)


def safe_skill_name(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}[a-z0-9]?", name):
        raise ConfigError("Invalid --skill-name. Use lowercase letters/digits/hyphens only (e.g. my-books).")
    if name.startswith("-") or name.endswith("-") or "--" in name:
        raise ConfigError("Invalid --skill-name. Avoid leading/trailing hyphens and consecutive '--'.")
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
        h = hashlib.sha256(str(path).encode("utf-8", errors="ignore")).hexdigest()[:12]
        doc_id = f"{base}-{h}"
    i = 2
    while doc_id in used:
        doc_id = f"{base}-{i}"
        i += 1
    used.add(doc_id)
    return doc_id


def derive_doc_id_by_content(title: str, content_sample: str) -> str:
    """基于标题和内容生成稳定的 doc_id。"""
    slug = _slugify_title(title)[:40]
    content_hash = hashlib.sha256(content_sample.encode("utf-8", errors="replace")).hexdigest()[:8]
    if slug:
        return f"{slug}-{content_hash}"
    return f"doc-{content_hash}"


def _slugify_title(text: str) -> str:
    """中文保留，移除特殊字符，空格转连字符。"""
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    text = re.sub(r"[^\w\u4e00-\u9fff-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def derive_doc_title(path: Path, md: str) -> str:
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            title = re.sub(r"^#{1,6}\s+", "", line).strip()
            title = normalize_title_whitespace(title)
            return title or path.stem
        break
    return path.stem


def _detect_encoding(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "gb18030", "cp1252", "latin-1"):
        try:
            data.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _complete_utf8_sample(f, sample: bytes) -> bytes:
    """Extend sample so the last character is not a truncated UTF-8 sequence."""
    if not sample:
        return sample
    # If the last byte is a continuation byte (10xxxxxx), keep reading
    # until we hit a non-continuation byte.
    while (sample[-1] & 0xC0) == 0x80:
        extra = f.read(1)
        if not extra:
            break
        sample += extra
    # If the last byte is a lead byte (11xxxxxx), read its continuation bytes.
    if (sample[-1] & 0xC0) == 0xC0:
        b = sample[-1]
        if b & 0xF0 == 0xF0:      # 4-byte sequence
            need = 3
        elif b & 0xE0 == 0xE0:    # 3-byte sequence
            need = 2
        else:                     # 2-byte sequence
            need = 1
        extra = f.read(need)
        sample += extra
    return sample


def read_text(path: Path) -> str:
    """Read text from *path* with encoding auto-detection.

    Streams large files in chunks to avoid loading multi-gigabyte files
    entirely into memory.
    """
    size = path.stat().st_size
    if size <= 1024 * 1024:
        data = path.read_bytes()
        return data.decode(_detect_encoding(data), errors="replace")

    with path.open("rb") as f:
        sample = f.read(65536)
        sample = _complete_utf8_sample(f, sample)
        encoding = _detect_encoding(sample)
        f.seek(0)

        out_parts: list[str] = []
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            out_parts.append(chunk.decode(encoding, errors="replace"))
    return "".join(out_parts)


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


def write_tsv(
    path: Path,
    rows: Iterable[Tuple[str, ...]],
    header: Optional[Tuple[str, ...]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        if header:
            f.write("# " + "\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(row) + "\n")
