from __future__ import annotations

import logging
import re
import subprocess
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from .utils.fs import BuildError, read_text, which
from .ir.identity import derive_span_id
from .types import AtomicSpan
from .utils.text import normalize_canonical_text
from .utils.safe_subprocess import run_subprocess_safe


def _extract_pdf_to_text(path: Path, *, pdf_fallback: str) -> str:
    pdftotext = which("pdftotext")
    if not pdftotext:
        if str(pdf_fallback).strip().lower() == "pypdf":
            try:
                from pypdf import PdfReader  # type: ignore[import-not-found]
            except ImportError:
                try:
                    from PyPDF2 import PdfReader  # type: ignore[import-not-found]
                except ImportError:
                    raise BuildError(
                        f"PDF import fallback requested (--pdf-fallback pypdf), but `pypdf` is not installed.\n"
                        "Install it (recommended): pip install pypdf\n"
                        "Or install `pdftotext` (poppler-utils), or convert PDF → .txt/.md first."
                    )
            try:
                reader = PdfReader(str(path))
            except Exception as exc:
                raise BuildError(f"pypdf failed to read PDF: {path.name} ({type(exc).__name__}: {exc})")
            out: List[str] = []
            for idx, page in enumerate(getattr(reader, "pages", []) or []):
                try:
                    text = page.extract_text() or ""
                except Exception as exc:
                    logger = logging.getLogger(__name__)
                    logger.warning("pypdf failed to extract text: %s page=%d (%s: %s)", path.name, idx, type(exc).__name__, exc)
                    text = ""
                if text:
                    out.append(text)
            extracted = "\n\n".join(out).strip()
            if not extracted:
                raise BuildError(
                    f"pypdf extracted empty text from {path.name}. Try installing `pdftotext` (poppler-utils), or convert PDF → .txt/.md first."
                )
            return extracted + "\n"
        raise BuildError(
            f"PDF import requires `pdftotext` (poppler-utils) for {path.name}. Install it, or convert PDF to .txt/.md first.\n"
            "Tip (Ubuntu): sudo apt-get install poppler-utils\n"
            "Tip: If you can't install it, try: --pdf-fallback pypdf (best-effort, requires `pypdf`)."
        )
    proc = run_subprocess_safe(
        [pdftotext, "-layout", str(path), "-"],
        timeout=120.0,
        max_output_bytes=256 * 1024 * 1024,  # PDFs can be large
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        raise BuildError(f"pdftotext failed for {path.name}: {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout


def _docx_image_relationships(docx_path: Path) -> dict[str, str]:
    try:
        with zipfile.ZipFile(docx_path) as z:
            rels_xml = z.read("word/_rels/document.xml.rels")
    except zipfile.BadZipFile:
        return {}
    except KeyError:
        return {}

    try:
        root = ET.fromstring(rels_xml)
    except ET.ParseError:
        return {}

    ns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
    out: dict[str, str] = {}
    for rel in root.findall("r:Relationship", ns):
        rid = rel.attrib.get("Id") or ""
        typ = rel.attrib.get("Type") or ""
        target = rel.attrib.get("Target") or ""
        if not rid or not target:
            continue
        if "relationships/image" not in typ:
            continue
        out[rid] = target
    return out


def _docx_paragraphs(docx_path: Path) -> List[Tuple[Optional[int], str]]:
    try:
        with zipfile.ZipFile(docx_path) as z:
            xml = z.read("word/document.xml")
    except zipfile.BadZipFile:
        raise BuildError(f"Invalid DOCX (bad zip): {docx_path.name}. Try converting DOCX → MD/TXT first.")
    except KeyError:
        raise BuildError(f"DOCX missing word/document.xml: {docx_path.name}. Try converting DOCX → MD/TXT first.")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        raise BuildError(f"DOCX parse failed: {docx_path.name}. Try converting DOCX → MD/TXT first.")
    ns = {
        "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
        "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    img_rels = _docx_image_relationships(docx_path)
    paras: List[Tuple[Optional[int], str]] = []

    for p in root.findall(".//w:p", ns):
        style_val: Optional[str] = None
        ppr = p.find("./w:pPr", ns)
        if ppr is not None:
            pstyle = ppr.find("./w:pStyle", ns)
            if pstyle is not None:
                style_val = pstyle.attrib.get(f"{{{ns['w']}}}val")

        text_only = "".join((t.text or "") for t in p.findall(".//w:t", ns)).strip()
        if not text_only:
            continue

        # Detect figure captions like "(1.2.3)" — these often contain inline images.
        include_images = bool(re.search(r"\(\d+(?:\.\d+)+\)", text_only))
        if not include_images:
            text = text_only
        else:
            parts: List[str] = []
            for r_el in p.findall("./w:r", ns):
                parts.append("".join((t.text or "") for t in r_el.findall(".//w:t", ns)))
                for blip in r_el.findall(".//a:blip", ns):
                    embed = blip.attrib.get(f"{{{ns['r']}}}embed") or ""
                    if not embed:
                        continue
                    target = img_rels.get(embed) or embed
                    parts.append(f" [[IMAGE:{target}]] ")
            text = "".join(parts).strip()
        if not text:
            continue

        heading_level: Optional[int] = None
        if style_val:
            m = re.match(r"Heading([1-6])", style_val)
            if m:
                heading_level = int(m.group(1))
        paras.append((heading_level, text))
    return paras


def _extract_docx_to_markdown(path: Path) -> str:
    paras = _docx_paragraphs(path)
    if not paras:
        raise BuildError(f"Failed to extract DOCX paragraphs: {path.name}. Try converting DOCX → MD/TXT first.")
    out: List[str] = []
    for level, text in paras:
        if level is not None:
            out.append("#" * max(1, min(6, level)) + " " + text)
        else:
            out.append(text)
    return "\n\n".join(out).strip() + "\n"


def _infer_text_headings_to_markdown(text: str) -> str:
    lines = [ln.rstrip() for ln in text.splitlines()]

    # Pass 1: 处理下划线式标题 (=== → H1, --- → H2)
    out: List[str] = []
    for ln in lines:
        s = ln.strip()
        if not s:
            out.append("")
            continue
        if re.fullmatch(r"[=]{3,}", s) and out:
            prev = out.pop().strip()
            out.append("# " + prev)
            continue
        if re.fullmatch(r"[-]{3,}", s) and out:
            prev = out.pop().strip()
            out.append("## " + prev)
            continue
        out.append(ln)

    # Pass 2: 短行启发式 + 编号/章节模式
    result: List[str] = []
    for i, ln in enumerate(out):
        s = ln.strip()
        if not s or s.startswith("#"):
            result.append(ln)
            continue
        prev_blank = (i == 0) or (not out[i - 1].strip())
        next_blank = (i == len(out) - 1) or (not out[i + 1].strip())
        if prev_blank and next_blank:
            # Chapter / 第X章 模式 → H1
            if re.match(r"(?i)^Chapter\s+", s):
                result.append("# " + s)
                continue
            if re.match(r"^第[一二三四五六七八九十百千万\d]+[章节篇部]", s):
                result.append("# " + s)
                continue
            # 编号模式: 1.1 → H2, 1.2.3 → H3
            num_m = re.match(r"^(\d+(?:\.\d+)+)\s+\S", s)
            if num_m:
                depth = num_m.group(1).count(".") + 1
                result.append("#" * min(depth, 6) + " " + s)
                continue
            # 短行启发式 (<60 字符, 非列表项) → H3
            if len(s) < 60:
                if re.match(r"^[-*•·]\s", s):
                    result.append(ln)
                    continue
                if re.match(r"^\d+\.\s", s) and not re.match(r"^\d+\.\d+", s):
                    result.append(ln)
                    continue
                result.append("### " + s)
                continue
        result.append(ln)

    md = "\n".join(result)
    if not md.endswith("\n"):
        md += "\n"
    return md


def _iter_block_ranges(canonical_text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    block_start: int | None = None
    block_end = 0
    cursor = 0

    for line in canonical_text.splitlines(keepends=True):
        line_start = cursor
        cursor += len(line)
        if line.strip():
            if block_start is None:
                block_start = line_start
            block_end = cursor
            continue
        if block_start is not None:
            ranges.append((block_start, block_end))
            block_start = None

    if block_start is not None:
        ranges.append((block_start, block_end))
    return ranges


def spans_from_markdown(text: str, *, doc_id: str = "") -> list[AtomicSpan]:
    canonical = normalize_canonical_text(text)
    spans: list[AtomicSpan] = []
    for reading_order, (char_start, char_end) in enumerate(_iter_block_ranges(canonical)):
        spans.append(
            AtomicSpan(
                doc_id=str(doc_id),
                span_id=derive_span_id(doc_id, char_start, char_end),
                char_start=char_start,
                char_end=char_end,
                reading_order=reading_order,
            )
        )
    return spans


def extract_to_markdown(path: Path, *, pdf_fallback: str = "none") -> str:
    suffix = path.suffix.lower()
    if suffix == ".md":
        return read_text(path)
    if suffix == ".txt":
        return _infer_text_headings_to_markdown(read_text(path))
    if suffix == ".docx":
        return _extract_docx_to_markdown(path)
    if suffix == ".pdf":
        return _infer_text_headings_to_markdown(_extract_pdf_to_text(path, pdf_fallback=pdf_fallback))
    raise BuildError(f"Unsupported input type: {path.name} (supported: .md .txt .docx .pdf)")
