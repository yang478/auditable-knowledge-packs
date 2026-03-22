from __future__ import annotations

import re
import subprocess
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from .fs_utils import die, read_text, which


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
                    die(
                        "PDF import fallback requested (--pdf-fallback pypdf), but `pypdf` is not installed.\n"
                        "Install it (recommended): pip install pypdf\n"
                        "Or install `pdftotext` (poppler-utils), or convert PDF → .txt/.md first."
                    )
            try:
                reader = PdfReader(str(path))
            except Exception as exc:
                die(f"pypdf failed to read PDF: {path.name} ({type(exc).__name__}: {exc})")
            out: List[str] = []
            for idx, page in enumerate(getattr(reader, "pages", []) or []):
                try:
                    text = page.extract_text() or ""
                except Exception as exc:
                    die(f"pypdf failed to extract text: {path.name} page={idx} ({type(exc).__name__}: {exc})")
                if text:
                    out.append(text)
            extracted = "\n\n".join(out).strip()
            if not extracted:
                die(
                    "pypdf extracted empty text. Try installing `pdftotext` (poppler-utils), or convert PDF → .txt/.md first."
                )
            return extracted + "\n"
        die(
            "PDF import requires `pdftotext` (poppler-utils). Install it, or convert PDF to .txt/.md first.\n"
            "Tip (Ubuntu): sudo apt-get install poppler-utils\n"
            "Tip: If you can't install it, try: --pdf-fallback pypdf (best-effort, requires `pypdf`)."
        )
    proc = subprocess.run(
        [pdftotext, "-layout", str(path), "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        die(f"pdftotext failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout


def _docx_image_relationships(docx_path: Path) -> dict[str, str]:
    try:
        with zipfile.ZipFile(docx_path) as z:
            try:
                rels_xml = z.read("word/_rels/document.xml.rels")
            except KeyError:
                return {}
    except zipfile.BadZipFile:
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
            try:
                xml = z.read("word/document.xml")
            except KeyError:
                die(f"DOCX missing word/document.xml: {docx_path.name}. Try converting DOCX → MD/TXT first.")
    except zipfile.BadZipFile:
        die(f"Invalid DOCX (bad zip): {docx_path.name}. Try converting DOCX → MD/TXT first.")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        die(f"DOCX parse failed: {docx_path.name}. Try converting DOCX → MD/TXT first.")
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
        die(f"Failed to extract DOCX paragraphs: {path.name}. Try converting DOCX → MD/TXT first.")
    out: List[str] = []
    for level, text in paras:
        if level is not None:
            out.append("#" * max(1, min(6, level)) + " " + text)
        else:
            out.append(text)
    return "\n\n".join(out).strip() + "\n"


def _infer_text_headings_to_markdown(text: str) -> str:
    lines = [ln.rstrip() for ln in text.splitlines()]
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
        out.append(ln)
    md = "\n".join(out)
    if not md.endswith("\n"):
        md += "\n"
    return md


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
    die(f"Unsupported input type: {path.name} (supported: .md .txt .docx .pdf)")
