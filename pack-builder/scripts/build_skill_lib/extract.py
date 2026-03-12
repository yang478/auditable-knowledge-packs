from __future__ import annotations

import re
import subprocess
import zipfile
from pathlib import Path
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from .fs_utils import die, read_text, which


def _extract_pdf_to_text(path: Path) -> str:
    pdftotext = which("pdftotext")
    if not pdftotext:
        die(
            "PDF import requires `pdftotext` (poppler-utils). Install it, or convert PDF to .txt/.md first.\n"
            "Tip (Ubuntu): sudo apt-get install poppler-utils"
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
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paras: List[Tuple[Optional[int], str]] = []

    for p in root.findall(".//w:p", ns):
        style_val: Optional[str] = None
        ppr = p.find("./w:pPr", ns)
        if ppr is not None:
            pstyle = ppr.find("./w:pStyle", ns)
            if pstyle is not None:
                style_val = pstyle.attrib.get(f"{{{ns['w']}}}val")

        runs: List[str] = []
        for t in p.findall(".//w:t", ns):
            runs.append(t.text or "")
        text = "".join(runs).strip()
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


def extract_to_markdown(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".md":
        return read_text(path)
    if suffix == ".txt":
        return _infer_text_headings_to_markdown(read_text(path))
    if suffix == ".docx":
        return _extract_docx_to_markdown(path)
    if suffix == ".pdf":
        return _infer_text_headings_to_markdown(_extract_pdf_to_text(path))
    die(f"Unsupported input type: {path.name} (supported: .md .txt .docx .pdf)")
