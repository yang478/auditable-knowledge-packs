"""Tests for build_skill_lib.extract."""

import sys
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.extract import (
    _infer_text_headings_to_markdown,
    extract_to_markdown,
)
from build_skill_lib.utils.fs import BuildError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_docx_bytes(paragraphs_xml: str) -> bytes:
    """Build a minimal DOCX zip archive in memory."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("word/document.xml", paragraphs_xml)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _infer_text_headings_to_markdown
# ---------------------------------------------------------------------------


def test_infer_underline_h1() -> None:
    text = "Title\n===\nbody"
    result = _infer_text_headings_to_markdown(text)
    assert "# Title\n" in result
    assert "===" not in result


def test_infer_underline_h2() -> None:
    text = "Subtitle\n---\nbody"
    result = _infer_text_headings_to_markdown(text)
    assert "## Subtitle\n" in result
    assert "---" not in result


def test_infer_chapter_h1() -> None:
    text = "\n第一章 绪论\n\n正文"
    result = _infer_text_headings_to_markdown(text)
    assert "# 第一章 绪论\n" in result


def test_infer_numbered_headings() -> None:
    text = "\n1.1 概述\n\n1.2.3 细节\n\n正文"
    result = _infer_text_headings_to_markdown(text)
    assert "## 1.1 概述\n" in result
    assert "### 1.2.3 细节\n" in result


def test_infer_short_line_h3() -> None:
    text = (
        "\nShort heading\n\n"
        "This is a long line that should definitely not become a heading because it is way more than sixty characters.\n"
    )
    result = _infer_text_headings_to_markdown(text)
    lines = result.splitlines()
    assert any(line == "### Short heading" for line in lines)
    assert not any(line.startswith("#") and "long line" in line for line in lines)


def test_infer_list_items_not_headings() -> None:
    text = "- item one\n\n1. item two\n\n正文"
    result = _infer_text_headings_to_markdown(text)
    assert "- item one" in result
    assert "1. item two" in result
    assert "### item one" not in result


# ---------------------------------------------------------------------------
# extract_to_markdown — happy paths
# ---------------------------------------------------------------------------


def test_md_returns_content(tmp_path: Path) -> None:
    path = tmp_path / "doc.md"
    path.write_text("# Hello\n\nworld\n", encoding="utf-8")
    assert extract_to_markdown(path) == "# Hello\n\nworld\n"


def test_txt_runs_infer_headings(tmp_path: Path) -> None:
    path = tmp_path / "doc.txt"
    path.write_text("\n第一章 介绍\n\n这是正文。\n", encoding="utf-8")
    result = extract_to_markdown(path)
    assert "# 第一章 介绍\n" in result
    assert "这是正文" in result


# ---------------------------------------------------------------------------
# Unsupported extension
# ---------------------------------------------------------------------------


def test_unsupported_extension_raises(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_text("a,b,c\n", encoding="utf-8")
    with pytest.raises(BuildError) as exc_info:
        extract_to_markdown(path)
    assert "Unsupported" in str(exc_info.value)


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------


def test_pdf_pdftotext_exists(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "test.pdf"
    pdf.write_text("fake pdf", encoding="utf-8")
    monkeypatch.setattr("build_skill_lib.extract.which", lambda cmd: "/usr/bin/pdftotext")
    proc_mock = MagicMock(returncode=0, stdout="Extracted PDF text\n", stderr="")
    monkeypatch.setattr("build_skill_lib.extract.run_subprocess_safe", lambda *a, **kw: proc_mock)
    result = extract_to_markdown(pdf)
    assert "Extracted PDF text" in result


def test_pdf_pdftotext_fails(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "test.pdf"
    pdf.write_text("fake pdf", encoding="utf-8")
    monkeypatch.setattr("build_skill_lib.extract.which", lambda cmd: "/usr/bin/pdftotext")
    proc_mock = MagicMock(returncode=1, stdout="", stderr="pdftotext crashed")
    monkeypatch.setattr("build_skill_lib.extract.run_subprocess_safe", lambda *a, **kw: proc_mock)
    with pytest.raises(BuildError) as exc_info:
        extract_to_markdown(pdf)
    assert "pdftotext failed" in str(exc_info.value)


def test_pdf_pypdf_fallback(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "test.pdf"
    pdf.write_text("fake pdf", encoding="utf-8")
    monkeypatch.setattr("build_skill_lib.extract.which", lambda cmd: None)

    mock_page = MagicMock()
    mock_page.extract_text.return_value = "pypdf extracted text"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]
    mock_pypdf = MagicMock()
    mock_pypdf.PdfReader.return_value = mock_reader

    with patch.dict(sys.modules, {"pypdf": mock_pypdf}):
        result = extract_to_markdown(pdf, pdf_fallback="pypdf")
    assert "pypdf extracted text" in result


# ---------------------------------------------------------------------------
# DOCX extraction
# ---------------------------------------------------------------------------

DOCX_NS = (
    'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
)


def test_docx_basic(tmp_path: Path) -> None:
    xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document {DOCX_NS}>
  <w:body>
    <w:p>
      <w:pPr><w:pStyle w:val="Heading1"/></w:pPr>
      <w:r><w:t>Heading One</w:t></w:r>
    </w:p>
    <w:p>
      <w:r><w:t>Paragraph text.</w:t></w:r>
    </w:p>
  </w:body>
</w:document>"""
    path = tmp_path / "test.docx"
    path.write_bytes(_make_docx_bytes(xml))
    result = extract_to_markdown(path)
    assert "# Heading One" in result
    assert "Paragraph text." in result


def test_docx_bad_zip(tmp_path: Path) -> None:
    path = tmp_path / "bad.docx"
    path.write_text("this is not a zip", encoding="utf-8")
    with pytest.raises(BuildError) as exc_info:
        extract_to_markdown(path)
    assert "bad zip" in str(exc_info.value).lower()
