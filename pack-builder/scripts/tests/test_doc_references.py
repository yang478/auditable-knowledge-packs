"""文档生成单元测试 — 覆盖 references.py 核心逻辑。"""

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.doc.references import (
    _extract_heading_entries,
    _heading_stack_at,
    _chunk_title,
    _build_chapter_toc,
    generate_doc,
)
from build_skill_lib.types import InputDoc
from build_skill_lib.utils.fs import ConfigError


class HeadingExtractionTests(unittest.TestCase):
    def test_extract_no_headings(self) -> None:
        entries = _extract_heading_entries("No headings here.")
        self.assertEqual(entries, [])

    def test_extract_single_heading(self) -> None:
        entries = _extract_heading_entries("# Title\n")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].title, "Title")
        self.assertEqual(entries[0].level, 1)
        self.assertEqual(entries[0].char_start, 0)

    def test_extract_multiple_levels(self) -> None:
        text = "# H1\n\n## H2\n\n### H3\n"
        entries = _extract_heading_entries(text)
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0].title, "H1")
        self.assertEqual(entries[0].level, 1)
        self.assertEqual(entries[1].title, "H2")
        self.assertEqual(entries[1].level, 2)
        self.assertEqual(entries[2].title, "H3")
        self.assertEqual(entries[2].level, 3)

    def test_extract_ignores_non_headings(self) -> None:
        text = "# Real\nNot a # heading\n## Real2\n"
        entries = _extract_heading_entries(text)
        self.assertEqual(len(entries), 2)
        self.assertEqual([e.title for e in entries], ["Real", "Real2"])

    def test_extract_position_tracking(self) -> None:
        text = "prefix\n# Title\nsuffix"
        entries = _extract_heading_entries(text)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].char_start, 7)  # after "prefix\n"
        self.assertEqual(entries[0].char_end, 15)   # after "# Title\n" (1+1+5+1+1=9 chars)


class HeadingStackTests(unittest.TestCase):
    def test_stack_empty_no_headings(self) -> None:
        stack = _heading_stack_at(0, [])
        self.assertEqual(stack, [])

    def test_stack_before_any_heading(self) -> None:
        headings = _extract_heading_entries("abc\n# Title\n")
        stack = _heading_stack_at(0, headings)
        self.assertEqual(stack, [])

    def test_stack_at_heading(self) -> None:
        headings = _extract_heading_entries("# Title\n")
        stack = _heading_stack_at(0, headings)
        self.assertEqual(stack, ["Title"])

    def test_stack_hierarchical(self) -> None:
        text = "# Doc\n\n## Section A\n\n### Sub A1\n"
        headings = _extract_heading_entries(text)
        # 在 Sub A1 处
        stack = _heading_stack_at(headings[2].char_start, headings)
        self.assertEqual(stack, ["Doc", "Section A", "Sub A1"])

    def test_stack_sibling_replaces(self) -> None:
        text = "# Doc\n\n## A\n\n## B\n"
        headings = _extract_heading_entries(text)
        # 在 B 处，A 应被替换
        stack = _heading_stack_at(headings[2].char_start, headings)
        self.assertEqual(stack, ["Doc", "B"])

    def test_stack_level_rollback(self) -> None:
        text = "# Doc\n\n## A\n\n### A1\n\n## B\n"
        headings = _extract_heading_entries(text)
        # 在 B 处，A1 应被弹出
        stack = _heading_stack_at(headings[3].char_start, headings)
        self.assertEqual(stack, ["Doc", "B"])


class ChunkTitleTests(unittest.TestCase):
    def test_title_from_plain_text(self) -> None:
        text = "Hello world\nMore text"
        title = _chunk_title("chunk-0001", text)
        self.assertIn("chunk-0001", title)
        self.assertIn("Hello world", title)

    def test_title_strips_heading_markers(self) -> None:
        text = "## Section Title\nBody text"
        title = _chunk_title("chunk-0001", text)
        self.assertIn("Section Title", title)
        self.assertNotIn("##", title)

    def test_title_truncates_long_lines(self) -> None:
        long_line = "A" * 100
        text = f"{long_line}\nbody"
        title = _chunk_title("chunk-0001", text)
        self.assertLess(len(title), 100)

    def test_title_fallback_to_chunk_id(self) -> None:
        title = _chunk_title("chunk-0001", "")
        self.assertEqual(title, "chunk-0001")

    def test_title_skips_blank_lines(self) -> None:
        text = "\n\n\nReal title\n"
        title = _chunk_title("chunk-0001", text)
        self.assertIn("Real title", title)

    def test_title_removes_quotes(self) -> None:
        text = '## "Quoted" Title\nbody'
        title = _chunk_title("chunk-0001", text)
        self.assertNotIn('"', title)


class ChapterTocTests(unittest.TestCase):
    def test_toc_empty(self) -> None:
        toc = _build_chapter_toc("Doc", [])
        self.assertIn("无章节信息", toc)

    def test_toc_single_doc_title_groups_by_next_level(self) -> None:
        chunks = [
            ("chunk-0001", ["Document", "Chapter 1", "Section 1.1"]),
            ("chunk-0002", ["Document", "Chapter 1", "Section 1.2"]),
            ("chunk-0003", ["Document", "Chapter 2"]),
        ]
        toc = _build_chapter_toc("Doc", chunks)
        self.assertIn("Chapter 1", toc)
        self.assertIn("Chapter 2", toc)
        self.assertIn("chunk-0001", toc)
        self.assertIn("chunk-0003", toc)

    def test_toc_multiple_top_groups_by_top_level(self) -> None:
        chunks = [
            ("chunk-0001", ["Part A", "Chapter 1"]),
            ("chunk-0002", ["Part B", "Chapter 2"]),
        ]
        toc = _build_chapter_toc("Doc", chunks)
        self.assertIn("Part A", toc)
        self.assertIn("Part B", toc)

    def test_toc_no_headings(self) -> None:
        chunks = [
            ("chunk-0001", []),
            ("chunk-0002", []),
        ]
        toc = _build_chapter_toc("Doc", chunks)
        self.assertIn("未分类", toc)

    def test_toc_single_heading_fallback(self) -> None:
        chunks = [
            ("chunk-0001", ["Only Heading"]),
        ]
        toc = _build_chapter_toc("Doc", chunks)
        self.assertIn("Only Heading", toc)


class GenerateDocValidationTests(unittest.TestCase):
    def test_chunk_size_zero_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test")
            out = Path(tmp) / "skill"
            out.mkdir()
            with self.assertRaises(ConfigError):
                generate_doc(doc, "# Test\n", out, chunk_size=0, overlap=0)

    def test_overlap_negative_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test")
            out = Path(tmp) / "skill"
            out.mkdir()
            with self.assertRaises(ConfigError):
                generate_doc(doc, "# Test\n", out, chunk_size=100, overlap=-1)

    def test_overlap_gte_chunk_size_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test")
            out = Path(tmp) / "skill"
            out.mkdir()
            with self.assertRaises(ConfigError):
                generate_doc(doc, "# Test\n", out, chunk_size=100, overlap=100)

    def test_generates_expected_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test Doc")
            out = Path(tmp) / "skill"
            out.mkdir()
            md = "# Test Doc\n\nParagraph one.\n\nParagraph two.\n"
            heading_rows, nodes = generate_doc(
                doc, md, out, chunk_size=50, overlap=0
            )
            doc_dir = out / "references" / "test"
            self.assertTrue(doc_dir.exists())
            self.assertTrue((doc_dir / "doc.md").exists())
            self.assertTrue((doc_dir / "toc.md").exists())
            self.assertTrue((doc_dir / "chunks").is_dir())
            # 至少生成 doc node + 一些 chunk nodes
            self.assertGreaterEqual(len(nodes), 1)
            self.assertEqual(nodes[0].kind, "doc")

    def test_nodes_linked_prev_next(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test")
            out = Path(tmp) / "skill"
            out.mkdir()
            md = "# T\n\n" + "A" * 200  # long enough to split
            heading_rows, nodes = generate_doc(
                doc, md, out, chunk_size=50, overlap=0
            )
            chunks = [n for n in nodes if n.kind == "chunk"]
            self.assertGreaterEqual(len(chunks), 2, "Input should produce at least 2 chunks")
            self.assertEqual(chunks[0].prev_id, None)
            self.assertEqual(chunks[0].next_id, chunks[1].node_id)
            self.assertEqual(chunks[1].prev_id, chunks[0].node_id)


if __name__ == "__main__":
    unittest.main()
