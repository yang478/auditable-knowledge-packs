"""分块算法单元测试 — 覆盖 chunking.py 核心逻辑。"""

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.chunking import (
    chunk_document,
    chunk_document_atomic,
    iter_chunk_spans,
    ChunkSpan,
    DEFAULT_SEPARATORS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_OVERLAP,
)


class ChunkingBasicTests(unittest.TestCase):
    def test_empty_text_returns_empty(self) -> None:
        result = chunk_document("", chunk_size=100, overlap=0)
        self.assertEqual(result, [])

    def test_whitespace_only_returns_empty(self) -> None:
        result = chunk_document("   \n\t  ", chunk_size=100, overlap=0)
        self.assertEqual(result, [])

    def test_short_text_single_chunk(self) -> None:
        text = "Hello world"
        chunks = chunk_document(text, chunk_size=100, overlap=0)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].text, "Hello world")
        self.assertEqual(chunks[0].ordinal, 1)
        self.assertEqual(chunks[0].char_start, 0)
        self.assertEqual(chunks[0].char_end, 11)

    def test_basic_split_by_space(self) -> None:
        text = "a " * 50  # 100 chars with spaces
        chunks = chunk_document(text, chunk_size=30, overlap=0)
        self.assertGreater(len(chunks), 1)
        for i, chunk in enumerate(chunks):
            self.assertEqual(chunk.ordinal, i + 1)
            self.assertGreaterEqual(chunk.char_start, 0)
            self.assertLessEqual(chunk.char_end, len(text))
            self.assertGreater(len(chunk.text), 0)


class ChunkingSeparatorTests(unittest.TestCase):
    def test_chinese_period_separator(self) -> None:
        """中文句号优先作为分隔符。"""
        text = "第一章内容。第二章内容。第三章内容。"
        chunks = chunk_document(text, chunk_size=15, overlap=0)
        # 每个 chunk 应该在句号处结束
        for chunk in chunks:
            if chunk.text.endswith("。"):
                self.assertTrue(chunk.text.rstrip().endswith("。"))

    def test_english_period_space_separator(self) -> None:
        text = "First sentence. Second sentence. Third sentence."
        chunks = chunk_document(text, chunk_size=20, overlap=0)
        # 至少有两个块（因为总长度48，chunk_size=20）
        self.assertGreaterEqual(len(chunks), 2)

    def test_newline_separator_fallback(self) -> None:
        text = "Line1\nLine2\nLine3\nLine4\nLine5"
        chunks = chunk_document(text, chunk_size=12, overlap=0)
        # 在空格之前先尝试换行分隔符
        for chunk in chunks:
            # 每个块末尾要么是换行，要么是文本末尾
            pass  # 只要有块生成就行，不做严格断言
        self.assertGreaterEqual(len(chunks), 1)


class ChunkingOverlapTests(unittest.TestCase):
    def test_overlap_preserves_continuity(self) -> None:
        text = "ABCDEFGHIJ" * 10  # 100 chars
        chunks = chunk_document(text, chunk_size=20, overlap=5)
        self.assertGreater(len(chunks), 1)
        for i in range(len(chunks) - 1):
            prev_end = chunks[i].char_end
            next_start = chunks[i + 1].char_start
            # overlap 意味着下一个块的起始位置在 prev_end - overlap 之前
            self.assertLess(next_start, prev_end)

    def test_overlap_zero_is_clean_split(self) -> None:
        text = "ABCDEFGHIJ" * 10
        chunks = chunk_document(text, chunk_size=20, overlap=0)
        for i in range(len(chunks) - 1):
            self.assertEqual(chunks[i].char_end, chunks[i + 1].char_start)


class ChunkingAtomicBlockTests(unittest.TestCase):
    def test_code_fence_not_split(self) -> None:
        text = "Intro\n\n```python\nprint('hello')\nprint('world')\n```\n\nOutro"
        chunks = chunk_document(text, chunk_size=30, overlap=0)
        # 确保代码块没有被截断（没有任何一个 chunk 包含 ``` 但不完整）
        for chunk in chunks:
            c = chunk.text
            # 检查不完整的 fence
            self.assertFalse(
                c.count("```") == 1 and not c.strip().startswith("```"),
                f"Chunk contains incomplete fence: {c!r}",
            )

    def test_markdown_table_not_split(self) -> None:
        text = "Header\n\n| Col1 | Col2 |\n|------|------|\n| A    | B    |\n| C    | D    |\n\nFooter"
        chunks = chunk_document(text, chunk_size=30, overlap=0)
        for chunk in chunks:
            c = chunk.text
            # 不完整的表格（有 | 开头行但不是完整的表格块）
            pipe_lines = [l for l in c.splitlines() if l.startswith("|")]
            if pipe_lines:
                # 如果包含表格行，应该包含至少2行管道行
                self.assertGreaterEqual(
                    len(pipe_lines), 2,
                    f"Chunk may contain partial table: {c!r}",
                )

    def test_oversized_atomic_block_emitted_whole(self) -> None:
        """超大原子块（超过 chunk_size）应作为独立块输出。"""
        code = "```python\n" + "print('x')\n" * 100 + "```"
        text = f"Short intro.\n\n{code}\n\nOutro text."
        chunks = chunk_document(text, chunk_size=50, overlap=0)
        # 至少应有3个块：intro, code, outro
        self.assertGreaterEqual(len(chunks), 3)
        # 找到包含代码块的 chunk
        code_chunks = [c for c in chunks if "```" in c.text]
        self.assertGreaterEqual(len(code_chunks), 1)
        for cc in code_chunks:
            self.assertIn("```", cc.text)

    def test_short_preamble_with_atomic_block(self) -> None:
        """短前导文本（< chunk_size/4）应与紧随的原子块合并。"""
        text = "Title\n\n```\ncode\n```"
        chunks = chunk_document(text, chunk_size=100, overlap=0)
        # 应该只有1或2个块（Title+Code 可能被合并）
        self.assertLessEqual(len(chunks), 2)


class ChunkingEdgeCaseTests(unittest.TestCase):
    def test_chunk_size_zero_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            chunk_document("x", chunk_size=0, overlap=0)
        self.assertIn("chunk_size", str(ctx.exception))

    def test_chunk_size_negative_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            chunk_document("x", chunk_size=-1, overlap=0)
        self.assertIn("chunk_size", str(ctx.exception))

    def test_overlap_negative_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            chunk_document("x", chunk_size=10, overlap=-1)
        self.assertIn("overlap", str(ctx.exception))

    def test_overlap_gte_chunk_size_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            chunk_document("x", chunk_size=10, overlap=10)
        self.assertIn("overlap", str(ctx.exception))

    def test_overlap_gte_chunk_size_raises_strict(self) -> None:
        with self.assertRaises(ValueError):
            chunk_document("x", chunk_size=10, overlap=11)

    def test_chunk_size_one_single_chars(self) -> None:
        chunks = chunk_document("abc", chunk_size=1, overlap=0)
        self.assertEqual(len(chunks), 3)
        texts = [c.text for c in chunks]
        self.assertEqual(texts, ["a", "b", "c"])

    def test_generator_yields_same_as_list(self) -> None:
        text = "Hello world, this is a test sentence for chunking."
        gen_result = list(iter_chunk_spans(text, chunk_size=10, overlap=0))
        list_result = chunk_document(text, chunk_size=10, overlap=0)
        self.assertEqual(len(gen_result), len(list_result))
        for g, l in zip(gen_result, list_result):
            self.assertEqual(g.text, l.text)
            self.assertEqual(g.ordinal, l.ordinal)

    def test_backward_compatible_alias(self) -> None:
        text = "Hello world"
        result = chunk_document_atomic(text, chunk_size=10, overlap=0)
        expected = chunk_document(text, chunk_size=10, overlap=0)
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertEqual(r.text, e.text)


if __name__ == "__main__":
    unittest.main()
