"""triage 模块单元测试 — 覆盖 templates/kbtool_lib/triage.py 中的纯逻辑函数。"""

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
sys.path.insert(0, str(TEMPLATES_DIR))

from kbtool_lib.triage import (
    _split_query_tokens,
    _merge_dedup,
    _render_search_md,
    _render_files_md,
    _extract_first_token,
    _build_candidate_file_list,
)


class SplitQueryTokensTests(unittest.TestCase):
    """_split_query_tokens: CJK bigram 拆分、英文 token 拆分、混合、空输入。"""

    def test_empty_string(self) -> None:
        result = _split_query_tokens("")
        self.assertEqual(result, [])

    def test_english_tokens(self) -> None:
        result = _split_query_tokens("hello world test")
        self.assertIn("hello", result)
        self.assertIn("world", result)
        self.assertIn("test", result)

    def test_cjk_bigram_splitting(self) -> None:
        """纯 CJK 字符串应按 2-gram 滑动窗口拆分。"""
        result = _split_query_tokens("林黛玉葬花")
        # 2-gram: 林黛, 黛玉, 玉葬, 葬花
        self.assertIn("林黛", result)
        self.assertIn("黛玉", result)
        self.assertIn("玉葬", result)
        self.assertIn("葬花", result)

    def test_cjk_two_chars(self) -> None:
        """两个 CJK 字符应该产生一个 bigram。"""
        result = _split_query_tokens("混凝土")
        self.assertIn("混凝", result)
        self.assertIn("凝土", result)

    def test_mixed_cjk_and_english(self) -> None:
        """中英混合：空格拆分后各自处理。"""
        result = _split_query_tokens("混凝土 strength test")
        # 空格拆分后："混凝土"、"strength"、"test"
        self.assertIn("strength", result)
        self.assertIn("test", result)

    def test_max_token_limit(self) -> None:
        """token 数量上限为 6。"""
        result = _split_query_tokens("a b c d e f g h i j")
        self.assertLessEqual(len(result), 6)

    def test_single_english_char_skipped(self) -> None:
        """单个字符（< 2）在空格拆分阶段会被跳过。"""
        result = _split_query_tokens("a big cat")
        # "a" 长度 < 2, 被跳过
        self.assertNotIn("a", result)
        self.assertIn("big", result)
        self.assertIn("cat", result)

    def test_slash_replaced_by_space(self) -> None:
        result = _split_query_tokens("hello/world")
        self.assertIn("hello", result)
        self.assertIn("world", result)

    def test_dash_replaced_by_space(self) -> None:
        result = _split_query_tokens("pre-stress")
        self.assertIn("pre", result)
        self.assertIn("stress", result)


class MergeDedupTests(unittest.TestCase):
    """_merge_dedup: primary 优先、file:line 去重、limit 限制。"""

    def _make_match(self, file: str, line: int, text: str = "") -> dict:
        return {"file": file, "line_number": line, "line_text": text}

    def test_empty_both(self) -> None:
        result = _merge_dedup(
            {"matches": []},
            {"matches": []},
        )
        self.assertEqual(result["matches"], [])

    def test_primary_priority(self) -> None:
        primary = {"matches": [self._make_match("a.txt", 1, "primary")]}
        secondary = {"matches": [self._make_match("b.txt", 1, "secondary")]}
        result = _merge_dedup(primary, secondary)
        self.assertEqual(len(result["matches"]), 2)
        self.assertEqual(result["matches"][0]["line_text"], "primary")

    def test_dedup_by_file_line(self) -> None:
        """相同 file:line_number 的条目应去重。"""
        primary = {"matches": [self._make_match("a.txt", 10, "p")]}
        secondary = {"matches": [self._make_match("a.txt", 10, "s")]}
        result = _merge_dedup(primary, secondary)
        # secondary 的重复条目应被跳过
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["line_text"], "p")

    def test_limit_zero_means_no_limit(self) -> None:
        matches = [self._make_match(f"f{i}.txt", i) for i in range(10)]
        result = _merge_dedup({"matches": matches}, {"matches": []}, limit=0)
        self.assertEqual(len(result["matches"]), 10)

    def test_limit_enforced(self) -> None:
        matches = [self._make_match(f"f{i}.txt", i) for i in range(10)]
        result = _merge_dedup({"matches": matches}, {"matches": []}, limit=3)
        self.assertEqual(len(result["matches"]), 3)

    def test_secondary_fills_after_primary(self) -> None:
        primary = {"matches": [self._make_match("p.txt", 1)]}
        secondary = {"matches": [self._make_match("s.txt", 1)]}
        result = _merge_dedup(primary, secondary, limit=2)
        self.assertEqual(len(result["matches"]), 2)
        self.assertEqual(result["matches"][0]["file"], "p.txt")
        self.assertEqual(result["matches"][1]["file"], "s.txt")

    def test_preserves_tool_and_cmd_fields(self) -> None:
        primary = {"pattern": "test", "fixed": True, "matches": []}
        result = _merge_dedup(primary, {"matches": []})
        self.assertEqual(result["tool"], "kbtool")
        self.assertEqual(result["cmd"], "search")
        self.assertEqual(result["pattern"], "test")


class RenderSearchMdTests(unittest.TestCase):
    """_render_search_md: 输出格式（有/无匹配）。"""

    def test_with_matches(self) -> None:
        search = {
            "pattern": "test",
            "fixed": True,
            "matches": [
                {"file": "a.txt", "line_number": 10, "line_text": "found here"},
            ],
        }
        md = _render_search_md(search, label="(skipped)")
        self.assertIn("test", md)
        self.assertIn("hits: 1", md)
        self.assertIn("a.txt:10", md)
        self.assertIn("found here", md)

    def test_without_matches(self) -> None:
        search = {
            "pattern": "test",
            "fixed": False,
            "matches": [],
        }
        md = _render_search_md(search, label="(skipped)")
        self.assertIn("hits: 0", md)

    def test_empty_pattern_shows_note(self) -> None:
        search = {"pattern": "", "fixed": True, "matches": []}
        md = _render_search_md(search, label="(skipped: no --pattern)")
        self.assertIn("(skipped: no --pattern)", md)

    def test_nonempty_pattern_hides_note(self) -> None:
        search = {"pattern": "query", "fixed": True, "matches": []}
        md = _render_search_md(search, label="(some label)")
        self.assertNotIn("(some label)", md)

    def test_pattern_regex_included(self) -> None:
        search = {
            "pattern": "test",
            "pattern_regex": "te\\.st",
            "fixed": False,
            "punct_fallback": True,
            "matches": [],
        }
        md = _render_search_md(search, label="")
        self.assertIn("te\\.st", md)
        self.assertIn("punct_fallback: `True`", md)


class RenderFilesMdTests(unittest.TestCase):
    """_render_files_md: 输出格式（有/无文件）。"""

    def test_with_files(self) -> None:
        files = {"pattern": "*.txt", "files": ["a.txt", "b.txt"]}
        md = _render_files_md(files, label="(skipped)")
        self.assertIn("*.txt", md)
        self.assertIn("hits: 2", md)
        self.assertIn("a.txt", md)
        self.assertIn("b.txt", md)

    def test_without_files(self) -> None:
        files = {"pattern": "*.txt", "files": []}
        md = _render_files_md(files, label="(skipped)")
        self.assertIn("hits: 0", md)

    def test_empty_pattern_shows_note(self) -> None:
        files = {"pattern": "", "files": []}
        md = _render_files_md(files, label="(skipped: no --file-pattern)")
        self.assertIn("(skipped: no --file-pattern)", md)

    def test_nonempty_pattern_hides_note(self) -> None:
        files = {"pattern": "*.md", "files": []}
        md = _render_files_md(files, label="(some label)")
        self.assertNotIn("(some label)", md)

    def test_strips_empty_file_entries(self) -> None:
        files = {"pattern": "*", "files": ["a.txt", "", "  ", "b.txt"]}
        md = _render_files_md(files, label="")
        self.assertIn("a.txt", md)
        self.assertIn("b.txt", md)


class ExtractFirstTokenTests(unittest.TestCase):
    """_extract_first_token: 提取第一个有意义的 token。"""

    def test_normal_text(self) -> None:
        result = _extract_first_token("hello world")
        self.assertEqual(result, "hello")

    def test_with_slash(self) -> None:
        result = _extract_first_token("path/to/file")
        self.assertEqual(result, "path")

    def test_with_dash(self) -> None:
        result = _extract_first_token("pre-stress")
        self.assertEqual(result, "pre")

    def test_single_char_fallback(self) -> None:
        result = _extract_first_token("a b c")
        # "a" 长度 < 2, 尝试 "b" 也 < 2, 尝试 "c" 也 < 2
        # 全部 < 2 时取前 4 字符
        self.assertEqual(result, "a b ")

    def test_short_text_fallback(self) -> None:
        result = _extract_first_token("x")
        self.assertEqual(result, "x")

    def test_empty_text(self) -> None:
        result = _extract_first_token("")
        self.assertEqual(result, "")

    def test_cjk_text(self) -> None:
        result = _extract_first_token("混凝土")
        self.assertEqual(result, "混凝土")

    def test_prefers_first_token_over_2char(self) -> None:
        result = _extract_first_token("ab cd")
        self.assertEqual(result, "ab")


class BuildCandidateFileListTests(unittest.TestCase):
    """_build_candidate_file_list: 去重和路径解析。"""

    def test_empty_rendered(self) -> None:
        result = _build_candidate_file_list(Path("/tmp"), [])
        self.assertEqual(result, [])

    def test_dedup_by_ref_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            # 创建文件
            (root / "chunk1.md").write_text("content", encoding="utf-8")
            rendered = [
                {"ref_path": "chunk1.md"},
                {"ref_path": "chunk1.md"},  # 重复
            ]
            result = _build_candidate_file_list(root, rendered)
            self.assertEqual(len(result), 1)

    def test_skips_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rendered = [
                {"ref_path": "nonexistent.md"},
            ]
            result = _build_candidate_file_list(root, rendered)
            self.assertEqual(result, [])

    def test_skips_empty_ref_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            rendered = [
                {"ref_path": ""},
                {"ref_path": "   "},
            ]
            result = _build_candidate_file_list(root, rendered)
            self.assertEqual(result, [])

    def test_resolves_to_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "sub").mkdir()
            (root / "sub" / "chunk.md").write_text("content", encoding="utf-8")
            rendered = [
                {"ref_path": "sub/chunk.md"},
            ]
            result = _build_candidate_file_list(root, rendered)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], root / "sub" / "chunk.md")

    def test_mixed_valid_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "valid.md").write_text("content", encoding="utf-8")
            rendered = [
                {"ref_path": "valid.md"},
                {"ref_path": ""},
                {"ref_path": "missing.md"},
            ]
            result = _build_candidate_file_list(root, rendered)
            self.assertEqual(len(result), 1)
            self.assertTrue(str(result[0]).endswith("valid.md"))


if __name__ == "__main__":
    unittest.main()
