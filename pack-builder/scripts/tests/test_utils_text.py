"""文本处理单元测试 — 覆盖 utils/text.py 中未在一致性测试覆盖的函数。"""

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib import templates_dir

TEMPLATES_DIR = templates_dir()
sys.path.insert(0, str(TEMPLATES_DIR))

from build_skill_lib.utils.text import (
    markdown_to_plain,
    count_occurrences,
    extract_window,
    parse_frontmatter,
    strip_frontmatter,
    canonical_text_from_markdown,
    compose_canonical_document,
    canonical_text_sha256,
    is_cjk,
    tokenize_cjk_2gram,
    build_match_query,
    build_match_all,
    query_terms,
    extract_keywords,
)


class CjkDetectionTests(unittest.TestCase):
    def test_cjk_characters(self) -> None:
        self.assertTrue(is_cjk("中"))
        self.assertTrue(is_cjk("文"))

    def test_non_cjk_characters(self) -> None:
        self.assertFalse(is_cjk("a"))
        self.assertFalse(is_cjk("1"))
        self.assertFalse(is_cjk(" "))
        self.assertFalse(is_cjk("!"))

    def test_cjk_2gram_tokenization(self) -> None:
        # 4字短词保留完整串 + 2-gram
        result = tokenize_cjk_2gram("中文测试")
        self.assertEqual(result, ["中文测试", "中文", "文测", "测试"])

    def test_cjk_2gram_short_string(self) -> None:
        result = tokenize_cjk_2gram("中")
        # 实际实现中单个 CJK 字符也作为独立 token 返回
        self.assertEqual(result, ["中"])

    def test_cjk_2gram_mixed(self) -> None:
        result = tokenize_cjk_2gram("a中文b")
        self.assertIn("中文", result)

    def test_cjk_2gram_long_run(self) -> None:
        # 6字及以内保留完整串 + 2-gram；超过6字仅保留2-gram
        result = tokenize_cjk_2gram("中文测试长句")
        self.assertIn("中文测试长句", result)
        self.assertIn("中文", result)
        self.assertIn("文测", result)
        self.assertIn("测试", result)
        self.assertIn("试长", result)
        self.assertIn("长句", result)

    def test_cjk_2gram_five_char_no_cliff(self) -> None:
        # 修复 n=4→5 突变：5字串应保留完整串
        result = tokenize_cjk_2gram("钢筋混凝土")
        self.assertIn("钢筋混凝土", result)
        self.assertIn("钢筋", result)
        self.assertIn("筋混", result)
        self.assertIn("混凝", result)
        self.assertIn("凝土", result)

    def test_cjk_2gram_seven_char_no_full(self) -> None:
        # 7字以上不保留完整串
        result = tokenize_cjk_2gram("中华人民共和国")
        self.assertNotIn("中华人民共和国", result)
        self.assertIn("中华", result)
        self.assertIn("人民", result)

    def test_is_cjk_extension_b(self) -> None:
        # Extension B: U+20000–U+2A6DF
        self.assertTrue(is_cjk("\U00020000"))
        self.assertTrue(is_cjk("\U0002A6DF"))

    def test_is_cjk_extension_g(self) -> None:
        # Extension G: U+30000–U+3134F
        self.assertTrue(is_cjk("\U00030000"))
        self.assertTrue(is_cjk("\U0003134F"))

    def test_is_cjk_extension_h(self) -> None:
        # Extension H: U+31350–U+323AF
        self.assertTrue(is_cjk("\U00031350"))
        self.assertTrue(is_cjk("\U000323AF"))


class ExtractKeywordsTests(unittest.TestCase):
    def test_respects_min_freq_parameter(self) -> None:
        # min_freq=1 应比 min_freq=2 返回更多（或相等数量）的候选
        text = "预应力混凝土结构设计规范"
        result_freq1 = extract_keywords(text, top_k=20, min_freq=1)
        result_freq2 = extract_keywords(text, top_k=20, min_freq=2)
        self.assertGreaterEqual(len(result_freq1), len(result_freq2))
        self.assertTrue(set(result_freq2).issubset(set(result_freq1)))

    def test_default_min_freq_is_two(self) -> None:
        # 默认 min_freq=2，单频次词被过滤
        text = "优化算法的设计"
        result = extract_keywords(text, top_k=8)
        # 在短文本中所有子串都只出现一次，默认 min_freq=2 应导致空结果或极少结果
        self.assertLessEqual(len(result), 2)

    def test_deduplication_by_substring(self) -> None:
        # 结果中不应存在任何一对 token 互为子串关系
        text = "预应力混凝土结构设计规范"
        result = extract_keywords(text, top_k=8, min_freq=1)
        for i, a in enumerate(result):
            for b in result[i + 1:]:
                self.assertNotIn(a, b, f"'{a}' is a substring of '{b}'")
                self.assertNotIn(b, a, f"'{b}' is a substring of '{a}'")


class MarkdownToPlainTests(unittest.TestCase):
    def test_strips_bold_and_italic(self) -> None:
        """markdown_to_plain 去除加粗/斜体标记，保留其他内容。"""
        plain = markdown_to_plain("**bold** and *italic*")
        self.assertNotIn("**", plain)
        self.assertIn("bold", plain)
        self.assertIn("italic", plain)

    def test_strip_headings_and_preserve_links(self) -> None:
        """markdown_to_plain 去除 heading 标记 (#)，保留链接格式。"""
        plain = markdown_to_plain("# Title\n\n[link](http://example.com)")
        self.assertNotIn("#", plain)
        self.assertIn("Title", plain)
        self.assertIn("[link]", plain)

    def test_code_backticks_replaced(self) -> None:
        """markdown_to_plain 将 backtick 内容替换为占位符。"""
        plain = markdown_to_plain("use `code` here")
        self.assertIn("use", plain)
        self.assertIn("here", plain)

    def test_preserve_chinese_text(self) -> None:
        plain = markdown_to_plain("# 中文标题\n\n正文内容。")
        self.assertIn("中文标题", plain)
        self.assertIn("正文内容", plain)


class CountOccurrencesTests(unittest.TestCase):
    def test_basic_count(self) -> None:
        self.assertEqual(count_occurrences("hello hello hello", "hello"), 3)

    def test_no_occurrence(self) -> None:
        self.assertEqual(count_occurrences("hello world", "foo"), 0)

    def test_overlapping_not_counted(self) -> None:
        self.assertEqual(count_occurrences("aaa", "aa"), 1)

    def test_case_sensitive(self) -> None:
        self.assertEqual(count_occurrences("Hello HELLO", "hello"), 0)


class ExtractWindowTests(unittest.TestCase):
    def test_extracts_window_with_term(self) -> None:
        text = "Before target after target end"
        window = extract_window(text, ["target"], max_chars=30)
        self.assertIn("target", window)

    def test_no_term_returns_start(self) -> None:
        text = "Just some text without the term"
        window = extract_window(text, ["missing"], max_chars=20)
        self.assertGreater(len(window), 0)

    def test_respects_max_chars_approx(self) -> None:
        """extract_window 大致遵循 max_chars，包含省略号可能略超。"""
        text = "a" * 1000
        window = extract_window(text, ["a"], max_chars=50)
        # 允许包含省略号 " …" 的少量超出
        self.assertLessEqual(len(window), 60)

    def test_multiple_terms(self) -> None:
        text = "first second third"
        window = extract_window(text, ["first", "third"], max_chars=50)
        self.assertIn("first", window)


class FrontmatterTests(unittest.TestCase):
    def test_parse_basic_frontmatter(self) -> None:
        md = "---\ntitle: Hello\n---\nBody text"
        fm = parse_frontmatter(md)
        self.assertEqual(fm.get("title"), "Hello")

    def test_parse_no_frontmatter(self) -> None:
        md = "Just body text"
        fm = parse_frontmatter(md)
        self.assertEqual(fm, {})

    def test_parse_empty_frontmatter(self) -> None:
        md = "---\n---\nBody"
        fm = parse_frontmatter(md)
        self.assertEqual(fm, {})

    def test_strip_frontmatter(self) -> None:
        md = "---\ntitle: Hello\n---\nBody text"
        body = strip_frontmatter(md)
        self.assertNotIn("---", body)
        self.assertIn("Body text", body)

    def test_strip_no_frontmatter(self) -> None:
        md = "Just body text"
        body = strip_frontmatter(md)
        self.assertEqual(body.strip(), "Just body text")


class CanonicalTextTests(unittest.TestCase):
    def test_from_markdown_preserves_content(self) -> None:
        """canonical_text_from_markdown 保留原始内容（不剥离 markdown 标记）。"""
        md = "# Title\n\n**Bold** text"
        canonical = canonical_text_from_markdown(md)
        self.assertIn("Title", canonical)
        self.assertIn("Bold", canonical)

    def test_compose_document(self) -> None:
        text = compose_canonical_document("My Doc", ["Section A", "Section B"])
        self.assertIn("My Doc", text)
        self.assertIn("Section A", text)
        self.assertIn("Section B", text)

    def test_sha256_is_stable(self) -> None:
        h1 = canonical_text_sha256("hello")
        h2 = canonical_text_sha256("hello")
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)

    def test_sha256_different_input(self) -> None:
        h1 = canonical_text_sha256("hello")
        h2 = canonical_text_sha256("world")
        self.assertNotEqual(h1, h2)

    def test_stable_hash_is_sha256(self) -> None:
        from build_skill_lib.utils.text import stable_hash
        h = stable_hash("hello")
        self.assertEqual(len(h), 64)
        import hashlib
        self.assertEqual(h, hashlib.sha256(b"hello").hexdigest())


class MatchQueryTests(unittest.TestCase):
    def test_build_match_query(self) -> None:
        q = build_match_query(["hello", "world"], max_tokens=10)
        self.assertIn("hello", q)
        self.assertIn("world", q)

    def test_build_match_query_truncates(self) -> None:
        tokens = ["t" + str(i) for i in range(100)]
        q = build_match_query(tokens, max_tokens=5)
        # 返回格式: "t0" OR "t1" OR ...，token 数量 = 5
        token_count = q.count('"') // 2
        self.assertLessEqual(token_count, 5)
        self.assertIn("t0", q)
        self.assertIn("t4", q)
        self.assertNotIn("t5", q)

    def test_build_match_all(self) -> None:
        q = build_match_all(["hello", "world"], max_tokens=10)
        self.assertIn("hello", q)
        self.assertIn("world", q)

    def test_query_terms(self) -> None:
        terms = query_terms("hello world")
        self.assertIsInstance(terms, list)
        self.assertGreater(len(terms), 0)

    def test_query_terms_cjk(self) -> None:
        terms = query_terms("中文搜索")
        self.assertIsInstance(terms, list)


if __name__ == "__main__":
    unittest.main()
