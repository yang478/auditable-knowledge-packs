"""文件系统工具单元测试 — 覆盖 utils/fs.py 核心逻辑。"""

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.utils.fs import (
    slugify_ascii,
    derive_doc_id,
    derive_doc_id_by_content,
    derive_doc_title,
    normalize_title_whitespace,
    read_text,
    safe_skill_name,
    write_text,
)


class SlugifyAsciiTests(unittest.TestCase):
    def test_basic_lowercase(self) -> None:
        self.assertEqual(slugify_ascii("Hello World"), "hello-world")

    def test_version_number(self) -> None:
        self.assertEqual(slugify_ascii("v1.2.3"), "v1-2-3")

    def test_number_letter_boundary(self) -> None:
        self.assertEqual(slugify_ascii("api2go"), "api-2-go")

    def test_cjk_removed(self) -> None:
        self.assertEqual(slugify_ascii("中文标题"), "")

    def test_multiple_special_chars_collapsed(self) -> None:
        self.assertEqual(slugify_ascii("a!!!b"), "a-b")

    def test_leading_trailing_hyphens_trimmed(self) -> None:
        self.assertEqual(slugify_ascii("---hello---"), "hello")

    def test_empty_result(self) -> None:
        self.assertEqual(slugify_ascii("!!!"), "")


class DeriveDocIdTests(unittest.TestCase):
    def test_basic(self) -> None:
        used: set[str] = set()
        doc_id = derive_doc_id(Path("my-doc.md"), used)
        self.assertEqual(doc_id, "my-doc")
        self.assertIn(doc_id, used)

    def test_duplicate_appends_hash(self) -> None:
        used: set[str] = {"my-doc"}
        doc_id = derive_doc_id(Path("my-doc.md"), used)
        # 首次重复时附加 SHA1 hash 前缀，不是数字
        self.assertTrue(doc_id.startswith("my-doc-"))
        self.assertNotEqual(doc_id, "my-doc")
        self.assertGreater(len(doc_id), len("my-doc"))

    def test_duplicate_appends_counter_after_hash(self) -> None:
        used: set[str] = {"my-doc"}
        doc_id1 = derive_doc_id(Path("my-doc.md"), used)
        # hash 版本也已占用，第二次用数字
        doc_id2 = derive_doc_id(Path("my-doc.md"), used)
        self.assertTrue(doc_id2.startswith("my-doc-"))
        self.assertNotEqual(doc_id1, doc_id2)

    def test_long_name_truncated(self) -> None:
        long_name = "a" * 60
        used: set[str] = set()
        doc_id = derive_doc_id(Path(f"{long_name}.md"), used)
        self.assertLessEqual(len(doc_id), 48)

    def test_empty_stem_falls_back(self) -> None:
        # Python 3.9+ 中 Path(".md").stem == ".md"，slugify 后变为 "md"
        used: set[str] = set()
        doc_id = derive_doc_id(Path(".md"), used)
        self.assertIn(doc_id, {"doc", "md"})


class DeriveDocIdByContentTests(unittest.TestCase):
    def test_stable_output(self) -> None:
        result1 = derive_doc_id_by_content("My Title", "sample content")
        result2 = derive_doc_id_by_content("My Title", "sample content")
        self.assertEqual(result1, result2)

    def test_different_content_different_id(self) -> None:
        result1 = derive_doc_id_by_content("Title", "content A")
        result2 = derive_doc_id_by_content("Title", "content B")
        self.assertNotEqual(result1, result2)

    def test_includes_slug_and_hash(self) -> None:
        doc_id = derive_doc_id_by_content("My Title", "x")
        self.assertIn("-", doc_id)
        self.assertLessEqual(len(doc_id), 50)


class DeriveDocTitleTests(unittest.TestCase):
    def test_from_heading(self) -> None:
        title = derive_doc_title(Path("file.md"), "# My Title\nbody")
        self.assertEqual(title, "My Title")

    def test_from_heading_strips_hash(self) -> None:
        title = derive_doc_title(Path("file.md"), "## Section\nbody")
        self.assertEqual(title, "Section")

    def test_fallback_to_stem(self) -> None:
        title = derive_doc_title(Path("fallback.md"), "no heading here")
        self.assertEqual(title, "fallback")

    def test_empty_content_fallback(self) -> None:
        title = derive_doc_title(Path("name.md"), "")
        self.assertEqual(title, "name")


class NormalizeTitleWhitespaceTests(unittest.TestCase):
    def test_collapses_multiple_spaces(self) -> None:
        self.assertEqual(normalize_title_whitespace("a   b   c"), "a b c")

    def test_strips_leading_trailing(self) -> None:
        self.assertEqual(normalize_title_whitespace("  hello  "), "hello")

    def test_removes_cjk_inter_space(self) -> None:
        self.assertEqual(normalize_title_whitespace("中 文"), "中文")

    def test_preserves_cjk_english_space(self) -> None:
        self.assertEqual(normalize_title_whitespace("中 en"), "中 en")

    def test_empty_returns_empty(self) -> None:
        self.assertEqual(normalize_title_whitespace(""), "")


class ReadTextTests(unittest.TestCase):
    def test_utf8(self) -> None:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write("Hello UTF-8 中文".encode("utf-8"))
            path = Path(f.name)
        try:
            self.assertEqual(read_text(path), "Hello UTF-8 中文")
        finally:
            path.unlink()

    def test_utf8_bom_preserved(self) -> None:
        """read_text 先尝试 utf-8（不自动去 BOM），BOM 字符保留在结果中。"""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write("\ufeffHello".encode("utf-8"))
            path = Path(f.name)
        try:
            text = read_text(path)
            self.assertIn("Hello", text)
            # BOM 作为零宽无断空格保留（utf-8 解码不报错）
            self.assertTrue(text.startswith("\ufeff"))
        finally:
            path.unlink()

    def test_gbk_fallback(self) -> None:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write("中文GBK".encode("gb18030"))
            path = Path(f.name)
        try:
            self.assertEqual(read_text(path), "中文GBK")
        finally:
            path.unlink()


class SafeSkillNameTests(unittest.TestCase):
    def test_valid_name(self) -> None:
        self.assertEqual(safe_skill_name("my-books"), "my-books")

    def test_valid_single_char(self) -> None:
        self.assertEqual(safe_skill_name("a"), "a")

    def test_valid_digits(self) -> None:
        self.assertEqual(safe_skill_name("doc-123"), "doc-123")

    def test_leading_hyphen_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("-invalid")

    def test_trailing_hyphen_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("invalid-")

    def test_consecutive_hyphens_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("a--b")

    def test_uppercase_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("MySkill")

    def test_underscore_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("my_skill")

    def test_empty_rejected(self) -> None:
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("")

    def test_too_long_rejected(self) -> None:
        # 正则 [a-z0-9][a-z0-9-]{0,62}[a-z0-9]? 最多允许 64 字符
        from build_skill_lib.utils.fs import ConfigError
        with self.assertRaises(ConfigError):
            safe_skill_name("a" * 65)


class WriteTextTests(unittest.TestCase):
    def test_creates_parent_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a" / "b" / "file.txt"
            write_text(path, "hello")
            self.assertTrue(path.exists())
            self.assertEqual(path.read_text(encoding="utf-8"), "hello")

    def test_overwrites_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "file.txt"
            path.write_text("old", encoding="utf-8")
            write_text(path, "new")
            self.assertEqual(path.read_text(encoding="utf-8"), "new")


if __name__ == "__main__":
    unittest.main()
