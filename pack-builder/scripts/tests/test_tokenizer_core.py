"""tokenizer_core.py 全面单元测试 — 零依赖纯函数，参数化覆盖所有场景。"""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

import pytest
from build_skill_lib.tokenizer_core import (
    build_match_all,
    build_match_expression,
    build_match_query,
    build_punctuation_tolerant_regex,
    count_occurrences,
    derive_source_version,
    extract_keywords,
    extract_window,
    fts_tokens,
    is_cjk,
    markdown_to_plain,
    normalize_alias_text,
    parse_frontmatter,
    query_terms,
    stable_hash,
    stable_hash_sha1,
    strip_frontmatter,
    tokenize_cjk_2gram,
)


# ---------------------------------------------------------------------------
# is_cjk
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "ch,expected",
    [
        # 基本 CJK Unified Ideographs (U+4E00–U+9FFF)
        ("中", True),
        ("文", True),
        ("字", True),
        # ASCII 字符
        ("a", False),
        ("A", False),
        ("1", False),
        (" ", False),
        ("!", False),
        # Extension A (U+3400–U+4DBF)
        ("㐀", True),
        ("㐁", True),
        # Compatibility Ideographs (U+F900–U+FAFF)
        ("豈", True),
        ("更", True),
        # Extension B (U+20000–U+2A6DF)
        ("𠀀", True),
        # 非 CJK 标点 / 符号
        ("，", False),
        ("。", False),
    ],
)
def test_is_cjk(ch: str, expected: bool) -> None:
    assert is_cjk(ch) is expected


# ---------------------------------------------------------------------------
# tokenize_cjk_2gram
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,expected",
    [
        # 2–6 字短词：保留完整串 + 2-gram
        ("中文", ["中文", "中文"]),
        ("红楼梦", ["红楼梦", "红楼", "楼梦"]),
        ("钢筋混凝土", ["钢筋混凝土", "钢筋", "筋混", "混凝", "凝土"]),
        # 7 字以上长词：仅保留 2-gram
        (
            "一二三四五六七八",
            ["一二", "二三", "三四", "四五", "五六", "六七", "七八"],
        ),
        # 单字 CJK：保留为单 token
        ("中", ["中"]),
        # CJK 与 ASCII 混排时正确拆分 run
        ("中a文", ["中", "文"]),
        ("hello中文world", ["中文", "中文"]),
        ("ab中cd文ef", ["中", "文"]),
        # 空串
        ("", []),
        # 纯 ASCII
        ("hello", []),
        # 多个 CJK run
        (
            "中国 usa 美国",
            ["中国", "中国", "美国", "美国"],
        ),
        # 4 字边界
        ("一二三四", ["一二三四", "一二", "二三", "三四"]),
        # 6 字边界
        (
            "一二三四五六",
            ["一二三四五六", "一二", "二三", "三四", "四五", "五六"],
        ),
        # 7 字仅 2-gram
        (
            "一二三四五六七",
            ["一二", "二三", "三四", "四五", "五六", "六七"],
        ),
    ],
)
def test_tokenize_cjk_2gram(text: str, expected: list[str]) -> None:
    assert tokenize_cjk_2gram(text) == expected


# ---------------------------------------------------------------------------
# fts_tokens
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,expected",
    [
        # 混合 CJK 和 ASCII 文本
        (
            "hello world 中文测试",
            ["中文测试", "中文", "文测", "测试", "hello", "world"],
        ),
        # CJK 标点归一化：逗号将 CJK run 拆分
        (
            "你好，世界",
            ["你好", "你好", "世界", "世界"],
        ),
        # 停用词过滤（的/the 等被移除）
        (
            "the 的 test",
            ["test"],
        ),
        # 空文本返回空列表
        ("", []),
        # 纯停用词
        (
            "the a an 的 了 在",
            [],
        ),
        # 引号类归一化
        (
            '「test」',
            ['test'],  # 「」→ [] → 非 CJK，仅 test 保留
        ),
        # 全角冒号分号：将 CJK run 拆分为多个 2 字 run
        (
            "结论：正确；错误",
            ["结论", "结论", "正确", "正确", "错误", "错误"],
        ),
    ],
)
def test_fts_tokens(text: str, expected: list[str]) -> None:
    assert fts_tokens(text) == expected


# ---------------------------------------------------------------------------
# build_match_query
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "tokens,max_tokens,expected",
    [
        # 基本 OR 查询构建
        (["hello", "world"], 64, '"hello" OR "world"'),
        # 去重
        (["hello", "hello", "world"], 64, '"hello" OR "world"'),
        # 去空
        (["hello", "", "world"], 64, '"hello" OR "world"'),
        # 引号转义：FTS5 中字面量双引号用 "" 表示
        (['he"llo', "world"], 64, '"he""llo" OR "world"'),
        # 超过 max_tokens 截断
        (["a", "b", "c"], 2, '"a" OR "b"'),
        # 空列表
        ([], 64, ""),
        # 全部空串
        (["", ""], 64, ""),
    ],
)
def test_build_match_query(
    tokens: list[str], max_tokens: int, expected: str, capsys: pytest.CaptureFixture[str]
) -> None:
    result = build_match_query(tokens, max_tokens=max_tokens)
    assert result == expected
    if len(tokens) > max_tokens and tokens and any(t for t in tokens):
        captured = capsys.readouterr()
        assert "[WARN] FTS query truncated" in captured.err


# ---------------------------------------------------------------------------
# build_match_all
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "tokens,max_tokens,expected",
    [
        # 基本 AND 查询构建
        (["hello", "world"], 16, '"hello" AND "world"'),
        # 去重
        (["hello", "hello", "world"], 16, '"hello" AND "world"'),
        # 去空
        (["hello", "", "world"], 16, '"hello" AND "world"'),
        # 超过 max_tokens 截断（无 stderr 输出）
        (["a", "b", "c"], 2, '"a" AND "b"'),
        # 空列表
        ([], 16, ""),
    ],
)
def test_build_match_all(tokens: list[str], max_tokens: int, expected: str) -> None:
    assert build_match_all(tokens, max_tokens=max_tokens) == expected


# ---------------------------------------------------------------------------
# build_match_expression
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw_query,query_mode,must_terms,max_tokens,expected",
    [
        # and 模式
        (
            "hello world",
            "and",
            [],
            64,
            '"hello" AND "world"',
        ),
        # or 模式
        (
            "hello world",
            "or",
            [],
            64,
            '"hello" OR "world"',
        ),
        # must_terms 生成独立子句 (and 模式)
        (
            "test",
            "and",
            ["must1"],
            64,
            '"must1" AND "test"',
        ),
        # must_terms 多个 + and
        (
            "query",
            "and",
            ["m1", "m2"],
            64,
            '"m1" AND "m2" AND "query"',
        ),
        # must_terms + or 模式
        (
            "query",
            "or",
            ["must1"],
            64,
            '"must1" AND "query"',
        ),
        # 空 must_terms + 空 query
        ("", "or", [], 64, ""),
        # and 模式下 query 被拆词
        (
            "foo bar",
            "and",
            [],
            64,
            '"foo" AND "bar"',
        ),
    ],
)
def test_build_match_expression(
    raw_query: str,
    query_mode: str,
    must_terms: list[str],
    max_tokens: int,
    expected: str,
) -> None:
    result = build_match_expression(
        raw_query,
        query_mode=query_mode,
        must_terms=must_terms,
        max_tokens=max_tokens,
    )
    assert result == expected


# ---------------------------------------------------------------------------
# query_terms
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw_query,expected",
    [
        ("hello world", ["hello", "world"]),
        ("hello  world", ["hello", "world"]),
        # 制表符
        ("hello\tworld", ["hello", "world"]),
        # 换行
        ("hello\nworld", ["hello", "world"]),
        # 多空格混合
        ("a   b\t\t c\n d", ["a", "b", "c", "d"]),
        # 空串
        ("", []),
        # 仅空白
        ("   \t\n  ", []),
        # 单字
        ("hello", ["hello"]),
    ],
)
def test_query_terms(raw_query: str, expected: list[str]) -> None:
    assert query_terms(raw_query) == expected


# ---------------------------------------------------------------------------
# extract_keywords
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,top_k,min_freq,expected",
    [
        # 混合 CJK/ASCII 文本：hello/world 各只出现一次，min_freq=2 时被过滤
        (
            "hello world 中文测试 中文测试",
            8,
            2,
            ["中文测试"],
        ),
        # min_freq=1 时单出现也会保留
        (
            "hello 中文测试",
            8,
            1,
            ["hello", "中文测试"],  # 按 (-count, -len, token) 排序，hello 长度 5 > 4
        ),
        # min_freq=2 时单出现被过滤
        (
            "hello 中文测试",
            8,
            2,
            [],
        ),
        # 去停用词
        (
            "的 了 在 hello hello",
            8,
            2,
            ["hello"],
        ),
        # 去重子串："钢筋"出现 4 次（含子串计数），排在"钢筋混凝土"(2 次)之前，
        # 两者互不包含，故均保留。
        (
            "钢筋混凝土 钢筋混凝土 钢筋 钢筋",
            8,
            2,
            ["钢筋", "钢筋混凝土"],
        ),
        # 空文本
        ("", 8, 2, []),
    ],
)
def test_extract_keywords(
    text: str, top_k: int, min_freq: int, expected: list[str]
) -> None:
    result = extract_keywords(text, top_k=top_k, min_freq=min_freq)
    assert result == expected


# ---------------------------------------------------------------------------
# count_occurrences
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "haystack,needle,expected",
    [
        ("hello world", "o", 2),
        # 空串
        ("", "test", 0),
        ("test", "", 0),
        ("", "", 0),
        # 无命中
        ("hello", "z", 0),
        # 重叠
        ("aaa", "aa", 1),
    ],
)
def test_count_occurrences(haystack: str, needle: str, expected: int) -> None:
    assert count_occurrences(haystack, needle) == expected


# ---------------------------------------------------------------------------
# extract_window
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,terms,max_chars,expected",
    [
        # 命中窗口前后扩展（命中在靠前位置，start=0，无前省略号）
        (
            "0123456789" * 10,
            ["345"],
            20,
            "01234567890123456789 …",
        ),
        # 多个 terms 取最先命中（"def" 在 index 4，窗口向后扩展）
        (
            "abc def ghi",
            ["def", "ghi"],
            10,
            "… bc def ghi",
        ),
        # 无命中时返回前 max_chars
        (
            "abcdefghij",
            ["xyz"],
            5,
            "abcde",
        ),
        # max_chars <= 0
        (
            "abc",
            ["a"],
            0,
            "",
        ),
        # 命中在开头，无前省略号
        (
            "hello world",
            ["hello"],
            20,
            "hello world",
        ),
        # 命中在末尾，无后省略号
        (
            "hello world",
            ["world"],
            20,
            "hello world",
        ),
    ],
)
def test_extract_window(
    text: str, terms: list[str], max_chars: int, expected: str
) -> None:
    assert extract_window(text, terms, max_chars) == expected


# ---------------------------------------------------------------------------
# markdown_to_plain
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "md,expected",
    [
        # 去除 heading 标记
        ("# Hello", "Hello\n"),
        ("## World", "World\n"),
        ("###### Deep", "Deep\n"),
        # 去除代码反引号
        ("`code`", "code\n"),
        # 去除链接，保留描述
        ("[desc](url)", "[desc]\n"),
        # 去除加粗斜体
        ("**bold**", "bold\n"),
        ("*italic*", "italic\n"),
        ("__bold__", "bold\n"),
        # 多行混合
        (
            "# Title\nSome **bold** text and `code`.\n",
            "Title\nSome bold text and code.\n",
        ),
        # 空行保留
        ("line1\n\nline2", "line1\n\nline2\n"),
    ],
)
def test_markdown_to_plain(md: str, expected: str) -> None:
    assert markdown_to_plain(md) == expected


# ---------------------------------------------------------------------------
# parse_frontmatter / strip_frontmatter
# ---------------------------------------------------------------------------
FM_SAMPLE = """---
title: "My Title"
version: 1
---
Body content
"""

FM_SIMPLE = "---\ntitle: Test\n---"


@pytest.mark.parametrize(
    "md,expected",
    [
        (FM_SAMPLE, {"title": "My Title", "version": "1"}),
        # 无前导 ---
        ("no frontmatter", {}),
        # 少于 3 个 ---
        ("---\nonly", {}),
        # 简单合法 frontmatter
        (FM_SIMPLE, {"title": "Test"}),
    ],
)
def test_parse_frontmatter(md: str, expected: dict[str, str]) -> None:
    assert parse_frontmatter(md) == expected


@pytest.mark.parametrize(
    "md,expected",
    [
        (FM_SAMPLE, "Body content\n"),
        # 无前导 ---
        ("no frontmatter", "no frontmatter"),
        # 简单合法 frontmatter，body 为空
        (FM_SIMPLE, ""),
    ],
)
def test_strip_frontmatter(md: str, expected: str) -> None:
    assert strip_frontmatter(md) == expected


# ---------------------------------------------------------------------------
# stable_hash / stable_hash_sha1
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "hello",
        "中文",
        "",  # 空串
        "a" * 10000,
    ],
)
def test_stable_hash_determinism(text: str) -> None:
    h1 = stable_hash(text)
    h2 = stable_hash(text)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex

    s1 = stable_hash_sha1(text)
    s2 = stable_hash_sha1(text)
    assert s1 == s2
    assert len(s1) == 40  # SHA-1 hex


def test_stable_hash_different_inputs() -> None:
    assert stable_hash("a") != stable_hash("b")
    assert stable_hash_sha1("a") != stable_hash_sha1("b")


# ---------------------------------------------------------------------------
# derive_source_version
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name,title,expected",
    [
        # 空格分隔的 V2
        ("doc V2", "Title", "v2"),
        # title 中包含版本
        ("doc", "Spec V3", "v3"),
        ("doc", "Spec v10", "v10"),
        # 无版本号
        ("abc", "def", "current"),
        # 大小写不敏感
        ("DOC V5", "Title", "v5"),
        # 下划线连接不会触发 word boundary
        ("doc_v2", "Title", "current"),
    ],
)
def test_derive_source_version(name: str, title: str, expected: str) -> None:
    assert derive_source_version(name, title) == expected


# ---------------------------------------------------------------------------
# normalize_alias_text
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text,expected",
    [
        # 基本过滤：保留 CJK 和 ASCII 字母数字
        ("Hello World 中文", "helloworld中文"),
        # 全角字符 NFKC 归一化
        ("ＡＢＣ", "abc"),
        # 去除标点
        ("中，文；test！", "中文test"),
        # 空格去除
        ("  hello  world  ", "helloworld"),
        # 空串
        ("", ""),
        # 纯标点
        ("！？。，", ""),
    ],
)
def test_normalize_alias_text(text: str, expected: str) -> None:
    assert normalize_alias_text(text) == expected


# ---------------------------------------------------------------------------
# build_punctuation_tolerant_regex
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "literal,expected",
    [
        # 基本无特殊字符
        ("hello", "hello"),
        # 逗号容忍
        ("你，好", "你[，,]好"),
        # 句号容忍
        ("你好。", "你好[。.]"),
        # 冒号容忍
        ("结论：", "结论[：:]"),
        # 分号容忍
        ("a；b", "a[；;]b"),
        # 问号容忍（同时容忍全角问号）
        ("什么？", "什么[？?]"),
        # 感叹号容忍（同时容忍全角感叹号）
        ("哇！", "哇[！!]"),
        # 句点属于标点等价组，生成字符类而非转义（按 sorted(group) 顺序）
        ("a.b", "a[.。]b"),  # 实际顺序取决于sorted(group)
        # regex 元字符转义
        ("a+b", r"a\+b"),
        ("a*b", r"a\*b"),
        # 问号属于标点等价组
        ("a?b", "a[?？]b"),
        ("a[b", r"a\[b"),
        # 空串
        ("", ""),
        # None-like
        (None, ""),
    ],
)
def test_build_punctuation_tolerant_regex(literal: str | None, expected: str) -> None:
    assert build_punctuation_tolerant_regex(literal) == expected
