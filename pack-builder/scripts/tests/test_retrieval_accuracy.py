"""Tests for batch-3 retrieval accuracy fixes (C, D, E, N, O).

Generated as part of the quality improvement initiative.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add paths so imports work inside tests/
SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
sys.path.insert(0, str(TEMPLATES_DIR))

from build_skill_lib.tokenizer_core import (
    extract_keywords,
    fts_tokens,
)


class TestQueryStopWordFiltering:
    """C: Query-time stop-word filtering in fts_tokens()."""

    def test_cjk_stop_word_removed(self):
        # "的" is a CJK stop word.
        tokens = fts_tokens("我的书")
        assert "的" not in tokens, f"Stop word '的' should be filtered, got {tokens}"

    def test_english_stop_word_removed(self):
        tokens = fts_tokens("the quick brown fox")
        assert "the" not in tokens, f"Stop word 'the' should be filtered, got {tokens}"

    def test_meaningful_tokens_preserved(self):
        tokens = fts_tokens("中国")
        assert "中国" in tokens

    def test_mixed_text_filtering(self):
        tokens = fts_tokens("the 中国的崛起")
        assert "the" not in tokens
        assert "中国" in tokens
        # Note: 2-grams crossing stop-word boundaries (e.g. "国的") are NOT
        # removed because that would destroy recall for meaningful compounds.
        # Only pure stop-word tokens (e.g. "的" itself) are filtered.


class TestCJKPunctuationNormalization:
    """D: CJK punctuation normalization before tokenization."""

    def test_comma_normalized(self):
        tokens = fts_tokens("中国，美国")
        assert "中国" in tokens
        assert "美国" in tokens

    def test_brackets_normalized(self):
        tokens = fts_tokens("（重要）")
        assert "重要" in tokens

    def test_various_punctuation(self):
        # 「」『』【】should be normalized to [] or removed
        text = "「北京」『上海』【广州】"
        tokens = fts_tokens(text)
        assert "北京" in tokens
        assert "上海" in tokens
        assert "广州" in tokens


class TestKeywordMinFreq:
    """O: extract_keywords uses min_freq=1 for chunks."""

    def test_single_occurrence_keyword_with_min_freq_1(self):
        text = "钢筋混凝土结构设计规范"
        kws = extract_keywords(text, top_k=8, min_freq=1)
        # With min_freq=1, even single-occurrence substrings should be candidates.
        assert len(kws) > 0

    def test_min_freq_2_excludes_single_occurrence(self):
        text = "独一的词"
        kws = extract_keywords(text, top_k=8, min_freq=2)
        # "独一的" appears only once; with min_freq=2 it should be excluded.
        assert "独一的" not in kws


class TestTokenizerConsistency:
    """Verify build-time and runtime tokenizer_core.py are identical."""

    def test_build_and_runtime_tokenizers_match(self):

        build_path = SCRIPTS_DIR / "build_skill_lib" / "tokenizer_core.py"
        runtime_path = TEMPLATES_DIR / "kbtool_lib" / "tokenizer_core.py"
        build_src = build_path.read_text(encoding="utf-8")
        runtime_src = runtime_path.read_text(encoding="utf-8")
        assert build_src == runtime_src, "tokenizer_core.py must be identical between build-time and runtime"
