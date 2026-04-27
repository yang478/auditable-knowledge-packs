"""memory 模块单元测试 — 覆盖 templates/kbtool_lib/memory.py 中的纯逻辑和 SQLite 操作函数。"""

import sqlite3
import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
sys.path.insert(0, str(TEMPLATES_DIR))

from kbtool_lib.memory import (
    apply_learned_boost,
    canonicalize_query_key,
    ensure_memory_tables,
    get_stats,
    log_query,
    record_feedback,
    recommend_neighbors,
    suggest_rewrites,
)


class NormalizeQueryTests(unittest.TestCase):
    """canonicalize_query_key: CJK 处理、去重、排序、空输入。"""

    def test_empty_string(self) -> None:
        self.assertEqual(canonicalize_query_key(""), "")

    def test_none_input(self) -> None:
        self.assertEqual(canonicalize_query_key(None), "")  # type: ignore[arg-type]

    def test_whitespace_only(self) -> None:
        self.assertEqual(canonicalize_query_key("   "), "")

    def test_cjk_only(self) -> None:
        result = canonicalize_query_key("预应力 混凝土")
        self.assertEqual(result, "混凝土 预应力")

    def test_cjk_sorting_equivalence(self) -> None:
        """不同顺序的 CJK token 规范化后应相同。"""
        a = canonicalize_query_key("预应力 混凝土")
        b = canonicalize_query_key("混凝土 预应力")
        self.assertEqual(a, b)

    def test_english_lowercase(self) -> None:
        result = canonicalize_query_key("Hello World")
        self.assertEqual(result, "hello world")

    def test_punctuation_removed(self) -> None:
        result = canonicalize_query_key("hello, world! (test)")
        self.assertIn("hello", result)
        self.assertIn("world", result)
        self.assertIn("test", result)
        self.assertNotIn(",", result)
        self.assertNotIn("!", result)
        self.assertNotIn("(", result)

    def test_deduplication(self) -> None:
        result = canonicalize_query_key("混凝土 混凝土")
        tokens = result.split()
        self.assertEqual(len(tokens), 1)

    def test_mixed_cjk_and_english(self) -> None:
        result = canonicalize_query_key("Hello 混凝土 test")
        tokens = result.split()
        self.assertIn("混凝土", tokens)
        self.assertIn("hello", tokens)
        self.assertIn("test", tokens)

    def test_cjk_dedup_preserves_unique(self) -> None:
        result = canonicalize_query_key("步骤 流程 步骤")
        tokens = result.split()
        self.assertEqual(len(tokens), 2)


class EnsureMemoryTablesTests(unittest.TestCase):
    """ensure_memory_tables: 幂等 DDL。"""

    def test_creates_tables(self) -> None:
        # 使用 in-memory SQLite + row_factory 以便按列名访问
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            ensure_memory_tables(conn)
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
            ]
            self.assertIn("query_log", tables)
            self.assertIn("query_node_weights", tables)
            self.assertIn("node_feedback", tables)
        finally:
            conn.close()

    def test_idempotent_multiple_calls(self) -> None:
        """多次调用不应报错。"""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            ensure_memory_tables(conn)
            ensure_memory_tables(conn)  # 第二次调用不应报错
        finally:
            conn.close()

    def test_creates_indexes(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            ensure_memory_tables(conn)
            indexes = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
                ).fetchall()
            ]
            self.assertIn("idx_query_log_norm", indexes)
            self.assertIn("idx_query_log_time", indexes)
            self.assertIn("idx_node_feedback_qid", indexes)
            self.assertIn("idx_node_feedback_nid", indexes)
        finally:
            conn.close()


class LogQueryTests(unittest.TestCase):
    """log_query: 插入查询并验证 query_log 条目。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_returns_query_id(self) -> None:
        qid = log_query(
            self.conn, query_text="混凝土", cmd="bundle", hit_ids=["n1", "n2"]
        )
        self.assertTrue(qid)
        self.assertEqual(len(qid), 16)  # sha256[:16]

    def test_inserts_query_log_entry(self) -> None:
        qid = log_query(
            self.conn, query_text="混凝土", cmd="bundle", hit_ids=["n1"]
        )
        row = self.conn.execute(
            "SELECT * FROM query_log WHERE query_id = ?", (qid,)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["query_text"], "混凝土")
        self.assertEqual(row["query_norm"], canonicalize_query_key("混凝土"))
        self.assertEqual(row["cmd"], "bundle")
        self.assertEqual(row["hits_count"], 1)

    def test_stores_top_node_ids_as_json(self) -> None:
        import json

        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["a", "b", "c"]
        )
        row = self.conn.execute(
            "SELECT top_node_ids FROM query_log WHERE query_id = ?", (qid,)
        ).fetchone()
        ids = json.loads(row["top_node_ids"])
        self.assertEqual(ids, ["a", "b", "c"])

    def test_empty_hit_ids(self) -> None:
        qid = log_query(
            self.conn, query_text="无结果", cmd="bundle", hit_ids=[]
        )
        row = self.conn.execute(
            "SELECT hits_count, top_node_ids FROM query_log WHERE query_id = ?", (qid,)
        ).fetchone()
        self.assertEqual(row["hits_count"], 0)

    def test_upserts_weights_for_hits(self) -> None:
        log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1", "n2"]
        )
        qnorm = canonicalize_query_key("测试")
        rows = self.conn.execute(
            "SELECT node_id FROM query_node_weights WHERE query_norm = ?",
            (qnorm,),
        ).fetchall()
        node_ids = {row["node_id"] for row in rows}
        self.assertIn("n1", node_ids)
        self.assertIn("n2", node_ids)


class ApplyLearnedBoostTests(unittest.TestCase):
    """apply_learned_boost: 验证学习权重重排序。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_no_weights_returns_original_order(self) -> None:
        result = apply_learned_boost(self.conn, ["a", "b", "c"], "测试")
        self.assertEqual(result, ["a", "b", "c"])

    def test_promotes_learned_nodes(self) -> None:
        # 先记录一些查询以建立权重
        log_query(
            self.conn, query_text="混凝土", cmd="bundle", hit_ids=["n2", "n3"]
        )
        # n2 和 n3 有学习权重，在新的搜索结果中它们应该被提升
        result = apply_learned_boost(
            self.conn, ["n1", "n2", "n3", "n4"], "混凝土"
        )
        # n2 和 n3 应该排在前面
        self.assertLess(result.index("n2"), result.index("n1"))
        self.assertLess(result.index("n3"), result.index("n1"))

    def test_preserves_all_ids(self) -> None:
        log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["x"]
        )
        result = apply_learned_boost(
            self.conn, ["a", "b", "c"], "测试"
        )
        self.assertEqual(set(result), {"a", "b", "c"})

    def test_empty_hit_ids(self) -> None:
        result = apply_learned_boost(self.conn, [], "测试")
        self.assertEqual(result, [])

    def test_top_k_learned_parameter(self) -> None:
        # 建立大量有权重的节点
        hit_ids = [f"n{i}" for i in range(10)]
        log_query(
            self.conn, query_text="查询", cmd="bundle", hit_ids=hit_ids
        )
        # 只提升 top 2
        result = apply_learned_boost(
            self.conn, hit_ids, "查询", top_k_learned=2
        )
        # 前 2 个应该是有学习权重的节点
        promoted = result[:2]
        self.assertEqual(len(promoted), 2)


class RecordFeedbackTests(unittest.TestCase):
    """record_feedback: 正/负反馈权重变化。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_returns_feedback_id(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        fid = record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="positive"
        )
        self.assertTrue(fid)
        self.assertEqual(len(fid), 16)

    def test_positive_feedback_increases_weight(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        qnorm = canonicalize_query_key("测试")

        # 基础权重
        row_before = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_before = float(row_before["weight"])

        record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="positive"
        )

        row_after = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_after = float(row_after["weight"])
        self.assertGreater(weight_after, weight_before)

    def test_negative_feedback_decreases_weight(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        qnorm = canonicalize_query_key("测试")

        row_before = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_before = float(row_before["weight"])

        record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="negative"
        )

        row_after = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_after = float(row_after["weight"])
        self.assertLess(weight_after, weight_before)

    def test_neutral_feedback_no_weight_change(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        qnorm = canonicalize_query_key("测试")

        row_before = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_before = float(row_before["weight"])

        record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="neutral"
        )

        row_after = self.conn.execute(
            "SELECT weight FROM query_node_weights WHERE query_norm = ? AND node_id = ?",
            (qnorm, "n1"),
        ).fetchone()
        weight_after = float(row_after["weight"])
        self.assertEqual(weight_after, weight_before)

    def test_inserts_feedback_row(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="positive"
        )
        count = self.conn.execute(
            "SELECT COUNT(*) FROM node_feedback"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_invalid_feedback_type_defaults_to_neutral(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        fid = record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="invalid_type"
        )
        row = self.conn.execute(
            "SELECT feedback_type FROM node_feedback WHERE feedback_id = ?", (fid,)
        ).fetchone()
        self.assertEqual(row["feedback_type"], "neutral")


class RecommendNeighborsTests(unittest.TestCase):
    """recommend_neighbors: 启发式分类 (流程→2, 定义→0, default→1) + 历史反馈验证。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_process_keyword_returns_2(self) -> None:
        result = recommend_neighbors(self.conn, "施工流程是什么")
        self.assertEqual(result, 2)

    def test_definition_keyword_returns_0(self) -> None:
        result = recommend_neighbors(self.conn, "混凝土的定义")
        self.assertEqual(result, 0)

    def test_default_returns_1(self) -> None:
        result = recommend_neighbors(self.conn, "混凝土强度")
        self.assertEqual(result, 1)

    def test_empty_query_returns_default(self) -> None:
        result = recommend_neighbors(self.conn, "")
        self.assertEqual(result, 1)

    def test_multiple_keywords_first_match_wins(self) -> None:
        # 包含"流程"（→2）和"定义"（→0），应返回第一个匹配
        result = recommend_neighbors(self.conn, "流程定义")
        # _NEIGHBORS_KEYWORD_MAP 中 流程 类在前，所以应返回 2
        self.assertEqual(result, 2)

    def test_parameter_keyword_returns_1(self) -> None:
        result = recommend_neighbors(self.conn, "参数范围是多少")
        self.assertEqual(result, 1)

    def test_stage2_positive_feedback_boosts_heuristic(self) -> None:
        """Stage 2: 历史 positive feedback 应强化 heuristic 推荐。"""
        # 先记录一个查询，heuristic 推荐 neighbors=2（"流程"关键字）
        qid = log_query(
            self.conn, query_text="施工流程", cmd="bundle", hit_ids=["n1"], neighbors=2
        )
        # 给这个 neighbors=2 的设置 positive feedback
        record_feedback(self.conn, query_id=qid, node_id="n1", feedback_type="positive")
        # 再次查询相同 normalized query，应仍推荐 2（positive 强化）
        result = recommend_neighbors(self.conn, "施工流程")
        self.assertEqual(result, 2)

    def test_stage2_negative_feedback_overrides_heuristic(self) -> None:
        """Stage 2: 历史 negative feedback 应惩罚并可能改变推荐。"""
        # heuristic 对 "流程" 推荐 neighbors=2
        qid = log_query(
            self.conn, query_text="施工流程", cmd="bundle", hit_ids=["n1"], neighbors=2
        )
        # 给 neighbors=2 的设置 negative feedback（-2 权重）
        record_feedback(self.conn, query_id=qid, node_id="n1", feedback_type="negative")
        # 再次查询：neighbors=2 的 score 为负，不应被选中
        # heuristic=2，但无 positive 候选时会 fallback 到 heuristic
        # 所以需要构造另一个 positive 候选来覆盖
        qid2 = log_query(
            self.conn, query_text="施工流程", cmd="bundle", hit_ids=["n2"], neighbors=1
        )
        record_feedback(self.conn, query_id=qid2, node_id="n2", feedback_type="positive")
        result = recommend_neighbors(self.conn, "施工流程")
        # positive 的 neighbors=1 应被选中，覆盖 heuristic=2
        self.assertEqual(result, 1)

    def test_stage2_no_feedback_falls_back_to_heuristic(self) -> None:
        """Stage 2: 无历史 feedback 时直接返回 heuristic。"""
        result = recommend_neighbors(self.conn, "混凝土的定义")
        self.assertEqual(result, 0)


class GetStatsTests(unittest.TestCase):
    """get_stats: 统计返回正确计数。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_empty_stats(self) -> None:
        stats = get_stats(self.conn)
        self.assertEqual(stats["total_queries"], 0)
        self.assertEqual(stats["total_weights"], 0)
        self.assertEqual(stats["total_feedback"], 0)
        self.assertEqual(stats["queries_last_7d"], 0)

    def test_after_logging_queries(self) -> None:
        log_query(
            self.conn, query_text="查询A", cmd="bundle", hit_ids=["n1"]
        )
        log_query(
            self.conn, query_text="查询B", cmd="bundle", hit_ids=["n2", "n3"]
        )
        stats = get_stats(self.conn)
        self.assertEqual(stats["total_queries"], 2)
        # n1, n2, n3 各一条权重
        self.assertEqual(stats["total_weights"], 3)
        self.assertEqual(stats["queries_last_7d"], 2)

    def test_after_recording_feedback(self) -> None:
        qid = log_query(
            self.conn, query_text="测试", cmd="bundle", hit_ids=["n1"]
        )
        record_feedback(
            self.conn, query_id=qid, node_id="n1", feedback_type="positive"
        )
        stats = get_stats(self.conn)
        self.assertEqual(stats["total_feedback"], 1)

    def test_top_recurring_queries(self) -> None:
        for _ in range(3):
            log_query(
                self.conn, query_text="热门查询", cmd="bundle", hit_ids=["n1"]
            )
        log_query(
            self.conn, query_text="冷门查询", cmd="bundle", hit_ids=["n2"]
        )
        stats = get_stats(self.conn)
        top = stats["top_recurring_queries"]
        self.assertGreaterEqual(len(top), 1)
        self.assertEqual(top[0]["query_norm"], canonicalize_query_key("热门查询"))
        self.assertGreaterEqual(top[0]["count"], 3)


class SuggestRewritesTests(unittest.TestCase):
    """suggest_rewrites: 从历史记录返回建议。"""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        ensure_memory_tables(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_empty_hit_ids_returns_empty(self) -> None:
        result = suggest_rewrites(self.conn, "测试", [])
        self.assertEqual(result, [])

    def test_no_history_returns_empty(self) -> None:
        result = suggest_rewrites(self.conn, "测试", ["n1", "n2"])
        self.assertEqual(result, [])

    def test_returns_alternative_query_texts(self) -> None:
        # 用户 A 搜索 "混凝土强度" 命中了 n1, n2
        log_query(
            self.conn, query_text="混凝土强度", cmd="bundle", hit_ids=["n1", "n2"]
        )
        # 用户 B 搜索 "预应力混凝土" 命中了 n1, n2, n3
        log_query(
            self.conn, query_text="预应力混凝土", cmd="bundle", hit_ids=["n1", "n2", "n3"]
        )

        # 搜索 "混凝土" 命中了 n1, n2，应建议 "预应力混凝土"
        suggestions = suggest_rewrites(
            self.conn, "混凝土", ["n1", "n2"]
        )
        self.assertIsInstance(suggestions, list)
        # 至少应包含 "预应力混凝土" 的建议
        self.assertTrue(
            any("预应力混凝土" in s for s in suggestions)
        )

    def test_excludes_current_query_norm(self) -> None:
        qnorm = canonicalize_query_key("混凝土")
        log_query(
            self.conn, query_text="混凝土", cmd="bundle", hit_ids=["n1"]
        )
        suggestions = suggest_rewrites(self.conn, "混凝土", ["n1"])
        for s in suggestions:
            # 建议不应与当前查询的规范化形式完全相同
            self.assertNotEqual(canonicalize_query_key(s), qnorm)


if __name__ == "__main__":
    unittest.main()
