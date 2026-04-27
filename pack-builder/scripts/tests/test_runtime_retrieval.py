"""retrieval 模块单元测试 — 覆盖 templates/kbtool_lib/retrieval.py 中的纯逻辑函数。"""

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
sys.path.insert(0, str(TEMPLATES_DIR))

from kbtool_lib.retrieval import (
    graph_edge_types,
    Node,
    node_to_dict,
)
from kbtool_lib.sql_utils import build_in_placeholders


class BuildInPlaceholdersTests(unittest.TestCase):
    """build_in_placeholders: 各种大小（含空列表）。"""

    def test_empty_list(self) -> None:
        result = build_in_placeholders([])
        self.assertEqual(result, "")

    def test_single_item(self) -> None:
        result = build_in_placeholders(["a"])
        self.assertEqual(result, "?")

    def test_three_items(self) -> None:
        result = build_in_placeholders([1, 2, 3])
        self.assertEqual(result, "?,?,?")

    def test_many_items(self) -> None:
        items = list(range(100))
        result = build_in_placeholders(items)
        self.assertEqual(result, ",".join("?" for _ in range(100)))

    def test_one_item(self) -> None:
        result = build_in_placeholders(["only"])
        self.assertEqual(result, "?")

    def test_returns_string(self) -> None:
        result = build_in_placeholders(["a", "b"])
        self.assertIsInstance(result, str)


class GraphEdgeTypesTests(unittest.TestCase):
    """graph_edge_types: 默认、自定义、空输入。"""

    def test_none_returns_defaults(self) -> None:
        result = graph_edge_types(None)
        self.assertEqual(
            result,
            ("prev", "next", "references", "alias_mention", "title_mention"),
        )

    def test_empty_list_returns_defaults(self) -> None:
        result = graph_edge_types([])
        self.assertEqual(
            result,
            ("prev", "next", "references", "alias_mention", "title_mention"),
        )

    def test_empty_string_item_filtered(self) -> None:
        result = graph_edge_types(["", "  ", "references"])
        self.assertIn("references", result)
        self.assertNotIn("", result)
        self.assertNotIn("  ", result)

    def test_custom_single_type(self) -> None:
        result = graph_edge_types(["references"])
        self.assertEqual(result, ("references",))

    def test_custom_multiple_types(self) -> None:
        result = graph_edge_types(["prev", "next", "references"])
        self.assertEqual(result, ("prev", "next", "references"))

    def test_comma_separated_string(self) -> None:
        result = graph_edge_types(["prev, next, references"])
        self.assertEqual(result, ("prev", "next", "references"))

    def test_deduplication(self) -> None:
        result = graph_edge_types(["prev", "prev", "next"])
        self.assertEqual(result, ("prev", "next"))

    def test_all_defaults_explicitly(self) -> None:
        defaults = ["prev", "next", "references", "alias_mention", "title_mention"]
        result = graph_edge_types(defaults)
        self.assertEqual(
            result,
            ("prev", "next", "references", "alias_mention", "title_mention"),
        )

    def test_returns_tuple(self) -> None:
        result = graph_edge_types(["prev"])
        self.assertIsInstance(result, tuple)


class NodeTests(unittest.TestCase):
    """Node 数据类构造。"""

    def _make_node(self, **overrides) -> Node:
        defaults = {
            "node_id": "n1",
            "doc_id": "d1",
            "doc_title": "测试文档",
            "source_file": "test.md",
            "source_path": "docs/test.md",
            "source_version": "v1",
            "kind": "chunk",
            "label": "测试",
            "title": "测试标题",
            "parent_id": None,
            "prev_id": None,
            "next_id": None,
            "ordinal": 0,
            "ref_path": "docs/test.md#n1",
            "is_leaf": True,
            "body_md": "# 正文",
            "body_plain": "正文",
            "keywords": "",
        }
        defaults.update(overrides)
        return Node(**defaults)

    def test_construction_defaults(self) -> None:
        node = self._make_node()
        self.assertEqual(node.node_id, "n1")
        self.assertEqual(node.doc_id, "d1")
        self.assertEqual(node.kind, "chunk")
        self.assertEqual(node.ordinal, 0)
        self.assertTrue(node.is_leaf)
        self.assertEqual(node.keywords, "")

    def test_frozen(self) -> None:
        node = self._make_node()
        with self.assertRaises(AttributeError):
            node.node_id = "changed"  # type: ignore[misc]

    def test_with_optional_fields_set(self) -> None:
        node = self._make_node(
            parent_id="p1",
            prev_id="prev1",
            next_id="next1",
            keywords="关键词1 关键词2",
        )
        self.assertEqual(node.parent_id, "p1")
        self.assertEqual(node.prev_id, "prev1")
        self.assertEqual(node.next_id, "next1")
        self.assertEqual(node.keywords, "关键词1 关键词2")

    def test_with_optional_fields_none(self) -> None:
        node = self._make_node(parent_id=None, prev_id=None, next_id=None)
        self.assertIsNone(node.parent_id)
        self.assertIsNone(node.prev_id)
        self.assertIsNone(node.next_id)

    def test_equality_by_value(self) -> None:
        """frozen dataclass 的相等性基于所有字段值。"""
        a = self._make_node()
        b = self._make_node()
        self.assertEqual(a, b)

    def test_inequality_by_different_field(self) -> None:
        a = self._make_node()
        b = self._make_node(node_id="n2")
        self.assertNotEqual(a, b)


class NodeToDictTests(unittest.TestCase):
    """node_to_dict: include_body=True/False 的输出字段。"""

    def _make_node(self, **overrides) -> Node:
        defaults = {
            "node_id": "n1",
            "doc_id": "d1",
            "doc_title": "测试文档",
            "source_file": "test.md",
            "source_path": "docs/test.md",
            "source_version": "v1",
            "kind": "chunk",
            "label": "测试",
            "title": "测试标题",
            "parent_id": "p1",
            "prev_id": "prev1",
            "next_id": "next1",
            "ordinal": 5,
            "ref_path": "docs/test.md#n1",
            "is_leaf": True,
            "body_md": "# 正文内容",
            "body_plain": "正文内容",
            "keywords": "关键词",
        }
        defaults.update(overrides)
        return Node(**defaults)

    def test_without_body(self) -> None:
        node = self._make_node()
        d = node_to_dict(node, include_body=False)
        self.assertEqual(d["node_id"], "n1")
        self.assertEqual(d["doc_id"], "d1")
        self.assertEqual(d["doc_title"], "测试文档")
        self.assertEqual(d["kind"], "chunk")
        self.assertEqual(d["title"], "测试标题")
        self.assertEqual(d["ordinal"], 5)
        self.assertTrue(d["is_leaf"])
        self.assertEqual(d["parent_id"], "p1")
        self.assertEqual(d["prev_id"], "prev1")
        self.assertEqual(d["next_id"], "next1")
        # 不应包含 body 字段
        self.assertNotIn("body_md", d)
        self.assertNotIn("body_plain", d)

    def test_with_body(self) -> None:
        node = self._make_node()
        d = node_to_dict(node, include_body=True)
        self.assertIn("body_md", d)
        self.assertIn("body_plain", d)
        self.assertEqual(d["body_md"], "# 正文内容")
        self.assertEqual(d["body_plain"], "正文内容")

    def test_all_fields_present_without_body(self) -> None:
        node = self._make_node()
        d = node_to_dict(node, include_body=False)
        expected_keys = {
            "node_id", "doc_id", "doc_title", "source_file", "source_path",
            "source_version", "kind", "label", "title", "parent_id",
            "prev_id", "next_id", "ordinal", "ref_path", "is_leaf",
        }
        self.assertEqual(set(d.keys()), expected_keys)

    def test_all_fields_present_with_body(self) -> None:
        node = self._make_node()
        d = node_to_dict(node, include_body=True)
        expected_keys = {
            "node_id", "doc_id", "doc_title", "source_file", "source_path",
            "source_version", "kind", "label", "title", "parent_id",
            "prev_id", "next_id", "ordinal", "ref_path", "is_leaf",
            "body_md", "body_plain",
        }
        self.assertEqual(set(d.keys()), expected_keys)

    def test_ordinal_is_int(self) -> None:
        node = self._make_node(ordinal=7)
        d = node_to_dict(node, include_body=False)
        self.assertIsInstance(d["ordinal"], int)
        self.assertEqual(d["ordinal"], 7)

    def test_is_leaf_is_bool(self) -> None:
        node = self._make_node(is_leaf=False)
        d = node_to_dict(node, include_body=False)
        self.assertIsInstance(d["is_leaf"], bool)
        self.assertFalse(d["is_leaf"])

    def test_none_optional_fields(self) -> None:
        node = self._make_node(parent_id=None, prev_id=None, next_id=None)
        d = node_to_dict(node, include_body=False)
        self.assertIsNone(d["parent_id"])
        self.assertIsNone(d["prev_id"])
        self.assertIsNone(d["next_id"])


if __name__ == "__main__":
    unittest.main()
