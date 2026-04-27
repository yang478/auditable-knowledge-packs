import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]  # pack-builder/scripts
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib import templates_dir  # noqa: E402
import build_skill_lib.utils.text as build_text  # noqa: E402
from build_skill_lib.types import AliasRecord, EdgeRecord, NodeRecord  # noqa: E402

TEMPLATES_DIR = templates_dir()
sys.path.insert(0, str(TEMPLATES_DIR))

import kbtool_lib.reindex as runtime_reindex  # noqa: E402
import kbtool_lib.text as runtime_text  # noqa: E402


class ConsistencyTests(unittest.TestCase):
    def test_build_text_utils_matches_runtime_text(self) -> None:
        q = "第3条 适用范围 Scope"
        self.assertEqual(build_text.normalize_alias_text(q), runtime_text.normalize_alias_text(q))
        self.assertEqual(build_text.core_alias_title("第3条 适用范围"), runtime_text.core_alias_title("第3条 适用范围"))
        self.assertEqual(build_text.stable_hash(q), runtime_text.stable_hash(q))
        self.assertEqual(build_text.fts_tokens(q), " ".join(runtime_text.fts_tokens(q)))

    def test_node_row_matches_node_record_core_fields(self) -> None:
        node_row = getattr(runtime_reindex, "NodeRow", None)
        edge_row = getattr(runtime_reindex, "EdgeRow", None)
        alias_row = getattr(runtime_reindex, "AliasRow", None)

        self.assertTrue(hasattr(node_row, "__dataclass_fields__"))
        self.assertTrue(hasattr(edge_row, "__dataclass_fields__"))
        self.assertTrue(hasattr(alias_row, "__dataclass_fields__"))

        node_row_fields = set(node_row.__dataclass_fields__.keys())
        node_record_fields = set(NodeRecord.__dataclass_fields__.keys())
        self.assertEqual(node_record_fields - node_row_fields, {"doc_title", "heading_path"})
        self.assertEqual(node_row_fields - node_record_fields, set())

        edge_row_fields = set(edge_row.__dataclass_fields__.keys())
        edge_record_fields = set(EdgeRecord.__dataclass_fields__.keys())
        self.assertEqual(edge_row_fields, edge_record_fields)

        alias_row_fields = set(alias_row.__dataclass_fields__.keys())
        alias_record_fields = set(AliasRecord.__dataclass_fields__.keys())
        self.assertEqual(alias_row_fields, alias_record_fields)
