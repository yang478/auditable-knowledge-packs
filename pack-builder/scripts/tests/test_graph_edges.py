"""图边构建单元测试 — 覆盖 graph/ 子包核心逻辑。"""

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.graph.builder import build_graph_edges, build_structure_edges, dedupe_edges
from build_skill_lib.graph.alias_mention import build_alias_mention_edges
from build_skill_lib.graph.cooccurrence import build_cooccurrence_edges, _feature_tokens
from build_skill_lib.graph.title_mention import build_title_mention_edges
from build_skill_lib.types import EdgeRecord, NodeRecord


def _node(
    node_id: str,
    *,
    ordinal: int = 1,
    body: str = "",
    parent_id: str | None = None,
    prev_id: str | None = None,
    next_id: str | None = None,
    is_active: bool = True,
    kind: str = "chunk",
    doc_id: str = "doc",
    title: str | None = None,
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        doc_id=doc_id,
        doc_title="Doc",
        kind=kind,
        label=node_id,
        title=title if title is not None else node_id,
        parent_id=parent_id,
        prev_id=prev_id,
        next_id=next_id,
        ordinal=ordinal,
        ref_path=f"references/{doc_id}/chunks/{node_id}.md",
        is_leaf=True,
        body_md=body,
        body_plain=body,
        source_version="current",
        is_active=is_active,
    )


def _make_db(nodes: list[NodeRecord], aliases: list[tuple] | None = None) -> sqlite3.Connection:
    """Create an in-memory DB with the production schema for graph edge testing.

    NOTE: This schema must be kept in sync with build_skill_lib/db/schema.py.
    If the production schema changes, update this helper accordingly.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE nodes (
            node_key TEXT PRIMARY KEY,
            node_id TEXT NOT NULL, doc_id TEXT NOT NULL, source_version TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1, kind TEXT NOT NULL, label TEXT NOT NULL, title TEXT NOT NULL,
            parent_id TEXT, prev_id TEXT, next_id TEXT,
            ordinal INTEGER NOT NULL, ref_path TEXT NOT NULL, is_leaf INTEGER NOT NULL,
            raw_span_start INTEGER NOT NULL, raw_span_end INTEGER NOT NULL,
            node_hash TEXT NOT NULL, confidence REAL NOT NULL
        );
        CREATE TABLE node_text (
            node_key TEXT PRIMARY KEY,
            body_md TEXT NOT NULL, body_plain TEXT NOT NULL,
            keywords TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE aliases (
            doc_id TEXT NOT NULL, alias TEXT NOT NULL, normalized_alias TEXT NOT NULL,
            target_node_id TEXT NOT NULL, alias_level TEXT NOT NULL,
            source_version TEXT NOT NULL, is_active INTEGER NOT NULL DEFAULT 1,
            confidence REAL NOT NULL, source TEXT NOT NULL
        );
        """
    )
    for n in nodes:
        conn.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                n.node_key, n.node_id, n.doc_id, n.source_version,
                1 if n.is_active else 0, n.kind, n.label, n.title,
                n.parent_id, n.prev_id, n.next_id,
                n.ordinal, n.ref_path, 1 if n.is_leaf else 0,
                n.raw_span_start, n.raw_span_end, n.node_hash, n.confidence,
            ),
        )
        conn.execute(
            "INSERT INTO node_text VALUES (?,?,?,?)",
            (n.node_key, n.body_md, n.body_plain, ""),
        )
    if aliases:
        for a in aliases:
            conn.execute(
                "INSERT INTO aliases (doc_id, alias, normalized_alias, target_node_id, source_version, confidence, is_active, alias_level, source) VALUES (?,?,?,?,?,?,?,?,?)",
                a + (1, "exact", "test"),
            )
    return conn


class StructureEdgeTests(unittest.TestCase):
    def test_parent_edge(self) -> None:
        child = _node("child", parent_id="parent")
        parent = _node("parent", kind="doc")
        edges = build_structure_edges([child, parent])
        parent_edges = [e for e in edges if e.edge_type == "parent"]
        self.assertEqual(len(parent_edges), 1)
        self.assertEqual(parent_edges[0].from_node_id, "child")
        self.assertEqual(parent_edges[0].to_node_id, "parent")
        self.assertEqual(parent_edges[0].confidence, 1.0)

    def test_prev_next_edges(self) -> None:
        a = _node("a", prev_id=None, next_id="b")
        b = _node("b", prev_id="a", next_id=None)
        edges = build_structure_edges([a, b])
        prev_edges = [e for e in edges if e.edge_type == "prev"]
        next_edges = [e for e in edges if e.edge_type == "next"]
        self.assertEqual(len(prev_edges), 1)
        self.assertEqual(len(next_edges), 1)
        self.assertEqual(prev_edges[0].to_node_id, "a")
        self.assertEqual(next_edges[0].to_node_id, "b")

    def test_skips_missing_targets(self) -> None:
        """Target node does not exist in active_node_ids → skip edge."""
        child = _node("child", parent_id="missing")
        edges = build_structure_edges([child])
        self.assertEqual(edges, [])

    def test_skips_inactive_targets(self) -> None:
        """Target node exists but is_active=False → skip edge."""
        child = _node("child", parent_id="parent")
        parent = _node("parent", is_active=False)
        edges = build_structure_edges([child, parent])
        self.assertEqual(edges, [])

    def test_skips_inactive_nodes(self) -> None:
        inactive = _node("inactive", parent_id="parent", is_active=False)
        parent = _node("parent")
        edges = build_structure_edges([inactive, parent])
        self.assertEqual(edges, [])


class DedupeEdgeTests(unittest.TestCase):
    def test_no_duplicates(self) -> None:
        edges = [
            EdgeRecord("doc", "parent", "a", "b", "v1"),
            EdgeRecord("doc", "next", "a", "c", "v1"),
        ]
        result = dedupe_edges(edges)
        self.assertEqual(len(result), 2)

    def test_removes_exact_duplicates(self) -> None:
        edges = [
            EdgeRecord("doc", "parent", "a", "b", "v1"),
            EdgeRecord("doc", "parent", "a", "b", "v1"),
        ]
        result = dedupe_edges(edges)
        self.assertEqual(len(result), 1)

    def test_keeps_different_edge_types(self) -> None:
        edges = [
            EdgeRecord("doc", "parent", "a", "b", "v1"),
            EdgeRecord("doc", "next", "a", "b", "v1"),
        ]
        result = dedupe_edges(edges)
        self.assertEqual(len(result), 2)

    def test_keeps_different_source_versions(self) -> None:
        edges = [
            EdgeRecord("doc", "parent", "a", "b", "v1"),
            EdgeRecord("doc", "parent", "a", "b", "v2"),
        ]
        result = dedupe_edges(edges)
        self.assertEqual(len(result), 2)


class AliasMentionEdgeTests(unittest.TestCase):
    def test_alias_found_in_body(self) -> None:
        nodes = [
            _node("chunk-a", body="This mentions Beta concept."),
            _node("chunk-b", body="Beta target"),
        ]
        aliases = [("doc", "Beta", "beta", "chunk-b", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertTrue(any(
                e.edge_type == "alias_mention" and e.from_node_id == "chunk-a" and e.to_node_id == "chunk-b"
                for e in edges
            ))
        finally:
            conn.close()

    def test_self_reference_skipped(self) -> None:
        nodes = [
            _node("chunk-a", body="This mentions Beta."),
        ]
        aliases = [("doc", "Beta", "beta", "chunk-a", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_short_alias_skipped(self) -> None:
        nodes = [
            _node("chunk-a", body="This mentions x."),
        ]
        aliases = [("doc", "x", "x", "chunk-b", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_no_aliases_returns_empty(self) -> None:
        nodes = [_node("chunk-a", body="Some text.")]
        conn = _make_db(nodes)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertEqual(edges, [])
        finally:
            conn.close()


class CooccurrenceEdgeTests(unittest.TestCase):
    def test_shared_tokens_create_edge(self) -> None:
        nodes = [
            _node("a", body="apple banana cherry date"),
            _node("b", body="apple banana cherry fig"),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_cooccurrence_edges(conn, min_shared=3, top_k=12)
            self.assertTrue(any(e.edge_type == "co_occurrence" for e in edges))
        finally:
            conn.close()

    def test_below_threshold_no_edge(self) -> None:
        nodes = [
            _node("a", body="apple banana"),
            _node("b", body="cherry date"),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_cooccurrence_edges(conn, min_shared=3, top_k=12)
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_max_nodes_per_doc_limits(self) -> None:
        nodes = [_node(f"chunk-{i}", body=f"word{i} shared") for i in range(10)]
        conn = _make_db(nodes)
        try:
            edges = build_cooccurrence_edges(conn, max_nodes_per_doc=5)
            # 只有前5个节点参与计算
            self.assertLessEqual(len(edges), 10)  # C(5,2)=10 max
        finally:
            conn.close()

    def test_feature_tokens_excludes_short_and_numeric(self) -> None:
        tokens = _feature_tokens("a 1 hello world 123 test", top_k=10)
        self.assertNotIn("a", tokens)       # 长度 < 2
        self.assertNotIn("1", tokens)       # 纯数字
        self.assertNotIn("123", tokens)     # 纯数字
        self.assertIn("hello", tokens)
        self.assertIn("world", tokens)


class TitleMentionEdgeTests(unittest.TestCase):
    def test_title_found_in_body(self) -> None:
        nodes = [
            _node("sec-1", kind="section", body="Section One content", title="Section One"),
            _node("chunk-a", body="As described in Section One content."),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_title_mention_edges(conn)
            self.assertTrue(any(
                e.edge_type == "title_mention" and e.from_node_id == "chunk-a" and e.to_node_id == "sec-1"
                for e in edges
            ))
        finally:
            conn.close()

    def test_self_reference_skipped(self) -> None:
        nodes = [
            _node("chunk-a", kind="section", body="Chunk A content"),
            _node("chunk-b", body="Chunk A content again"),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_title_mention_edges(conn)
            # 不会给自己建边
            self.assertFalse(
                any(e.from_node_id == e.to_node_id for e in edges)
            )
        finally:
            conn.close()

    def test_short_title_skipped(self) -> None:
        nodes = [
            _node("sec-1", kind="section", body="A"),
            _node("chunk-a", body="Mentions A concept."),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_title_mention_edges(conn)
            # "A" 太短，不应该作为标题目标
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_no_targets_returns_empty(self) -> None:
        nodes = [
            _node("chunk-a", body="Just some text."),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_title_mention_edges(conn)
            self.assertEqual(edges, [])
        finally:
            conn.close()


class GraphBuilderIntegrationTests(unittest.TestCase):
    def test_build_graph_edges_combines_all_types(self) -> None:
        nodes = [
            _node("doc-root", kind="doc"),
            _node("chunk-a", ordinal=1, body="Alpha mentions Beta", parent_id="doc-root", next_id="chunk-b"),
            _node("chunk-b", ordinal=2, body="Beta target", parent_id="doc-root", prev_id="chunk-a"),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_graph_edges(nodes, conn, include_cooccurrence=False)
            types = {e.edge_type for e in edges}
            self.assertIn("parent", types)
            # co_occurrence 被禁用
            self.assertNotIn("co_occurrence", types)
        finally:
            conn.close()

    def test_build_graph_edges_dedupes(self) -> None:
        nodes = [
            _node("doc-root", kind="doc"),
            _node("chunk-a", ordinal=1, parent_id="doc-root"),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_graph_edges(nodes, conn)
            # 同一对节点不应有重复边
            keys = [(e.edge_type, e.from_node_id, e.to_node_id) for e in edges]
            self.assertEqual(len(keys), len(set(keys)))
        finally:
            conn.close()


class WordBoundaryTests(unittest.TestCase):
    """ASCII word-boundary checks in alias_mention and title_mention."""

    def test_ascii_alias_substring_not_whole_word_skipped(self) -> None:
        """ASCII alias 'AI' as substring of 'main' should NOT create edge (whole-word only)."""
        nodes = [
            _node("chunk-a", body="This is the main concept."),
            _node("chunk-b", body="AI target"),
        ]
        aliases = [("doc", "AI", "ai", "chunk-b", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            # "AI" appears as substring of "main" but not as whole word
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_ascii_alias_whole_word_match_creates_edge(self) -> None:
        """ASCII alias 'AI' as whole word should create edge."""
        nodes = [
            _node("chunk-a", body="The AI system works well."),
            _node("chunk-b", body="AI target"),
        ]
        aliases = [("doc", "AI", "ai", "chunk-b", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertEqual(len(edges), 1)
            self.assertEqual(edges[0].edge_type, "alias_mention")
            self.assertEqual(edges[0].from_node_id, "chunk-a")
            self.assertEqual(edges[0].to_node_id, "chunk-b")
        finally:
            conn.close()

    def test_ascii_title_substring_not_whole_word_skipped(self) -> None:
        """ASCII title 'API' as substring of 'rapid' should NOT create edge."""
        nodes = [
            _node("sec-1", kind="section", body="API docs", title="API"),
            _node("chunk-a", body="A rapid development process."),
        ]
        conn = _make_db(nodes)
        try:
            edges = build_title_mention_edges(conn)
            # "API" appears as substring of "rapid" but not as whole word
            self.assertEqual(edges, [])
        finally:
            conn.close()

    def test_max_edges_per_doc_truncation(self) -> None:
        """When potential edges exceed max_edges_per_doc (2000), result should be truncated."""
        # build_alias_mention_edges hard-codes max_edges_per_doc=2000
        nodes = [_node(f"chunk-{i}", body=f"Text mentioning Beta.") for i in range(2100)]
        nodes.append(_node("target", body="Beta target"))
        aliases = [("doc", "Beta", "beta", "target", "current", 1.0)]
        conn = _make_db(nodes, aliases)
        try:
            edges = build_alias_mention_edges(conn)
            self.assertLessEqual(len(edges), 2000)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
