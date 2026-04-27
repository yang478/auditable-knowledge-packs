import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "pack-builder" / "scripts"
TEMPLATES_DIR = ROOT / "pack-builder" / "templates"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(TEMPLATES_DIR))

from build_skill_lib.cli import main as build_cli_main  # noqa: E402
from build_skill_lib.render import render_generated_skill_md  # noqa: E402
from build_skill_lib.db import write_kb_sqlite_db  # noqa: E402
from build_skill_lib.types import EdgeRecord, InputDoc, NodeRecord  # noqa: E402
from kbtool_lib import cli as runtime_cli  # noqa: E402
from kbtool_lib import retrieval  # noqa: E402


def _node(node_id: str, *, ordinal: int, body: str, prev_id: str | None = None, next_id: str | None = None) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        doc_id="doc",
        doc_title="Doc",
        kind="chunk",
        label=node_id,
        title=node_id,
        parent_id=None,
        prev_id=prev_id,
        next_id=next_id,
        ordinal=ordinal,
        ref_path=f"references/doc/chunks/{node_id}.md",
        is_leaf=True,
        body_md=body,
        body_plain=body,
        source_version="current",
    )


def _write_test_db(db_path: Path) -> None:
    doc = InputDoc(path=Path("doc.md"), doc_id="doc", title="Doc")
    nodes = [
        _node("chunk-a", ordinal=1, body="Alpha mentions Beta", next_id="chunk-b"),
        _node("chunk-b", ordinal=2, body="Beta target", prev_id="chunk-a"),
        _node("chunk-c", ordinal=3, body="Noisy co occurrence only"),
    ]
    edges = [
        EdgeRecord("doc", "alias_mention", "chunk-a", "chunk-b", "current"),
        EdgeRecord("doc", "co_occurrence", "chunk-a", "chunk-c", "current"),
    ]
    write_kb_sqlite_db(db_path, [doc], nodes, edges, [])


class GraphEnhancementTests(unittest.TestCase):
    def test_runtime_parser_uses_safe_small_graph_defaults(self) -> None:
        parser = runtime_cli.build_parser()

        bundle_args = parser.parse_args(["bundle", "--query", "alpha"])
        self.assertEqual(bundle_args.preset, "quick")
        self.assertEqual(bundle_args.graph_depth, 1)
        self.assertEqual(bundle_args.edge_types, None)

        triage_args = parser.parse_args(["triage", "--query", "alpha"])
        self.assertEqual(triage_args.preset, "quick")
        self.assertEqual(triage_args.graph_depth, 1)
        self.assertEqual(triage_args.search_limit, 12)
        self.assertEqual(triage_args.files_limit, 20)

        search_args = parser.parse_args(["search", "--query", "alpha"])
        self.assertEqual(search_args.limit, 20)

    def test_graph_expansion_defaults_exclude_noisy_cooccurrence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            _write_test_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                default_ids = retrieval.expand_hits(
                    conn,
                    ["chunk-a"],
                    neighbors=0,
                    graph_depth=1,
                    edge_types=retrieval.graph_edge_types(None),
                    limit=10,
                )
                self.assertIn("chunk-a", default_ids)
                self.assertIn("chunk-b", default_ids)
                self.assertNotIn("chunk-c", default_ids)

                opt_in_ids = retrieval.expand_hits(
                    conn,
                    ["chunk-a"],
                    neighbors=0,
                    graph_depth=1,
                    edge_types=retrieval.graph_edge_types(["co_occurrence"]),
                    limit=10,
                )
                self.assertIn("chunk-c", opt_in_ids)
            finally:
                conn.close()

    def test_build_cli_enables_graph_edges_by_default_and_can_disable(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_build_skill(**kwargs: object) -> Path:
            calls.append(kwargs)
            return Path("unused")

        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "doc.md"
            input_path.write_text("# Doc\n\nAlpha", encoding="utf-8")
            base_args = [
                "--skill-name",
                "demo",
                "--out-dir",
                str(Path(tmp) / "out"),
                "--inputs",
                str(input_path),
            ]

            self.assertEqual(build_cli_main(base_args, build_skill_fn=fake_build_skill), 0)
            self.assertEqual(calls[-1]["enable_graph_edges"], True)

            self.assertEqual(
                build_cli_main([*base_args, "--disable-graph-edges"], build_skill_fn=fake_build_skill),
                0,
            )
            self.assertEqual(calls[-1]["enable_graph_edges"], False)

    def test_generated_skill_guides_concise_graph_usage(self) -> None:
        skill_md = render_generated_skill_md(
            "demo",
            "Demo KB",
            [InputDoc(path=Path("doc.md"), doc_id="doc", title="Doc")],
        )

        self.assertLess(len(skill_md.encode("utf-8")), 6000)
        self.assertIn('./kbtool triage --query "问题" --out runs/r1-triage.md', skill_md)
        self.assertIn("先读再决定", skill_md)
        self.assertIn("最多 3 轮", skill_md)
        self.assertIn("--graph-depth 0", skill_md)


if __name__ == "__main__":
    unittest.main()
