"""数据库 CRUD 边界测试 — 覆盖 db/crud.py 的异常条件和边界情况。"""

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.db.crud import write_kb_sqlite_db
from build_skill_lib.types import InputDoc, NodeRecord


def _node(node_id: str, **kwargs: object) -> NodeRecord:
    defaults = dict(
        doc_id="doc",
        doc_title="Doc",
        kind="chunk",
        label=node_id,
        title=node_id,
        parent_id=None,
        prev_id=None,
        next_id=None,
        ordinal=1,
        ref_path=f"references/doc/chunks/{node_id}.md",
        is_leaf=True,
        body_md="body",
        body_plain="body",
        source_version="current",
    )
    defaults.update(kwargs)
    return NodeRecord(node_id=node_id, **defaults)


class WriteKbDbTests(unittest.TestCase):
    def test_empty_data_writes_valid_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            write_kb_sqlite_db(db_path, [], [], [], [])
            conn = sqlite3.connect(db_path)
            try:
                # 验证 schema 存在
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                table_names = {t[0] for t in tables}
                self.assertIn("docs", table_names)
                self.assertIn("nodes", table_names)
                self.assertIn("edges", table_names)
                self.assertIn("aliases", table_names)
            finally:
                conn.close()

    def test_writes_docs_and_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            doc = InputDoc(path=Path("test.md"), doc_id="test", title="Test")
            nodes = [
                _node("chunk-0001", ordinal=1),
                _node("chunk-0002", ordinal=2),
            ]
            write_kb_sqlite_db(db_path, [doc], nodes, [], [])
            conn = sqlite3.connect(db_path)
            try:
                docs = conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
                self.assertEqual(docs, 1)
                node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
                self.assertEqual(node_count, 2)
            finally:
                conn.close()

    def test_leaf_node_body_read_from_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            base_dir = Path(tmp) / "base"
            refs = base_dir / "references" / "doc" / "chunks"
            refs.mkdir(parents=True)
            (refs / "chunk-0001.md").write_text("---\n---\nFile body\n", encoding="utf-8")

            nodes = [
                _node("chunk-0001", body_md="", body_plain=""),
            ]
            write_kb_sqlite_db(
                db_path, [], nodes, [], [], base_dir=base_dir
            )
            conn = sqlite3.connect(db_path)
            try:
                text = conn.execute(
                    "SELECT body_plain FROM node_text WHERE node_key = ?",
                    (nodes[0].node_key,),
                ).fetchone()
                self.assertIsNotNone(text)
                self.assertIn("File body", text[0])
            finally:
                conn.close()

    def test_leaf_node_no_body_no_base_dir_raises(self) -> None:
        from build_skill_lib.utils.fs import DataIntegrityError
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            nodes = [
                _node("chunk-0001", body_md="", body_plain=""),
            ]
            with self.assertRaises(DataIntegrityError):
                write_kb_sqlite_db(db_path, [], nodes, [], [])

    def test_non_leaf_node_plain_from_md(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "kb.sqlite"
            nodes = [
                _node("doc-root", kind="doc", is_leaf=False, body_md="Doc body", body_plain=""),
            ]
            write_kb_sqlite_db(db_path, [], nodes, [], [])
            conn = sqlite3.connect(db_path)
            try:
                text = conn.execute(
                    "SELECT body_plain FROM node_text WHERE node_key = ?",
                    (nodes[0].node_key,),
                ).fetchone()
                self.assertIsNotNone(text)
                self.assertIn("Doc body", text[0])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
