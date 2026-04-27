"""精简集成测试 — 验证 pack-builder 核心端到端流程。

覆盖范围：
- build_skill CLI 构建知识库
- kbtool search / bundle / triage / get-node / docs 子命令
- graph edges 默认启用
- 安全边界（拒绝 root 外写入）
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BUILDER = ROOT / "pack-builder" / "scripts" / "build_skill.py"
FIXTURES = ROOT / "pack-builder" / "scripts" / "tests" / "fixtures" / "retrieval"


def _build_skill(tmp_path: Path, *fixture_names: str) -> Path:
    """构建一个临时 skill 并返回其根目录。"""
    input_paths = []
    for name in fixture_names:
        src = FIXTURES / name
        dst = tmp_path / name
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        input_paths.append(dst)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    subprocess.run(
        [
            "python3",
            str(BUILDER),
            "--skill-name",
            "test-skill",
            "--out-dir",
            str(out_dir),
            "--inputs",
            *[str(p) for p in input_paths],
            "--title",
            "Test KB",
        ],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(ROOT),
    )
    return out_dir / "test-skill"


def _kbtool(skill_root: Path, *args: str) -> subprocess.CompletedProcess:
    """在 skill 根目录下运行 kbtool 子命令。"""
    return subprocess.run(
        ["python3", str(skill_root / "scripts" / "kbtool.py"), *args],
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(skill_root),
    )


class BuildAndStructureTests(unittest.TestCase):
    def test_build_creates_expected_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            self.assertTrue((skill / "kb.sqlite").exists())
            self.assertTrue((skill / "manifest.json").exists())
            self.assertTrue((skill / "build_state.json").exists())
            self.assertTrue((skill / "references").is_dir())
            self.assertTrue((skill / "indexes").is_dir())
            self.assertTrue((skill / "scripts" / "kbtool.py").exists())
            self.assertTrue((skill / "kbtool").exists())  # shell wrapper

    def test_manifest_contains_doc_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            manifest = json.loads((skill / "manifest.json").read_text(encoding="utf-8"))
            self.assertIn("docs", manifest)
            self.assertEqual(len(manifest["docs"]), 1)
            self.assertEqual(manifest["docs"][0]["doc_id"], "handbook")
            self.assertTrue(manifest["docs"][0]["active_version"])

    def test_references_contains_chunk_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            refs = skill / "references" / "handbook" / "chunks"
            self.assertTrue(refs.is_dir())
            chunks = list(refs.glob("*.md"))
            self.assertGreaterEqual(len(chunks), 1)


class SearchAndBundleTests(unittest.TestCase):
    def test_search_returns_json_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            proc = _kbtool(skill, "search", "--query", "部署准备")
            data = json.loads(proc.stdout)
            self.assertEqual(data["cmd"], "search")
            self.assertTrue(len(data["matches"]) > 0)
            # 放宽断言：查询词出现在任意匹配项中即可，不假设排序
            self.assertTrue(
                any("部署准备" in m["line_text"] for m in data["matches"]),
                "Expected '部署准备' to appear in at least one match",
            )

    def test_bundle_generates_markdown_with_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            _kbtool(skill, "bundle", "--query", "部署准备", "--out", "runs/bundle.md")
            bundle = (skill / "runs" / "bundle.md").read_text(encoding="utf-8")
            self.assertIn("# Bundle", bundle)
            self.assertIn("## Evidence", bundle)
            self.assertIn("## References", bundle)
            self.assertIn("部署准备", bundle)

    def test_bundle_outputs_trace_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            proc = _kbtool(skill, "bundle", "--query", "部署准备", "--out", "runs/bundle.md")
            data = json.loads(proc.stdout)
            self.assertEqual(data["cmd"], "bundle")
            self.assertTrue(len(data["hits"]) > 0)
            self.assertIn("handbook:chunk", data["hits"][0])

    def test_triage_combines_search_and_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            proc = _kbtool(skill, "triage", "--query", "部署准备", "--out", "runs/triage.md")
            data = json.loads(proc.stdout)
            self.assertEqual(data["cmd"], "triage")
            self.assertIn("bundle", data)
            self.assertIn("search", data)

    def test_get_node_returns_full_node_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            # 动态推断第一个 chunk node_id，避免硬编码
            chunks_dir = skill / "references" / "handbook" / "chunks"
            chunk_files = sorted(chunks_dir.glob("*.md"))
            self.assertTrue(chunk_files, "Expected at least one chunk file")
            # 从文件名推断 node_id (e.g. chunk-000001.md -> handbook:chunk:000001)
            first_chunk_name = chunk_files[0].stem  # e.g. "chunk-000001"
            kind, ordinal = first_chunk_name.split("-", 1)
            node_id = f"handbook:{kind}:{ordinal}"

            proc = _kbtool(skill, "get-node", node_id)
            data = json.loads(proc.stdout)
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 1)
            node = data[0]
            self.assertEqual(node["node_id"], node_id)
            self.assertIn("body_md", node)
            self.assertIn("body_plain", node)

    def test_docs_lists_documents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            _kbtool(skill, "docs")
            self.assertTrue((skill / "runs" / "docs.md").exists())


class GraphAndDefaultsTests(unittest.TestCase):
    def test_graph_edges_enabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "standard_v1.md")
            # build_state.json 中 edge_fingerprints 非空说明 graph edges 已生成
            state = json.loads((skill / "build_state.json").read_text(encoding="utf-8"))
            docs = state.get("documents", {})
            self.assertTrue(len(docs) > 0)
            for doc_versions in docs.values():
                # documents_state uses nested structure: doc_id -> source_version -> state
                for doc_state in doc_versions.values():
                    self.assertIn("edge_fingerprints", doc_state)
                    self.assertTrue(len(doc_state["edge_fingerprints"]) > 0)

    def test_bundle_includes_graph_depth_in_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            proc = _kbtool(skill, "bundle", "--query", "部署准备", "--out", "runs/bundle.md")
            data = json.loads(proc.stdout)
            self.assertEqual(data["graph_depth"], 1)
            self.assertIn("edge_types", data)


class SecurityTests(unittest.TestCase):
    def test_refuses_to_write_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            with self.assertRaises(subprocess.CalledProcessError):
                _kbtool(skill, "bundle", "--query", "x", "--out", "/tmp/hacked.md")

    def test_search_refuses_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            with self.assertRaises(subprocess.CalledProcessError):
                _kbtool(skill, "search", "--query", "x", "--out", "../../../etc/passwd")

    def test_docs_refuses_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            with self.assertRaises(subprocess.CalledProcessError):
                _kbtool(skill, "docs", "--out", "../../../etc/passwd")

    def test_triage_refuses_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            with self.assertRaises(subprocess.CalledProcessError):
                _kbtool(skill, "triage", "--query", "x", "--out", "../../../etc/passwd")


if __name__ == "__main__":
    unittest.main()
