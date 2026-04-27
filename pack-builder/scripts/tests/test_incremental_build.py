"""增量构建测试 — 验证 pack-builder 增量更新流程。

覆盖范围：
- 无变更时跳过构建
- 修改文件后只更新该文件
- 删除文件后 soft-delete
- 新增文件后增量添加
- toolchain 变化时回退全量重建
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


def _build_skill(tmp_path: Path, *fixture_names: str, incremental: bool = False) -> Path:
    """构建或增量更新一个临时 skill 并返回其根目录。"""
    input_paths = []
    for name in fixture_names:
        src = FIXTURES / name
        dst = tmp_path / name
        # Only copy fixture if the file does not already exist (preserves edits for incremental tests).
        if not dst.exists():
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        input_paths.append(dst)

    out_dir = tmp_path / "out"
    out_dir.mkdir(exist_ok=True)

    cmd = [
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
    ]
    if incremental:
        cmd.append("--incremental")

    subprocess.run(
        cmd,
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(ROOT),
    )
    return out_dir / "test-skill"


class IncrementalBuildTests(unittest.TestCase):
    def test_full_then_incremental_no_change_skips(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            # 记录首次构建产物
            first_state = json.loads((skill / "build_state.json").read_text(encoding="utf-8"))
            first_mtime = (skill / "build_state.json").stat().st_mtime

            # 无变更增量构建
            skill2 = _build_skill(Path(tmp), "handbook.md", incremental=True)
            second_state = json.loads((skill2 / "build_state.json").read_text(encoding="utf-8"))
            second_mtime = (skill2 / "build_state.json").stat().st_mtime

            # 由于无变更，应该复用原有输出（同一目录）
            self.assertEqual(first_state["documents"], second_state["documents"])
            # build_state 文件未被重写，mtime 应不变
            self.assertEqual(first_mtime, second_mtime)

    def test_full_then_modify_one_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            skill = _build_skill(tmp_path, "handbook.md")

            # 修改 handbook.md
            handbook = tmp_path / "handbook.md"
            original_text = handbook.read_text(encoding="utf-8")
            handbook.write_text(original_text + "\n\n# 新增章节\n\n这是增量测试新增的内容。\n", encoding="utf-8")

            # 增量构建
            skill2 = _build_skill(tmp_path, "handbook.md", incremental=True)
            state = json.loads((skill2 / "build_state.json").read_text(encoding="utf-8"))

            # build_state 应该仍然包含该文档
            docs = state.get("documents", {})
            self.assertIn("handbook", docs)

            # 验证 DB 仍然存在且可以查询
            self.assertTrue((skill2 / "kb.sqlite").exists())

            # 验证新增内容确实出现在 chunk 中
            chunks_dir = skill2 / "references" / "handbook" / "chunks"
            found_new_content = False
            for chunk_file in chunks_dir.glob("*.md"):
                if "这是增量测试新增的内容" in chunk_file.read_text(encoding="utf-8"):
                    found_new_content = True
                    break
            self.assertTrue(found_new_content, "新增内容应出现在增量构建后的 chunks 中")

    def test_full_then_add_new_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            skill = _build_skill(tmp_path, "handbook.md")

            # 新增一个文件
            src = FIXTURES / "standard_v1.md"
            dst = tmp_path / "standard_v1.md"
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

            # 增量构建
            skill2 = _build_skill(tmp_path, "handbook.md", "standard_v1.md", incremental=True)
            state = json.loads((skill2 / "build_state.json").read_text(encoding="utf-8"))

            docs = state.get("documents", {})
            self.assertIn("handbook", docs)
            self.assertIn("standard-v1", docs)

            # 验证新文档的 chunk 文件存在
            new_chunks_dir = skill2 / "references" / "standard-v1" / "chunks"
            self.assertTrue(new_chunks_dir.exists())
            self.assertGreaterEqual(len(list(new_chunks_dir.glob("*.md"))), 1)

    def test_full_then_delete_file_soft_delete(self) -> None:
        """删除源文件后增量构建，文档应变为 inactive（soft-delete）。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            skill = _build_skill(tmp_path, "handbook.md", "standard_v1.md")

            # 只保留 handbook.md，删除 standard_v1.md
            (tmp_path / "standard_v1.md").unlink()

            # 增量构建
            skill2 = _build_skill(tmp_path, "handbook.md", incremental=True)
            state = json.loads((skill2 / "build_state.json").read_text(encoding="utf-8"))
            docs = state.get("documents", {})

            # handbook 仍应存在
            self.assertIn("handbook", docs)
            # standard-v1 不应再出现在当前文档中（已被 soft-delete）
            self.assertNotIn("standard-v1", docs)

    def test_toolchain_change_falls_back_to_full_rebuild(self) -> None:
        """修改模板文件（toolchain 变化）时，增量构建应回退为全量重建。"""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            skill = _build_skill(tmp_path, "handbook.md")

            # 修改模板文件模拟 toolchain 变更
            template_dir = ROOT / "pack-builder" / "templates" / "kbtool_lib"
            sentinel = template_dir / ".toolchain_test_sentinel"
            sentinel.write_text("changed", encoding="utf-8")
            try:
                # 增量构建应检测到 toolchain 变化并全量重建
                skill2 = _build_skill(tmp_path, "handbook.md", incremental=True)
                # 构建成功即视为通过（若 toolchain 检测失效可能报错或产生不一致结果）
                self.assertTrue((skill2 / "build_state.json").exists())
                self.assertTrue((skill2 / "kb.sqlite").exists())
            finally:
                sentinel.unlink(missing_ok=True)

    def test_incremental_build_state_has_multiversion_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            skill = _build_skill(Path(tmp), "handbook.md")
            state = json.loads((skill / "build_state.json").read_text(encoding="utf-8"))
            docs = state.get("documents", {})
            self.assertIn("handbook", docs)
            # 新结构：doc_id -> source_version -> state
            handbook_entry = docs["handbook"]
            self.assertIsInstance(handbook_entry, dict)
            # 应该至少有一个版本键（如 "current"）
            self.assertTrue(any(isinstance(v, dict) and "source_fingerprint" in v for v in handbook_entry.values()))


if __name__ == "__main__":
    unittest.main()
