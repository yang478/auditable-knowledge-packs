"""Tests for build_skill_lib.build."""

import json
import shutil
import sys
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.build import (
    _atomic_replace,
    _write_manifest,
    build_skill,
)
from build_skill_lib.types import InputDoc

# ---------------------------------------------------------------------------
# Parameter validation — die paths
# ---------------------------------------------------------------------------


def test_chunk_size_zero_dies(tmp_path: Path, die_exception, no_die) -> None:
    md = tmp_path / "input.md"
    md.write_text("# T\n\nb\n", encoding="utf-8")
    with pytest.raises(die_exception) as exc_info:
        build_skill("skill", "Title", [md], tmp_path / "out", force=True, chunk_size=0)
    assert "--chunk-size" in str(exc_info.value)


def test_overlap_negative_dies(tmp_path: Path, die_exception, no_die) -> None:
    md = tmp_path / "input.md"
    md.write_text("# T\n\nb\n", encoding="utf-8")
    with pytest.raises(die_exception) as exc_info:
        build_skill("skill", "Title", [md], tmp_path / "out", force=True, overlap=-1)
    assert "--overlap" in str(exc_info.value)


def test_overlap_gte_chunk_size_dies(tmp_path: Path, die_exception, no_die) -> None:
    md = tmp_path / "input.md"
    md.write_text("# T\n\nb\n", encoding="utf-8")
    with pytest.raises(die_exception) as exc_info:
        build_skill("skill", "Title", [md], tmp_path / "out", force=True, chunk_size=100, overlap=100)
    assert "--overlap" in str(exc_info.value)


def test_target_exists_no_force_dies(tmp_path: Path, die_exception, no_die) -> None:
    out_dir = tmp_path / "out"
    target = out_dir / "skill"
    target.mkdir(parents=True)
    md = tmp_path / "input.md"
    md.write_text("# T\n\nb\n", encoding="utf-8")
    with pytest.raises(die_exception) as exc_info:
        build_skill("skill", "Title", [md], out_dir, force=False)
    assert "already exists" in str(exc_info.value)


# ---------------------------------------------------------------------------
# force=True preserves bin/ and hooks/
# ---------------------------------------------------------------------------


def test_force_true_copies_bin_and_hooks(tmp_path: Path, monkeypatch) -> None:
    out_dir = tmp_path / "out"
    target = out_dir / "skill"
    target.mkdir(parents=True)
    (target / "bin").mkdir()
    (target / "bin" / "rg").write_text("rg", encoding="utf-8")
    (target / "hooks").mkdir()
    (target / "hooks" / "hook.sh").write_text("sh", encoding="utf-8")

    md = tmp_path / "input.md"
    md.write_text("# Title\n\nbody\n", encoding="utf-8")

    copied = []
    orig_copytree = shutil.copytree

    def tracking_copytree(src, dst, **kw):
        copied.append((Path(src), Path(dst)))
        return orig_copytree(src, dst, **kw)

    monkeypatch.setattr("build_skill_lib.build.shutil.copytree", tracking_copytree)

    build_skill("skill", "Title", [md], out_dir, force=True, chunk_size=500, overlap=0)

    srcs = [src for src, _ in copied]
    assert target / "bin" in srcs
    assert target / "hooks" in srcs
    # Final target should still contain the preserved directories
    assert (target / "bin" / "rg").exists()
    assert (target / "hooks" / "hook.sh").exists()


# ---------------------------------------------------------------------------
# _write_manifest
# ---------------------------------------------------------------------------


def test_write_manifest(tmp_path: Path) -> None:
    out_dir = tmp_path / "skill"
    out_dir.mkdir()
    docs = [
        InputDoc(
            path=Path("/docs/a.md"),
            doc_id="doc-a",
            title="Doc A",
            source_version="current",
            doc_hash="hash1",
        ),
        InputDoc(
            path=Path("/docs/b.md"),
            doc_id="doc-b",
            title="Doc B",
            source_version="v2",
            doc_hash="hash2",
        ),
    ]
    _write_manifest(out_dir, skill_name="my-skill", title="My Skill", docs=docs)
    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["skill_name"] == "my-skill"
    assert manifest["title"] == "My Skill"
    assert len(manifest["docs"]) == 2
    assert manifest["docs"][0]["doc_id"] == "doc-a"
    assert manifest["docs"][1]["source_version"] == "v2"


# ---------------------------------------------------------------------------
# _atomic_replace
# ---------------------------------------------------------------------------


def test_atomic_replace_success(tmp_path: Path) -> None:
    tmp = tmp_path / "tmp"
    target = tmp_path / "target"
    target.mkdir()
    (target / "old.txt").write_text("old", encoding="utf-8")
    tmp.mkdir()
    (tmp / "new.txt").write_text("new", encoding="utf-8")

    result = _atomic_replace(tmp, target)
    assert result == target
    assert (target / "new.txt").read_text(encoding="utf-8") == "new"
    assert not (tmp_path / "target.old").exists()
    assert not tmp.exists()


def test_atomic_replace_failure_restores_backup(tmp_path: Path, monkeypatch) -> None:
    tmp = tmp_path / "tmp"
    target = tmp_path / "target"
    target.mkdir()
    (target / "old.txt").write_text("old", encoding="utf-8")
    tmp.mkdir()
    (tmp / "new.txt").write_text("new", encoding="utf-8")

    orig_move = shutil.move
    call_count = 0

    def bad_move(src, dst):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("move failed")
        return orig_move(src, dst)

    monkeypatch.setattr("build_skill_lib.build.shutil.move", bad_move)

    with pytest.raises(OSError, match="move failed"):
        _atomic_replace(tmp, target)

    assert (target / "old.txt").read_text(encoding="utf-8") == "old"
    assert not (tmp_path / "target.old").exists()


# ---------------------------------------------------------------------------
# E2E
# ---------------------------------------------------------------------------


def test_e2e_md_input(tmp_path: Path) -> None:
    md = tmp_path / "input.md"
    md.write_text(
        "# Hello World\n\n"
        "This is a test document with enough text to avoid empty chunks.\n\n"
        "## Section A\n\n"
        "Some content here that is longer than a few characters so chunking behaves.\n\n"
        "### Subsection\n\n"
        "More details.\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    target = build_skill(
        skill_name="test-skill",
        title="Test Skill",
        inputs=[md],
        out_dir=out_dir,
        force=True,
        chunk_size=200,
        overlap=0,
        enable_graph_edges=False,
    )
    assert target.exists()

    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["skill_name"] == "test-skill"
    assert manifest["title"] == "Test Skill"
    assert len(manifest["docs"]) == 1
    doc_manifest = manifest["docs"][0]
    assert doc_manifest["title"] == "Hello World"

    # Headings index
    headings_idx = target / "indexes" / "headings"
    assert headings_idx.exists()
    assert any(headings_idx.iterdir())

    # DB checks
    import sqlite3

    conn = sqlite3.connect(str(target / "kb.sqlite"))
    try:
        cur = conn.execute("SELECT COUNT(*) FROM docs")
        assert cur.fetchone()[0] == 1
        cur = conn.execute("SELECT COUNT(*) FROM nodes")
        node_count = cur.fetchone()[0]
        assert node_count >= 2  # doc + at least one chunk
        cur = conn.execute(
            "SELECT kind FROM nodes WHERE doc_id = ?",
            (doc_manifest["doc_id"],),
        )
        kinds = {row[0] for row in cur.fetchall()}
        assert "doc" in kinds
        assert "chunk" in kinds
    finally:
        conn.close()

    # Canonical text
    canonical_path = target / "canonical_text" / f"{doc_manifest['doc_id']}--current.txt"
    assert canonical_path.exists()
    canonical_text = canonical_path.read_text(encoding="utf-8")
    assert "Hello World" in canonical_text


def test_e2e_force_rebuild_updates_manifest(tmp_path: Path) -> None:
    md = tmp_path / "input.md"
    md.write_text(
        "# First Title\n\n" + "Body text here that is long enough for chunking.\n" * 10,
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    target1 = build_skill(
        "skill1",
        "Title1",
        [md],
        out_dir,
        force=True,
        chunk_size=200,
        overlap=0,
        enable_graph_edges=False,
    )
    manifest1 = json.loads((target1 / "manifest.json").read_text(encoding="utf-8"))

    md.write_text(
        "# Second Title\n\n" + "Different body text that is also long enough for chunking.\n" * 10,
        encoding="utf-8",
    )
    target2 = build_skill(
        "skill1",
        "Title2",
        [md],
        out_dir,
        force=True,
        chunk_size=200,
        overlap=0,
        enable_graph_edges=False,
    )
    manifest2 = json.loads((target2 / "manifest.json").read_text(encoding="utf-8"))

    assert manifest2["title"] == "Title2"
    assert manifest2["docs"][0]["title"] == "Second Title"
    assert manifest2["generated_at"] != manifest1["generated_at"]
