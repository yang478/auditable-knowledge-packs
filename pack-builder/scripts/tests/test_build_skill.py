import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
import zipfile
import sqlite3
import platform
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]  # repo root
BUILDER = ROOT / "pack-builder" / "scripts" / "build_skill.py"
FIXTURE_ROOT = ROOT / "pack-builder" / "scripts" / "tests" / "fixtures" / "retrieval"


def fixture_text(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


def write_fixture(tmp_path: Path, name: str) -> Path:
    target = tmp_path / name
    target.write_text(fixture_text(name), encoding="utf-8")
    return target


def build_retrieval_skill(tmp_path: Path, *fixture_names: str) -> Path:
    input_paths = [write_fixture(tmp_path, name) for name in fixture_names]
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    subprocess.run(
        [
            "python3",
            str(BUILDER),
            "--skill-name",
            "my-books",
            "--out-dir",
            str(out_dir),
            "--inputs",
            *[str(path) for path in input_paths],
            "--title",
            "Controlled Retrieval KB",
        ],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(ROOT),
    )
    return out_dir / "my-books"

def build_skill_from_paths(
    tmp_path: Path,
    input_paths: list[Path],
    *,
    skill_name: str = "my-books",
    title: str = "Controlled Retrieval KB",
    extra_args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> Path:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    env = {**os.environ, "PYTHONUTF8": "1"}
    if extra_env:
        env.update(extra_env)
    subprocess.run(
        [
            "python3",
            str(BUILDER),
            "--skill-name",
            skill_name,
            "--out-dir",
            str(out_dir),
            "--inputs",
            *[str(path) for path in input_paths],
            "--title",
            title,
            *(extra_args or []),
        ],
        check=True,
        env=env,
        cwd=str(ROOT),
    )
    return out_dir / skill_name


def build_skill_from_ir_jsonl(
    tmp_path: Path,
    *,
    lines: list[dict],
    skill_name: str = "my-books",
    title: str = "IR Knowledge Base",
) -> Path:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    ir_path = tmp_path / "ir.jsonl"
    ir_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in lines) + "\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            "python3",
            str(BUILDER),
            "--skill-name",
            skill_name,
            "--out-dir",
            str(out_dir),
            "--ir-jsonl",
            str(ir_path),
            "--title",
            title,
        ],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(ROOT),
    )
    return out_dir / skill_name


def table_columns(db_path: Path, table_name: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    finally:
        conn.close()
    return {str(row[1]) for row in rows}


def run_bundle(generated: Path, *, query: str, extra_args: list[str] | None = None) -> str:
    bundle_path = generated / "bundle.md"
    args = [
        "python3",
        str(generated / "scripts" / "kbtool.py"),
        "bundle",
        "--query",
        query,
        "--out",
        str(bundle_path),
    ]
    if extra_args:
        args.extend(extra_args)
    subprocess.run(
        args,
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(generated),
    )
    return bundle_path.read_text(encoding="utf-8")


def run_search(generated: Path, *, query: str, extra_args: list[str] | None = None) -> str:
    search_path = generated / "search.md"
    args = [
        "python3",
        str(generated / "scripts" / "kbtool.py"),
        "search",
        "--query",
        query,
        "--out",
        str(search_path),
    ]
    if extra_args:
        args.extend(extra_args)
    subprocess.run(
        args,
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(generated),
    )
    return search_path.read_text(encoding="utf-8")


def switch_standard_metadata(generated: Path, fixture_name: str) -> None:
    title = fixture_text(fixture_name).splitlines()[0].removeprefix("# ").strip()
    metadata_path = generated / "references" / "standard-v1" / "metadata.md"
    metadata_path.write_text(
        f"# {title}\n\n- 源文件：`standard_v1.md`\n",
        encoding="utf-8",
    )


def run_reindex(generated: Path) -> str:
    proc = subprocess.run(
        ["python3", str(generated / "scripts" / "kbtool.py"), "reindex"],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(generated),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.stdout


def active_doc_title(db_path: Path, *, doc_id: str = "standard-v1") -> str:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT doc_title FROM docs WHERE doc_id = ? AND is_active = 1 ORDER BY source_version DESC LIMIT 1",
            (doc_id,),
        ).fetchone()
    finally:
        conn.close()
    return str(row[0]) if row else ""


def inactive_doc_titles(db_path: Path, *, doc_id: str = "standard-v1") -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT doc_title FROM docs WHERE doc_id = ? AND is_active = 0 ORDER BY source_version",
            (doc_id,),
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows]

def run_categories(generated: Path) -> str:
    out_path = generated / "categories.md"
    subprocess.run(
        ["python3", str(generated / "scripts" / "kbtool.py"), "categories", "--out", str(out_path)],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(generated),
    )
    return out_path.read_text(encoding="utf-8")


def run_docs(generated: Path, *, category: str) -> str:
    out_path = generated / "docs.md"
    subprocess.run(
        [
            "python3",
            str(generated / "scripts" / "kbtool.py"),
            "docs",
            "--category",
            category,
            "--out",
            str(out_path),
        ],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(generated),
    )
    return out_path.read_text(encoding="utf-8")


def run_kbtool_json(generated: Path, argv: list[str], *, cwd: Path | None = None) -> object:
    proc = subprocess.run(
        ["python3", str(generated / "scripts" / "kbtool.py"), *argv],
        check=True,
        env={**os.environ, "PYTHONUTF8": "1"},
        cwd=str(cwd or generated),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out = proc.stdout.strip()
    if not out:
        raise AssertionError(f"kbtool produced no stdout. stderr:\n{proc.stderr}")
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise AssertionError(f"kbtool stdout is not valid JSON: {e}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}") from e


def add_aliases_to_article(article_path: Path, aliases: list[str]) -> None:
    text = article_path.read_text(encoding="utf-8")
    marker = "---\n\n"
    if marker not in text:
        raise AssertionError(f"Missing frontmatter terminator in {article_path}")
    frontmatter, body = text.split(marker, 1)
    updated = frontmatter + f'aliases: {json.dumps(aliases, ensure_ascii=False)}\n' + marker + body
    article_path.write_text(updated, encoding="utf-8")


class BuildSkillTests(unittest.TestCase):
    def test_build_skill_from_ir_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_skill_from_ir_jsonl(
                tmp_path,
                lines=[
                    {
                        "type": "doc",
                        "doc_id": "standard-v1",
                        "title": "标准文本 V1",
                        "source_file": "standard_v1.md",
                        "source_version": "v1",
                    },
                    {
                        "type": "node",
                        "doc_id": "standard-v1",
                        "node_id": "standard-v1:article:0003",
                        "kind": "article",
                        "label": "第3条",
                        "title": "第3条 适用范围",
                        "ordinal": 3,
                        "body_md": "第3条 适用范围：这里是适用范围的正文。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "standard-v1",
                        "node_id": "standard-v1:block:0004",
                        "kind": "block",
                        "label": "block-0004",
                        "title": "仅正文命中块",
                        "ordinal": 4,
                        "body_md": "这里也提到了适用范围，但标题不包含关键字。\n",
                    },
                ],
            )
            self.assertTrue((generated / "kb.sqlite").exists())
            search = run_search(generated, query="适用范围")
            self.assertLess(search.index("article:003"), search.index("block:0004"))

    def test_build_generates_catalog_by_category(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "infectious_a.md"
            b = tmp_path / "infectious_b.md"
            c = tmp_path / "cardio_c.md"
            a.write_text("# Infectious Disease - Alpha\n\nA.\n", encoding="utf-8")
            b.write_text("# Infectious Disease - Beta\n\nB.\n", encoding="utf-8")
            c.write_text("# Cardiology - Gamma\n\nC.\n", encoding="utf-8")

            generated = build_skill_from_paths(tmp_path, [a, b, c])
            cats = generated / "catalog" / "categories.md"
            self.assertTrue(cats.exists())
            self.assertTrue((generated / "catalog" / "categories" / "infectious-disease.md").exists())
            self.assertTrue((generated / "catalog" / "categories" / "cardiology.md").exists())

            cats_md = cats.read_text(encoding="utf-8")
            self.assertIn("Infectious Disease", cats_md)
            self.assertIn("Cardiology", cats_md)

            infectious_md = (generated / "catalog" / "categories" / "infectious-disease.md").read_text(encoding="utf-8")
            self.assertIn("infectious-a", infectious_md)
            self.assertIn("infectious-b", infectious_md)

    def test_skill_md_omits_full_doc_list_when_over_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            inputs: list[Path] = []
            for i in range(6):
                p = tmp_path / f"bulk_{i}.md"
                p.write_text(f"# BulkCat - Doc{i}\n\nBody {i}.\n", encoding="utf-8")
                inputs.append(p)

            generated = build_skill_from_paths(
                tmp_path,
                inputs,
                extra_env={"PACK_BUILDER_SKILL_MD_DOCS_LIMIT": "2"},
            )
            skill_md = (generated / "SKILL.md").read_text(encoding="utf-8")
            self.assertIn("catalog/categories.md", skill_md)
            self.assertNotIn("bulk-5", skill_md)

    def test_kbtool_can_list_categories_and_docs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "infectious_a.md"
            b = tmp_path / "infectious_b.md"
            c = tmp_path / "cardio_c.md"
            a.write_text("# Infectious Disease - Alpha\n\nA.\n", encoding="utf-8")
            b.write_text("# Infectious Disease - Beta\n\nB.\n", encoding="utf-8")
            c.write_text("# Cardiology - Gamma\n\nC.\n", encoding="utf-8")

            generated = build_skill_from_paths(tmp_path, [a, b, c])
            cats = run_categories(generated)
            self.assertIn("Infectious Disease", cats)
            self.assertIn("Cardiology", cats)

            docs = run_docs(generated, category="Infectious Disease")
            self.assertIn("infectious-a", docs)
            self.assertIn("infectious-b", docs)
            self.assertNotIn("cardio-c", docs)

    def test_build_catalog_can_use_external_assignments(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "a.md"
            b = tmp_path / "b.md"
            a.write_text("# Totally Random Title\n\nA.\n", encoding="utf-8")
            b.write_text("# Another Random Title\n\nB.\n", encoding="utf-8")

            def sha1(text: str) -> str:
                return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

            taxonomy_path = tmp_path / "taxonomy.json"
            taxonomy_path.write_text(
                json.dumps(
                    {
                        "name": "domain-taxonomy",
                        "version": "v1",
                        "categories": [
                            {"id": "medical", "label": "医疗健康"},
                            {"id": "engineering", "label": "工程与计算"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments_path = tmp_path / "assignments.jsonl"
            assignments = [
                {"doc_hash": sha1(a.read_text(encoding="utf-8")), "primary_category_id": "engineering"},
                {"doc_hash": sha1(b.read_text(encoding="utf-8")), "primary_category_id": "medical"},
            ]
            assignments_path.write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in assignments) + "\n",
                encoding="utf-8",
            )

            generated = build_skill_from_paths(
                tmp_path,
                [a, b],
                extra_args=[
                    "--catalog-taxonomy",
                    str(taxonomy_path),
                    "--catalog-assignments",
                    str(assignments_path),
                ],
                extra_env={"PACK_BUILDER_SKILL_MD_DOCS_LIMIT": "0"},
            )

            cats_md = (generated / "catalog" / "categories.md").read_text(encoding="utf-8")
            self.assertIn("医疗健康", cats_md)
            self.assertIn("工程与计算", cats_md)
            self.assertNotIn("未分类", cats_md)
            self.assertTrue((generated / "catalog" / "categories" / "医疗健康.md").exists())
            self.assertTrue((generated / "catalog" / "categories" / "工程与计算.md").exists())

            skill_md = (generated / "SKILL.md").read_text(encoding="utf-8")
            self.assertIn("医疗健康", skill_md)
            self.assertIn("工程与计算", skill_md)
            self.assertNotIn("未分类", skill_md)

    def test_build_fails_on_unknown_assignment_category_id(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "a.md"
            a.write_text("# Totally Random Title\n\nA.\n", encoding="utf-8")

            def sha1(text: str) -> str:
                return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

            taxonomy_path = tmp_path / "taxonomy.json"
            taxonomy_path.write_text(
                json.dumps(
                    {
                        "name": "domain-taxonomy",
                        "version": "v1",
                        "categories": [
                            {"id": "medical", "label": "医疗健康"},
                            {"id": "engineering", "label": "工程与计算"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments_path = tmp_path / "assignments.jsonl"
            assignments_path.write_text(
                json.dumps({"doc_hash": sha1(a.read_text(encoding="utf-8")), "primary_category_id": "not-in-taxonomy"}, ensure_ascii=False)
                + "\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            proc = subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(a),
                    "--title",
                    "Controlled Retrieval KB",
                    "--catalog-taxonomy",
                    str(taxonomy_path),
                    "--catalog-assignments",
                    str(assignments_path),
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("not-in-taxonomy", proc.stdout)

    def test_build_fails_on_duplicate_doc_hash_in_assignments(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "a.md"
            a.write_text("# Totally Random Title\n\nA.\n", encoding="utf-8")

            def sha1(text: str) -> str:
                return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

            taxonomy_path = tmp_path / "taxonomy.json"
            taxonomy_path.write_text(
                json.dumps(
                    {
                        "name": "domain-taxonomy",
                        "version": "v1",
                        "categories": [
                            {"id": "medical", "label": "医疗健康"},
                            {"id": "engineering", "label": "工程与计算"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments_path = tmp_path / "assignments.jsonl"
            h = sha1(a.read_text(encoding="utf-8"))
            assignments_path.write_text(
                "\n".join(
                    [
                        json.dumps({"doc_hash": h, "primary_category_id": "medical"}, ensure_ascii=False),
                        json.dumps({"doc_hash": h, "primary_category_id": "engineering"}, ensure_ascii=False),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            proc = subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(a),
                    "--title",
                    "Controlled Retrieval KB",
                    "--catalog-taxonomy",
                    str(taxonomy_path),
                    "--catalog-assignments",
                    str(assignments_path),
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("duplicate", proc.stdout.lower())

    def test_kbtool_categories_and_docs_prefer_assignments_when_present(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            a = tmp_path / "a.md"
            b = tmp_path / "b.md"
            a.write_text("# Totally Random Title\n\nA.\n", encoding="utf-8")
            b.write_text("# Another Random Title\n\nB.\n", encoding="utf-8")

            def sha1(text: str) -> str:
                return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()

            taxonomy_path = tmp_path / "taxonomy.json"
            taxonomy_path.write_text(
                json.dumps(
                    {
                        "name": "domain-taxonomy",
                        "version": "v1",
                        "categories": [
                            {"id": "medical", "label": "医疗健康"},
                            {"id": "engineering", "label": "工程与计算"},
                            {"id": "other", "label": "综合/其他"},
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments_path = tmp_path / "assignments.jsonl"
            assignments = [
                {"doc_hash": sha1(a.read_text(encoding="utf-8")), "primary_category_id": "engineering"},
                {"doc_hash": sha1(b.read_text(encoding="utf-8")), "primary_category_id": "medical"},
            ]
            assignments_path.write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in assignments) + "\n",
                encoding="utf-8",
            )

            generated = build_skill_from_paths(
                tmp_path,
                [a, b],
                extra_args=[
                    "--catalog-taxonomy",
                    str(taxonomy_path),
                    "--catalog-assignments",
                    str(assignments_path),
                ],
            )

            cats = run_categories(generated)
            self.assertIn("医疗健康", cats)
            self.assertIn("工程与计算", cats)
            self.assertNotIn("未分类", cats)

            docs = run_docs(generated, category="医疗健康")
            self.assertIn("`b`", docs)
            self.assertNotIn("`a`", docs)

    def test_manifest_includes_active_version_and_doc_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            manifest = json.loads((generated / "manifest.json").read_text(encoding="utf-8"))
            doc = manifest["docs"][0]
            self.assertEqual(doc["doc_id"], "standard-v1")
            self.assertTrue(doc["active_version"])
            self.assertTrue(doc["doc_hash"])

    def test_kb_sqlite_contains_node_hash_and_raw_span_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            conn = sqlite3.connect(generated / "kb.sqlite")
            try:
                row = conn.execute("SELECT node_hash, raw_span_start, raw_span_end FROM nodes LIMIT 1").fetchone()
            finally:
                conn.close()

            self.assertTrue(row[0])
            self.assertGreaterEqual(row[1], 0)
            self.assertGreater(row[2], row[1])

    def test_nodes_table_has_v1_metadata_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            cols = table_columns(generated / "kb.sqlite", "nodes")

            self.assertIn("node_hash", cols)
            self.assertIn("raw_span_start", cols)
            self.assertIn("raw_span_end", cols)
            self.assertIn("confidence", cols)

    def test_docs_table_has_version_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            cols = table_columns(generated / "kb.sqlite", "docs")

            self.assertIn("doc_hash", cols)
            self.assertIn("source_version", cols)
            self.assertIn("is_active", cols)

    def test_rebuild_keeps_old_version_until_atomic_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            switch_standard_metadata(generated, "standard_v2.md")
            run_reindex(generated)

            self.assertEqual(active_doc_title(generated / "kb.sqlite"), "标准文本 V2")
            self.assertIn("标准文本 V1", inactive_doc_titles(generated / "kb.sqlite"))

    def test_build_creates_references_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            conn = sqlite3.connect(generated / "kb.sqlite")
            try:
                rows = conn.execute(
                    "SELECT edge_type, from_node_id, to_node_id FROM edges WHERE edge_type='references'"
                ).fetchall()
            finally:
                conn.close()

            self.assertGreaterEqual(len(rows), 1)

    def test_bundle_can_include_referenced_article_when_triggered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="适用范围", extra_args=["--debug-triggers"])
            self.assertIn("补查触发", bundle)
            self.assertIn("references/", bundle)

    def test_exact_alias_matches_without_body_token_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="质保期")
            self.assertIn("质量保证期限", bundle)

    def test_soft_alias_requires_supporting_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            search = run_search(generated, query="上线")
            self.assertNotIn("仅软别名命中的噪声节点", search)

    def test_query_normalization_handles_chinese_numerals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="第三条的适用范围")
            self.assertIn("`standard-v1:article:003`", bundle)

    def test_title_hits_rank_above_body_hits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            search = run_search(generated, query="适用范围")
            node_ids = re.findall(r"^- node_id: `([^`]+)`", search, flags=re.M)
            self.assertIn("standard-v1:article:003", node_ids)
            first_block_idx = next(i for i, nid in enumerate(node_ids) if ":block:" in nid)
            self.assertLess(node_ids.index("standard-v1:article:003"), first_block_idx)

    def test_kbtool_hooks_can_filter_hits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            hooks_dir = generated / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)
            (hooks_dir / "post_search.py").write_text(
                "\n".join(
                    [
                        "def run(payload):",
                        "    hits = payload.get('hits') or []",
                        "    hits = [h for h in hits if ':block:' not in h]",
                        "    return {'hits': hits}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            baseline = run_search(generated, query="适用范围")
            baseline_node_ids = re.findall(r"^- node_id: `([^`]+)`", baseline, flags=re.M)
            self.assertTrue(any(":block:" in nid for nid in baseline_node_ids))

            filtered = run_search(generated, query="适用范围", extra_args=["--enable-hooks"])
            filtered_node_ids = re.findall(r"^- node_id: `([^`]+)`", filtered, flags=re.M)
            self.assertFalse(any(":block:" in nid for nid in filtered_node_ids))

    def test_kbtool_pre_search_hook_can_rewrite_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            hooks_dir = generated / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)
            (hooks_dir / "pre_search.py").write_text(
                "\n".join(
                    [
                        "def run(payload):",
                        "    q = str(payload.get('query') or '')",
                        "    if q == '保修期':",
                        "        return {'query': '质保期'}",
                        "    return {}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaises(subprocess.CalledProcessError):
                run_search(generated, query="保修期")

            rewritten = run_search(generated, query="保修期", extra_args=["--enable-hooks"])
            self.assertIn("质量保证期限", rewritten)

    def test_kbtool_pre_expand_hook_can_add_node_to_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            hooks_dir = generated / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)
            (hooks_dir / "pre_expand.py").write_text(
                "\n".join(
                    [
                        "def run(payload):",
                        "    hits = payload.get('hits') or []",
                        "    hits = list(hits)",
                        "    hits.append('standard-v1:article:0004')",
                        "    return {'hits': hits}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            baseline = run_bundle(generated, query="质量保证期限")
            self.assertNotIn("article:004", baseline)

            expanded = run_bundle(generated, query="质量保证期限", extra_args=["--enable-hooks"])
            self.assertIn("article:004", expanded)

    def test_kbtool_pre_render_hook_can_redact_bundle_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            hooks_dir = generated / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)
            (hooks_dir / "pre_render.py").write_text(
                "\n".join(
                    [
                        "def run(payload):",
                        "    node = payload.get('node') or {}",
                        "    node_id = str(node.get('node_id') or '')",
                        "    if node_id.endswith('standard-v1:article:0002'):",
                        "        return {'body_md': 'REDACTED\\n'}",
                        "    return {}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            baseline = run_bundle(generated, query="质量保证期限")
            self.assertNotIn("REDACTED", baseline)

            redacted = run_bundle(generated, query="质量保证期限", extra_args=["--enable-hooks"])
            self.assertIn("REDACTED", redacted)
            self.assertIn("hook: pre_render", redacted)

    def test_triggered_expansion_adds_definition_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="定义是什么")
            self.assertIn("## 补查记录", bundle)
            self.assertIn("definition", bundle)

    def test_triggered_expansion_runs_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="适用范围和例外")
            self.assertEqual(bundle.count("## 补查记录"), 1)

    def test_reindex_rebuilds_shadow_db_before_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            switch_standard_metadata(generated, "standard_v2.md")
            stdout = run_reindex(generated)
            self.assertIn("shadow rebuild", stdout)
            self.assertIn("atomic switch", stdout)

    def test_only_one_doc_version_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            switch_standard_metadata(generated, "standard_v2.md")
            run_reindex(generated)

            conn = sqlite3.connect(generated / "kb.sqlite")
            try:
                active = conn.execute(
                    "SELECT COUNT(*) FROM docs WHERE doc_id = ? AND is_active = 1",
                    ("standard-v1",),
                ).fetchone()[0]
                versions = conn.execute(
                    "SELECT COUNT(*) FROM docs WHERE doc_id = ?",
                    ("standard-v1",),
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(active, 1)
            self.assertEqual(versions, 2)

    def test_reindex_keeps_inactive_nodes_edges_and_aliases_for_old_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            add_aliases_to_article(
                generated / "references" / "standard-v1" / "articles" / "article-0002.md",
                ["保修期"],
            )
            run_reindex(generated)
            switch_standard_metadata(generated, "standard_v2.md")
            run_reindex(generated)

            conn = sqlite3.connect(generated / "kb.sqlite")
            try:
                inactive_nodes = conn.execute(
                    "SELECT COUNT(*) FROM nodes WHERE doc_id = ? AND source_version = ? AND is_active = 0",
                    ("standard-v1", "v1"),
                ).fetchone()[0]
                inactive_edges = conn.execute(
                    "SELECT COUNT(*) FROM edges WHERE source_version = ? AND is_active = 0",
                    ("v1",),
                ).fetchone()[0]
                inactive_aliases = conn.execute(
                    "SELECT COUNT(*) FROM aliases WHERE normalized_alias = ? AND source_version = ? AND is_active = 0",
                    ("保修期", "v1"),
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertGreater(inactive_nodes, 0)
            self.assertGreater(inactive_edges, 0)
            self.assertGreater(inactive_aliases, 0)

    def test_force_rebuild_preserves_previous_doc_graph_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out_dir = tmp_path / "out"
            out_dir.mkdir()
            standard_v1 = write_fixture(tmp_path, "standard_v1.md")
            handbook = write_fixture(tmp_path, "handbook.md")

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(standard_v1),
                    str(handbook),
                    "--title",
                    "Controlled Retrieval KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / "my-books"
            standard_v1.write_text(fixture_text("standard_v2.md"), encoding="utf-8")
            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(standard_v1),
                    str(handbook),
                    "--title",
                    "Controlled Retrieval KB",
                    "--force",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            self.assertEqual(active_doc_title(generated / "kb.sqlite"), "标准文本 V2")
            self.assertIn("标准文本 V1", inactive_doc_titles(generated / "kb.sqlite"))

            conn = sqlite3.connect(generated / "kb.sqlite")
            try:
                inactive_nodes = conn.execute(
                    "SELECT COUNT(*) FROM nodes WHERE doc_id = ? AND source_version = ? AND is_active = 0",
                    ("standard-v1", "v1"),
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertGreater(inactive_nodes, 0)

    def test_curated_reference_alias_can_extend_recall_without_soft_matching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            add_aliases_to_article(
                generated / "references" / "standard-v1" / "articles" / "article-0002.md",
                ["保修期"],
            )
            run_reindex(generated)

            bundle = run_bundle(generated, query="保修期")
            self.assertIn("质量保证期限", bundle)

    def test_generated_skill_mentions_triggered_expansion_and_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            skill_md = (generated / "SKILL.md").read_text(encoding="utf-8")
            self.assertIn("标题/正文/术语 三路召回", skill_md)
            self.assertIn("一轮补查", skill_md)
            self.assertIn("原子重建", skill_md)

    def test_generated_skill_includes_llm_tldr_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            skill_md = (generated / "SKILL.md").read_text(encoding="utf-8")
            self.assertIn("TL;DR (LLM/Agent)", skill_md)
            self.assertIn("./kbtool --skill", skill_md)
            self.assertIn("./kbtool bundle --query", skill_md)
            self.assertIn("## 参考依据", skill_md)
            self.assertIn("不要直接调用 `scripts/` 或 `bin/`", skill_md)

    def test_build_skill_creates_kb_skill_from_md(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "book.md"
            input_md.write_text(
                "# Chapter 1 Intro\n\nHello.\n\n## 1.1 Scope\n\nDetails.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            self.assertTrue((generated / "SKILL.md").exists())
            self.assertTrue((generated / "manifest.json").exists())
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            self.assertTrue((doc_dirs[0] / "toc.md").exists())
            self.assertTrue((generated / "indexes" / "headings" / "_shards.tsv").exists())
            self.assertTrue((generated / "indexes" / "kw" / "_shards.tsv").exists())

    def test_build_skill_outputs_sqlite_and_kbtool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "book.md"
            input_md.write_text(
                "# Chapter 1 Intro\n\nHello.\n\n## 1.1 Scope\n\nDetails.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            self.assertTrue((generated / "kb.sqlite").exists())
            self.assertTrue((generated / "scripts" / "kbtool.py").exists())

    def test_kbtool_bundle_produces_bundle_with_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "book.md"
            input_md.write_text(
                "# Chapter 1 Intro\n\nHello.\n\n## 1.1 Scope\n\nDetails.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "Scope",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("## 参考依据", bundle)
            self.assertIn("references/", bundle)

    def test_bundle_limit_orders_by_bm25_best_match_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            long_tail = "x" * 3500
            input_md.write_text(
                f"Paragraph one foo. {long_tail}\n\n"
                "Paragraph two " + ("foo " * 800) + "\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "foo",
                    "--limit",
                    "1",
                    "--neighbors",
                    "0",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("`plain:block:0002`", bundle)
            self.assertNotIn("`plain:block:0001`", bundle)

    def test_bundle_long_repetitive_query_still_matches_rare_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            unique = "UNIQUE_TOKEN_123"
            input_md.write_text(
                f"Only this paragraph contains {unique}.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            long_query = ("foo " * 70 + unique).strip()
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    long_query,
                    "--neighbors",
                    "0",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn(unique, bundle)

    def test_bundle_order_chronological_outputs_in_document_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            long_tail = "x" * 3500
            input_md.write_text(
                f"Paragraph one foo. {long_tail}\n\n"
                "Paragraph two " + ("foo " * 800) + "\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "foo",
                    "--limit",
                    "2",
                    "--neighbors",
                    "0",
                    "--order",
                    "chronological",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertLess(bundle.index("`plain:block:0001`"), bundle.index("`plain:block:0002`"))

    def test_bundle_reference_citations_include_node_id_and_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_skill_from_ir_jsonl(
                tmp_path,
                lines=[
                    {
                        "type": "doc",
                        "doc_id": "standard-v1",
                        "title": "标准文本 V1",
                        "source_file": "GB-TEST-002.md",
                        "source_path": "books/GB-TEST-002.md",
                        "source_version": "v1",
                    },
                    {
                        "type": "node",
                        "doc_id": "standard-v1",
                        "node_id": "standard-v1:article:0003",
                        "kind": "article",
                        "label": "第3条",
                        "title": "第3条 适用范围",
                        "ordinal": 3,
                        "body_md": "第3条 适用范围：这里是适用范围的正文。\n",
                    },
                ],
            )

            bundle = run_bundle(generated, query="适用范围")
            ref_section = bundle.split("## 参考依据\n", 1)[1]
            self.assertIn("standard-v1:article:003", ref_section)
            self.assertIn("books/GB-TEST-002.md", ref_section)

    def test_bundle_body_mode_snippet_limits_context_and_marks_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            long_body = "foo " + ("x" * 2000) + " TAILMARKER\n"
            generated = build_skill_from_ir_jsonl(
                tmp_path,
                lines=[
                    {
                        "type": "doc",
                        "doc_id": "doc1",
                        "title": "Big Doc",
                        "source_file": "big.md",
                        "source_path": "books/big.md",
                        "source_version": "current",
                    },
                    {
                        "type": "node",
                        "doc_id": "doc1",
                        "node_id": "doc1:article:0001",
                        "kind": "article",
                        "label": "第1条",
                        "title": "第1条 Foo",
                        "ordinal": 1,
                        "body_md": long_body,
                    },
                ],
            )

            bundle = run_bundle(
                generated,
                query="foo",
                extra_args=[
                    "--body",
                    "snippet",
                    "--neighbors",
                    "0",
                    "--limit",
                    "5",
                    "--per-node-max-chars",
                    "120",
                    "--max-chars",
                    "2000",
                ],
            )
            self.assertIn("*(SNIPPET)*", bundle)
            self.assertNotIn("TAILMARKER", bundle)

    def test_bundle_query_mode_and_requires_all_terms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            long_tail = "x" * 3500
            input_md.write_text(
                f"Paragraph one foo. {long_tail}\n\n"
                f"Paragraph two bar. {long_tail}\n\n"
                f"Paragraph three foo bar. {long_tail}\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "foo bar",
                    "--query-mode",
                    "and",
                    "--neighbors",
                    "0",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("`plain:block:0003`", bundle)
            self.assertNotIn("`plain:block:0001`", bundle)
            self.assertNotIn("`plain:block:0002`", bundle)

    def test_bundle_must_terms_filter_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            long_tail = "x" * 3500
            input_md.write_text(
                f"Paragraph one foo. {long_tail}\n\n"
                f"Paragraph two foo. {long_tail}\n\n"
                f"Paragraph three foo bar. {long_tail}\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "foo",
                    "--must",
                    "bar",
                    "--neighbors",
                    "0",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("`plain:block:0003`", bundle)
            self.assertNotIn("`plain:block:0001`", bundle)
            self.assertNotIn("`plain:block:0002`", bundle)

    def test_bundle_emits_search_trace_and_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            first = run_bundle(generated, query="适用范围")
            second = run_bundle(generated, query="适用范围")
            self.assertEqual(first, second)

            self.assertIn("## 检索轨迹", first)
            self.assertIn("round=0", first)
            self.assertIn("stop:", first)

    def test_kbtool_refuses_to_write_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")
            outside = generated.parent / "pwn.md"

            proc = subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "适用范围",
                    "--neighbors",
                    "0",
                    "--out",
                    "../pwn.md",
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("Refusing to write outside skill root", proc.stdout)
            self.assertFalse(outside.exists())

    def test_bundle_accepts_timeout_ms_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            bundle = run_bundle(generated, query="适用范围", extra_args=["--timeout-ms", "5000", "--neighbors", "0"])
            self.assertIn("## 参考依据", bundle)

    def test_kbtool_hook_allowlist_enforced_when_present(self) -> None:
        import hashlib

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            hooks_dir = generated / "hooks"
            hooks_dir.mkdir(parents=True, exist_ok=True)
            hook_path = hooks_dir / "pre_search.py"
            hook_path.write_text(
                "\n".join(
                    [
                        "def run(payload):",
                        "    return {}",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            digest = hashlib.sha1(hook_path.read_bytes()).hexdigest()
            (hooks_dir / "allowlist.sha1").write_text("sha1=deadbeef\n", encoding="utf-8")

            proc = subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "search",
                    "--query",
                    "适用范围",
                    "--out",
                    str(generated / "search.md"),
                    "--enable-hooks",
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("Hook not allowlisted", proc.stdout)

            (hooks_dir / "allowlist.sha1").write_text(f"sha1={digest}\n", encoding="utf-8")
            proc2 = subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "search",
                    "--query",
                    "适用范围",
                    "--out",
                    str(generated / "search.md"),
                    "--enable-hooks",
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.assertEqual(proc2.returncode, 0, proc2.stdout)

    def test_bundle_iterative_search_focuses_to_few_articles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_skill_from_ir_jsonl(
                tmp_path,
                lines=[
                    {
                        "type": "doc",
                        "doc_id": "demo",
                        "title": "Demo Doc",
                        "source_file": "demo.md",
                        "source_version": "v1",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0001",
                        "kind": "article",
                        "label": "第1条",
                        "title": "第1条 流程",
                        "ordinal": 1,
                        "body_md": "本条只包含：流程。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0002",
                        "kind": "article",
                        "label": "第2条",
                        "title": "第2条 审批",
                        "ordinal": 2,
                        "body_md": "本条只包含：审批。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0003",
                        "kind": "article",
                        "label": "第3条",
                        "title": "第3条 回滚",
                        "ordinal": 3,
                        "body_md": "本条只包含：回滚。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0004",
                        "kind": "article",
                        "label": "第4条",
                        "title": "第4条 流程与审批",
                        "ordinal": 4,
                        "body_md": "本条包含：流程 审批。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0005",
                        "kind": "article",
                        "label": "第5条",
                        "title": "第5条 流程审批回滚总览",
                        "ordinal": 5,
                        "body_md": "本条包含：流程 审批 回滚。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0006",
                        "kind": "article",
                        "label": "第6条",
                        "title": "第6条 其他",
                        "ordinal": 6,
                        "body_md": "无关内容。\n",
                    },
                ],
            )

            bundle = run_bundle(generated, query="流程 审批 回滚")
            self.assertIn("## 检索轨迹", bundle)
            self.assertIn("round=1", bundle)
            self.assertIn("query_mode=and", bundle)

            node_ids = re.findall(r"- node_id: `([^`]+)`", bundle)
            article_ids = {nid for nid in node_ids if ":article:" in nid}
            self.assertLessEqual(len(article_ids), 3)
            self.assertTrue(any(nid.endswith(":article:005") for nid in article_ids))

    def test_bundle_iterative_search_can_be_capped_to_single_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_skill_from_ir_jsonl(
                tmp_path,
                lines=[
                    {
                        "type": "doc",
                        "doc_id": "demo",
                        "title": "Demo Doc",
                        "source_file": "demo.md",
                        "source_version": "v1",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0001",
                        "kind": "article",
                        "label": "第1条",
                        "title": "第1条 流程",
                        "ordinal": 1,
                        "body_md": "本条只包含：流程。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0002",
                        "kind": "article",
                        "label": "第2条",
                        "title": "第2条 审批",
                        "ordinal": 2,
                        "body_md": "本条只包含：审批。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0003",
                        "kind": "article",
                        "label": "第3条",
                        "title": "第3条 回滚",
                        "ordinal": 3,
                        "body_md": "本条只包含：回滚。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0004",
                        "kind": "article",
                        "label": "第4条",
                        "title": "第4条 流程与审批",
                        "ordinal": 4,
                        "body_md": "本条包含：流程 审批。\n",
                    },
                    {
                        "type": "node",
                        "doc_id": "demo",
                        "node_id": "demo:article:0005",
                        "kind": "article",
                        "label": "第5条",
                        "title": "第5条 流程审批回滚总览",
                        "ordinal": 5,
                        "body_md": "本条包含：流程 审批 回滚。\n",
                    },
                ],
            )

            bundle = run_bundle(generated, query="流程 审批 回滚", extra_args=["--iter-max-rounds", "1"])
            self.assertIn("## 检索轨迹", bundle)
            self.assertIn("round=0", bundle)
            self.assertNotIn("round=1", bundle)

    def test_kbtool_search_writes_markdown_with_snippets_and_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            input_md.write_text(
                "Paragraph one foo.\n\n"
                "Paragraph two bar.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            search_path = generated / "search.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "search",
                    "--query",
                    "foo",
                    "--out",
                    str(search_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            search_md = search_path.read_text(encoding="utf-8")
            self.assertIn("references/", search_md)
            self.assertIn("`plain:block:0001`", search_md)
            self.assertIn("foo", search_md)

    def test_kbtool_reindex_updates_search_after_reference_edit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "GB-TEST-003.md"
            input_md.write_text(
                "# 总则\n\n"
                "第1条 目的\n"
                "（一）适用范围A。\n"
                "（二）适用范围B。\n\n"
                "第2条 定义\n"
                "（一）术语A。\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            doc_dir = doc_dirs[0]
            articles = sorted((doc_dir / "articles").glob("article-*.md"))
            self.assertGreaterEqual(len(articles), 1)

            unique = "UNIQUE_TOKEN_123"
            articles[0].write_text(articles[0].read_text(encoding="utf-8") + f"\n\n{unique}\n", encoding="utf-8")

            bundle_path = generated / "bundle.md"
            proc_before = subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    unique,
                    "--out",
                    str(bundle_path),
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertNotEqual(proc_before.returncode, 0)

            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "reindex",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    unique,
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn(unique, bundle)

    def test_chinese_articles_and_items_index_and_bundle_elevates_to_article(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "GB-TEST-001.md"
            input_md.write_text(
                "# 总则\n\n"
                "第1条 目的\n"
                "（一）适用范围A。\n"
                "（二）适用范围B。\n\n"
                "第2条 定义\n"
                "（一）术语A。\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            doc_dir = doc_dirs[0]
            self.assertTrue((doc_dir / "articles").exists())
            self.assertTrue((doc_dir / "items").exists())

            conn = sqlite3.connect(str(generated / "kb.sqlite"))
            try:
                article_count = conn.execute("SELECT COUNT(*) FROM nodes WHERE kind='article'").fetchone()[0]
                item_count = conn.execute("SELECT COUNT(*) FROM nodes WHERE kind='item'").fetchone()[0]
            finally:
                conn.close()

            self.assertGreaterEqual(article_count, 2)
            self.assertGreaterEqual(item_count, 3)

            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "术语A",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("## 参考依据", bundle)
            self.assertIn("/articles/", bundle)
            self.assertNotIn("/items/", bundle)

    def test_markdown_heading_articles_are_parsed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "GB-TEST-002.md"
            input_md.write_text(
                "# 总则\n\n"
                "#### 第1条 目的\n"
                "（一）适用范围A。\n"
                "（二）适用范围B。\n\n"
                "#### 第2条 定义\n"
                "（一）术语A。\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            doc_dir = doc_dirs[0]
            self.assertTrue((doc_dir / "articles").exists())

            conn = sqlite3.connect(str(generated / "kb.sqlite"))
            try:
                article_count = conn.execute("SELECT COUNT(*) FROM nodes WHERE kind='article'").fetchone()[0]
            finally:
                conn.close()

            self.assertGreaterEqual(article_count, 2)

            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "术语A",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )
            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("/articles/", bundle)

    def test_no_headings_generates_blocks_and_bundle_uses_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "plain.md"
            input_md.write_text(
                "Paragraph one about Alpha.\n\n"
                "Paragraph two about Beta.\n\n"
                "Paragraph three about Gamma.\n",
                encoding="utf-8",
            )

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            doc_dir = doc_dirs[0]
            self.assertTrue((doc_dir / "blocks").exists())
            block_files = sorted((doc_dir / "blocks").glob("*.md"))
            self.assertGreaterEqual(len(block_files), 1)

            bundle_path = generated / "bundle.md"
            subprocess.run(
                [
                    "python3",
                    str(generated / "scripts" / "kbtool.py"),
                    "bundle",
                    "--query",
                    "Beta",
                    "--out",
                    str(bundle_path),
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(generated),
            )

            bundle = bundle_path.read_text(encoding="utf-8")
            self.assertIn("/blocks/", bundle)

    def test_invalid_docx_fails_with_actionable_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bad_docx = tmp_path / "bad.docx"
            with zipfile.ZipFile(bad_docx, "w") as zf:
                zf.writestr("word/document.xml", b"<w:document>")  # malformed

            out_dir = tmp_path / "out"
            out_dir.mkdir()

            proc = subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "bad-docx-kb",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(bad_docx),
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("DOCX", proc.stderr.upper())
            self.assertTrue(("DOCX →" in proc.stderr) or ("converting DOCX" in proc.stderr))

    def test_pdf_without_pdftotext_instructs_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            dummy_pdf = tmp_path / "a.pdf"
            dummy_pdf.write_bytes(b"%PDF-1.4\n% dummy\n")

            out_dir = tmp_path / "out"
            out_dir.mkdir()

            proc = subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "pdf-kb",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(dummy_pdf),
                ],
                env={**os.environ, "PYTHONUTF8": "1", "BOOK_SKILL_GENERATOR_NO_PDFTOTEXT": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("pdftotext", proc.stderr)

    def test_pdf_without_pdftotext_can_try_pypdf_fallback_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            dummy_pdf = tmp_path / "a.pdf"
            dummy_pdf.write_bytes(b"%PDF-1.4\n% dummy\n")

            out_dir = tmp_path / "out"
            out_dir.mkdir()

            proc = subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    "pdf-kb",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(dummy_pdf),
                    "--pdf-fallback",
                    "pypdf",
                ],
                env={**os.environ, "PYTHONUTF8": "1", "BOOK_SKILL_GENERATOR_NO_PDFTOTEXT": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("pypdf", proc.stderr.lower())

    def test_doc_id_derives_from_standardized_filename_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = tmp_path / "国标GB／T1234－2020 信息安全技术.md"
            input_md.write_text("# 标题\n\n内容。\n", encoding="utf-8")

            out_dir = tmp_path / "out"
            out_dir.mkdir()
            skill_name = "my-books"

            subprocess.run(
                [
                    "python3",
                    str(BUILDER),
                    "--skill-name",
                    skill_name,
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "My Books KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            generated = out_dir / skill_name
            refs_root = generated / "references"
            doc_dirs = [p for p in refs_root.iterdir() if p.is_dir()]
            self.assertEqual(len(doc_dirs), 1)
            self.assertEqual(doc_dirs[0].name, "gb-t-1234-2020")

    def test_kbtool_skill_flag_prints_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            data = run_kbtool_json(generated, ["--skill"])
            self.assertIsInstance(data, dict)
            self.assertEqual(data.get("tool"), "kbtool")
            self.assertEqual(data.get("deterministic"), True)
            self.assertEqual(data.get("skill", {}).get("name"), "my-books")
            self.assertIn("commands", data)
            cmd_names = {c.get("name") for c in data.get("commands", []) if isinstance(c, dict)}
            self.assertIn("bundle", cmd_names)
            self.assertIn("search", cmd_names)
            self.assertIn("get-node", cmd_names)
            self.assertIn("get-children", cmd_names)
            self.assertIn("get-parent", cmd_names)
            self.assertIn("get-siblings", cmd_names)
            self.assertIn("follow-references", cmd_names)

    def test_kbtool_atomic_commands_return_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            node = run_kbtool_json(generated, ["get-node", "standard-v1:article:0003"])
            self.assertEqual(node.get("node_id"), "standard-v1:article:0003")
            self.assertEqual(node.get("doc_id"), "standard-v1")
            self.assertEqual(node.get("kind"), "article")
            self.assertIn("适用范围", node.get("title", ""))
            self.assertIn("body_md", node)

            children = run_kbtool_json(generated, ["get-children", "standard-v1:section:chapter01/section-01-01"])
            self.assertIsInstance(children, dict)
            child_ids = [c.get("node_id") for c in children.get("children", []) if isinstance(c, dict)]
            self.assertIn("standard-v1:article:0001", child_ids)
            self.assertIn("standard-v1:article:0004", child_ids)

            parent = run_kbtool_json(generated, ["get-parent", "standard-v1:article:0002"])
            self.assertEqual(parent.get("parent", {}).get("node_id"), "standard-v1:section:chapter01/section-01-01")

            sibs = run_kbtool_json(generated, ["get-siblings", "standard-v1:article:0002", "--neighbors", "1"])
            self.assertIsInstance(sibs, dict)
            sib_ids = [n.get("node_id") for n in sibs.get("nodes", []) if isinstance(n, dict)]
            self.assertEqual(sib_ids, ["standard-v1:article:0001", "standard-v1:article:0002", "standard-v1:article:0003"])

            refs = run_kbtool_json(generated, ["follow-references", "standard-v1:article:0003", "--direction", "out"])
            self.assertIsInstance(refs, dict)
            ref_ids = [n.get("node_id") for n in refs.get("nodes", []) if isinstance(n, dict)]
            self.assertIn("standard-v1:article:0001", ref_ids)
            self.assertIn("standard-v1:article:0004", ref_ids)

    def test_kbtool_auto_root_works_outside_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as other:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            other_cwd = Path(other)
            data = run_kbtool_json(generated, ["--skill"], cwd=other_cwd)
            self.assertEqual(data.get("skill", {}).get("name"), "my-books")

    def test_build_skill_package_kbtool_flag_is_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = write_fixture(tmp_path, "standard_v1.md")
            out_dir = tmp_path / "out"
            out_dir.mkdir()

            proc = subprocess.run(
                [
                    sys.executable,
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "Controlled Retrieval KB",
                    "--package-kbtool",
                ],
                env={**os.environ, "PYTHONUTF8": "1", "PATH": ""},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout)
            generated = out_dir / "my-books"
            self.assertTrue((generated / "kb.sqlite").exists())

    def test_force_rebuild_preserves_existing_bin_binaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_md = write_fixture(tmp_path, "standard_v1.md")
            out_dir = tmp_path / "out"
            out_dir.mkdir()

            subprocess.run(
                [
                    sys.executable,
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "Controlled Retrieval KB",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )
            generated = out_dir / "my-books"
            bin_file = generated / "bin" / "windows-x86_64" / "kbtool.exe"
            bin_file.parent.mkdir(parents=True, exist_ok=True)
            bin_file.write_text("dummy", encoding="utf-8")

            subprocess.run(
                [
                    sys.executable,
                    str(BUILDER),
                    "--skill-name",
                    "my-books",
                    "--out-dir",
                    str(out_dir),
                    "--inputs",
                    str(input_md),
                    "--title",
                    "Controlled Retrieval KB",
                    "--force",
                ],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
            )

            self.assertTrue(bin_file.exists(), "Expected existing bin/ binaries to be preserved on --force rebuild")

    def test_generated_skill_writes_root_kbtool_wrappers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            kbtool = generated / "kbtool"
            kbtool_cmd = generated / "kbtool.cmd"
            self.assertTrue(kbtool.exists())
            self.assertTrue(kbtool_cmd.exists())

            proc = subprocess.run(
                [str(kbtool), "--skill"],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(tmp_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            data = json.loads(proc.stdout)
            self.assertEqual(data.get("tool"), "kbtool")

    def test_generated_skill_writes_kbtool_sha1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            sha_path = generated / "kbtool.sha1"
            self.assertTrue(sha_path.exists())
            sha = sha_path.read_text(encoding="utf-8").strip()
            self.assertRegex(sha, r"^[0-9a-f]{40}$")

    def test_kbtool_wrapper_prefers_fresh_binary_and_falls_back_when_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generated = build_retrieval_skill(tmp_path, "standard_v1.md", "handbook.md")

            system = (platform.system() or "").lower()
            if system.startswith("windows"):
                os_tag = "windows"
            elif system.startswith("linux"):
                os_tag = "linux"
            elif system.startswith("darwin"):
                os_tag = "macos"
            else:
                os_tag = system or "unknown"

            machine = (platform.machine() or "").lower()
            if machine in {"amd64", "x64"}:
                machine = "x86_64"
            if machine in {"aarch64"}:
                machine = "arm64"
            arch_tag = machine or "unknown"

            plat = f"{os_tag}-{arch_tag}"
            bin_dir = generated / "bin" / plat
            bin_dir.mkdir(parents=True, exist_ok=True)
            fake_bin = bin_dir / ("kbtool.exe" if os_tag == "windows" else "kbtool")
            fake_bin.write_text("#!/usr/bin/env sh\necho BINARY\n", encoding="utf-8")
            fake_bin.chmod(0o755)

            # Make hashes match => wrapper should use the binary
            root_sha = generated / "kbtool.sha1"
            root_sha.write_text("abc\n", encoding="utf-8")
            (bin_dir / "kbtool.sha1").write_text("abc\n", encoding="utf-8")

            proc = subprocess.run(
                [str(generated / "kbtool"), "--skill"],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(tmp_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(proc.stdout.strip(), "BINARY")

            # Make hashes mismatch => wrapper should fall back to python script (JSON output)
            (bin_dir / "kbtool.sha1").write_text("deadbeef\n", encoding="utf-8")
            proc2 = subprocess.run(
                [str(generated / "kbtool"), "--skill"],
                check=True,
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(tmp_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            data = json.loads(proc2.stdout)
            self.assertEqual(data.get("tool"), "kbtool")


if __name__ == "__main__":
    unittest.main()
