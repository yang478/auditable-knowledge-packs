import json
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]  # repo root
CATALOGER = ROOT / "pack-builder" / "scripts" / "llm_catalog.py"


class _MockChatHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:  # noqa: A002 - match base signature
        return

    def do_POST(self) -> None:  # noqa: N802 - match base signature
        if self.path != "/v1/chat/completions":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("content-length", "0") or "0")
        _ = self.rfile.read(length) if length else b""

        state = getattr(self.server, "_state", None)  # type: ignore[attr-defined]
        if not isinstance(state, dict):
            self.send_response(500)
            self.end_headers()
            return

        i = int(state.get("i", 0))
        responses = state.get("responses") or []
        if i >= len(responses):
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"no more mock responses")
            return

        content = responses[i]
        state["i"] = i + 1

        body = json.dumps({"choices": [{"message": {"content": content}}]}).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_mock_llm(responses: list[str]) -> tuple[HTTPServer, str]:
    httpd = HTTPServer(("127.0.0.1", 0), _MockChatHandler)
    httpd._state = {"i": 0, "responses": responses}  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address
    base_url = f"http://{host}:{port}/v1"
    return httpd, base_url


def _write_llm_config(path: Path, *, base_url: str) -> None:
    path.write_text(
        json.dumps(
            {"llm": {"base_url": base_url, "api_key": "", "model": "mock", "timeout_seconds": 30}},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


class LlmCatalogTests(unittest.TestCase):
    def test_help_lists_subcommands(self) -> None:
        proc = subprocess.run(
            ["python3", str(CATALOGER), "--help"],
            env={**os.environ, "PYTHONUTF8": "1"},
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertIn("calibrate", proc.stdout)
        self.assertIn("classify", proc.stdout)
        self.assertIn("validate", proc.stdout)

    def test_calibrate_fails_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            httpd, base_url = _start_mock_llm(
                [
                    json.dumps({"primary_category_id": "other", "confidence": 0.2}),
                ],
            )
            try:
                llm_cfg = tmp_path / "llm.json"
                _write_llm_config(llm_cfg, base_url=base_url)
                taxonomy = tmp_path / "taxonomy.json"
                taxonomy.write_text(
                    json.dumps(
                        {"categories": [{"id": "medical", "label": "医疗健康"}, {"id": "other", "label": "其他"}]},
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                gold = tmp_path / "gold.jsonl"
                gold.write_text(
                    json.dumps(
                        {"id": "ex1", "text": "糖尿病的诊断与治疗", "expected_primary_category_id": "medical"},
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                proc = subprocess.run(
                    [
                        "python3",
                        str(CATALOGER),
                        "calibrate",
                        "--llm-config",
                        str(llm_cfg),
                        "--taxonomy",
                        str(taxonomy),
                        "--gold",
                        str(gold),
                        "--min-accuracy",
                        "1.0",
                    ],
                    env={**os.environ, "PYTHONUTF8": "1"},
                    cwd=str(ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                self.assertNotEqual(proc.returncode, 0, proc.stdout)
            finally:
                httpd.shutdown()
                httpd.server_close()

    def test_classify_writes_assignments_and_supports_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            httpd, base_url = _start_mock_llm(
                [
                    json.dumps({"primary_category_id": "engineering", "confidence": 0.9}),
                    json.dumps({"primary_category_id": "medical", "confidence": 0.8}),
                ],
            )
            try:
                llm_cfg = tmp_path / "llm.json"
                _write_llm_config(llm_cfg, base_url=base_url)
                taxonomy = tmp_path / "taxonomy.json"
                taxonomy.write_text(
                    json.dumps(
                        {
                            "categories": [
                                {"id": "medical", "label": "医疗健康"},
                                {"id": "engineering", "label": "工程与计算"},
                                {"id": "other", "label": "其他"},
                            ]
                        },
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                # Minimal skill root with manifest.json
                skill_root = tmp_path / "skill"
                (skill_root / "references" / "d1").mkdir(parents=True)
                (skill_root / "references" / "d2").mkdir(parents=True)
                (skill_root / "references" / "d1" / "toc.md").write_text("# TOC\n\nA\n", encoding="utf-8")
                (skill_root / "references" / "d2" / "toc.md").write_text("# TOC\n\nB\n", encoding="utf-8")
                (skill_root / "manifest.json").write_text(
                    json.dumps(
                        {
                            "docs": [
                                {"doc_id": "d1", "title": "Doc 1", "source_file": "a.md", "source_path": "a.md", "doc_hash": "h1"},
                                {"doc_id": "d2", "title": "Doc 2", "source_file": "b.md", "source_path": "b.md", "doc_hash": "h2"},
                            ]
                        },
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                out_path = tmp_path / "assignments.jsonl"
                proc1 = subprocess.run(
                    [
                        "python3",
                        str(CATALOGER),
                        "classify",
                        "--llm-config",
                        str(llm_cfg),
                        "--taxonomy",
                        str(taxonomy),
                        "--skill-root",
                        str(skill_root),
                        "--out",
                        str(out_path),
                    ],
                    env={**os.environ, "PYTHONUTF8": "1"},
                    cwd=str(ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                self.assertEqual(proc1.returncode, 0, proc1.stdout)
                lines1 = out_path.read_text(encoding="utf-8").strip().splitlines()
                self.assertEqual(len(lines1), 2)
                rows = [json.loads(line) for line in lines1]
                self.assertEqual({r["doc_hash"] for r in rows}, {"h1", "h2"})
                self.assertEqual({r["primary_category_id"] for r in rows}, {"engineering", "medical"})

                # Resume should not append duplicates
                proc2 = subprocess.run(
                    [
                        "python3",
                        str(CATALOGER),
                        "classify",
                        "--llm-config",
                        str(llm_cfg),
                        "--taxonomy",
                        str(taxonomy),
                        "--skill-root",
                        str(skill_root),
                        "--out",
                        str(out_path),
                    ],
                    env={**os.environ, "PYTHONUTF8": "1"},
                    cwd=str(ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                self.assertEqual(proc2.returncode, 0, proc2.stdout)
                lines2 = out_path.read_text(encoding="utf-8").strip().splitlines()
                self.assertEqual(len(lines2), 2)
            finally:
                httpd.shutdown()
                httpd.server_close()

    def test_validate_fails_on_unknown_category_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            taxonomy = tmp_path / "taxonomy.json"
            taxonomy.write_text(
                json.dumps(
                    {"categories": [{"id": "medical", "label": "医疗健康"}, {"id": "other", "label": "其他"}]},
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            skill_root = tmp_path / "skill"
            (skill_root / "references" / "d1").mkdir(parents=True)
            (skill_root / "manifest.json").write_text(
                json.dumps(
                    {"docs": [{"doc_id": "d1", "title": "Doc 1", "source_file": "a.md", "source_path": "a.md", "doc_hash": "h1"}]},
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments = tmp_path / "assignments.jsonl"
            assignments.write_text(
                json.dumps({"doc_hash": "h1", "primary_category_id": "not-in-taxonomy"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "python3",
                    str(CATALOGER),
                    "validate",
                    "--taxonomy",
                    str(taxonomy),
                    "--assignments",
                    str(assignments),
                    "--skill-root",
                    str(skill_root),
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("not-in-taxonomy", proc.stdout)

    def test_validate_fails_below_min_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            taxonomy = tmp_path / "taxonomy.json"
            taxonomy.write_text(
                json.dumps(
                    {"categories": [{"id": "medical", "label": "医疗健康"}, {"id": "other", "label": "其他"}]},
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            skill_root = tmp_path / "skill"
            (skill_root / "references" / "d1").mkdir(parents=True)
            (skill_root / "references" / "d2").mkdir(parents=True)
            (skill_root / "manifest.json").write_text(
                json.dumps(
                    {
                        "docs": [
                            {"doc_id": "d1", "title": "Doc 1", "source_file": "a.md", "source_path": "a.md", "doc_hash": "h1"},
                            {"doc_id": "d2", "title": "Doc 2", "source_file": "b.md", "source_path": "b.md", "doc_hash": "h2"},
                        ]
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            assignments = tmp_path / "assignments.jsonl"
            assignments.write_text(
                json.dumps({"doc_hash": "h1", "primary_category_id": "medical"}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "python3",
                    str(CATALOGER),
                    "validate",
                    "--taxonomy",
                    str(taxonomy),
                    "--assignments",
                    str(assignments),
                    "--skill-root",
                    str(skill_root),
                    "--min-coverage",
                    "1.0",
                ],
                env={**os.environ, "PYTHONUTF8": "1"},
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            self.assertIn("coverage", proc.stdout.lower())
