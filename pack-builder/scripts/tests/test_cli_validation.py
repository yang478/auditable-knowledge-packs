"""CLI 参数验证单元测试 — 覆盖 cli.py 的输入校验和错误处理。"""

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.cli import build_parser, main as cli_main


class ParserDefaultTests(unittest.TestCase):
    def test_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "--skill-name", "demo",
            "--inputs", "doc.md",
        ])
        self.assertEqual(args.skill_name, "demo")
        self.assertEqual(args.inputs, ["doc.md"])
        self.assertEqual(args.out_dir, ".claude/skills")
        self.assertEqual(args.title, "Document Knowledge Base")
        self.assertFalse(args.force)
        self.assertTrue(args.enable_graph_edges)

    def test_disable_graph_edges(self) -> None:
        parser = build_parser()
        args = parser.parse_args([
            "--skill-name", "demo",
            "--inputs", "doc.md",
            "--disable-graph-edges",
        ])
        self.assertFalse(args.enable_graph_edges)

    def test_mutual_exclusive_source_group(self) -> None:
        parser = build_parser()
        # --inputs 和 --ir-jsonl 不能同时出现
        with self.assertRaises(SystemExit):
            parser.parse_args([
                "--skill-name", "demo",
                "--inputs", "doc.md",
                "--ir-jsonl", "ir.jsonl",
            ])


class CliValidationTests(unittest.TestCase):
    def test_invalid_skill_name_exits(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_build(**kwargs: object) -> Path:
            calls.append(kwargs)
            return Path("unused")

        with self.assertRaises(SystemExit) as ctx:
            cli_main(
                ["--skill-name", "Invalid_Name", "--inputs", "doc.md"],
                build_skill_fn=fake_build,
            )
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(len(calls), 0)

    def test_missing_input_file_exits(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_build(**kwargs: object) -> Path:
            calls.append(kwargs)
            return Path("unused")

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as ctx:
                cli_main(
                    [
                        "--skill-name",
                        "demo",
                        "--inputs",
                        str(Path(tmp) / "nonexistent.md"),
                    ],
                    build_skill_fn=fake_build,
                )
            self.assertEqual(ctx.exception.code, 2)
            self.assertEqual(len(calls), 0)

    def test_missing_ir_jsonl_file_exits(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_build(**kwargs: object) -> Path:
            calls.append(kwargs)
            return Path("unused")

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as ctx:
                cli_main(
                    [
                        "--skill-name",
                        "demo",
                        "--ir-jsonl",
                        str(Path(tmp) / "nonexistent.jsonl"),
                    ],
                    build_skill_fn=fake_build,
                )
            self.assertEqual(ctx.exception.code, 2)
            self.assertEqual(len(calls), 0)

    def test_valid_call_passes_through(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_build(**kwargs: object) -> Path:
            calls.append(kwargs)
            return Path("unused")

        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "doc.md"
            input_path.write_text("# Doc\n", encoding="utf-8")
            result = cli_main(
                [
                    "--skill-name",
                    "demo",
                    "--inputs",
                    str(input_path),
                    "--out-dir",
                    tmp,
                ],
                build_skill_fn=fake_build,
            )
            self.assertEqual(result, 0)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["skill_name"], "demo")
            self.assertEqual(calls[0]["enable_graph_edges"], True)


if __name__ == "__main__":
    unittest.main()
