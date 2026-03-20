import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]  # pack-builder/scripts
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.references import _pack_paragraphs_into_blocks  # noqa: E402


class PackParagraphBlocksTests(unittest.TestCase):
    def test_packs_many_short_paragraphs_into_few_blocks(self) -> None:
        paragraphs = [["a" * 1000] for _ in range(10)]
        blocks = _pack_paragraphs_into_blocks(paragraphs, max_chars=6000)
        self.assertEqual(len(blocks), 2)
        for block in blocks:
            self.assertLessEqual(len("\n".join(block)), 6000)

    def test_splits_single_long_line_into_chunks(self) -> None:
        long_line = "x" * 15000
        paragraphs = [[long_line]]
        blocks = _pack_paragraphs_into_blocks(paragraphs, max_chars=6000)
        self.assertEqual(len(blocks), 3)
        self.assertEqual("".join(block[0] for block in blocks), long_line)
        for block in blocks:
            self.assertEqual(len(block), 1)
            self.assertLessEqual(len(block[0]), 6000)

