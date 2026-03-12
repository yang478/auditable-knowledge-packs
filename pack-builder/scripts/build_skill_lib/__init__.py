from __future__ import annotations

from pathlib import Path


def pack_builder_dir() -> Path:
    """
    Return the `pack-builder/` directory containing `templates/` and `scripts/`.

    Layout:
      pack-builder/
        scripts/
          build_skill_lib/
            __init__.py   <- this file
        templates/
    """

    return Path(__file__).resolve().parents[2]


def templates_dir() -> Path:
    return pack_builder_dir() / "templates"

