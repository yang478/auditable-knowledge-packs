"""Shared pytest configuration for pack-builder tests."""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

import pytest


class DieCalled(Exception):
    """Custom exception raised when ``die()`` is invoked during tests."""


@pytest.fixture
def die_exception():
    """Return the :class:`DieCalled` exception class for use in assertions."""
    return DieCalled


@pytest.fixture
def no_die(monkeypatch):
    """Patch ``die()`` in ``utils.fs`` and ``build`` modules to raise :class:`DieCalled`."""
    import build_skill_lib.utils.fs as fs_mod
    import build_skill_lib.build as build_mod

    def _die(msg: str, code: int = 2) -> None:
        raise DieCalled(msg)

    monkeypatch.setattr(fs_mod, "die", _die)
    monkeypatch.setattr(build_mod, "die", _die)
