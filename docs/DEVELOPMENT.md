# Development Guide

## Repo structure

- `pack-builder/scripts/build_skill.py`: main generator CLI
- `pack-builder/templates/`: templates copied into the generated skill (`scripts/kbtool.py`, `scripts/reindex.py`, etc.)
- `pack-builder/scripts/tests/`: unittest-based regression tests (fixtures under `fixtures/`)

## Requirements

- Python 3.10+

## Run tests

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## Making changes safely

- Prefer small changes and add fixtures/tests for retrieval regressions.
- Keep output layout stable; if you must change it, update docs and tests together.
- Avoid adding non-stdlib dependencies unless there is a strong reason (this project intentionally stays lightweight).
