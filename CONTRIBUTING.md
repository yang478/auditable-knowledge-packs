# Contributing

Thanks for helping improve this project!

> 中文贡献者：可以直接用中文提 Issue/PR；我会尽量保持中英双语文档同步。

## Ways to contribute

- Report bugs (include a minimal repro input if possible)
- Request features / improvements
- Improve docs (typos, missing steps, better examples)
- Add fixtures/tests for new retrieval edge cases

## Development setup

Requirements:
- Python 3.10+

Run tests:

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## Pull requests

- Keep changes focused and small.
- Update docs when behavior changes (README + `docs/*`).
- Add/extend tests when fixing bugs or changing retrieval behavior.
- If you modify the generated output layout, update `pack-builder/SKILL.md` accordingly.
