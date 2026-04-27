# Development Guide

## Repo structure

- `pack-builder/scripts/build_skill.py`: main generator CLI
- `pack-builder/templates/`: templates copied into the generated skill (`scripts/kbtool.py`, `scripts/reindex.py`, etc.)
- generated artifact root: `references/`, `kb.sqlite`, `corpus_manifest.json`, `phase_a_artifact.json`, and `build_state.json`
- `pack-builder/scripts/tests/`: unittest-based regression tests (fixtures under `fixtures/`)

## Requirements

- Python 3.10+

## Run tests

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## Phase A benchmark workflow

Build a local artifact from the gated real corpus:

```bash
inputs=()
while IFS= read -r path; do
  inputs+=("$path")
done < <(find -L 测试文档2 -maxdepth 1 -type f -name '*.md' | sort)

python3 pack-builder/scripts/build_skill.py \
  --skill-name phase-a-real-corpus \
  --out-dir /tmp/phase-a-build \
  --inputs "${inputs[@]}" \
  --title "Phase A Real Corpus"
```

Run the benchmark twice against the same artifact and compare deterministic outputs:

```bash
python3 benchmarks/gualan-v2-alpha/run_eval \
  --artifact-root /tmp/phase-a-build/phase-a-real-corpus \
  --benchmark-root benchmarks/gualan-v2-alpha \
  --out-dir /tmp/gualan-v2-alpha-final

python3 benchmarks/gualan-v2-alpha/run_eval \
  --artifact-root /tmp/phase-a-build/phase-a-real-corpus \
  --benchmark-root benchmarks/gualan-v2-alpha \
  --out-dir /tmp/gualan-v2-alpha-repeat

diff -r --exclude run_manifest.json /tmp/gualan-v2-alpha-final /tmp/gualan-v2-alpha-repeat
```

Compare the candidate metrics with the frozen baseline snapshot in `benchmarks/gualan-v2-alpha/baseline/`.
`run_manifest.json` is expected to differ because it contains timestamps; `metrics.json`, `per_question.jsonl`, and `bundles/<qid>/bundle.{json,md}` must stay byte-identical on repeated runs.

If you are validating the artifact adapter path specifically, make sure `phase_a_artifact.json` exists after build or reindex. The adapter is expected to keep serving document/node metadata even if `kb.sqlite` is temporarily unavailable.

If you are validating incremental behavior, also check `build_state.json`. It should contain per-document source/text/span/node fingerprints, active parser/export checksums, and index binding hashes. The current Phase C build metrics are deterministic row-work estimates derived from dirty-document footprint, so they are stable across machines and suitable for regression comparisons.

## Making changes safely

- Prefer small changes and add fixtures/tests for retrieval regressions.
- Keep output layout stable; if you must change it, update docs and tests together.
- Avoid adding non-stdlib dependencies unless there is a strong reason (this project intentionally stays lightweight).
- Run `python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_phase_c_*.py' -v` before touching incremental or regression code paths.
