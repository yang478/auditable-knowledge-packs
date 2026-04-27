#!/usr/bin/env python3
"""CI check: verify build-time and runtime tokenizer_core.py copies are identical.

The pack-builder project maintains two physical copies of tokenizer_core.py:
  1. scripts/build_skill_lib/tokenizer_core.py  (build-time, indexing)
  2. templates/kbtool_lib/tokenizer_core.py     (runtime, querying)

These MUST be byte-identical. If they diverge, the FTS5 index and query
tokenizers will produce different tokens, causing silent search failures.

Usage:
    python scripts/check_tokenizer_consistency.py

Exit: 0 if identical, 1 if different.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD_COPY = ROOT / "scripts/build_skill_lib/tokenizer_core.py"
RUNTIME_COPY = ROOT / "templates/kbtool_lib/tokenizer_core.py"


def main() -> int:
    missing = []
    for path in (BUILD_COPY, RUNTIME_COPY):
        if not path.is_file():
            missing.append(path)
    if missing:
        print(f"ERROR: tokenizer_core.py not found: {missing[0]}")
        return 1

    b = BUILD_COPY.read_bytes()
    r = RUNTIME_COPY.read_bytes()

    if b == r:
        print(f"OK: tokenizer_core.py copies are identical ({len(b)} bytes)")
        return 0

    print("ERROR: tokenizer_core.py copies differ!")
    print(f"  Build:   {BUILD_COPY}  ({len(b)} bytes)")
    print(f"  Runtime: {RUNTIME_COPY}  ({len(r)} bytes)")
    print()
    print("To fix: manually sync the copies, or:")
    print(f"  cp {BUILD_COPY} {RUNTIME_COPY}")
    print()
    # Show first differing line
    build_lines = b.decode("utf-8").splitlines()
    runtime_lines = r.decode("utf-8").splitlines()
    for i, (bl, rl) in enumerate(zip(build_lines, runtime_lines), start=1):
        if bl != rl:
            print(f"First difference at line {i}:")
            print(f"  build:   {bl}")
            print(f"  runtime: {rl}")
            break
    return 1


if __name__ == "__main__":
    sys.exit(main())
