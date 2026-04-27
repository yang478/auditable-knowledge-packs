# 开发指南

## 目录结构

- `pack-builder/scripts/build_skill.py`：主生成器 CLI
- `pack-builder/templates/`：生成 skill 时复制进去的模板（`scripts/kbtool.py`、`scripts/reindex.py` 等）
- 生成后的 artifact 根目录：`references/`、`kb.sqlite`、`corpus_manifest.json`、`phase_a_artifact.json`、`build_state.json`
- `pack-builder/scripts/tests/`：基于 `unittest` 的回归测试（`fixtures/` 下为测试输入）

## 依赖

- Python 3.10+

## 运行测试

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## Phase A benchmark 工作流

先用 gated 真实语料构建本地 artifact：

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

然后对同一个 artifact 连续跑两次 benchmark，并比较确定性输出：

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

再把候选结果与 `benchmarks/gualan-v2-alpha/baseline/` 中冻结的 baseline snapshot 对比。
`run_manifest.json` 因为包含时间戳，预期会不同；重复运行时必须保持字节级一致的是 `metrics.json`、`per_question.jsonl` 和 `bundles/<qid>/bundle.{json,md}`。

如果你要专门验证 artifact adapter 路径，请确认 build 或 reindex 后存在 `phase_a_artifact.json`。即使 `kb.sqlite` 暂时不可用，adapter 也应该继续从这个导出契约里读取文档/节点元数据。

如果你要验证增量行为，还要检查 `build_state.json`。它会记录每个文档的 source/text/span/node 指纹、active parser/export checksum，以及各索引族的 binding hash。当前 Phase C 的 build metrics 使用 dirty-document footprint 推导出的 deterministic row-work 成本，因此跨机器稳定，适合做 regression 对比。

## 安全修改建议

- 尽量小步改动，并为检索相关回归增加 fixture/test。
- 尽量保持输出目录结构稳定；如必须变更，请同步更新文档与测试。
- 除非非常必要，避免引入第三方依赖（本项目刻意保持轻量、可移植）。
- 修改增量构建或回归比较逻辑前，先跑 `python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_phase_c_*.py' -v`。
