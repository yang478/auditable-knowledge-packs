# 开发指南

## 目录结构

- `pack-builder/scripts/build_skill.py`：主生成器 CLI
- `pack-builder/templates/`：生成 skill 时复制进去的模板（`scripts/kbtool.py`、`scripts/reindex.py` 等）
- `pack-builder/scripts/tests/`：基于 `unittest` 的回归测试（`fixtures/` 下为测试输入）

## 依赖

- Python 3.10+

## 运行测试

```bash
python3 -m unittest discover -s pack-builder/scripts/tests -p 'test_*.py' -q
```

## 安全修改建议

- 尽量小步改动，并为检索相关回归增加 fixture/test。
- 尽量保持输出目录结构稳定；如必须变更，请同步更新文档与测试。
- 除非非常必要，避免引入第三方依赖（本项目刻意保持轻量、可移植）。
