"""模板模块加载与模型注册表。"""
from __future__ import annotations

import importlib.util
import copy
import json
from pathlib import Path
from types import ModuleType
from typing import Any

from .fs import ConfigError


DEFAULT_MODEL_REGISTRY = {
    "components": {},
    "reranker": {
        "version": "",
        "fallback": "rules_only",
    },
}


def canonical_model_registry_json(registry: dict[str, Any] | None = None) -> str:
    payload = registry or DEFAULT_MODEL_REGISTRY
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_model_registry(path: Path) -> dict[str, Any]:
    candidate = DEFAULT_MODEL_REGISTRY
    if not Path(path).exists():
        return copy.deepcopy(candidate)
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return copy.deepcopy(candidate)
    if not isinstance(payload, dict):
        return copy.deepcopy(candidate)

    merged = copy.deepcopy(candidate)
    for key, value in payload.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def load_template_module(module_name: str, module_path: Path) -> ModuleType:
    """从已知路径安全加载模板模块。

    ARCHITECTURE NOTE — Build-Runtime Determinism:
    This function is used by build-time code (e.g. utils/text.py) to load
    runtime template modules from `templates/kbtool_lib/`. This ensures:

    1. FTS5 tokens generated at build time use the EXACT same tokenization
       as FTS5 queries at runtime.
    2. Any change to logic in `templates/kbtool_lib/text.py` is automatically
       reflected at both build and runtime without manual synchronization.
    3. The templates directory is the canonical source — build-time wrappers
       (like `utils/text.py`) are thin proxies that delegate to the loaded module.

    This coupling is INTENTIONAL and REQUIRED for search determinism.
    Do NOT extract shared functions to a separate module — that would create
    two copies (one in the build tree, one in the output skill) that could
    diverge and silently break search quality.

    The same pattern is used in `utils/contract.py` for artifact/state
    contract modules.

    Args:
        module_name: 模块注册名（需唯一，避免冲突）。
        module_path: 模块文件路径。
    """
    path = module_path
    if not path.exists() or not path.is_file():
        raise ConfigError(f"Missing template: {path} (pack-builder installation is incomplete)")

    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ConfigError(f"Failed to load template module: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module
