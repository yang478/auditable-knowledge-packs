"""Fingerprint 核心哈希函数 — 权威实现。

此文件是哈希函数的唯一定义点。构建流程将其复制到 templates/kbtool_lib/，
运行时直接引用，消除 build-time 和 runtime 之间的重复实现。

注意：权威源在 scripts/build_skill_lib/fingerprint_core.py。
此副本由构建流程自动同步。
"""
from __future__ import annotations

import hashlib


def sha256_text(text: str) -> str:
    """对文本内容做 SHA-256 摘要（UTF-8 编码）。"""
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    """对原始字节做 SHA-256 摘要。"""
    return hashlib.sha256(data).hexdigest()
