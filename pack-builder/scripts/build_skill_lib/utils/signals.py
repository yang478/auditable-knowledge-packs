"""Graceful signal handling for pack-builder build-time.

Prevents database corruption on Ctrl+C by setting a global shutdown flag
that long-running operations should poll.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)

# Global state ----------------------------------------------------------------
_shutdown_requested = threading.Event()
_original_sigint: Optional[signal.Handlers] = None
_original_sigterm: Optional[signal.Handlers] = None


def is_shutdown_requested() -> bool:
    """Poll this in long-running loops or before atomic operations."""
    return _shutdown_requested.is_set()


def request_shutdown(signum: int, _frame: Optional[object]) -> None:
    """Signal handler: set the shutdown flag but do NOT exit immediately.

    WARNING: This runs in a signal context — only async-signal-safe
    operations are permitted.  Do NOT use logging, locks, or stdio.
    """
    _shutdown_requested.set()
    # Re-raise the default handler on the *second* Ctrl+C for force-kill.
    if signum == signal.SIGINT and _original_sigint is not None:
        signal.signal(signal.SIGINT, _original_sigint)
    if signum == signal.SIGTERM and _original_sigterm is not None:
        signal.signal(signal.SIGTERM, _original_sigterm)
    # Use os.write(2, ...) instead of sys.stderr.write for async-signal safety.
    msg = (
        f"\n[pack-builder] Shutdown requested (signal {signum}). "
        f"Finishing safely (press Ctrl+C again to force quit)...\n"
    )
    os.write(2, msg.encode("utf-8", errors="replace"))


def install_signal_handlers() -> None:
    """Install graceful SIGINT / SIGTERM handlers.

    Idempotent: safe to call multiple times.
    """
    global _original_sigint, _original_sigterm
    if _original_sigint is None:
        _original_sigint = signal.signal(signal.SIGINT, request_shutdown)
    if _original_sigterm is None:
        _original_sigterm = signal.signal(signal.SIGTERM, request_shutdown)


def uninstall_signal_handlers() -> None:
    """Restore original handlers (useful in test teardown)."""
    global _original_sigint, _original_sigterm
    if _original_sigint is not None:
        signal.signal(signal.SIGINT, _original_sigint)
        _original_sigint = None
    if _original_sigterm is not None:
        signal.signal(signal.SIGTERM, _original_sigterm)
        _original_sigterm = None
    _shutdown_requested.clear()


@contextmanager
def graceful_shutdown_context():
    """Context manager that installs handlers on enter and restores on exit."""
    install_signal_handlers()
    try:
        yield
    finally:
        uninstall_signal_handlers()


def raise_if_shutdown() -> None:
    """Call inside tight loops to allow early abort without corrupting state."""
    if _shutdown_requested.is_set():
        raise SystemExit(130)


# ---------------------------------------------------------------------------
# ThreadPoolExecutor helpers
# ---------------------------------------------------------------------------

def shutdown_executor_now(executor: "object") -> None:
    """Force-shutdown a ThreadPoolExecutor without waiting for workers.

    This is useful when a signal arrives and we need to abort ASAP.
    """
    # executor is typed as object to avoid importing concurrent.futures here.
    # ThreadPoolExecutor has shutdown(wait=False, cancel_futures=True) on 3.9+.
    try:
        shutdown = getattr(executor, "shutdown", None)
        if shutdown is not None:
            shutdown(wait=False, cancel_futures=True)  # type: ignore[call-arg]
    except TypeError:
        # Python < 3.9 does not have cancel_futures
        try:
            shutdown(wait=False)  # type: ignore[call-arg]
        except Exception as _exc:
            logger.debug("shutdown_executor_now inner fallback failed: %s", _exc)
