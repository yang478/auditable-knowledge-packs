"""SQLite safety helpers for kbtool runtime.

Provides WAL-mode migration (backwards-compatible) and retry-on-locked
decorators.
"""
from __future__ import annotations

import functools
import logging
import sqlite3
import time
from pathlib import Path
from typing import Callable, Optional, TypeVar, Union

logger = logging.getLogger(__name__)

_WAL_RETRY_DELAYS = (0.05, 0.1, 0.2, 0.4)


def enable_wal(conn: sqlite3.Connection, *, busy_timeout_ms: int = 5000) -> None:
    """Enable WAL mode with basic retry for 'database is locked'.

    Safe to call on an already-WAL database (idempotent).
    Also sets busy_timeout so readers wait instead of erroring.
    """
    for delay in _WAL_RETRY_DELAYS:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            return
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower():
                logger.debug("WAL pragma locked, retry in %.3fs", delay)
                time.sleep(delay)
                continue
            raise
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")


def open_db_wal(db_path: Path, *, timeout: float = 30.0) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode, row factory, and busy timeout.

    Backwards-compatible: works on existing DELETE-journal DBs and
    newly-created DBs.  The journal_mode change is persistent.
    """
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    enable_wal(conn, busy_timeout_ms=int(timeout * 1000))
    return conn


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

F = TypeVar("F", bound=Callable[..., object])


def retry_on_locked(
    max_retries: int = 4,
    base_delay: float = 0.05,
    max_delay: float = 2.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (sqlite3.OperationalError,),
) -> Callable[[F], F]:
    """Decorator that retries a function on sqlite3 'database is locked' errors."""

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: object, **kwargs: object) -> object:
            delay = base_delay
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    msg = str(exc)
                    if "locked" not in msg.lower() and "busy" not in msg.lower():
                        raise
                    if attempt >= max_retries:
                        break
                    logger.debug(
                        "retry_on_locked: attempt %d/%d failed (%s), retry in %.3fs",
                        attempt + 1,
                        max_retries + 1,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff, max_delay)
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


def sqlite3_retry_exec(
    conn: sqlite3.Connection,
    sql: str,
    parameters: Optional[Union[tuple[object, ...], list[object]]] = None,
    *,
    max_retries: int = 4,
    base_delay: float = 0.05,
) -> sqlite3.Cursor:
    """Execute SQL with retry on 'database is locked'."""
    delay = base_delay
    params = parameters or ()
    for attempt in range(max_retries + 1):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            if attempt >= max_retries:
                raise
            time.sleep(delay)
            delay = min(delay * 2.0, 2.0)
    raise sqlite3.OperationalError(f"sqlite3_retry_exec exhausted retries: {sql}")  # pragma: no cover
