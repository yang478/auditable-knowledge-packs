"""Safe subprocess helpers for kbtool runtime.

Fixes PIPE deadlock (rg / fd / pdftotext large output) by using temp files
and enforces output size limits to prevent memory exhaustion.
"""
from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

# Default output limit: 128 MiB for runtime (smaller than build-time).
_DEFAULT_MAX_OUTPUT_BYTES = 128 * 1024 * 1024
# Size threshold above which we switch from PIPE to a temp file.
_PIPE_TO_FILE_THRESHOLD_BYTES = 64 * 1024


class SubprocessOutputTooLargeError(subprocess.SubprocessError):
    """Raised when subprocess output exceeds the configured limit."""

    def __init__(self, cmd: list[str], limit_bytes: int, actual_bytes: int) -> None:
        self.cmd = cmd
        self.limit_bytes = limit_bytes
        self.actual_bytes = actual_bytes
        super().__init__(
            f"Command output exceeded {limit_bytes} bytes limit "
            f"(got at least {actual_bytes}): {' '.join(cmd[:8])}..."
        )


def run_subprocess_safe(
    cmd: list[str],
    *,
    timeout: Optional[float] = 60.0,
    max_output_bytes: int = _DEFAULT_MAX_OUTPUT_BYTES,
    text: bool = True,
    encoding: str = "utf-8",
    errors: str = "replace",
    check: bool = False,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    stdin: Optional[int] = None,
    **popen_kwargs: object,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess with deadlock-safe output capture via temp files.

    This is the default-safe path: no PIPE buffers, no deadlock risk.
    """
    tmpdir = str(cwd) if cwd else None
    stdout_fd, stdout_path = tempfile.mkstemp(prefix="kbt_subout_", suffix=".txt", dir=tmpdir)
    stderr_fd, stderr_path = tempfile.mkstemp(prefix="kbt_suberr_", suffix=".txt", dir=tmpdir)
    try:
        with open(stdout_fd, "wb", closefd=True) as stdout_f, \
             open(stderr_fd, "wb", closefd=True) as stderr_f:
            proc = subprocess.Popen(
                cmd,
                stdout=stdout_f,
                stderr=stderr_f,
                text=False,
                cwd=cwd,
                env=env,
                stdin=stdin,
                **popen_kwargs,  # type: ignore[arg-type]
            )
            try:
                proc.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt, SystemExit):
                proc.kill()
                proc.wait()
                raise
            finally:
                # Ensure child is terminated if parent is interrupted after wait()
                # but before we read output (e.g. Ctrl+C during file I/O).
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()

        stdout_data = _read_limited(stdout_path, max_output_bytes, cmd)
        stderr_data = _read_limited(stderr_path, max_output_bytes, cmd)

        if text:
            stdout_str = stdout_data.decode(encoding, errors=errors)
            stderr_str = stderr_data.decode(encoding, errors=errors)
            result: subprocess.CompletedProcess[str] = subprocess.CompletedProcess(
                args=cmd,
                returncode=proc.returncode,
                stdout=stdout_str,
                stderr=stderr_str,
            )
        else:
            result = subprocess.CompletedProcess(
                args=cmd,
                returncode=proc.returncode,
                stdout=stdout_data,
                stderr=stderr_data,
            )

        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, output=result.stdout, stderr=result.stderr
            )
        return result
    finally:
        try:
            os.unlink(stdout_path)
        except OSError:
            pass
        try:
            os.unlink(stderr_path)
        except OSError:
            pass


def run_subprocess_safe_small(
    cmd: list[str],
    *,
    timeout: Optional[float] = 60.0,
    text: bool = True,
    encoding: str = "utf-8",
    errors: str = "replace",
    check: bool = False,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    stdin: Optional[int] = None,
    **popen_kwargs: object,
) -> subprocess.CompletedProcess[str]:
    """Fast path for commands known to produce small output.

    Uses PIPE but caps captured output at 64 KiB to prevent memory
    exhaustion if the command unexpectedly produces a lot of data.
    """
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=False,
        timeout=timeout,
        cwd=cwd,
        env=env,
        stdin=stdin,
        check=False,
        **popen_kwargs,  # type: ignore[arg-type]
    )
    stdout = proc.stdout
    stderr = proc.stderr
    max_out = _PIPE_TO_FILE_THRESHOLD_BYTES

    if isinstance(stdout, bytes) and len(stdout) > max_out:
        raise SubprocessOutputTooLargeError(cmd, max_out, len(stdout))
    if isinstance(stderr, bytes) and len(stderr) > max_out:
        raise SubprocessOutputTooLargeError(cmd, max_out, len(stderr))

    if text:
        stdout_s = stdout.decode(encoding, errors=errors) if isinstance(stdout, bytes) else stdout
        stderr_s = stderr.decode(encoding, errors=errors) if isinstance(stderr, bytes) else stderr
        result: subprocess.CompletedProcess[str] = subprocess.CompletedProcess(
            args=cmd,
            returncode=proc.returncode,
            stdout=stdout_s,
            stderr=stderr_s,
        )
    else:
        result = subprocess.CompletedProcess(
            args=cmd,
            returncode=proc.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=result.stdout, stderr=result.stderr
        )
    return result


def _read_limited(path: str, limit_bytes: int, cmd: list[str]) -> bytes:
    size = os.path.getsize(path)
    if size > limit_bytes:
        # Still read the allowed prefix so callers can log it.
        with open(path, "rb") as f:
            prefix = f.read(limit_bytes)
        raise SubprocessOutputTooLargeError(cmd, limit_bytes, size)
    with open(path, "rb") as f:
        return f.read()
