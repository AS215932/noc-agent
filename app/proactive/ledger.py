"""Per-day budget ledger + singleton run-lock for the proactive loop.

Ported from ``engineering-loop``'s ``daemon.py`` (``acquire_lock`` /
``load_ledger`` / ``update_ledger``). The loop runs in-process inside the
FastAPI service, so the lock is a defence-in-depth guard against a second
worker/process also driving cycles; the ledger is the hard daily budget that
caps how many expensive LLM investigations run per UTC day.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_LOCK_MAX_AGE_SECONDS = 2 * 60 * 60


class CorruptLedgerError(RuntimeError):
    """A persisted budget cannot be trusted, so spending must fail closed."""


def today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(state_dir: Path, *, max_age_seconds: int = DEFAULT_LOCK_MAX_AGE_SECONDS) -> Path | None:
    """Take the run lock; return ``None`` when another live run holds it."""
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / "proactive.lock"
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                before = lock_path.stat()
                holder = json.loads(lock_path.read_text(encoding="utf-8"))
                pid = int(holder.get("pid", -1))
                started = float(holder.get("started_at", 0.0))
            except FileNotFoundError, json.JSONDecodeError, ValueError, OSError:
                pid, started, before = -1, 0.0, None
            fresh = (time.time() - started) < max_age_seconds
            if pid > 0 and fresh and _pid_alive(pid):
                return None
            try:
                after = lock_path.stat()
                if before is not None and (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
                    return None
                lock_path.unlink()
            except FileNotFoundError:
                pass
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"pid": os.getpid(), "started_at": time.time()}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return lock_path


def release_lock(lock_path: Path | None) -> None:
    if lock_path is not None:
        lock_path.unlink(missing_ok=True)


def _ledger_path(state_dir: Path, day: str) -> Path:
    return state_dir / f"ledger-{day}.json"


def _empty_ledger() -> dict[str, Any]:
    return {
        "cycles": 0,
        "investigations": 0,
        "attempts": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "cost_usd": 0.0,
        "handoffs": 0,
    }


def load_ledger(state_dir: Path, day: str | None = None) -> dict[str, Any]:
    day = day or today()
    path = _ledger_path(state_dir, day)
    if not path.exists():
        return _empty_ledger()
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise CorruptLedgerError(f"cannot read proactive budget ledger {path.name}") from exc
    if not isinstance(loaded, dict):
        raise CorruptLedgerError(f"invalid proactive budget ledger {path.name}")
    merged = _empty_ledger()
    merged.update({k: loaded.get(k, merged[k]) for k in merged})
    # Versionless migration: old ledgers counted only successful investigations.
    # Treat those as both attempts and successes so an upgrade never grants extra
    # spend halfway through a UTC day.
    if "attempts" not in loaded:
        merged["attempts"] = int(merged["investigations"])
    if "succeeded" not in loaded:
        merged["succeeded"] = int(merged["investigations"])
    return merged


def update_ledger(
    state_dir: Path,
    day: str | None = None,
    *,
    cycles: int = 0,
    investigations: int = 0,
    attempts: int = 0,
    succeeded: int = 0,
    failed: int = 0,
    skipped: int = 0,
    cost_usd: float = 0.0,
    handoffs: int = 0,
) -> dict[str, Any]:
    day = day or today()
    ledger = load_ledger(state_dir, day)
    ledger["cycles"] = int(ledger.get("cycles", 0)) + cycles
    ledger["investigations"] = int(ledger.get("investigations", 0)) + investigations
    ledger["attempts"] = int(ledger.get("attempts", 0)) + attempts
    ledger["succeeded"] = int(ledger.get("succeeded", 0)) + succeeded
    ledger["failed"] = int(ledger.get("failed", 0)) + failed
    ledger["skipped"] = int(ledger.get("skipped", 0)) + skipped
    if min(int(ledger[key]) for key in ("cycles", "investigations", "attempts", "succeeded", "failed", "skipped")) < 0:
        raise ValueError("proactive ledger counters cannot be negative")
    ledger["cost_usd"] = round(float(ledger.get("cost_usd", 0.0)) + cost_usd, 6)
    ledger["handoffs"] = int(ledger.get("handoffs", 0)) + handoffs
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _ledger_path(state_dir, day)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=state_dir, prefix=f".{path.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(ledger, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    return ledger
