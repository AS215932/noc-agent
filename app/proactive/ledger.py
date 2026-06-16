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
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_LOCK_MAX_AGE_SECONDS = 2 * 60 * 60


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
    if lock_path.exists():
        try:
            holder = json.loads(lock_path.read_text(encoding="utf-8"))
            pid = int(holder.get("pid", -1))
            started = float(holder.get("started_at", 0.0))
        except (json.JSONDecodeError, ValueError, OSError):
            pid, started = -1, 0.0
        fresh = (time.time() - started) < max_age_seconds
        if pid > 0 and fresh and _pid_alive(pid):
            return None
        lock_path.unlink(missing_ok=True)
    lock_path.write_text(
        json.dumps({"pid": os.getpid(), "started_at": time.time()}),
        encoding="utf-8",
    )
    return lock_path


def release_lock(lock_path: Path | None) -> None:
    if lock_path is not None:
        lock_path.unlink(missing_ok=True)


def _ledger_path(state_dir: Path, day: str) -> Path:
    return state_dir / f"ledger-{day}.json"


def _empty_ledger() -> dict[str, Any]:
    return {"cycles": 0, "investigations": 0, "cost_usd": 0.0, "handoffs": 0}


def load_ledger(state_dir: Path, day: str | None = None) -> dict[str, Any]:
    day = day or today()
    path = _ledger_path(state_dir, day)
    if not path.exists():
        return _empty_ledger()
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _empty_ledger()
    if not isinstance(loaded, dict):
        return _empty_ledger()
    merged = _empty_ledger()
    merged.update({k: loaded.get(k, merged[k]) for k in merged})
    return merged


def update_ledger(
    state_dir: Path,
    day: str | None = None,
    *,
    cycles: int = 0,
    investigations: int = 0,
    cost_usd: float = 0.0,
    handoffs: int = 0,
) -> dict[str, Any]:
    day = day or today()
    ledger = load_ledger(state_dir, day)
    ledger["cycles"] = int(ledger.get("cycles", 0)) + cycles
    ledger["investigations"] = int(ledger.get("investigations", 0)) + investigations
    ledger["cost_usd"] = round(float(ledger.get("cost_usd", 0.0)) + cost_usd, 6)
    ledger["handoffs"] = int(ledger.get("handoffs", 0)) + handoffs
    state_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path(state_dir, day).write_text(
        json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return ledger
