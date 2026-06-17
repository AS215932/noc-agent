"""Operator ack/snooze suppression for proactive hotspots.

Once an operator acknowledges a hotspot (they've filed a tracking issue or
decided how to handle it — e.g. the monero/disk findings), it's suppressed from
the digest *and* from autonomous investigation until it resolves (or an optional
TTL expires). Resolved suppressions are pruned so a later recurrence re-alerts.

File-backed (under the proactive state dir) so the control-plane endpoints and
the running loop share state without coupling — the endpoint writes, the loop
reads it each cycle. All I/O is best-effort.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.proactive.models import utc_now


class SuppressionStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    # --- persistence ------------------------------------------------------

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            if not self.path.exists():
                return {}
            data = json.loads(self.path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self, data: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.path)

    # --- queries ----------------------------------------------------------

    def active(self, now: float | None = None) -> dict[str, dict[str, Any]]:
        """Non-expired suppressions, keyed by hotspot fingerprint."""
        now = now or time.time()
        out: dict[str, dict[str, Any]] = {}
        for fingerprint, entry in self._load().items():
            expires_at = entry.get("expires_at")
            if expires_at is None or float(expires_at) > now:
                out[fingerprint] = entry
        return out

    def entries(self) -> list[dict[str, Any]]:
        return list(self._load().values())

    # --- mutations --------------------------------------------------------

    def add(
        self,
        *,
        fingerprint: str,
        key: str = "",
        reason: str = "",
        issue: str = "",
        operator: str = "",
        ttl_seconds: float | None = None,
    ) -> dict[str, Any]:
        data = self._load()
        entry = {
            "fingerprint": fingerprint,
            "key": key,
            "reason": reason,
            "issue": issue,
            "operator": operator,
            "created_at": utc_now(),
            "expires_at": (time.time() + ttl_seconds) if ttl_seconds else None,
        }
        data[fingerprint] = entry
        self._save(data)
        return entry

    def remove(self, fingerprint: str) -> bool:
        data = self._load()
        if fingerprint in data:
            del data[fingerprint]
            self._save(data)
            return True
        return False

    def prune_resolved(self, firing_fingerprints: set[str]) -> list[str]:
        """Drop suppressions whose hotspot is no longer firing, so a future
        recurrence re-alerts. Returns the pruned fingerprints."""
        data = self._load()
        pruned = [fp for fp in data if fp not in firing_fingerprints]
        if pruned:
            for fp in pruned:
                del data[fp]
            self._save(data)
        return pruned
