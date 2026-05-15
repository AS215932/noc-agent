from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover - optional dependency fallback
    redis = None


WINDOW_SECONDS = 24 * 60 * 60
ACTIVE_SUPPRESSION_SECONDS = int(os.getenv("NOC_ACTIVE_INCIDENT_SUPPRESSION_SECONDS", "900"))


@dataclass
class _LocalIncidentMemory:
    history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    active: dict[str, dict[str, Any]] = field(default_factory=dict)
    summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class IncidentMemory:
    def __init__(self, redis_url: str | None = None):
        self.redis_url = redis_url or os.getenv("NOC_REDIS_URL", "")
        self._local = _LocalIncidentMemory()
        self._redis = None
        if self.redis_url and redis is not None:
            self._redis = redis.Redis.from_url(self.redis_url, decode_responses=True)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def correlate(self, resource_id: str, alert_payload: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        incident = {
            "ts": now,
            "resource_id": resource_id,
            "alertname": _alert_name(alert_payload),
            "severity": _alert_severity(alert_payload),
        }
        if self._redis is not None:
            return await self._correlate_redis(resource_id, incident, now)
        return await self._correlate_local(resource_id, incident, now)

    async def history_for(self, resource_id: str) -> list[dict[str, Any]]:
        cutoff = time.time() - WINDOW_SECONDS
        if self._redis is not None:
            raw = await self._redis.lrange(f"noc:history:{resource_id}", 0, -1)
            history = []
            for item in raw:
                parsed = json.loads(item)
                if parsed["ts"] >= cutoff:
                    history.append(parsed)
            return history
        async with self._local.lock:
            return [item for item in self._local.history.get(resource_id, []) if item["ts"] >= cutoff]

    async def put_summary(self, incident_id: str, summary: dict[str, Any]) -> None:
        if self._redis is not None:
            await self._redis.set(f"noc:summary:{incident_id}", json.dumps(summary), ex=WINDOW_SECONDS)
            return
        async with self._local.lock:
            self._local.summaries[incident_id] = summary

    async def get_summary(self, incident_id: str) -> dict[str, Any] | None:
        if self._redis is not None:
            raw = await self._redis.get(f"noc:summary:{incident_id}")
            return json.loads(raw) if raw else None
        async with self._local.lock:
            return self._local.summaries.get(incident_id)

    async def list_summaries(self) -> list[dict[str, Any]]:
        if self._redis is not None:
            summaries: list[dict[str, Any]] = []
            async for key in self._redis.scan_iter("noc:summary:*"):
                raw = await self._redis.get(key)
                if raw:
                    summaries.append(json.loads(raw))
            return summaries
        async with self._local.lock:
            return list(self._local.summaries.values())

    async def _correlate_local(self, resource_id: str, incident: dict[str, Any], now: float) -> dict[str, Any]:
        cutoff = now - WINDOW_SECONDS
        async with self._local.lock:
            active = self._local.active.get(resource_id)
            deduped = bool(active and now - active["ts"] <= ACTIVE_SUPPRESSION_SECONDS)
            if not deduped:
                self._local.active[resource_id] = incident
                self._local.history.setdefault(resource_id, []).append(incident)
            self._local.history[resource_id] = [
                item for item in self._local.history.get(resource_id, []) if item["ts"] >= cutoff
            ]
            history = list(self._local.history[resource_id])
        return {"deduped": deduped, "history": history, "chronic": len(history) > 3}

    async def _correlate_redis(self, resource_id: str, incident: dict[str, Any], now: float) -> dict[str, Any]:
        active_key = f"noc:active:{resource_id}"
        history_key = f"noc:history:{resource_id}"
        deduped = await self._redis.exists(active_key) == 1
        if not deduped:
            await self._redis.set(active_key, json.dumps(incident), ex=ACTIVE_SUPPRESSION_SECONDS)
            await self._redis.rpush(history_key, json.dumps(incident))
            await self._redis.expire(history_key, WINDOW_SECONDS)
        history = await self.history_for(resource_id)
        return {"deduped": deduped, "history": history, "chronic": len(history) > 3}


def _labels(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels: dict[str, Any] = {}
    for key in ("groupLabels", "commonLabels"):
        if isinstance(alert_payload.get(key), dict):
            labels.update(alert_payload[key])
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alerts[0], dict):
        nested = alerts[0].get("labels")
        if isinstance(nested, dict):
            labels.update(nested)
    return labels


def _alert_name(alert_payload: dict[str, Any]) -> str:
    return str(_labels(alert_payload).get("alertname") or alert_payload.get("source") or "unknown")


def _alert_severity(alert_payload: dict[str, Any]) -> str:
    return str(_labels(alert_payload).get("severity") or "unknown")

