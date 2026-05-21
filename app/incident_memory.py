from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover - optional dependency fallback
    redis = None


WINDOW_SECONDS = 24 * 60 * 60
CASE_HISTORY_SECONDS = 7 * 24 * 60 * 60
ACTIVE_SUPPRESSION_SECONDS = int(os.getenv("NOC_ACTIVE_INCIDENT_SUPPRESSION_SECONDS", "900"))
CASE_IDLE_SECONDS = int(os.getenv("NOC_CASE_IDLE_SECONDS", str(2 * 60 * 60)))
RECOVERY_COOLDOWN_SECONDS = int(os.getenv("NOC_RECOVERY_COOLDOWN_SECONDS", str(10 * 60)))
INTAKE_LOCK_TTL_MS = int(os.getenv("NOC_INTAKE_LOCK_TTL_MS", "10000"))
MAX_CASE_EVENTS = int(os.getenv("NOC_CASE_MAX_EVENTS", "100"))
MAX_TOPOLOGY_DEPTH = int(os.getenv("NOC_TOPOLOGY_PARENT_MAX_DEPTH", "10"))
OPEN_CASE_STATUSES = {"investigating", "waiting_approval", "recovered_pending"}
CASE_EXPIRY_INDEX_KEY = "noc:case:expiry"


@dataclass
class _LocalIncidentMemory:
    history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    active: dict[str, dict[str, Any]] = field(default_factory=dict)
    summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    cases: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    fingerprint_index: dict[str, str] = field(default_factory=dict)
    resource_open_cases: dict[str, set[str]] = field(default_factory=dict)
    daily_counters: dict[str, int] = field(default_factory=dict)
    topology_parents: dict[str, list[str]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class CaseIntakeResult:
    action: str
    case: dict[str, Any] | None
    event: dict[str, Any]
    should_investigate: bool = False
    should_inject: bool = False
    parent_case: dict[str, Any] | None = None


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

    async def history_for(self, resource_id: str, *, window_seconds: int = WINDOW_SECONDS) -> list[dict[str, Any]]:
        cutoff = time.time() - window_seconds
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

    async def intake_alert(self, alert_payload: dict[str, Any]) -> CaseIntakeResult:
        event = case_event_from_alert(alert_payload)
        if self._redis is not None:
            return await self._intake_alert_redis(event, alert_payload)
        async with self._local.lock:
            return await self._intake_alert_local_locked(event, alert_payload, time.time())

    async def get_case(self, incident_id: str) -> dict[str, Any] | None:
        if self._redis is not None:
            incident_id = _redis_text(incident_id)
            raw = await self._redis.get(f"noc:case:{incident_id}")
            return json.loads(raw) if raw else None
        async with self._local.lock:
            case = self._local.cases.get(incident_id)
            return dict(case) if case else None

    async def case_events(self, incident_id: str) -> list[dict[str, Any]]:
        if self._redis is not None:
            incident_id = _redis_text(incident_id)
            raw = await self._redis.lrange(f"noc:case:events:{incident_id}", 0, -1)
            return [json.loads(item) for item in raw]
        async with self._local.lock:
            return list(self._local.events.get(incident_id, []))

    async def update_case(self, incident_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        fields = _jsonish_dict(fields)
        fields["updated_at"] = utc_now()
        if self._redis is not None:
            incident_id = _redis_text(incident_id)
            raw = await self._redis.get(f"noc:case:{incident_id}")
            if not raw:
                return None
            case = json.loads(raw)
            case.update(fields)
            await self._store_case_redis(case)
            await self._sync_case_indexes_redis(case)
            return case
        async with self._local.lock:
            case = self._local.cases.get(incident_id)
            if not case:
                return None
            case.update(fields)
            self._local.cases[incident_id] = case
            self._sync_case_indexes_local_locked(case)
            return dict(case)

    async def set_case_thread(self, incident_id: str, thread_id: str) -> dict[str, Any] | None:
        return await self.update_case(incident_id, {"thread_id": thread_id})

    async def case_context(self, incident_id: str) -> dict[str, Any]:
        case = await self.get_case(incident_id)
        if not case:
            return {}
        events = await self.case_events(incident_id)
        history = await self._chronic_history(case)
        return {
            "case_number": case.get("case_number", ""),
            "case_status": case.get("status", ""),
            "fingerprint": case.get("fingerprint", ""),
            "resource_key": case.get("resource_id", ""),
            "alert_identity": case.get("identity", {}),
            "event_timeline": [
                {
                    "received_at": event.get("received_at", ""),
                    "state": event.get("state", ""),
                    "summary": event.get("summary", ""),
                }
                for event in events[-10:]
            ],
            "latest_transition": case.get("latest_transition"),
            "chronic_history_7d": history,
            "topology_context": {
                "parents_checked": case.get("parents_checked", []),
                "linked_parent_case": case.get("linked_parent_case"),
                "downstream_victims": case.get("downstream_victims", []),
            },
            "required_output_policy": {
                "safe_automated_actions": "read-only or low-risk actions that could be automated later",
                "high_impact_approval_needed_actions": "restart, config, routing, firewall, VM, or storage changes",
            },
        }

    async def set_topology_parents(self, resource_key: str, parents: list[str]) -> None:
        normalized = _norm_key(resource_key)
        values = [_norm_key(parent) for parent in parents if _norm_key(parent)]
        if self._redis is not None:
            await self._redis.set(f"infra:parent:{normalized}", json.dumps(values))
            return
        async with self._local.lock:
            self._local.topology_parents[normalized] = values

    async def link_to_parent_case(
        self,
        child_case_id: str,
        parent_case_id: str,
        reason: str,
        evidence_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        if self._redis is not None:
            return await self._link_to_parent_case_redis(child_case_id, parent_case_id, reason, evidence_refs or [])
        async with self._local.lock:
            child = self._local.cases.get(child_case_id)
            parent = self._local.cases.get(parent_case_id)
            if not child or not parent:
                return {"ok": False, "error": "case_not_found"}
            return self._link_cases_local_locked(child, parent, reason, evidence_refs or [], time.time())

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

    async def _intake_alert_local_locked(
        self,
        event: dict[str, Any],
        alert_payload: dict[str, Any],
        now: float,
    ) -> CaseIntakeResult:
        self._expire_local_cases_locked(now)
        fingerprint = event["fingerprint"]
        existing_id = self._local.fingerprint_index.get(fingerprint)
        existing = self._local.cases.get(existing_id) if existing_id else None
        if existing and existing.get("status") in OPEN_CASE_STATUSES:
            case, action = self._append_event_local_locked(existing, event, now)
            return CaseIntakeResult(
                action=action,
                case=dict(case),
                event=event,
                should_investigate=False,
                should_inject=bool(case.get("thread_id")),
            )
        if event["is_recovery"]:
            return CaseIntakeResult(action="ignored_recovery", case=None, event=event)

        parent_case, parents_checked = self._find_open_parent_case_local_locked(event["resource_key"], now)
        if parent_case is not None:
            child_case = self._create_case_local_locked(event, alert_payload, now, status="linked")
            child_case["linked_parent_case"] = parent_case["incident_id"]
            child_case["parents_checked"] = parents_checked
            self._local.cases[child_case["incident_id"]] = child_case
            self._local.events[child_case["incident_id"]] = [event]
            self._local.fingerprint_index[event["fingerprint"]] = parent_case["incident_id"]
            parent_event = dict(event)
            parent_event["event_type"] = "downstream_victim"
            parent_event["child_case_id"] = child_case["incident_id"]
            parent = self._append_downstream_victim_local_locked(parent_case, parent_event, now)
            return CaseIntakeResult(
                action="linked_parent",
                case=dict(child_case),
                parent_case=dict(parent),
                event=event,
                should_investigate=False,
                should_inject=bool(parent.get("thread_id")),
            )

        case = self._create_case_local_locked(event, alert_payload, now, status="investigating")
        self._local.cases[case["incident_id"]] = case
        self._local.fingerprint_index[fingerprint] = case["incident_id"]
        self._local.resource_open_cases.setdefault(case["resource_id"], set()).add(case["incident_id"])
        self._local.events[case["incident_id"]] = [event]
        self._append_history_local_locked(case, event)
        return CaseIntakeResult(action="created", case=dict(case), event=event, should_investigate=True)

    async def _intake_alert_redis(self, event: dict[str, Any], alert_payload: dict[str, Any]) -> CaseIntakeResult:
        fingerprint = event["fingerprint"]
        lock_key = f"noc:intake:lock:{fingerprint}"
        owner = str(uuid4())
        acquired = False
        for _ in range(40):
            acquired = bool(await self._redis.set(lock_key, owner, nx=True, px=INTAKE_LOCK_TTL_MS))
            if acquired:
                break
            await asyncio.sleep(0.05)
        if not acquired:
            raise RuntimeError("Could not acquire NOC intake lock")
        try:
            return await self._intake_alert_redis_locked(event, alert_payload, time.time())
        finally:
            with contextlib.suppress(Exception):
                if _redis_text(await self._redis.get(lock_key)) == owner:
                    await self._redis.delete(lock_key)

    async def _intake_alert_redis_locked(
        self,
        event: dict[str, Any],
        alert_payload: dict[str, Any],
        now: float,
    ) -> CaseIntakeResult:
        await self._expire_redis_cases(now)
        existing_id = await self._redis.get(f"noc:case:fingerprint:{event['fingerprint']}")
        existing_id = _redis_text(existing_id) if existing_id else None
        existing = await self.get_case(existing_id) if existing_id else None
        if existing and existing.get("status") in OPEN_CASE_STATUSES:
            case, action = await self._append_event_redis(existing, event, now)
            return CaseIntakeResult(
                action=action,
                case=case,
                event=event,
                should_investigate=False,
                should_inject=bool(case.get("thread_id")),
            )
        if event["is_recovery"]:
            return CaseIntakeResult(action="ignored_recovery", case=None, event=event)

        parent_case, parents_checked = await self._find_open_parent_case_redis(event["resource_key"], now)
        if parent_case is not None:
            child_case = await self._create_case_redis(event, alert_payload, now, status="linked")
            child_case["linked_parent_case"] = parent_case["incident_id"]
            child_case["parents_checked"] = parents_checked
            await self._store_case_redis(child_case)
            await self._redis.rpush(f"noc:case:events:{child_case['incident_id']}", json.dumps(event))
            await self._redis.expire(f"noc:case:events:{child_case['incident_id']}", CASE_HISTORY_SECONDS)
            await self._redis.set(
                f"noc:case:fingerprint:{event['fingerprint']}",
                parent_case["incident_id"],
                ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS,
            )
            parent_event = dict(event)
            parent_event["event_type"] = "downstream_victim"
            parent_event["child_case_id"] = child_case["incident_id"]
            parent = await self._append_downstream_victim_redis(parent_case, parent_event, now)
            return CaseIntakeResult(
                action="linked_parent",
                case=child_case,
                parent_case=parent,
                event=event,
                should_investigate=False,
                should_inject=bool(parent.get("thread_id")),
            )

        case = await self._create_case_redis(event, alert_payload, now, status="investigating")
        await self._store_case_redis(case)
        await self._redis.set(f"noc:case:fingerprint:{event['fingerprint']}", case["incident_id"], ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS)
        await self._redis.sadd(f"noc:open:resource:{case['resource_id']}", case["incident_id"])
        await self._redis.expire(f"noc:open:resource:{case['resource_id']}", CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS)
        await self._redis.rpush(f"noc:case:events:{case['incident_id']}", json.dumps(event))
        await self._redis.expire(f"noc:case:events:{case['incident_id']}", CASE_HISTORY_SECONDS)
        await self._append_history_redis(case, event)
        return CaseIntakeResult(action="created", case=case, event=event, should_investigate=True)

    def _create_case_local_locked(
        self,
        event: dict[str, Any],
        alert_payload: dict[str, Any],
        now: float,
        *,
        status: str,
    ) -> dict[str, Any]:
        day = _case_day(now)
        self._local.daily_counters[day] = self._local.daily_counters.get(day, 0) + 1
        case_number = f"NOC-{day}-{self._local.daily_counters[day]:03d}"
        return _new_case(event, alert_payload, now, case_number=case_number, status=status)

    async def _create_case_redis(
        self,
        event: dict[str, Any],
        alert_payload: dict[str, Any],
        now: float,
        *,
        status: str,
    ) -> dict[str, Any]:
        day = _case_day(now)
        sequence = await self._redis.incr(f"noc:case:daily:{day}")
        await self._redis.expire(f"noc:case:daily:{day}", 3 * 24 * 60 * 60)
        case_number = f"NOC-{day}-{int(sequence):03d}"
        return _new_case(event, alert_payload, now, case_number=case_number, status=status)

    def _append_event_local_locked(
        self,
        case: dict[str, Any],
        event: dict[str, Any],
        now: float,
    ) -> tuple[dict[str, Any], str]:
        previous = case.get("latest_event") or {}
        same_fingerprint = event.get("fingerprint") == case.get("fingerprint")
        if event["is_recovery"] and same_fingerprint:
            action = "recovered"
            case["status"] = "recovered_pending"
            case["recovered_at"] = utc_now(now)
        elif event["is_recovery"]:
            action = "victim_recovered"
            event["event_type"] = "downstream_victim_recovery"
        elif case.get("status") == "recovered_pending" and same_fingerprint:
            action = "reopened"
            case["status"] = "investigating"
            case["recovered_at"] = None
        else:
            action = "escalated" if _state_rank(event.get("state")) > _state_rank(previous.get("state")) else "attached"
        self._update_case_with_event(case, event, previous, now)
        self._local.events.setdefault(case["incident_id"], []).append(event)
        self._local.events[case["incident_id"]] = self._local.events[case["incident_id"]][-MAX_CASE_EVENTS:]
        self._local.cases[case["incident_id"]] = case
        self._local.fingerprint_index[case["fingerprint"]] = case["incident_id"]
        if event.get("fingerprint") and event["fingerprint"] != case["fingerprint"]:
            self._local.fingerprint_index[event["fingerprint"]] = case["incident_id"]
        self._local.resource_open_cases.setdefault(case["resource_id"], set()).add(case["incident_id"])
        self._append_history_local_locked(case, event)
        return case, action

    async def _append_event_redis(
        self,
        case: dict[str, Any],
        event: dict[str, Any],
        now: float,
    ) -> tuple[dict[str, Any], str]:
        previous = case.get("latest_event") or {}
        same_fingerprint = event.get("fingerprint") == case.get("fingerprint")
        if event["is_recovery"] and same_fingerprint:
            action = "recovered"
            case["status"] = "recovered_pending"
            case["recovered_at"] = utc_now(now)
        elif event["is_recovery"]:
            action = "victim_recovered"
            event["event_type"] = "downstream_victim_recovery"
        elif case.get("status") == "recovered_pending" and same_fingerprint:
            action = "reopened"
            case["status"] = "investigating"
            case["recovered_at"] = None
        else:
            action = "escalated" if _state_rank(event.get("state")) > _state_rank(previous.get("state")) else "attached"
        self._update_case_with_event(case, event, previous, now)
        await self._store_case_redis(case)
        await self._redis.rpush(f"noc:case:events:{case['incident_id']}", json.dumps(event))
        await self._redis.ltrim(f"noc:case:events:{case['incident_id']}", -MAX_CASE_EVENTS, -1)
        await self._redis.expire(f"noc:case:events:{case['incident_id']}", CASE_HISTORY_SECONDS)
        await self._redis.set(f"noc:case:fingerprint:{case['fingerprint']}", case["incident_id"], ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS)
        if event.get("fingerprint") and event["fingerprint"] != case["fingerprint"]:
            await self._redis.set(
                f"noc:case:fingerprint:{event['fingerprint']}",
                case["incident_id"],
                ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS,
            )
        await self._redis.sadd(f"noc:open:resource:{case['resource_id']}", case["incident_id"])
        await self._redis.expire(f"noc:open:resource:{case['resource_id']}", CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS)
        await self._append_history_redis(case, event)
        return case, action

    def _update_case_with_event(
        self,
        case: dict[str, Any],
        event: dict[str, Any],
        previous: dict[str, Any],
        now: float,
    ) -> None:
        transition = _transition(previous, event)
        case["updated_at"] = utc_now(now)
        case["last_event_at"] = utc_now(now)
        case["event_count"] = int(case.get("event_count", 0)) + 1
        case["latest_event"] = event
        if transition is not None:
            case["latest_transition"] = transition
            if _state_rank(event.get("state")) > _state_rank(previous.get("state")):
                case["needs_reassessment"] = True
        case["status"] = case.get("status") or "investigating"

    def _append_downstream_victim_local_locked(
        self,
        parent: dict[str, Any],
        event: dict[str, Any],
        now: float,
    ) -> dict[str, Any]:
        victims = list(parent.get("downstream_victims", []))
        resource = event.get("resource_key")
        if resource and resource not in victims:
            victims.append(resource)
        parent["downstream_victims"] = victims
        parent["updated_at"] = utc_now(now)
        parent["latest_event"] = event
        parent["event_count"] = int(parent.get("event_count", 0)) + 1
        self._local.events.setdefault(parent["incident_id"], []).append(event)
        self._local.cases[parent["incident_id"]] = parent
        return parent

    async def _append_downstream_victim_redis(
        self,
        parent: dict[str, Any],
        event: dict[str, Any],
        now: float,
    ) -> dict[str, Any]:
        victims = list(parent.get("downstream_victims", []))
        resource = event.get("resource_key")
        if resource and resource not in victims:
            victims.append(resource)
        parent["downstream_victims"] = victims
        parent["updated_at"] = utc_now(now)
        parent["latest_event"] = event
        parent["event_count"] = int(parent.get("event_count", 0)) + 1
        await self._store_case_redis(parent)
        await self._redis.rpush(f"noc:case:events:{parent['incident_id']}", json.dumps(event))
        await self._redis.expire(f"noc:case:events:{parent['incident_id']}", CASE_HISTORY_SECONDS)
        return parent

    def _find_open_parent_case_local_locked(
        self,
        resource_key: str,
        now: float,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        visited: set[str] = set()
        parents_checked: list[str] = []
        frontier = [_norm_key(resource_key)]
        for _depth in range(MAX_TOPOLOGY_DEPTH):
            next_frontier: list[str] = []
            for resource in frontier:
                for parent in self._local.topology_parents.get(resource, []):
                    if parent in visited:
                        continue
                    visited.add(parent)
                    parents_checked.append(parent)
                    case = self._newest_open_case_local_locked(parent, now)
                    if case:
                        return case, parents_checked
                    next_frontier.append(parent)
            if not next_frontier:
                break
            frontier = next_frontier
        return None, parents_checked

    async def _find_open_parent_case_redis(
        self,
        resource_key: str,
        now: float,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        visited: set[str] = set()
        parents_checked: list[str] = []
        frontier = [_norm_key(resource_key)]
        for _depth in range(MAX_TOPOLOGY_DEPTH):
            next_frontier: list[str] = []
            for resource in frontier:
                for parent in await self._parents_for_redis(resource):
                    if parent in visited:
                        continue
                    visited.add(parent)
                    parents_checked.append(parent)
                    case = await self._newest_open_case_redis(parent, now)
                    if case:
                        return case, parents_checked
                    next_frontier.append(parent)
            if not next_frontier:
                break
            frontier = next_frontier
        return None, parents_checked

    def _newest_open_case_local_locked(self, resource_key: str, now: float) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for case_id in self._local.resource_open_cases.get(resource_key, set()):
            case = self._local.cases.get(case_id)
            if case and case.get("status") in OPEN_CASE_STATUSES and not _case_is_idle(case, now):
                candidates.append(case)
        return max(candidates, key=lambda item: item.get("updated_at", "")) if candidates else None

    async def _newest_open_case_redis(self, resource_key: str, now: float) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for case_id in await self._redis.smembers(f"noc:open:resource:{resource_key}"):
            case = await self.get_case(_redis_text(case_id))
            if case and case.get("status") in OPEN_CASE_STATUSES and not _case_is_idle(case, now):
                candidates.append(case)
        return max(candidates, key=lambda item: item.get("updated_at", "")) if candidates else None

    async def _parents_for_redis(self, resource_key: str) -> list[str]:
        key = f"infra:parent:{resource_key}"
        try:
            raw = await self._redis.get(key)
        except Exception:
            raw = None
        if raw:
            parsed = json.loads(_redis_text(raw))
            if isinstance(parsed, list):
                return [_norm_key(item) for item in parsed if _norm_key(item)]
        with contextlib.suppress(Exception):
            values = await self._redis.lrange(key, 0, -1)
            if values:
                return [_norm_key(_redis_text(item)) for item in values if _norm_key(_redis_text(item))]
        with contextlib.suppress(Exception):
            values = await self._redis.smembers(key)
            if values:
                return [_norm_key(_redis_text(item)) for item in values if _norm_key(_redis_text(item))]
        return []

    def _expire_local_cases_locked(self, now: float) -> None:
        for case in list(self._local.cases.values()):
            status = case.get("status")
            if status == "recovered_pending" and _seconds_since(case.get("recovered_at"), now) >= RECOVERY_COOLDOWN_SECONDS:
                case["status"] = "resolved"
                case["updated_at"] = utc_now(now)
                self._drop_open_indexes_local_locked(case)
            elif status in {"investigating", "waiting_approval"} and _case_is_idle(case, now):
                case["status"] = "expired"
                case["updated_at"] = utc_now(now)
                self._drop_open_indexes_local_locked(case)

    async def _expire_redis_cases(self, now: float) -> None:
        case_ids = await self._redis.zrangebyscore(CASE_EXPIRY_INDEX_KEY, "-inf", now)
        for raw_case_id in case_ids:
            case_id = _redis_text(raw_case_id)
            case = await self.get_case(case_id)
            if not case:
                await self._redis.zrem(CASE_EXPIRY_INDEX_KEY, case_id)
                continue
            expiry_at = _case_expiry_ts(case)
            if expiry_at is None:
                await self._redis.zrem(CASE_EXPIRY_INDEX_KEY, case_id)
                continue
            if expiry_at > now:
                await self._redis.zadd(CASE_EXPIRY_INDEX_KEY, {case_id: expiry_at})
                continue
            status = case.get("status")
            if status == "recovered_pending" and _seconds_since(case.get("recovered_at"), now) >= RECOVERY_COOLDOWN_SECONDS:
                case["status"] = "resolved"
                case["updated_at"] = utc_now(now)
                await self._store_case_redis(case)
                await self._drop_open_indexes_redis(case)
            elif status in {"investigating", "waiting_approval"} and _case_is_idle(case, now):
                case["status"] = "expired"
                case["updated_at"] = utc_now(now)
                await self._store_case_redis(case)
                await self._drop_open_indexes_redis(case)

    def _drop_open_indexes_local_locked(self, case: dict[str, Any]) -> None:
        if self._local.fingerprint_index.get(case.get("fingerprint")) == case.get("incident_id"):
            self._local.fingerprint_index.pop(case.get("fingerprint"), None)
        resource = case.get("resource_id")
        if resource in self._local.resource_open_cases:
            self._local.resource_open_cases[resource].discard(case["incident_id"])

    async def _drop_open_indexes_redis(self, case: dict[str, Any]) -> None:
        await self._redis.delete(f"noc:case:fingerprint:{case.get('fingerprint')}")
        await self._redis.srem(f"noc:open:resource:{case.get('resource_id')}", case.get("incident_id"))

    def _sync_case_indexes_local_locked(self, case: dict[str, Any]) -> None:
        if case.get("status") not in OPEN_CASE_STATUSES:
            self._drop_open_indexes_local_locked(case)
            return
        fingerprint = case.get("fingerprint")
        resource = case.get("resource_id")
        if fingerprint:
            self._local.fingerprint_index[fingerprint] = case["incident_id"]
        if resource:
            self._local.resource_open_cases.setdefault(resource, set()).add(case["incident_id"])

    async def _sync_case_indexes_redis(self, case: dict[str, Any]) -> None:
        if case.get("status") not in OPEN_CASE_STATUSES:
            await self._drop_open_indexes_redis(case)
            return
        fingerprint = case.get("fingerprint")
        resource = case.get("resource_id")
        if fingerprint:
            await self._redis.set(
                f"noc:case:fingerprint:{fingerprint}",
                case["incident_id"],
                ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS,
            )
        if resource:
            await self._redis.sadd(f"noc:open:resource:{resource}", case["incident_id"])
            await self._redis.expire(f"noc:open:resource:{resource}", CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS)

    def _append_history_local_locked(self, case: dict[str, Any], event: dict[str, Any]) -> None:
        item = _history_item(case, event)
        for key in {case["fingerprint"], case["resource_id"]}:
            self._local.history.setdefault(key, []).append(item)
            cutoff = time.time() - CASE_HISTORY_SECONDS
            self._local.history[key] = [entry for entry in self._local.history[key] if entry["ts"] >= cutoff]

    async def _append_history_redis(self, case: dict[str, Any], event: dict[str, Any]) -> None:
        item = json.dumps(_history_item(case, event))
        for key in {case["fingerprint"], case["resource_id"]}:
            redis_key = f"noc:history:{key}"
            await self._redis.rpush(redis_key, item)
            await self._redis.expire(redis_key, CASE_HISTORY_SECONDS)

    async def _store_case_redis(self, case: dict[str, Any]) -> None:
        case = _jsonish_dict(case)
        await self._redis.set(f"noc:case:{case['incident_id']}", json.dumps(case), ex=CASE_HISTORY_SECONDS)
        expiry_at = _case_expiry_ts(case)
        if expiry_at is None:
            await self._redis.zrem(CASE_EXPIRY_INDEX_KEY, case["incident_id"])
            return
        await self._redis.zadd(CASE_EXPIRY_INDEX_KEY, {case["incident_id"]: expiry_at})
        await self._redis.expire(CASE_EXPIRY_INDEX_KEY, CASE_HISTORY_SECONDS)

    def _link_cases_local_locked(
        self,
        child: dict[str, Any],
        parent: dict[str, Any],
        reason: str,
        evidence_refs: list[str],
        now: float,
    ) -> dict[str, Any]:
        child["status"] = "linked"
        child["linked_parent_case"] = parent["incident_id"]
        child["link_reason"] = reason
        child["link_evidence_refs"] = list(evidence_refs)
        child["updated_at"] = utc_now(now)
        self._drop_open_indexes_local_locked(child)
        if child.get("fingerprint"):
            self._local.fingerprint_index[child["fingerprint"]] = parent["incident_id"]
        parent_event = {
            "received_at": utc_now(now),
            "event_type": "linked_child_case",
            "child_case_id": child["incident_id"],
            "resource_key": child.get("resource_id"),
            "summary": reason,
            "evidence_refs": list(evidence_refs),
        }
        parent = self._append_downstream_victim_local_locked(parent, parent_event, now)
        self._local.cases[child["incident_id"]] = child
        return {"ok": True, "child_case": dict(child), "parent_case": dict(parent)}

    async def _link_to_parent_case_redis(
        self,
        child_case_id: str,
        parent_case_id: str,
        reason: str,
        evidence_refs: list[str],
    ) -> dict[str, Any]:
        child = await self.get_case(child_case_id)
        parent = await self.get_case(parent_case_id)
        if not child or not parent:
            return {"ok": False, "error": "case_not_found"}
        now = time.time()
        child["status"] = "linked"
        child["linked_parent_case"] = parent["incident_id"]
        child["link_reason"] = reason
        child["link_evidence_refs"] = list(evidence_refs)
        child["updated_at"] = utc_now(now)
        await self._drop_open_indexes_redis(child)
        await self._store_case_redis(child)
        if child.get("fingerprint"):
            await self._redis.set(
                f"noc:case:fingerprint:{child['fingerprint']}",
                parent["incident_id"],
                ex=CASE_IDLE_SECONDS + RECOVERY_COOLDOWN_SECONDS,
            )
        parent_event = {
            "received_at": utc_now(now),
            "event_type": "linked_child_case",
            "child_case_id": child["incident_id"],
            "resource_key": child.get("resource_id"),
            "summary": reason,
            "evidence_refs": list(evidence_refs),
        }
        parent = await self._append_downstream_victim_redis(parent, parent_event, now)
        return {"ok": True, "child_case": child, "parent_case": parent}

    async def _chronic_history(self, case: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        cutoff = now - CASE_HISTORY_SECONDS
        history = await self.history_for(case.get("fingerprint", ""), window_seconds=CASE_HISTORY_SECONDS)
        history.extend(await self.history_for(case.get("resource_id", ""), window_seconds=CASE_HISTORY_SECONDS))
        seen: set[tuple[str, float]] = set()
        events = []
        cases: set[str] = set()
        for item in history:
            marker = (item.get("case_number", ""), float(item.get("ts", 0)))
            if marker in seen or item.get("ts", 0) < cutoff:
                continue
            seen.add(marker)
            events.append(item)
            if item.get("case_number"):
                cases.add(str(item["case_number"]))
        last_seen = max((item.get("received_at", "") for item in events), default="")
        return {
            "event_count": len(events),
            "case_count": len(cases),
            "last_seen": last_seen,
            "pattern": "repeated alerts for the same identity" if len(events) > 1 else "",
        }


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


def fingerprint_alert(alert_payload: dict[str, Any]) -> str:
    identity = alert_identity(alert_payload)
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def alert_identity(alert_payload: dict[str, Any]) -> dict[str, str]:
    labels = _labels(alert_payload)
    source = _norm_key(alert_payload.get("source") or "alertmanager")
    alertname = _norm_key(labels.get("alertname") or alert_payload.get("source") or "unknown")
    resource_key = _resource_key(alert_payload, labels)
    service_key = _norm_key(labels.get("service") or labels.get("check_command") or labels.get("job") or alertname)
    instance = str(labels.get("instance") or "")
    instance_key = _norm_key(instance_host(instance) if (labels.get("host") or labels.get("hostname") or service_key) else instance)
    return {
        "source": source,
        "alertname": alertname,
        "resource_key": resource_key,
        "service_key": service_key,
        "instance_key": instance_key,
    }


def case_event_from_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels = _labels(alert_payload)
    identity = alert_identity(alert_payload)
    status = str(alert_payload.get("status") or _first_alert(alert_payload).get("status") or labels.get("state") or "unknown")
    state = str(labels.get("state") or labels.get("severity") or status or "unknown").upper()
    fingerprint = fingerprint_alert(alert_payload)
    return {
        "received_at": utc_now(),
        "source": identity["source"],
        "fingerprint": fingerprint,
        "identity": identity,
        "alertname": identity["alertname"],
        "display_alertname": _alert_name(alert_payload),
        "resource_key": identity["resource_key"],
        "display_resource": _display_resource(alert_payload, labels),
        "service_key": identity["service_key"],
        "status": status,
        "state": state,
        "severity": str(labels.get("severity") or "").upper(),
        "summary": _alert_summary(alert_payload),
        "labels": _jsonish_dict(labels),
        "is_recovery": is_recovery_alert(alert_payload),
        "raw_excerpt": _json_excerpt(alert_payload),
    }


def is_recovery_alert(alert_payload: dict[str, Any]) -> bool:
    status_value = str(alert_payload.get("status") or "").lower()
    if status_value in {"resolved", "recovery", "ok", "up"}:
        return True
    first_status = str(_first_alert(alert_payload).get("status") or "").lower()
    if first_status in {"resolved", "recovery", "ok", "up"}:
        return True
    labels = _labels(alert_payload)
    return str(labels.get("state") or "").upper() in {"OK", "UP"}


def case_display_title(case: dict[str, Any] | None, event: dict[str, Any] | None = None) -> str:
    if case is None and event is None:
        return "NOC: unknown alert"
    case_number = (case or {}).get("case_number") or "NOC"
    identity = (event or {}).get("identity") or (case or {}).get("identity") or {}
    alertname = (event or {}).get("display_alertname") or identity.get("alertname") or (event or {}).get("alertname") or "unknown"
    resource = (
        (event or {}).get("display_resource")
        or identity.get("resource_key")
        or (event or {}).get("resource_key")
        or (case or {}).get("resource_id")
        or "unknown"
    )
    return f"{case_number}: {alertname} on {resource}"


def utc_now(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts, timezone.utc) if ts is not None else datetime.now(timezone.utc)
    return dt.isoformat()


def _new_case(
    event: dict[str, Any],
    alert_payload: dict[str, Any],
    now: float,
    *,
    case_number: str,
    status: str,
) -> dict[str, Any]:
    incident_id = str(uuid4())
    return {
        "incident_id": incident_id,
        "case_number": case_number,
        "fingerprint": event["fingerprint"],
        "identity": dict(event["identity"]),
        "resource_id": event["resource_key"],
        "title": case_display_title({"case_number": case_number, "identity": event["identity"]}, event),
        "status": status,
        "created_at": utc_now(now),
        "updated_at": utc_now(now),
        "last_event_at": utc_now(now),
        "event_count": 1,
        "latest_event": event,
        "latest_transition": None,
        "thread_id": None,
        "downstream_victims": [],
        "parents_checked": [],
        "linked_parent_case": None,
        "recovered_at": utc_now(now) if event.get("is_recovery") else None,
        "needs_reassessment": False,
        "source_excerpt": _json_excerpt(alert_payload),
    }


def _history_item(case: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    return {
        "ts": _parse_ts(event.get("received_at")) or time.time(),
        "received_at": event.get("received_at", ""),
        "case_number": case.get("case_number", ""),
        "incident_id": case.get("incident_id", ""),
        "fingerprint": case.get("fingerprint", ""),
        "resource_id": case.get("resource_id", ""),
        "alertname": event.get("alertname", ""),
        "state": event.get("state", ""),
        "summary": event.get("summary", ""),
    }


def _transition(previous: dict[str, Any], event: dict[str, Any]) -> dict[str, str] | None:
    old = str(previous.get("state") or "").upper()
    new = str(event.get("state") or "").upper()
    if not old or not new or old == new:
        return None
    meaning = "severity escalation for same issue" if _state_rank(new) > _state_rank(old) else "state transition for same issue"
    return {"from": old, "to": new, "meaning": meaning}


def _case_day(now: float) -> str:
    return datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%d")


def _resource_key(alert_payload: dict[str, Any], labels: dict[str, Any]) -> str:
    host = labels.get("host") or labels.get("hostname")
    if host:
        return _norm_key(host)
    instance = labels.get("instance")
    if instance:
        return _norm_key(instance_host(str(instance)))
    return _norm_key(alert_payload.get("source") or "unknown")


def _display_resource(alert_payload: dict[str, Any], labels: dict[str, Any]) -> str:
    host = labels.get("host") or labels.get("hostname")
    if host:
        return str(host)
    instance = labels.get("instance")
    if instance:
        return instance_host(str(instance))
    return str(alert_payload.get("source") or "unknown")


def instance_host(instance: str) -> str:
    if instance.startswith("[") and "]" in instance:
        return instance[1:instance.index("]")]
    if instance.count(":") == 1:
        return instance.rsplit(":", 1)[0]
    return instance


def _first_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alerts[0], dict):
        return alerts[0]
    return {}


def _alert_summary(alert_payload: dict[str, Any]) -> str:
    first = _first_alert(alert_payload)
    annotations = {}
    for key in ("commonAnnotations", "annotations"):
        if isinstance(alert_payload.get(key), dict):
            annotations.update(alert_payload[key])
    if isinstance(first.get("annotations"), dict):
        annotations.update(first["annotations"])
    return str(annotations.get("summary") or annotations.get("description") or alert_payload.get("output") or "").strip()


def _norm_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _jsonish_dict(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _json_excerpt(value: Any, limit: int = 3000) -> dict[str, Any] | str:
    safe = _jsonish_dict(value)
    text = json.dumps(safe, sort_keys=True, separators=(",", ":"))
    if len(text) <= limit:
        return safe
    return text[: limit - 3] + "..."


def _state_rank(value: Any) -> int:
    state = str(value or "").upper()
    if state in {"OK", "UP", "RESOLVED", "RECOVERY"}:
        return 0
    if state in {"WARNING", "WARN", "DEGRADED", "MEDIUM"}:
        return 1
    if state in {"CRITICAL", "CRIT", "HIGH", "FIRING", "DOWN"}:
        return 2
    return 1 if state else 0


def _parse_ts(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _seconds_since(value: Any, now: float) -> float:
    ts = _parse_ts(value)
    if ts is None:
        return 0
    return now - ts


def _case_is_idle(case: dict[str, Any], now: float) -> bool:
    return _seconds_since(case.get("last_event_at") or case.get("updated_at"), now) >= CASE_IDLE_SECONDS


def _case_expiry_ts(case: dict[str, Any]) -> float | None:
    status = case.get("status")
    if status == "recovered_pending":
        recovered_at = _parse_ts(case.get("recovered_at"))
        return (recovered_at or time.time()) + RECOVERY_COOLDOWN_SECONDS
    if status in {"investigating", "waiting_approval"}:
        last_event_at = _parse_ts(case.get("last_event_at") or case.get("updated_at"))
        return (last_event_at or time.time()) + CASE_IDLE_SECONDS
    return None
