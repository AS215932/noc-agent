"""CaseService-backed graph memory adapter.

This adapter exposes the small incident-memory surface the LangGraph runtime
needs while storing graph context/summaries on CaseService projections/events.
It is used for CaseService-primary reactive cases so new case paths no longer
write graph context through legacy IncidentMemory.
"""

from __future__ import annotations

import json
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, cast

from app.cases.models import AtomicCaseProjection, CaseEvent, CaseStatus, utc_now
from app.cases.store import CaseStore

_TERMINAL = {"resolved", "closed", "expired", "linked"}
_GRAPH_AUTHORED_CASE_STATUSES = {"investigating", "waiting_approval", "recovered_pending"}


class CaseServiceGraphMemory:
    """IncidentMemory-compatible adapter backed by CaseService storage."""

    def __init__(self, store: CaseStore) -> None:
        self.store = store

    async def resolve_case_identifier(self, identifier: str) -> str | None:
        text = str(identifier or "").strip()
        if not text:
            return None
        direct = await self.store.get_case(text)
        if isinstance(direct, AtomicCaseProjection):
            return direct.case_id
        for alias_type in ("source_fp", "source_event_id", "legacy_incident_id", "legacy_case_number"):
            case_id = await self.store.resolve_alias(alias_type, text)
            if case_id:
                return case_id
        for case in await self.store.list_cases(kind="atomic", limit=500):
            if isinstance(case, AtomicCaseProjection) and str(case.case_number or "").strip() == text:
                return case.case_id
        return None

    async def get_case(self, incident_id: str) -> dict[str, Any] | None:
        case = await self._case(incident_id)
        return _case_dict(case) if case is not None else None

    async def case_events(self, incident_id: str) -> list[dict[str, Any]]:
        case_id = await self.resolve_case_identifier(incident_id)
        if not case_id:
            return []
        events = await self.store.case_events(case_id)
        return [_event_dict(event) for event in events]

    async def case_context(self, incident_id: str) -> dict[str, Any]:
        case = await self._case(incident_id)
        if case is None:
            return {}
        events = await self.store.case_events(case.case_id)
        history = await self._history_for_case(case)
        return {
            "case_number": _safe_string(case.case_number, limit=128),
            "case_status": _safe_string(case.status, limit=64),
            "fingerprint": _safe_string(case.fingerprint, limit=128),
            "resource_key": _safe_string(case.resource_id, limit=240),
            "alert_identity": _bounded_json(case.identity),
            "event_timeline": [_timeline_event(event, case) for event in events[-10:]],
            "latest_transition": None,
            "chronic_history_7d": {
                "event_count": len(history),
                "case_count": len({item.get("case_id") for item in history if item.get("case_id")}),
                "last_seen": max((str(item.get("received_at") or "") for item in history), default=""),
                "pattern": "repeated CaseService observations for this resource" if len(history) > 1 else "",
            },
            "topology_context": {
                "parents_checked": [],
                "linked_parent_case": "",
                "downstream_victims": [],
            },
            "required_output_policy": {
                "safe_automated_actions": "read-only or low-risk actions that could be automated later",
                "high_impact_approval_needed_actions": "restart, config, routing, firewall, VM, or storage changes",
            },
        }

    async def correlate(self, resource_id: str, alert_payload: dict[str, Any]) -> dict[str, Any]:
        history = await self.history_for(resource_id)
        return {"deduped": False, "history": history, "chronic": len(history) > 3}

    async def link_to_parent_case(
        self,
        child_case_id: str,
        parent_case_id: str,
        reason: str,
        evidence_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        child = await self._case(child_case_id)
        parent = await self._case(parent_case_id)
        if child is None or parent is None:
            return {"ok": False, "error": "case_not_found"}
        refs = [_safe_string(item, limit=160) for item in list(evidence_refs or [])[:20]]
        safe_reason = _safe_string(reason, limit=500)
        diagnosis = dict(child.last_diagnosis or {})
        diagnosis["linked_parent_case"] = parent.case_id
        diagnosis["link_reason"] = safe_reason
        diagnosis["link_evidence_refs"] = refs
        child.status = "linked"
        child.resolution_reason = "linked_parent"
        child.resolved_at = utc_now()
        child.last_diagnosis = diagnosis
        child.updated_at = utc_now()
        child = cast(AtomicCaseProjection, await self.store.upsert_case(child))
        await self.store.append_event(
            CaseEvent(
                case_id=child.case_id,
                event_type="case_linked_to_parent",
                actor_type="graph",
                payload={"parent_case_id": parent.case_id, "reason": safe_reason, "evidence_refs": refs},
            )
        )
        await self.store.append_event(
            CaseEvent(
                case_id=parent.case_id,
                event_type="linked_child_case",
                actor_type="graph",
                payload={
                    "child_case_id": child.case_id,
                    "resource_key": _safe_string(child.resource_id, limit=240),
                    "summary": safe_reason,
                    "evidence_refs": refs,
                },
            )
        )
        return {"ok": True, "child_case": _case_dict(child), "parent_case": _case_dict(parent)}

    async def history_for(self, resource_id: str, *, window_seconds: int = 7 * 24 * 3600) -> list[dict[str, Any]]:
        now = time.time()
        cutoff = now - window_seconds
        rows: list[dict[str, Any]] = []
        for case in await self.store.list_cases(kind="atomic", limit=500):
            if not isinstance(case, AtomicCaseProjection):
                continue
            if _resource_key(case.resource_id) != _resource_key(resource_id):
                continue
            event_rows = []
            for event in await self.store.case_events(case.case_id):
                if event.event_type not in {"case_created", "case_observed_unhealthy", "case_signal_changed"}:
                    continue
                ts = _parse_ts(event.observed_at or event.occurred_at)
                if ts >= cutoff:
                    event_rows.append(_history_event_row(case, event, ts))
            if event_rows:
                rows.extend(event_rows)
                continue
            ts = _parse_ts(case.last_seen or case.updated_at or case.opened_at)
            if ts < cutoff:
                continue
            rows.append(_history_row(case, ts))
        rows.sort(key=lambda item: float(item.get("ts") or 0), reverse=True)
        return rows[:50]

    async def put_summary(self, incident_id: str, summary: dict[str, Any]) -> None:
        case = await self._require_case(incident_id)
        rendered = _summary_payload(summary)
        diagnosis = dict(case.last_diagnosis or {})
        diagnosis["graph_summary"] = rendered
        if rendered.get("thread_id"):
            diagnosis["thread_id"] = _safe_string(rendered.get("thread_id"), limit=128)
        case.last_diagnosis = diagnosis
        rendered_status = str(rendered.get("status") or "")
        if rendered_status in _GRAPH_AUTHORED_CASE_STATUSES:
            case.status = cast(CaseStatus, rendered_status)
        if rendered.get("title"):
            case.summary = _safe_string(rendered.get("title"), limit=1000)
        case.updated_at = utc_now()
        await self.store.upsert_case(case)
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="graph_summary_recorded",
                actor_type="graph",
                payload={
                    "status": _safe_string(rendered.get("status"), limit=64),
                    "thread_id": _safe_string(rendered.get("thread_id"), limit=128),
                    "title": _safe_string(rendered.get("title"), limit=500),
                },
            )
        )

    async def get_summary(self, incident_id: str) -> dict[str, Any] | None:
        case = await self._case(incident_id)
        if case is None:
            return None
        summary = (case.last_diagnosis or {}).get("graph_summary")
        if isinstance(summary, dict):
            return cast(dict[str, Any], _bounded_json(summary))
        return None

    async def list_summaries(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for case in await self.store.list_cases(kind="atomic", limit=500):
            if not isinstance(case, AtomicCaseProjection):
                continue
            summary = (case.last_diagnosis or {}).get("graph_summary")
            if isinstance(summary, dict):
                rows.append(cast(dict[str, Any], _bounded_json(summary)))
        return rows

    async def update_case(self, incident_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        case = await self._require_case(incident_id)
        safe_fields = cast(dict[str, Any], _bounded_json(fields))
        status = str(safe_fields.get("status") or "")
        if status:
            case.status = cast(CaseStatus, _case_status(status, fallback=case.status))
            if case.status in _TERMINAL and case.resolved_at is None:
                case.resolved_at = utc_now()
        if safe_fields.get("diagnostic_summary"):
            case.summary = _safe_string(safe_fields.get("diagnostic_summary"), limit=1000)
        diagnosis = dict(case.last_diagnosis or {})
        stored_graph_summary = diagnosis.get("graph_summary")
        graph_summary: dict[str, Any] = stored_graph_summary if isinstance(stored_graph_summary, dict) else {}
        graph_update = dict(safe_fields)
        graph_update.pop("case_context", None)
        if graph_update.get("decision_status"):
            graph_update["status"] = str(graph_update.get("decision_status") or "")
        graph_summary = {**graph_summary, **graph_update}
        graph_summary.setdefault("incident_id", case.case_id)
        graph_summary.setdefault("case_number", case.case_number)
        diagnosis["graph_summary"] = _summary_payload(graph_summary)
        if safe_fields.get("thread_id"):
            diagnosis["thread_id"] = _safe_string(safe_fields.get("thread_id"), limit=128)
        case.last_diagnosis = diagnosis
        case.updated_at = utc_now()
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="graph_case_updated",
                actor_type="graph",
                payload={
                    "status": _safe_string(status, limit=64),
                    "thread_id": _safe_string(safe_fields.get("thread_id"), limit=128),
                    "diagnostic_summary": _safe_string(safe_fields.get("diagnostic_summary"), limit=500),
                },
            )
        )
        return _case_dict(case)

    async def set_case_thread(self, incident_id: str, thread_id: str) -> dict[str, Any] | None:
        return await self.update_case(incident_id, {"thread_id": thread_id})

    async def _case(self, incident_id: str) -> AtomicCaseProjection | None:
        case_id = await self.resolve_case_identifier(incident_id) or str(incident_id or "")
        case = await self.store.get_case(case_id)
        return case if isinstance(case, AtomicCaseProjection) else None

    async def _require_case(self, incident_id: str) -> AtomicCaseProjection:
        case = await self._case(incident_id)
        if case is None:
            raise KeyError(f"CaseService graph case not found: {incident_id}")
        return case

    async def _history_for_case(self, case: AtomicCaseProjection) -> list[dict[str, Any]]:
        resource_id = case.resource_id or case.fingerprint
        if not resource_id:
            return []
        return await self.history_for(resource_id)


def _case_dict(case: AtomicCaseProjection) -> dict[str, Any]:
    diagnosis = case.last_diagnosis or {}
    stored_graph_summary = diagnosis.get("graph_summary")
    graph_summary: dict[str, Any] = stored_graph_summary if isinstance(stored_graph_summary, dict) else {}
    return {
        "incident_id": case.case_id,
        "case_id": case.case_id,
        "case_number": case.case_number,
        "status": case.status,
        "fingerprint": case.fingerprint,
        "identity": _bounded_json(case.identity),
        "resource_id": _safe_string(case.resource_id, limit=240),
        "title": _safe_string(case.title, limit=240),
        "summary": _safe_string(case.summary, limit=1000),
        "event_count": len(case.trace_ids) + len(case.feedback_ids),
        "thread_id": str(diagnosis.get("thread_id") or graph_summary.get("thread_id") or ""),
        "created_at": case.opened_at,
        "updated_at": case.updated_at,
        "last_event_at": case.last_seen or case.updated_at,
        "latest_transition": None,
        "downstream_victims": [],
        "parents_checked": [],
        "needs_reassessment": False,
        "source": "case_service",
    }


def _event_dict(event: CaseEvent) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    return {
        "received_at": event.occurred_at,
        "event_type": event.event_type,
        "source": event.source,
        "state": _safe_string(payload.get("status") or payload.get("state") or "", limit=64),
        "summary": _safe_string(
            payload.get("summary") or payload.get("diagnostic_summary") or payload.get("title") or event.event_type,
            limit=1000,
        ),
        "payload": _bounded_json(payload),
    }


def _timeline_event(event: CaseEvent, case: AtomicCaseProjection) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    return {
        "received_at": event.occurred_at,
        "state": _safe_string(payload.get("status") or payload.get("state") or case.status, limit=64),
        "summary": _safe_string(
            payload.get("summary") or payload.get("diagnostic_summary") or payload.get("title") or case.summary or case.title,
            limit=1000,
        ),
    }


def _history_row(case: AtomicCaseProjection, ts: float) -> dict[str, Any]:
    return {
        "ts": ts,
        "case_id": case.case_id,
        "case_number": _safe_string(case.case_number, limit=128),
        "resource_id": _safe_string(case.resource_id, limit=240),
        "alertname": _safe_string(case.detector or case.rule_id, limit=120),
        "severity": _safe_string(case.severity, limit=32),
        "status": case.status,
        "received_at": case.last_seen or case.updated_at or case.opened_at,
    }


def _history_event_row(case: AtomicCaseProjection, event: CaseEvent, ts: float) -> dict[str, Any]:
    row = _history_row(case, ts)
    row["event_type"] = _safe_string(event.event_type, limit=80)
    row["received_at"] = event.observed_at or event.occurred_at
    payload = event.payload if isinstance(event.payload, dict) else {}
    if payload.get("observation_id"):
        row["observation_id"] = _safe_string(payload.get("observation_id"), limit=128)
    return row


def _summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    compact_summary = {key: value for key, value in summary.items() if key != "case_context"}
    payload = cast(dict[str, Any], _bounded_json(compact_summary, string_limit=2000, max_depth=6))
    if "incident_id" in payload:
        payload["incident_id"] = _safe_string(payload.get("incident_id"), limit=128)
    if "thread_id" in payload:
        payload["thread_id"] = _safe_string(payload.get("thread_id"), limit=128)
    return payload


def _bounded_json(value: Any, *, string_limit: int = 1000, max_depth: int = 5) -> Any:
    if max_depth <= 0:
        return _safe_string(value, limit=string_limit)
    if isinstance(value, str):
        return _safe_string(value, limit=string_limit)
    if isinstance(value, bool | int | float) or value is None:
        return value
    if isinstance(value, list | tuple):
        return [_bounded_json(item, string_limit=string_limit, max_depth=max_depth - 1) for item in list(value)[:50]]
    if isinstance(value, dict):
        return {
            _safe_string(key, limit=80): _bounded_json(child, string_limit=string_limit, max_depth=max_depth - 1)
            for key, child in list(value.items())[:100]
        }
    try:
        return _bounded_json(json.loads(json.dumps(value, default=str)), string_limit=string_limit, max_depth=max_depth - 1)
    except Exception:
        return _safe_string(value, limit=string_limit)


def _safe_string(value: Any, *, limit: int) -> str:
    text = str(value or "").replace("\x00", "")
    cleaned = "".join(
        char
        for char in text
        if char in {"\n", "\r", "\t"} or not unicodedata.category(char).startswith("C")
    )
    return cleaned[:limit]


def _case_status(value: str, *, fallback: str) -> str:
    if value == "approved":
        return "resolved"
    if value in {"rejected", "finalized"}:
        return "resolved"
    if value in {
        "investigating",
        "waiting_approval",
        "recovered_pending",
        "resolved",
        "expired",
        "linked",
        "closed",
    }:
        return value
    return fallback


def _resource_key(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _parse_ts(value: str) -> float:
    try:
        text = str(value or "")
        if not text:
            return 0.0
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
    except Exception:
        return 0.0
