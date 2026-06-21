"""CaseStore boundary and in-memory reference backend.

The production plan moves this boundary to Postgres. The in-memory backend here
is deliberately small and deterministic so service logic can be developed and
tested without reusing the old Redis/local IncidentMemory as a second source of
truth.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    MetaCaseProjection,
    ObservationRecord,
    OperatorFeedback,
    OutboxIntent,
    TraceRecord,
)

CaseProjection = AtomicCaseProjection | MetaCaseProjection


@dataclass(frozen=True)
class CaseLinkResult:
    child_case: AtomicCaseProjection
    parent_case: AtomicCaseProjection
    moved_aliases: list[CaseIdentityAlias]
    events: list[CaseEvent]


@runtime_checkable
class CaseStore(Protocol):
    async def put_observation(self, observation: ObservationRecord) -> ObservationRecord: ...

    async def get_observation(self, observation_id: str) -> ObservationRecord | None: ...

    async def upsert_case(self, case: CaseProjection) -> CaseProjection: ...

    async def create_atomic_case(
        self,
        case: AtomicCaseProjection,
        *,
        aliases: list[CaseIdentityAlias],
        events: list[CaseEvent],
    ) -> tuple[AtomicCaseProjection, list[CaseEvent], bool]: ...

    async def claim_investigation(
        self,
        case_id: str,
        *,
        expected_signal_signature: str,
        expected_diagnosis_signature: str,
        expected_last_investigated_at: str,
        status: str,
        policy_version: str,
        now: str,
        event: CaseEvent,
    ) -> AtomicCaseProjection | None: ...

    async def get_case(self, case_id: str) -> CaseProjection | None: ...

    async def list_cases(self, *, kind: str | None = None, limit: int = 100) -> list[CaseProjection]: ...

    async def append_event(self, event: CaseEvent) -> CaseEvent: ...

    async def case_events(self, case_id: str) -> list[CaseEvent]: ...

    async def record_alias(self, alias: CaseIdentityAlias) -> CaseIdentityAlias: ...

    async def resolve_alias(self, alias_type: str, alias_value: str) -> str | None: ...

    async def link_child_case(
        self,
        child_case_id: str,
        parent_case_id: str,
        *,
        reason: str,
        evidence_refs: list[str],
        now: str,
    ) -> CaseLinkResult | None: ...

    async def enqueue_outbox(self, intent: OutboxIntent) -> OutboxIntent: ...

    async def update_outbox(self, intent: OutboxIntent) -> OutboxIntent: ...

    async def list_outbox(self, *, status: str | None = None) -> list[OutboxIntent]: ...

    async def record_feedback(self, feedback: OperatorFeedback) -> OperatorFeedback: ...

    async def list_feedback(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[OperatorFeedback]: ...

    async def record_trace(self, trace: TraceRecord) -> TraceRecord: ...

    async def list_traces(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[TraceRecord]: ...


class InMemoryCaseStore:
    """Reference implementation for tests and local service development.

    It enforces the important uniqueness invariants that will become DB unique
    indexes later: active aliases are unique by `(alias_type, alias_value)` and
    outbox intents are unique by `idempotency_key`.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._observations: dict[str, ObservationRecord] = {}
        self._cases: dict[str, CaseProjection] = {}
        self._events: dict[str, CaseEvent] = {}
        self._case_events: dict[str, list[str]] = {}
        self._aliases: dict[str, CaseIdentityAlias] = {}
        self._alias_index: dict[tuple[str, str], str] = {}
        self._outbox: dict[str, OutboxIntent] = {}
        self._outbox_index: dict[str, str] = {}
        self._feedback: dict[str, OperatorFeedback] = {}
        self._traces: dict[str, TraceRecord] = {}

    async def put_observation(self, observation: ObservationRecord) -> ObservationRecord:
        async with self._lock:
            existing = self._observations.get(observation.observation_id)
            if existing is not None:
                return existing.model_copy(deep=True)
            stored = observation.model_copy(deep=True)
            self._observations[stored.observation_id] = stored
            return stored.model_copy(deep=True)

    async def get_observation(self, observation_id: str) -> ObservationRecord | None:
        async with self._lock:
            observation = self._observations.get(str(observation_id or ""))
            return observation.model_copy(deep=True) if observation else None

    async def upsert_case(self, case: CaseProjection) -> CaseProjection:
        async with self._lock:
            stored = case.model_copy(deep=True)
            self._cases[stored.case_id] = stored
            return stored.model_copy(deep=True)

    async def create_atomic_case(
        self,
        case: AtomicCaseProjection,
        *,
        aliases: list[CaseIdentityAlias],
        events: list[CaseEvent],
    ) -> tuple[AtomicCaseProjection, list[CaseEvent], bool]:
        async with self._lock:
            for alias in aliases:
                if not alias.alias_value:
                    raise ValueError("alias_value is required")
                existing_id = self._alias_index.get(_alias_key(alias.alias_type, alias.alias_value))
                if existing_id:
                    existing_alias = self._aliases[existing_id]
                    existing_case = self._cases.get(existing_alias.case_id)
                    if isinstance(existing_case, AtomicCaseProjection):
                        return existing_case.model_copy(deep=True), [], False
                    raise ValueError(f"active alias {alias.alias_type}:{alias.alias_value} points to missing case")
            for existing_case in self._cases.values():
                if (
                    isinstance(existing_case, AtomicCaseProjection)
                    and existing_case.fingerprint
                    and existing_case.fingerprint == case.fingerprint
                    and existing_case.status not in {"resolved", "expired", "closed", "linked"}
                ):
                    return existing_case.model_copy(deep=True), [], False
            stored_case = case.model_copy(deep=True)
            self._cases[stored_case.case_id] = stored_case
            for alias in aliases:
                stored_alias = alias.model_copy(deep=True)
                self._aliases[stored_alias.alias_id] = stored_alias
                self._alias_index[_alias_key(stored_alias.alias_type, stored_alias.alias_value)] = stored_alias.alias_id
            stored_events = [self._store_event_locked(event) for event in events]
            return stored_case.model_copy(deep=True), [event.model_copy(deep=True) for event in stored_events], True

    async def claim_investigation(
        self,
        case_id: str,
        *,
        expected_signal_signature: str,
        expected_diagnosis_signature: str,
        expected_last_investigated_at: str,
        status: str,
        policy_version: str,
        now: str,
        event: CaseEvent,
    ) -> AtomicCaseProjection | None:
        async with self._lock:
            case = self._cases.get(str(case_id or ""))
            if not isinstance(case, AtomicCaseProjection):
                return None
            if str(case.status or "") in {"resolved", "closed", "expired", "linked"}:
                return None
            if (
                case.signal_signature != expected_signal_signature
                or case.diagnosis_signature != expected_diagnosis_signature
                or case.last_investigated_at != expected_last_investigated_at
            ):
                return None
            claimed = case.model_copy(deep=True)
            claimed.last_investigated_at = now
            claimed.diagnosis_signature = claimed.signal_signature or claimed.fingerprint or claimed.case_id
            claimed.investigation_status = status
            claimed.investigation_error = ""
            claimed.updated_at = now
            claimed.policy_version = policy_version
            self._cases[claimed.case_id] = claimed
            self._store_event_locked(event)
            return claimed.model_copy(deep=True)

    async def get_case(self, case_id: str) -> CaseProjection | None:
        async with self._lock:
            case = self._cases.get(str(case_id or ""))
            return case.model_copy(deep=True) if case else None

    async def list_cases(self, *, kind: str | None = None, limit: int = 100) -> list[CaseProjection]:
        async with self._lock:
            cases = list(self._cases.values())
            if kind:
                cases = [case for case in cases if case.kind == kind]
            cases.sort(key=lambda case: getattr(case, "updated_at", getattr(case, "opened_at", "")), reverse=True)
            return [case.model_copy(deep=True) for case in cases[: max(0, limit)]]

    async def append_event(self, event: CaseEvent) -> CaseEvent:
        async with self._lock:
            return self._store_event_locked(event).model_copy(deep=True)

    def _store_event_locked(self, event: CaseEvent) -> CaseEvent:
        existing = self._events.get(event.event_id)
        if existing is not None:
            return existing
        stored = event.model_copy(deep=True)
        self._events[stored.event_id] = stored
        for target in (stored.case_id, stored.meta_case_id):
            if target:
                self._case_events.setdefault(target, []).append(stored.event_id)
        return stored

    async def case_events(self, case_id: str) -> list[CaseEvent]:
        async with self._lock:
            ids = list(self._case_events.get(str(case_id or ""), []))
            return [self._events[event_id].model_copy(deep=True) for event_id in ids if event_id in self._events]

    async def record_alias(self, alias: CaseIdentityAlias) -> CaseIdentityAlias:
        if not alias.alias_value:
            raise ValueError("alias_value is required")
        key = _alias_key(alias.alias_type, alias.alias_value)
        async with self._lock:
            existing_id = self._alias_index.get(key)
            if existing_id:
                existing = self._aliases[existing_id]
                if existing.retired_at is None and existing.case_id != alias.case_id:
                    raise ValueError(
                        f"active alias {alias.alias_type}:{alias.alias_value} already points to {existing.case_id}"
                    )
                if existing.retired_at is None:
                    return existing.model_copy(deep=True)
            stored = alias.model_copy(deep=True)
            self._aliases[stored.alias_id] = stored
            if stored.retired_at is None:
                self._alias_index[key] = stored.alias_id
            return stored.model_copy(deep=True)

    async def resolve_alias(self, alias_type: str, alias_value: str) -> str | None:
        key = _alias_key(alias_type, alias_value)
        async with self._lock:
            alias_id = self._alias_index.get(key)
            if not alias_id:
                return None
            alias = self._aliases.get(alias_id)
            if alias is None or alias.retired_at is not None:
                return None
            return alias.case_id

    async def link_child_case(
        self,
        child_case_id: str,
        parent_case_id: str,
        *,
        reason: str,
        evidence_refs: list[str],
        now: str,
    ) -> CaseLinkResult | None:
        if child_case_id == parent_case_id:
            return None
        async with self._lock:
            child = self._cases.get(str(child_case_id or ""))
            parent = self._cases.get(str(parent_case_id or ""))
            if not isinstance(child, AtomicCaseProjection) or not isinstance(parent, AtomicCaseProjection):
                return None
            child_diagnosis = dict(child.last_diagnosis or {})
            if child.status == "linked":
                if child_diagnosis.get("linked_parent_case") == parent.case_id:
                    return CaseLinkResult(
                        child_case=child.model_copy(deep=True),
                        parent_case=parent.model_copy(deep=True),
                        moved_aliases=[],
                        events=[],
                    )
                return None
            linked_child = child.model_copy(deep=True)
            linked_parent = parent.model_copy(deep=True)
            child_diagnosis["linked_parent_case"] = parent.case_id
            child_diagnosis["link_reason"] = reason
            child_diagnosis["link_evidence_refs"] = list(evidence_refs)
            linked_child.status = "linked"
            linked_child.resolution_reason = "linked_parent"
            linked_child.resolved_at = now
            linked_child.last_diagnosis = child_diagnosis
            linked_child.updated_at = now
            linked_parent.updated_at = now
            self._cases[linked_child.case_id] = linked_child
            self._cases[linked_parent.case_id] = linked_parent
            moved_aliases = self._reassign_aliases_locked(linked_child.case_id, linked_parent.case_id, now)
            child_event = CaseEvent(
                case_id=linked_child.case_id,
                event_type="case_linked_to_parent",
                actor_type="graph",
                occurred_at=now,
                payload={
                    "parent_case_id": linked_parent.case_id,
                    "reason": reason,
                    "evidence_refs": list(evidence_refs),
                    "moved_alias_count": len(moved_aliases),
                },
            )
            parent_event = CaseEvent(
                case_id=linked_parent.case_id,
                event_type="linked_child_case",
                actor_type="graph",
                occurred_at=now,
                payload={
                    "child_case_id": linked_child.case_id,
                    "resource_key": linked_child.resource_id,
                    "summary": reason,
                    "evidence_refs": list(evidence_refs),
                    "moved_alias_count": len(moved_aliases),
                },
            )
            stored_events = [self._store_event_locked(child_event), self._store_event_locked(parent_event)]
            return CaseLinkResult(
                child_case=linked_child.model_copy(deep=True),
                parent_case=linked_parent.model_copy(deep=True),
                moved_aliases=[alias.model_copy(deep=True) for alias in moved_aliases],
                events=[event.model_copy(deep=True) for event in stored_events],
            )

    def _reassign_aliases_locked(self, from_case_id: str, to_case_id: str, now: str) -> list[CaseIdentityAlias]:
        moved: list[CaseIdentityAlias] = []
        active_aliases = [
            alias
            for alias in self._aliases.values()
            if alias.case_id == from_case_id and alias.retired_at is None
        ]
        for alias in active_aliases:
            key = _alias_key(alias.alias_type, alias.alias_value)
            indexed_id = self._alias_index.get(key)
            indexed_alias = self._aliases.get(indexed_id) if indexed_id else None
            target_already_has_alias = bool(
                indexed_alias and indexed_alias.case_id == to_case_id and indexed_alias.retired_at is None
            )
            if indexed_id == alias.alias_id:
                self._alias_index.pop(key, None)
            alias.retired_at = now
            if target_already_has_alias or key in self._alias_index:
                continue
            replacement = CaseIdentityAlias(
                case_id=to_case_id,
                alias_type=alias.alias_type,
                alias_value=alias.alias_value,
                source=alias.source or "case_link",
                confidence=alias.confidence,
                created_at=now,
            )
            self._aliases[replacement.alias_id] = replacement
            self._alias_index[key] = replacement.alias_id
            moved.append(replacement)
        return moved

    async def enqueue_outbox(self, intent: OutboxIntent) -> OutboxIntent:
        async with self._lock:
            existing_id = self._outbox_index.get(intent.idempotency_key)
            if existing_id:
                return self._outbox[existing_id].model_copy(deep=True)
            stored = intent.model_copy(deep=True)
            self._outbox[stored.outbox_id] = stored
            self._outbox_index[stored.idempotency_key] = stored.outbox_id
            return stored.model_copy(deep=True)

    async def update_outbox(self, intent: OutboxIntent) -> OutboxIntent:
        async with self._lock:
            if intent.outbox_id not in self._outbox:
                raise KeyError(f"outbox intent not found: {intent.outbox_id}")
            stored = intent.model_copy(deep=True)
            self._outbox[stored.outbox_id] = stored
            self._outbox_index[stored.idempotency_key] = stored.outbox_id
            return stored.model_copy(deep=True)

    async def list_outbox(self, *, status: str | None = None) -> list[OutboxIntent]:
        async with self._lock:
            rows = list(self._outbox.values())
            if status:
                rows = [row for row in rows if row.status == status]
            rows.sort(key=lambda row: row.created_at)
            return [row.model_copy(deep=True) for row in rows]

    async def record_feedback(self, feedback: OperatorFeedback) -> OperatorFeedback:
        async with self._lock:
            existing = self._feedback.get(feedback.feedback_id)
            if existing is not None:
                return existing.model_copy(deep=True)
            stored = feedback.model_copy(deep=True)
            self._feedback[stored.feedback_id] = stored
            return stored.model_copy(deep=True)

    async def list_feedback(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[OperatorFeedback]:
        async with self._lock:
            rows = list(self._feedback.values())
            if case_id is not None:
                rows = [row for row in rows if row.case_id == case_id]
            if meta_case_id is not None:
                rows = [row for row in rows if row.meta_case_id == meta_case_id]
            rows.sort(key=lambda row: row.created_at)
            return [row.model_copy(deep=True) for row in rows]

    async def record_trace(self, trace: TraceRecord) -> TraceRecord:
        async with self._lock:
            existing = self._traces.get(trace.trace_id)
            if existing is not None:
                return existing.model_copy(deep=True)
            stored = trace.model_copy(deep=True)
            self._traces[stored.trace_id] = stored
            return stored.model_copy(deep=True)

    async def list_traces(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[TraceRecord]:
        async with self._lock:
            rows = list(self._traces.values())
            if case_id is not None:
                rows = [row for row in rows if row.case_id == case_id]
            if meta_case_id is not None:
                rows = [row for row in rows if row.meta_case_id == meta_case_id]
            rows.sort(key=lambda row: row.created_at)
            return [row.model_copy(deep=True) for row in rows]


def _alias_key(alias_type: str, alias_value: str) -> tuple[str, str]:
    return (str(alias_type or "").strip(), str(alias_value or "").strip())
