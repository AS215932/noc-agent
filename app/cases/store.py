"""CaseStore boundary and in-memory reference backend.

The production backend for this boundary is Postgres. The in-memory backend
here is deliberately small and deterministic so service logic can be developed
and tested without a second source of truth.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, cast, runtime_checkable

from app.cases.lhp import (
    CallbackInboxRecord,
    CaseHandoff,
    HandoffStatus,
    HandoffTransportDelivery,
    HandoffUpdate,
    KnowledgeArtifact,
    OutcomeRecord,
    TERMINAL_HANDOFF_STATUSES,
    VERIFIER_ONLY_HANDOFF_STATUSES,
    VerificationObjective,
    require_handoff_transition,
)
from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    CaseStatus,
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


@dataclass(frozen=True)
class HandoffCreateResult:
    handoff: CaseHandoff
    objectives: list[VerificationObjective]
    case: AtomicCaseProjection
    event: CaseEvent | None
    outbox_intent: OutboxIntent | None
    created: bool


@dataclass(frozen=True)
class HandoffUpdateResult:
    update: HandoffUpdate
    handoff: CaseHandoff
    case: AtomicCaseProjection
    event: CaseEvent | None
    created: bool


@dataclass(frozen=True)
class CallbackClaimResult:
    callback: CallbackInboxRecord
    created: bool


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

    async def list_cases(self, *, kind: str | None = None, status: str | None = None, limit: int = 100) -> list[CaseProjection]: ...

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

    async def create_handoff_with_objectives(
        self,
        handoff: CaseHandoff,
        *,
        objectives: list[VerificationObjective],
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
        outbox_intent: OutboxIntent | None = None,
    ) -> HandoffCreateResult: ...

    async def get_handoff(self, handoff_id: str) -> CaseHandoff | None: ...

    async def list_handoffs(self, *, case_id: str | None = None, status: str | None = None) -> list[CaseHandoff]: ...

    async def record_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery: ...

    async def update_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery: ...

    async def append_handoff_update(
        self,
        update: HandoffUpdate,
        *,
        handoff_status: str | None = None,
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
    ) -> HandoffUpdateResult: ...

    async def claim_callback_event(self, callback: CallbackInboxRecord) -> CallbackClaimResult: ...

    async def upsert_verification_objective(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective: ...

    async def update_verification_objective_result(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective: ...

    async def list_due_verification_objectives(self, *, now: str, limit: int = 100) -> list[VerificationObjective]: ...

    async def list_verification_objectives(self, *, case_id: str | None = None) -> list[VerificationObjective]: ...

    async def mark_handoff_verified(self, handoff_id: str, *, now: str, event: CaseEvent | None = None) -> CaseHandoff: ...

    async def resolve_case_with_outcome(
        self,
        case_id: str,
        *,
        handoff_id: str = "",
        outcome: OutcomeRecord,
        now: str,
        event: CaseEvent | None = None,
    ) -> AtomicCaseProjection: ...

    async def record_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact: ...

    async def update_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact: ...

    async def list_knowledge_artifacts(self, *, case_id: str | None = None) -> list[KnowledgeArtifact]: ...

    async def record_outcome(self, outcome: OutcomeRecord, *, event: CaseEvent | None = None) -> OutcomeRecord: ...

    async def list_outcomes(self, *, case_id: str | None = None) -> list[OutcomeRecord]: ...

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
        self._handoffs: dict[str, CaseHandoff] = {}
        self._handoff_idempotency_index: dict[str, str] = {}
        self._active_handoff_index: dict[tuple[str, str, str], str] = {}
        self._handoff_updates: dict[str, HandoffUpdate] = {}
        self._handoff_update_external_index: dict[tuple[str, str], str] = {}
        self._verification_objectives: dict[str, VerificationObjective] = {}
        self._verification_objective_index: dict[tuple[str, str], str] = {}
        self._knowledge_artifacts: dict[str, KnowledgeArtifact] = {}
        self._knowledge_artifact_index: dict[tuple[str, str, int], str] = {}
        self._outcomes: dict[str, OutcomeRecord] = {}
        self._callback_inbox: dict[str, CallbackInboxRecord] = {}
        self._callback_index: dict[tuple[str, str], str] = {}
        self._handoff_deliveries: dict[str, HandoffTransportDelivery] = {}
        self._handoff_delivery_index: dict[str, str] = {}
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

    async def list_cases(self, *, kind: str | None = None, status: str | None = None, limit: int = 100) -> list[CaseProjection]:
        async with self._lock:
            cases = list(self._cases.values())
            if kind:
                cases = [case for case in cases if case.kind == kind]
            if status:
                cases = [case for case in cases if str(getattr(case, "status", "")) == status]
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
            return self._enqueue_outbox_locked(intent).model_copy(deep=True)

    def _enqueue_outbox_locked(self, intent: OutboxIntent) -> OutboxIntent:
        existing_id = self._outbox_index.get(intent.idempotency_key)
        if existing_id:
            return self._outbox[existing_id]
        stored = intent.model_copy(deep=True)
        self._outbox[stored.outbox_id] = stored
        self._outbox_index[stored.idempotency_key] = stored.outbox_id
        return stored

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

    async def create_handoff_with_objectives(
        self,
        handoff: CaseHandoff,
        *,
        objectives: list[VerificationObjective],
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
        outbox_intent: OutboxIntent | None = None,
    ) -> HandoffCreateResult:
        async with self._lock:
            case = self._require_atomic_case_locked(handoff.case_id)
            existing_handoff = self._existing_handoff_locked(handoff)
            if existing_handoff is not None:
                return HandoffCreateResult(
                    handoff=existing_handoff.model_copy(deep=True),
                    objectives=self._objectives_for_handoff_locked(existing_handoff.handoff_id),
                    case=case.model_copy(deep=True),
                    event=None,
                    outbox_intent=None,
                    created=False,
                )
            stored_handoff = handoff.model_copy(deep=True)
            self._handoffs[stored_handoff.handoff_id] = stored_handoff
            self._handoff_idempotency_index[stored_handoff.idempotency_key] = stored_handoff.handoff_id
            if _handoff_active(stored_handoff.status):
                self._active_handoff_index[_handoff_key(stored_handoff)] = stored_handoff.handoff_id
            stored_objectives = []
            for objective in objectives:
                if objective.case_id != stored_handoff.case_id:
                    raise ValueError("verification objective case_id must match handoff case_id")
                if objective.handoff_id and objective.handoff_id != stored_handoff.handoff_id:
                    raise ValueError("verification objective handoff_id must match handoff_id")
                if not objective.handoff_id:
                    objective = objective.model_copy(update={"handoff_id": stored_handoff.handoff_id})
                stored_objectives.append(self._upsert_objective_locked(objective))
            updated_case = case.model_copy(deep=True)
            if case_status is not None:
                updated_case.status = case_status
            updated_case.handoff_status = stored_handoff.status
            updated_case.last_handoff_at = stored_handoff.updated_at or stored_handoff.created_at
            updated_case.updated_at = stored_handoff.updated_at or stored_handoff.created_at
            self._cases[updated_case.case_id] = updated_case
            stored_event = self._store_event_locked(event) if event is not None else None
            stored_outbox = self._enqueue_outbox_locked(outbox_intent) if outbox_intent is not None else None
            return HandoffCreateResult(
                handoff=stored_handoff.model_copy(deep=True),
                objectives=[objective.model_copy(deep=True) for objective in stored_objectives],
                case=updated_case.model_copy(deep=True),
                event=stored_event.model_copy(deep=True) if stored_event else None,
                outbox_intent=stored_outbox.model_copy(deep=True) if stored_outbox else None,
                created=True,
            )

    async def get_handoff(self, handoff_id: str) -> CaseHandoff | None:
        async with self._lock:
            handoff = self._handoffs.get(str(handoff_id or ""))
            return handoff.model_copy(deep=True) if handoff else None

    async def list_handoffs(self, *, case_id: str | None = None, status: str | None = None) -> list[CaseHandoff]:
        async with self._lock:
            rows = list(self._handoffs.values())
            if case_id is not None:
                rows = [row for row in rows if row.case_id == case_id]
            if status is not None:
                rows = [row for row in rows if row.status == status]
            rows.sort(key=lambda row: row.updated_at, reverse=True)
            return [row.model_copy(deep=True) for row in rows]

    async def record_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        async with self._lock:
            self._require_handoff_locked(delivery.handoff_id)
            existing_id = self._handoff_delivery_index.get(delivery.idempotency_key)
            if existing_id:
                return self._handoff_deliveries[existing_id].model_copy(deep=True)
            stored = delivery.model_copy(deep=True)
            self._handoff_deliveries[stored.delivery_id] = stored
            self._handoff_delivery_index[stored.idempotency_key] = stored.delivery_id
            return stored.model_copy(deep=True)

    async def update_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        async with self._lock:
            if delivery.delivery_id not in self._handoff_deliveries:
                raise KeyError(f"handoff delivery not found: {delivery.delivery_id}")
            stored = delivery.model_copy(deep=True)
            self._handoff_deliveries[stored.delivery_id] = stored
            self._handoff_delivery_index[stored.idempotency_key] = stored.delivery_id
            return stored.model_copy(deep=True)

    async def append_handoff_update(
        self,
        update: HandoffUpdate,
        *,
        handoff_status: str | None = None,
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
    ) -> HandoffUpdateResult:
        async with self._lock:
            external_key = (update.source_loop, update.external_event_id)
            existing_id = self._handoff_update_external_index.get(external_key)
            if existing_id:
                existing_update = self._handoff_updates[existing_id]
                handoff = self._require_handoff_locked(existing_update.handoff_id)
                case = self._require_atomic_case_locked(handoff.case_id)
                return HandoffUpdateResult(
                    update=existing_update.model_copy(deep=True),
                    handoff=handoff.model_copy(deep=True),
                    case=case.model_copy(deep=True),
                    event=None,
                    created=False,
                )
            handoff = self._require_handoff_locked(update.handoff_id)
            case = self._require_atomic_case_locked(handoff.case_id)
            target_status = cast(HandoffStatus, handoff_status or update.status)
            if target_status in VERIFIER_ONLY_HANDOFF_STATUSES:
                raise ValueError("verified/resolved require the dedicated NOC verifier path")
            require_handoff_transition(handoff.status, target_status, actor_loop=update.source_loop)
            stored_update = update.model_copy(deep=True)
            self._handoff_updates[stored_update.update_id] = stored_update
            self._handoff_update_external_index[external_key] = stored_update.update_id
            old_key = _handoff_key(handoff)
            updated_handoff = handoff.model_copy(deep=True)
            updated_handoff.status = target_status
            updated_handoff.updated_at = stored_update.created_at
            self._handoffs[updated_handoff.handoff_id] = updated_handoff
            self._active_handoff_index.pop(old_key, None)
            if _handoff_active(updated_handoff.status):
                self._active_handoff_index[_handoff_key(updated_handoff)] = updated_handoff.handoff_id
            updated_case = case.model_copy(deep=True)
            if case_status is not None:
                updated_case.status = case_status
            updated_case.handoff_status = updated_handoff.status
            updated_case.last_handoff_at = stored_update.created_at
            updated_case.updated_at = stored_update.created_at
            self._cases[updated_case.case_id] = updated_case
            stored_event = self._store_event_locked(event) if event is not None else None
            return HandoffUpdateResult(
                update=stored_update.model_copy(deep=True),
                handoff=updated_handoff.model_copy(deep=True),
                case=updated_case.model_copy(deep=True),
                event=stored_event.model_copy(deep=True) if stored_event else None,
                created=True,
            )

    async def claim_callback_event(self, callback: CallbackInboxRecord) -> CallbackClaimResult:
        async with self._lock:
            key = (callback.source_loop, callback.external_event_id)
            existing_id = self._callback_index.get(key)
            if existing_id:
                return CallbackClaimResult(callback=self._callback_inbox[existing_id].model_copy(deep=True), created=False)
            stored = callback.model_copy(deep=True)
            self._callback_inbox[stored.callback_id] = stored
            self._callback_index[key] = stored.callback_id
            return CallbackClaimResult(callback=stored.model_copy(deep=True), created=True)

    async def upsert_verification_objective(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective:
        async with self._lock:
            self._require_atomic_case_locked(objective.case_id)
            stored = self._upsert_objective_locked(objective)
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def update_verification_objective_result(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective:
        async with self._lock:
            if objective.objective_id not in self._verification_objectives:
                raise KeyError(f"verification objective not found: {objective.objective_id}")
            stored = objective.model_copy(deep=True)
            self._verification_objectives[stored.objective_id] = stored
            self._verification_objective_index[(stored.case_id, stored.objective_key)] = stored.objective_id
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def list_due_verification_objectives(self, *, now: str, limit: int = 100) -> list[VerificationObjective]:
        async with self._lock:
            rows = [
                row
                for row in self._verification_objectives.values()
                if row.status not in {"pass", "skipped"} and (not row.next_check_at or row.next_check_at <= now)
            ]
            rows.sort(key=lambda row: (row.next_check_at or "", row.created_at, row.objective_id))
            return [row.model_copy(deep=True) for row in rows[: _bounded_limit(limit)]]

    async def list_verification_objectives(self, *, case_id: str | None = None) -> list[VerificationObjective]:
        async with self._lock:
            rows = list(self._verification_objectives.values())
            if case_id is not None:
                rows = [row for row in rows if row.case_id == case_id]
            rows.sort(key=lambda row: (row.created_at, row.objective_id))
            return [row.model_copy(deep=True) for row in rows]

    async def mark_handoff_verified(self, handoff_id: str, *, now: str, event: CaseEvent | None = None) -> CaseHandoff:
        async with self._lock:
            handoff = self._require_handoff_locked(handoff_id)
            require_handoff_transition(handoff.status, "verified", actor_loop="noc")
            old_key = _handoff_key(handoff)
            stored = handoff.model_copy(deep=True)
            stored.status = "verified"
            stored.updated_at = now
            self._handoffs[stored.handoff_id] = stored
            self._active_handoff_index.pop(old_key, None)
            if _handoff_active(stored.status):
                self._active_handoff_index[_handoff_key(stored)] = stored.handoff_id
            case = self._require_atomic_case_locked(stored.case_id)
            updated_case = case.model_copy(deep=True)
            updated_case.status = "verification_pending"
            updated_case.handoff_status = stored.status
            updated_case.last_handoff_at = now
            updated_case.updated_at = now
            self._cases[updated_case.case_id] = updated_case
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def resolve_case_with_outcome(
        self,
        case_id: str,
        *,
        handoff_id: str = "",
        outcome: OutcomeRecord,
        now: str,
        event: CaseEvent | None = None,
    ) -> AtomicCaseProjection:
        async with self._lock:
            case = self._require_atomic_case_locked(case_id)
            if outcome.work_item_id != case.case_id:
                raise ValueError("outcome work_item_id must match case_id")
            if handoff_id:
                handoff = self._require_handoff_locked(handoff_id)
                require_handoff_transition(handoff.status, "resolved", actor_loop="noc")
                old_key = _handoff_key(handoff)
                stored_handoff = handoff.model_copy(deep=True)
                stored_handoff.status = "resolved"
                stored_handoff.updated_at = now
                self._handoffs[stored_handoff.handoff_id] = stored_handoff
                self._active_handoff_index.pop(old_key, None)
            self._outcomes[outcome.outcome_id] = outcome.model_copy(deep=True)
            updated_case = case.model_copy(deep=True)
            updated_case.status = "resolved"
            updated_case.resolved_at = now
            updated_case.resolution_reason = "lhp_outcome_verified"
            updated_case.updated_at = now
            self._cases[updated_case.case_id] = updated_case
            if event is not None:
                self._store_event_locked(event)
            return updated_case.model_copy(deep=True)

    async def record_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact:
        async with self._lock:
            self._require_atomic_case_locked(artifact.case_id)
            key = (artifact.case_id, artifact.artifact_type, artifact.version)
            existing_id = self._knowledge_artifact_index.get(key)
            if existing_id:
                return self._knowledge_artifacts[existing_id].model_copy(deep=True)
            stored = artifact.model_copy(deep=True)
            self._knowledge_artifacts[stored.artifact_id] = stored
            self._knowledge_artifact_index[key] = stored.artifact_id
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def update_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact:
        async with self._lock:
            if artifact.artifact_id not in self._knowledge_artifacts:
                raise KeyError(f"knowledge artifact not found: {artifact.artifact_id}")
            self._require_atomic_case_locked(artifact.case_id)
            stored = artifact.model_copy(deep=True)
            self._knowledge_artifacts[stored.artifact_id] = stored
            self._knowledge_artifact_index[(stored.case_id, stored.artifact_type, stored.version)] = stored.artifact_id
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def list_knowledge_artifacts(self, *, case_id: str | None = None) -> list[KnowledgeArtifact]:
        async with self._lock:
            rows = list(self._knowledge_artifacts.values())
            if case_id is not None:
                rows = [row for row in rows if row.case_id == case_id]
            rows.sort(key=lambda row: (row.created_at, row.artifact_id))
            return [row.model_copy(deep=True) for row in rows]

    async def record_outcome(self, outcome: OutcomeRecord, *, event: CaseEvent | None = None) -> OutcomeRecord:
        async with self._lock:
            self._require_atomic_case_locked(outcome.work_item_id)
            existing = self._outcomes.get(outcome.outcome_id)
            if existing is not None:
                return existing.model_copy(deep=True)
            stored = outcome.model_copy(deep=True)
            self._outcomes[stored.outcome_id] = stored
            if event is not None:
                self._store_event_locked(event)
            return stored.model_copy(deep=True)

    async def list_outcomes(self, *, case_id: str | None = None) -> list[OutcomeRecord]:
        async with self._lock:
            rows = list(self._outcomes.values())
            if case_id is not None:
                rows = [row for row in rows if row.work_item_id == case_id]
            rows.sort(key=lambda row: (row.created_at, row.outcome_id))
            return [row.model_copy(deep=True) for row in rows]

    def _require_atomic_case_locked(self, case_id: str) -> AtomicCaseProjection:
        case = self._cases.get(str(case_id or ""))
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found: {case_id}")
        return case

    def _require_handoff_locked(self, handoff_id: str) -> CaseHandoff:
        handoff = self._handoffs.get(str(handoff_id or ""))
        if handoff is None:
            raise KeyError(f"handoff not found: {handoff_id}")
        return handoff

    def _existing_handoff_locked(self, handoff: CaseHandoff) -> CaseHandoff | None:
        existing_id = self._handoff_idempotency_index.get(handoff.idempotency_key)
        if existing_id:
            return self._handoffs[existing_id]
        active_id = self._active_handoff_index.get(_handoff_key(handoff))
        if active_id:
            return self._handoffs[active_id]
        return None

    def _upsert_objective_locked(self, objective: VerificationObjective) -> VerificationObjective:
        key = (objective.case_id, objective.objective_key)
        existing_id = self._verification_objective_index.get(key)
        stored = objective.model_copy(deep=True)
        if existing_id:
            stored.objective_id = existing_id
        self._verification_objectives[stored.objective_id] = stored
        self._verification_objective_index[key] = stored.objective_id
        return stored

    def _objectives_for_handoff_locked(self, handoff_id: str) -> list[VerificationObjective]:
        rows = [row for row in self._verification_objectives.values() if row.handoff_id == handoff_id]
        rows.sort(key=lambda row: (row.created_at, row.objective_id))
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


def _handoff_key(handoff: CaseHandoff) -> tuple[str, str, str]:
    return (handoff.case_id, handoff.target_loop, handoff.objective_key)


def _handoff_active(status: str) -> bool:
    return status not in TERMINAL_HANDOFF_STATUSES


def _bounded_limit(limit: int, *, default: int = 100, maximum: int = 500) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)
