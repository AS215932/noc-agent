"""Deterministic service boundary for case-grounded state.

This is the first dormant service slice: it proves observations can be applied
through a single owner that writes observations, aliases, projections, events,
and side-effect intents through a CaseStore. Reactive-primary, proactive,
and control-primary app paths now use this boundary.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, cast

from app.cases.lhp import (
    CallbackInboxRecord,
    CaseHandoff,
    HandoffTransportDelivery,
    HandoffUpdate,
    KnowledgeArtifact,
    OutcomeRecord,
    MAX_VERIFICATION_OBJECTIVES_PER_HANDOFF,
    VerificationObjective,
    sanitize_lhp_payload,
)
from app.cases.models import (
    ActorType,
    AliasType,
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    CaseStatus,
    ObservationRecord,
    OperatorFeedback,
    OutboxIntent,
    TraceRecord,
    stable_json,
    utc_now,
)
from app.cases.policy import CasePolicy
from app.cases.store import (
    CallbackClaimResult,
    CaseStore,
    HandoffCreateResult,
    HandoffUpdateResult,
    case_status_for_handoff,
)
from app.model_metrics import (
    record_lhp_case_resolved,
    record_lhp_handoff_request,
    record_lhp_handoff_update,
    record_lhp_handoff_verified,
    record_lhp_knowledge_event,
    record_lhp_verification_result,
)

ObserveAction = Literal[
    "created",
    "updated",
    "resolved_positive_clean",
    "clean_without_case",
    "ignored_unknown_status",
]


@dataclass(slots=True)
class ObserveResult:
    action: ObserveAction
    observation: ObservationRecord
    case: AtomicCaseProjection | None
    events: list[CaseEvent]


class CaseService:
    """Single owner for atomic case lifecycle decisions.

    Later phases will extend this into the production state machine and replace
    side-store writes in the proactive loop. For now, it establishes the API and
    the core invariants: clean is explicit, source-degraded clean is not
    positive-clean evidence, aliases map identity drift to one stable case id,
    and side effects are requested through idempotent outbox intents.
    """

    def __init__(self, store: CaseStore, *, policy: CasePolicy | None = None) -> None:
        self.store = store
        self.policy = policy or CasePolicy()

    async def case_for_alias(self, alias_type: AliasType, alias_value: str) -> AtomicCaseProjection | None:
        case_id = await self.store.resolve_alias(alias_type, alias_value)
        if not case_id:
            return None
        case = await self.store.get_case(case_id)
        return case if isinstance(case, AtomicCaseProjection) else None

    async def observe(self, observation: ObservationRecord) -> ObserveResult:
        observation = await self.store.put_observation(observation)
        alias_type, alias_value = observation_alias(observation)
        case_id = await self.store.resolve_alias(alias_type, alias_value)
        case = await self.store.get_case(case_id) if case_id else None
        if case is not None and case.kind != "atomic":
            raise ValueError(f"observation alias resolved to non-atomic case {case.case_id}")
        atomic_case = case if isinstance(case, AtomicCaseProjection) else None

        if observation.status == "firing":
            if atomic_case is None:
                return await self._create_from_unhealthy(observation, alias_type, alias_value)
            return await self._update_unhealthy(atomic_case, observation)
        if observation.status in {"clean", "resolved"}:
            if atomic_case is None:
                event = await self.store.append_event(
                    CaseEvent(
                        event_type="observation_clean_without_case",
                        actor_type="monitor",
                        source=observation.source,
                        observed_at=observation.observed_at,
                        policy_version=self.policy.policy_version,
                        payload={"observation_id": observation.observation_id, "alias_type": alias_type},
                    )
                )
                return ObserveResult("clean_without_case", observation, None, [event])
            return await self._record_clean(atomic_case, observation)

        event = await self.store.append_event(
            CaseEvent(
                case_id=atomic_case.case_id if atomic_case else None,
                event_type="observation_ignored_unknown_status",
                actor_type="monitor",
                source=observation.source,
                observed_at=observation.observed_at,
                policy_version=self.policy.policy_version,
                payload={"observation_id": observation.observation_id, "status": observation.status},
            )
        )
        return ObserveResult("ignored_unknown_status", observation, atomic_case, [event])

    def report_state_signature(self, case: AtomicCaseProjection) -> str:
        return hashlib.sha256(
            stable_json(
                {
                    "case_id": case.case_id,
                    "status": case.status,
                    "severity": case.severity,
                    "signal_signature": case.signal_signature,
                    "resolution_reason": case.resolution_reason,
                    "issue_url": case.issue_url,
                    "suppressed_until": case.suppressed_until,
                }
            ).encode("utf-8")
        ).hexdigest()[:16]

    def should_report(self, case: AtomicCaseProjection, *, now: datetime | None = None) -> bool:
        signature = self.report_state_signature(case)
        if case.last_reported_signature != signature:
            return True
        last = _parse_iso_time(case.last_reported_at)
        if last is None:
            return True
        now = now or datetime.now(timezone.utc)
        return (now - last) >= timedelta(seconds=self.policy.report_reassert_s)

    async def request_report(
        self,
        case: AtomicCaseProjection,
        *,
        state_signature: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> OutboxIntent:
        """Create an idempotent report intent instead of sending directly."""

        state_signature = state_signature or self.report_state_signature(case)
        return await self.store.enqueue_outbox(
            OutboxIntent(
                case_id=case.case_id,
                intent_type="report",
                idempotency_key=f"report:{case.case_id}:{state_signature}",
                state_signature=state_signature,
                payload=payload or {},
            )
        )

    async def mark_reported(self, case_id: str, *, state_signature: str, reasserted: bool = False) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = utc_now()
        case.last_reported_at = now
        case.last_reported_signature = state_signature
        if reasserted:
            case.last_reasserted_at = now
        case.updated_at = now
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="case_reasserted" if reasserted else "case_reported",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={"state_signature": state_signature},
            )
        )
        return case

    def should_investigate(self, case: AtomicCaseProjection, *, now: datetime | None = None) -> bool:
        """Case-grounded replacement for the investigations.json cooldown.

        Reinvestigate if there is no prior diagnosis, if the signal changed, or
        if the investigation is older than the policy cooldown.
        """

        if _investigation_blocked_status(case.status):
            return False
        if case.investigation_status == "failed":
            last_failed = _parse_iso_time(case.last_investigated_at)
            if last_failed is None:
                return True
            now = now or datetime.now(timezone.utc)
            return (now - last_failed) >= timedelta(seconds=self.policy.investigation_failure_retry_s)
        if not case.last_investigated_at or not case.diagnosis_signature:
            return True
        if case.signal_signature and case.diagnosis_signature != case.signal_signature:
            return True
        last = _parse_iso_time(case.last_investigated_at)
        if last is None:
            return True
        now = now or datetime.now(timezone.utc)
        return (now - last) >= timedelta(seconds=self.policy.investigation_cooldown_s)

    async def claim_investigation(self, case: AtomicCaseProjection, *, status: str = "in_progress") -> AtomicCaseProjection | None:
        """Atomically claim a queued/running investigation slot before graph execution."""

        if not self.should_investigate(case):
            return None
        event = CaseEvent(
            case_id=case.case_id,
            event_type="investigation_started",
            actor_type="system",
            policy_version=self.policy.policy_version,
            payload={"diagnosis_signature": _investigation_signature(case), "status": status},
        )
        return await self.store.claim_investigation(
            case.case_id,
            expected_signal_signature=case.signal_signature,
            expected_diagnosis_signature=case.diagnosis_signature,
            expected_last_investigated_at=case.last_investigated_at,
            status=status,
            policy_version=self.policy.policy_version,
            now=utc_now(),
            event=event,
        )

    async def record_investigation_result(
        self,
        case_id: str,
        *,
        diagnosis: dict[str, Any],
        recommendations: list[str] | None = None,
        status: str = "complete",
        error: str = "",
    ) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = utc_now()
        case.last_investigated_at = now
        case.last_diagnosis = diagnosis
        case.diagnosis_signature = _investigation_signature(case)
        case.recommendations = recommendations or []
        case.investigation_status = status
        case.investigation_error = error
        case.updated_at = now
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="investigation_recorded",
                actor_type="graph",
                policy_version=self.policy.policy_version,
                payload={
                    "diagnosis_signature": case.diagnosis_signature,
                    "recommendation_count": len(case.recommendations),
                    "status": status,
                    "error": error,
                },
            )
        )
        return case

    async def ack(self, case_id: str, *, operator: str, event_id: str = "") -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = utc_now()
        case.acknowledged_by = operator
        case.acknowledged_at = now
        case.updated_at = now
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        event_payload: dict[str, Any] = {
            "case_id": case.case_id,
            "event_type": "case_acknowledged",
            "actor_type": "operator",
            "actor_id": operator,
            "policy_version": self.policy.policy_version,
            "payload": {},
        }
        if event_id:
            event_payload["event_id"] = event_id
        await self.store.append_event(CaseEvent(**event_payload))
        return case

    async def suppress(
        self,
        case_id: str,
        *,
        reason: str,
        source: Literal["operator", "agent"],
        operator: str = "",
        ttl_seconds: int | None = None,
        event_id: str = "",
    ) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = datetime.now(timezone.utc)
        ttl = self.policy.suppression_default_ttl_s if ttl_seconds is None else ttl_seconds
        until = now + timedelta(seconds=max(0, ttl)) if ttl > 0 else None
        case.suppressed_until = until.isoformat() if until else ""
        case.snoozed_until = case.suppressed_until
        case.suppression_reason = reason
        case.suppression_source = source
        case.updated_at = now.isoformat()
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        event_payload: dict[str, Any] = {
            "case_id": case.case_id,
            "event_type": "case_suppressed",
            "actor_type": "operator" if source == "operator" else "system",
            "actor_id": operator,
            "policy_version": self.policy.policy_version,
            "payload": {"reason": reason, "source": source, "suppressed_until": case.suppressed_until},
        }
        if event_id:
            event_payload["event_id"] = event_id
        await self.store.append_event(CaseEvent(**event_payload))
        return case

    async def expire_suppression(self, case_id: str, *, now: datetime | None = None) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        expiry = _parse_iso_time(case.suppressed_until or case.snoozed_until)
        now = now or datetime.now(timezone.utc)
        if expiry is None or expiry > now:
            return case
        case.suppressed_until = ""
        case.snoozed_until = ""
        case.suppression_reason = ""
        case.suppression_source = ""
        case.updated_at = now.isoformat()
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="case_suppression_expired",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={},
            )
        )
        return case

    async def handoff_intent(
        self,
        case_id: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> OutboxIntent | None:
        case = await self._require_atomic_case(case_id)
        if case.issue_url or case.issue_id:
            await self.store.append_event(
                CaseEvent(
                    case_id=case.case_id,
                    event_type="handoff_skipped_existing_issue",
                    actor_type="system",
                    policy_version=self.policy.policy_version,
                    payload={"issue_url": case.issue_url, "issue_id": case.issue_id},
                )
            )
            return None
        return await self.store.enqueue_outbox(
            OutboxIntent(
                case_id=case.case_id,
                intent_type="handoff",
                idempotency_key=f"handoff:{case.case_id}:{self.policy.policy_version}",
                state_signature=case.signal_signature,
                payload=payload or {},
            )
        )

    async def request_lhp_handoff(
        self,
        handoff: CaseHandoff,
        *,
        objectives: list[VerificationObjective] | None = None,
        enqueue_delivery: bool = False,
    ) -> HandoffCreateResult:
        """Create one authoritative LHP handoff and optional delivery intent.

        This is dormant until LHP feature flags call it. The store owns the
        idempotency/active-handoff checks and writes case, handoff, objectives,
        event, and outbox intent atomically.
        """

        requested_objectives = objectives or []
        if len(requested_objectives) > MAX_VERIFICATION_OBJECTIVES_PER_HANDOFF:
            raise ValueError(
                f"handoff cannot define more than {MAX_VERIFICATION_OBJECTIVES_PER_HANDOFF} verification objectives"
            )
        case = await self._require_atomic_case(handoff.case_id)
        event = CaseEvent(
            case_id=case.case_id,
            event_type="lhp_handoff_requested",
            actor_type="system",
            correlation_id=handoff.correlation_id,
            policy_version=self.policy.policy_version,
            payload={
                "handoff_id": handoff.handoff_id,
                "target_loop": handoff.target_loop,
                "objective_key": handoff.objective_key,
                "idempotency_key": handoff.idempotency_key,
            },
        )
        outbox_intent = None
        if enqueue_delivery:
            outbox_intent = OutboxIntent(
                case_id=case.case_id,
                intent_type="engineering_handoff_requested",
                idempotency_key=f"engineering_handoff_requested:{handoff.handoff_id}",
                state_signature=case.signal_signature,
                payload={"handoff_id": handoff.handoff_id, "schema_version": handoff.schema_version},
            )
        result = await self.store.create_handoff_with_objectives(
            handoff,
            objectives=requested_objectives,
            case_status="handoff_requested",
            event=event,
            outbox_intent=outbox_intent,
        )
        record_lhp_handoff_request(
            target_loop=handoff.target_loop,
            case_type=handoff.case_type,
            outcome="created" if result.created else "deduplicated",
        )
        return result

    async def get_lhp_handoff(self, handoff_id: str) -> CaseHandoff | None:
        return await self.store.get_handoff(handoff_id)

    async def list_lhp_handoffs(self, *, case_id: str | None = None, status: str | None = None) -> list[CaseHandoff]:
        return await self.store.list_handoffs(case_id=case_id, status=status)

    async def record_lhp_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        return await self.store.record_handoff_delivery(delivery)

    async def update_lhp_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        return await self.store.update_handoff_delivery(delivery)

    async def claim_lhp_callback(self, callback: CallbackInboxRecord) -> CallbackClaimResult:
        return await self.store.claim_callback_event(callback)

    async def request_lhp_knowledge_context(
        self,
        case_id: str,
        *,
        handoff_id: str = "",
        objective_key: str = "",
        payload: dict[str, Any] | None = None,
    ) -> OutboxIntent:
        case = await self._require_atomic_case(case_id)
        safe_payload = sanitize_lhp_payload(payload or {})
        if not isinstance(safe_payload, dict):
            safe_payload = {"value": safe_payload}
        safe_payload["untrusted_evidence"] = True
        intent = await self.store.enqueue_outbox(
            OutboxIntent(
                case_id=case.case_id,
                intent_type="knowledge_context_requested",
                idempotency_key=f"knowledge_context_requested:{case.case_id}:{handoff_id or objective_key or 'case'}",
                state_signature=case.signal_signature,
                payload={
                    "case_id": case.case_id,
                    "handoff_id": handoff_id,
                    "objective_key": objective_key,
                    **safe_payload,
                },
            )
        )
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="lhp_knowledge_context_requested",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={"handoff_id": handoff_id, "objective_key": objective_key, "outbox_id": intent.outbox_id},
            )
        )
        record_lhp_knowledge_event(kind="context_requested", outcome="enqueued")
        return intent

    async def request_lhp_knowledge_artifact_proposal(
        self,
        case_id: str,
        *,
        handoff_id: str = "",
        outcome_id: str = "",
        payload: dict[str, Any] | None = None,
    ) -> OutboxIntent:
        case = await self._require_atomic_case(case_id)
        safe_payload = sanitize_lhp_payload(payload or {})
        if not isinstance(safe_payload, dict):
            safe_payload = {"value": safe_payload}
        safe_payload["untrusted_evidence"] = True
        intent = await self.store.enqueue_outbox(
            OutboxIntent(
                case_id=case.case_id,
                intent_type="knowledge_artifact_proposed",
                idempotency_key=f"knowledge_artifact_proposed:{case.case_id}:{handoff_id or outcome_id or 'case'}",
                state_signature=case.signal_signature,
                payload={
                    "case_id": case.case_id,
                    "handoff_id": handoff_id,
                    "outcome_id": outcome_id,
                    **safe_payload,
                },
            )
        )
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="lhp_knowledge_artifact_proposal_requested",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={"handoff_id": handoff_id, "outcome_id": outcome_id, "outbox_id": intent.outbox_id},
            )
        )
        record_lhp_knowledge_event(kind="artifact_proposed", outcome="enqueued")
        return intent

    async def record_lhp_handoff_update(
        self,
        update: HandoffUpdate,
        *,
        case_status: CaseStatus | None = None,
        case_handoff_status: str | None = None,
        event_actor_type: ActorType | None = None,
        event_actor_id: str = "",
        event_reason: str = "",
    ) -> HandoffUpdateResult:
        event_payload = {
            "handoff_id": update.handoff_id,
            "update_id": update.update_id,
            "update_type": update.update_type,
            "status": update.status,
            "external_event_id": update.external_event_id,
        }
        if event_reason:
            event_payload["reason"] = update.summary or event_reason
        event = CaseEvent(
            case_id=update.case_id,
            event_type="lhp_handoff_update_recorded",
            actor_type=event_actor_type or ("system" if update.source_loop == "noc" else "monitor"),
            actor_id=event_actor_id,
            source=update.source_loop,
            correlation_id=update.correlation_id,
            policy_version=self.policy.policy_version,
            payload=event_payload,
        )
        result = await self.store.append_handoff_update(
            update,
            case_status=case_status or case_status_for_handoff(update.status),
            case_handoff_status=case_handoff_status,
            event=event,
        )
        record_lhp_handoff_update(
            source_loop=update.source_loop,
            update_type=update.update_type,
            status=update.status,
            outcome="created" if result.created else "deduplicated",
        )
        return result

    async def cancel_lhp_handoff(
        self,
        handoff_id: str,
        *,
        actor_id: str,
        reason: str,
        external_event_id: str,
    ) -> HandoffUpdateResult:
        """Cancel one handoff through the same atomic transition ledger."""

        async with self.store.handoff_delivery_guard(handoff_id):
            handoff = await self.store.get_handoff(handoff_id)
            if handoff is None:
                raise KeyError(f"handoff not found: {handoff_id}")
            result = await self.record_lhp_handoff_update(
                HandoffUpdate(
                    handoff_id=handoff.handoff_id,
                    case_id=handoff.case_id,
                    source_loop="noc",
                    update_type="cancelled",
                    status="cancelled",
                    summary=reason,
                    external_event_id=external_event_id,
                    correlation_id=handoff.correlation_id,
                    payload={"actor_id": actor_id, "reason": reason, "source": "loop_console"},
                ),
                event_actor_type="operator",
                event_actor_id=actor_id,
                event_reason=reason,
            )
            await self._abandon_handoff_delivery_intents(handoff.handoff_id)
            await self._skip_cancelled_handoff_objectives(handoff)
            return result

    async def _abandon_handoff_delivery_intents(self, handoff_id: str) -> None:
        for intent in await self.store.list_outbox():
            if (
                intent.intent_type != "engineering_handoff_requested"
                or str(intent.payload.get("handoff_id") or "") != handoff_id
                or intent.status in {"succeeded", "abandoned"}
            ):
                continue
            abandoned = intent.model_copy(deep=True)
            abandoned.status = "abandoned"
            abandoned.completed_at = abandoned.completed_at or utc_now()
            abandoned.error = "handoff_cancelled"
            await self.store.update_outbox_if_status(abandoned, expected_status=intent.status)

    async def _skip_cancelled_handoff_objectives(self, handoff: CaseHandoff) -> None:
        objectives = await self.store.list_verification_objectives(case_id=handoff.case_id)
        for objective in objectives:
            if objective.handoff_id != handoff.handoff_id or objective.status != "pending":
                continue
            skipped = objective.model_copy(deep=True)
            skipped.status = "skipped"
            skipped.failure_reason = "handoff_cancelled"
            skipped.next_check_at = ""
            skipped.updated_at = utc_now()
            await self.record_lhp_verification_result(skipped)

    async def upsert_lhp_verification_objective(self, objective: VerificationObjective) -> VerificationObjective:
        event = CaseEvent(
            case_id=objective.case_id,
            event_type="lhp_verification_objective_upserted",
            actor_type="system",
            policy_version=self.policy.policy_version,
            payload={"objective_id": objective.objective_id, "objective_key": objective.objective_key},
        )
        return await self.store.upsert_verification_objective(objective, event=event)

    async def record_lhp_verification_result(
        self, objective: VerificationObjective, *, event_id: str = ""
    ) -> VerificationObjective:
        event_payload: dict[str, Any] = {
            "case_id": objective.case_id,
            "event_type": "lhp_verification_objective_updated",
            "actor_type": "system",
            "policy_version": self.policy.policy_version,
            "payload": {
                "objective_id": objective.objective_id,
                "objective_key": objective.objective_key,
                "status": objective.status,
                "consecutive_pass_count": objective.consecutive_pass_count,
            },
        }
        if event_id:
            event_payload["event_id"] = event_id
        event = CaseEvent(**event_payload)
        updated = await self.store.update_verification_objective_result(objective, event=event)
        record_lhp_verification_result(objective_type=updated.objective_type, status=updated.status)
        return updated

    async def list_due_lhp_verification_objectives(
        self, *, now: str | None = None, limit: int = 100
    ) -> list[VerificationObjective]:
        return await self.store.list_due_verification_objectives(now=now or utc_now(), limit=limit)

    async def list_lhp_verification_objectives(self, *, case_id: str | None = None) -> list[VerificationObjective]:
        return await self.store.list_verification_objectives(case_id=case_id)

    async def mark_lhp_handoff_verified(self, handoff_id: str) -> CaseHandoff:
        handoff = await self.store.get_handoff(handoff_id)
        if handoff is None:
            raise KeyError(f"handoff not found: {handoff_id}")
        now = utc_now()
        event = CaseEvent(
            case_id=handoff.case_id,
            event_type="lhp_handoff_verified",
            actor_type="system",
            occurred_at=now,
            policy_version=self.policy.policy_version,
            payload={"handoff_id": handoff_id},
        )
        verified = await self.store.mark_handoff_verified(handoff_id, now=now, event=event)
        record_lhp_handoff_verified(target_loop=verified.target_loop)
        return verified

    async def record_lhp_knowledge_artifact(self, artifact: KnowledgeArtifact) -> KnowledgeArtifact:
        event = CaseEvent(
            case_id=artifact.case_id,
            event_type="lhp_knowledge_artifact_recorded",
            actor_type="system",
            source="knowledge",
            policy_version=self.policy.policy_version,
            payload={
                "artifact_id": artifact.artifact_id,
                "artifact_type": artifact.artifact_type,
                "review_status": artifact.review_status,
            },
        )
        recorded = await self.store.record_knowledge_artifact(artifact, event=event)
        record_lhp_knowledge_event(kind=recorded.artifact_type, outcome=recorded.review_status)
        return recorded

    async def list_lhp_knowledge_artifacts(self, *, case_id: str | None = None) -> list[KnowledgeArtifact]:
        return await self.store.list_knowledge_artifacts(case_id=case_id)

    async def review_lhp_knowledge_artifact(
        self,
        artifact_id: str,
        *,
        review_status: str,
        actor_id: str,
        comment: str = "",
        event_id: str = "",
    ) -> KnowledgeArtifact:
        allowed = {"pending", "approved", "rejected", "superseded", "deprecated", "published"}
        if review_status not in allowed:
            raise ValueError("invalid knowledge artifact review_status")
        artifacts = await self.store.list_knowledge_artifacts()
        artifact = next((item for item in artifacts if item.artifact_id == artifact_id), None)
        if artifact is None:
            raise KeyError(f"knowledge artifact not found: {artifact_id}")
        payload = dict(artifact.payload or {})
        payload["review"] = sanitize_lhp_payload(
            {
                "actor_id": actor_id,
                "comment": comment,
                "review_status": review_status,
                "policy_version": self.policy.policy_version,
                "untrusted_operator_text": True,
                "model_consumption_allowed": False,
            }
        )
        artifact_data = artifact.model_dump(mode="json")
        artifact_data["review_status"] = review_status
        if review_status != "pending":
            artifact_data["status"] = review_status
        artifact_data["payload"] = payload
        reviewed = KnowledgeArtifact.model_validate(artifact_data)
        event_payload: dict[str, Any] = {
            "case_id": reviewed.case_id,
            "event_type": "lhp_knowledge_artifact_reviewed",
            "actor_type": "operator",
            "actor_id": actor_id,
            "source": "loop_console",
            "policy_version": self.policy.policy_version,
            "payload": {
                "artifact_id": reviewed.artifact_id,
                "review_status": reviewed.review_status,
                "comment": comment,
                "untrusted_operator_text": True,
                "model_consumption_allowed": False,
            },
        }
        if event_id:
            event_payload["event_id"] = event_id
        return await self.store.update_knowledge_artifact(reviewed, event=CaseEvent(**event_payload))

    async def record_lhp_outcome(self, outcome: OutcomeRecord) -> OutcomeRecord:
        event = CaseEvent(
            case_id=outcome.work_item_id,
            event_type="lhp_outcome_recorded",
            actor_type="system",
            policy_version=self.policy.policy_version,
            payload={"outcome_id": outcome.outcome_id, "case_type": outcome.case_type},
        )
        return await self.store.record_outcome(outcome, event=event)

    async def resolve_lhp_case_with_outcome(self, case_id: str, *, outcome: OutcomeRecord, handoff_id: str = "") -> AtomicCaseProjection:
        now = utc_now()
        event = CaseEvent(
            case_id=case_id,
            event_type="lhp_case_resolved_with_outcome",
            actor_type="system",
            occurred_at=now,
            policy_version=self.policy.policy_version,
            payload={"outcome_id": outcome.outcome_id, "handoff_id": handoff_id},
        )
        resolved = await self.store.resolve_case_with_outcome(case_id, handoff_id=handoff_id, outcome=outcome, now=now, event=event)
        record_lhp_case_resolved(case_type=outcome.case_type)
        return resolved

    async def list_lhp_outcomes(self, *, case_id: str | None = None) -> list[OutcomeRecord]:
        return await self.store.list_outcomes(case_id=case_id)

    async def record_trace(self, trace: TraceRecord) -> TraceRecord:
        stored = await self.store.record_trace(trace)
        if stored.case_id:
            case = await self._require_atomic_case(stored.case_id)
            if stored.trace_id not in case.trace_ids:
                case.trace_ids.append(stored.trace_id)
                case.updated_at = utc_now()
                case.policy_version = self.policy.policy_version
                await self.store.upsert_case(case)
        return stored

    async def record_operator_feedback(
        self,
        feedback: OperatorFeedback,
    ) -> OperatorFeedback:
        stored = await self.store.record_feedback(feedback)
        if stored.case_id:
            case = await self._require_atomic_case(stored.case_id)
            if stored.feedback_id in case.feedback_ids:
                return stored
            case.feedback_ids.append(stored.feedback_id)
            case.updated_at = utc_now()
            case.policy_version = self.policy.policy_version
            await self.store.upsert_case(case)
            await self.store.append_event(
                CaseEvent(
                    case_id=case.case_id,
                    event_type="operator_feedback_recorded",
                    actor_type="operator",
                    actor_id=stored.actor_id,
                    policy_version=self.policy.policy_version,
                    payload={"feedback_id": stored.feedback_id, "feedback_type": stored.feedback_type},
                )
            )
        return stored

    async def record_knowledge_citations(
        self,
        case_id: str,
        citations: list[dict[str, Any]],
        *,
        trace_id: str = "",
    ) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        existing = {stable_json(item): item for item in case.knowledge_citations}
        for citation in citations:
            existing.setdefault(stable_json(citation), citation)
        case.knowledge_citations = list(existing.values())
        if trace_id and trace_id not in case.trace_ids:
            case.trace_ids.append(trace_id)
        case.updated_at = utc_now()
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="hyrule_knowledge_retrieved",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={"citation_count": len(citations), "trace_id": trace_id},
            )
        )
        return case

    async def record_handoff_result(self, case_id: str, *, issue_url: str, issue_id: str = "") -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = utc_now()
        case.issue_url = issue_url
        case.issue_id = issue_id
        case.handoff_status = "created"
        case.last_handoff_at = now
        case.updated_at = now
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="handoff_created_issue",
                actor_type="system",
                policy_version=self.policy.policy_version,
                payload={"issue_url": issue_url, "issue_id": issue_id},
            )
        )
        return case

    async def _require_atomic_case(self, case_id: str) -> AtomicCaseProjection:
        case = await self.store.get_case(case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found: {case_id}")
        return case

    async def _create_from_unhealthy(
        self, observation: ObservationRecord, alias_type: AliasType, alias_value: str
    ) -> ObserveResult:
        now = utc_now()
        case = AtomicCaseProjection(
            fingerprint=alias_value,
            title=_case_title(observation),
            summary=_case_summary(observation),
            origin="proactive" if observation.source == "proactive" or observation.scan_cycle_id else "reactive",
            rule_id=observation.rule_id,
            detector=observation.detector or observation.rule_id,
            resource_id=_case_resource(observation),
            identity=_case_identity(observation),
            site=observation.site,
            customer=observation.customer,
            service=observation.service,
            severity=observation.severity,
            status="investigating",
            opened_at=now,
            updated_at=now,
            last_seen=observation.observed_at,
            last_observed_unhealthy=observation.observed_at,
            last_evaluated_at=observation.observed_at,
            last_scan_cycle_id=observation.scan_cycle_id,
            signal_snapshot=observation.signal_snapshot,
            signal_signature=observation.signal_signature,
            policy_version=self.policy.policy_version,
        )
        aliases = [CaseIdentityAlias(case_id=case.case_id, alias_type=alias_type, alias_value=alias_value)]
        if observation.source_fingerprint and (alias_type, alias_value) != ("source_fp", observation.source_fingerprint):
            aliases.append(CaseIdentityAlias(case_id=case.case_id, alias_type="source_fp", alias_value=observation.source_fingerprint))
        if observation.source_event_id and (alias_type, alias_value) != ("source_event_id", observation.source_event_id):
            aliases.append(
                CaseIdentityAlias(case_id=case.case_id, alias_type="source_event_id", alias_value=observation.source_event_id)
            )
        events = [
            CaseEvent(
                case_id=case.case_id,
                event_type="case_created",
                actor_type="system",
                source=observation.source,
                observed_at=observation.observed_at,
                policy_version=self.policy.policy_version,
                payload={"observation_id": observation.observation_id, "alias_type": alias_type},
            ),
            CaseEvent(
                case_id=case.case_id,
                event_type="case_observed_unhealthy",
                actor_type="monitor",
                source=observation.source,
                observed_at=observation.observed_at,
                policy_version=self.policy.policy_version,
                payload={
                    "observation_id": observation.observation_id,
                    "signal_signature": observation.signal_signature,
                },
            ),
        ]
        case, events, created = await self.store.create_atomic_case(case, aliases=aliases, events=events)
        if not created:
            return await self._update_unhealthy(case, observation)
        return ObserveResult("created", observation, case, events)

    async def _update_unhealthy(self, case: AtomicCaseProjection, observation: ObservationRecord) -> ObserveResult:
        now = utc_now()
        previous_signature = case.signal_signature
        case.previous_signal_signature = previous_signature if previous_signature != observation.signal_signature else case.previous_signal_signature
        case.signal_signature = observation.signal_signature
        case.signal_snapshot = observation.signal_snapshot
        case.severity = observation.severity
        case.status = "investigating" if case.status in {"recovered_pending", "resolved"} else case.status
        case.updated_at = now
        case.last_seen = observation.observed_at
        case.last_observed_unhealthy = observation.observed_at
        case.last_evaluated_at = observation.observed_at
        case.last_scan_cycle_id = observation.scan_cycle_id
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        event_type = "case_signal_changed" if previous_signature and previous_signature != observation.signal_signature else "case_observed_unhealthy"
        event = await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type=event_type,
                actor_type="monitor",
                source=observation.source,
                observed_at=observation.observed_at,
                policy_version=self.policy.policy_version,
                payload={
                    "observation_id": observation.observation_id,
                    "previous_signal_signature": previous_signature,
                    "signal_signature": observation.signal_signature,
                },
            )
        )
        return ObserveResult("updated", observation, case, [event])

    async def _record_clean(self, case: AtomicCaseProjection, observation: ObservationRecord) -> ObserveResult:
        now = utc_now()
        case.updated_at = now
        case.last_observed_clean = observation.observed_at
        case.last_evaluated_at = observation.observed_at
        case.last_scan_cycle_id = observation.scan_cycle_id
        case.policy_version = self.policy.policy_version
        action: ObserveAction = "updated"
        event_type = "case_observed_clean"
        if observation.is_positive_clean and self.policy.require_positive_clean_for_resolve:
            case.status = "resolved"
            case.resolved_at = now
            case.resolution_reason = "positive_clean_observation"
            action = "resolved_positive_clean"
            event_type = "case_resolved_positive_clean"
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        event = await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type=event_type,
                actor_type="monitor",
                source=observation.source,
                observed_at=observation.observed_at,
                policy_version=self.policy.policy_version,
                payload={
                    "observation_id": observation.observation_id,
                    "source_health": observation.source_health,
                    "positive_clean": observation.is_positive_clean,
                },
            )
        )
        return ObserveResult(action, observation, case, [event])


def observation_alias(observation: ObservationRecord) -> tuple[AliasType, str]:
    if observation.source_fingerprint:
        return "source_fp", observation.source_fingerprint
    if observation.dedup_key:
        return "source_event_id", observation.dedup_key
    return "rule_entity", observation_identity_fingerprint(observation)


def observation_identity_fingerprint(observation: ObservationRecord) -> str:
    identity = _case_identity(observation)
    return hashlib.sha256(stable_json(identity).encode("utf-8")).hexdigest()[:16]


def _investigation_signature(case: AtomicCaseProjection) -> str:
    return case.signal_signature or case.fingerprint or case.case_id


def _investigation_blocked_status(status: str) -> bool:
    return str(status or "") in {"resolved", "closed", "expired", "linked"}


def _parse_iso_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _case_identity(observation: ObservationRecord) -> dict[str, Any]:
    return {
        "source": observation.source,
        "detector": observation.detector or observation.rule_id,
        "rule_id": observation.rule_id,
        "entity": observation.entity,
        "resource": observation.resource,
        "interface": observation.interface,
        "service": observation.service,
        "site": observation.site,
        "customer": observation.customer,
    }


def _case_resource(observation: ObservationRecord) -> str:
    return observation.entity or observation.resource or observation.service or observation.site or observation.customer


def _case_title(observation: ObservationRecord) -> str:
    detector = observation.detector or observation.rule_id or observation.source or "observation"
    resource = _case_resource(observation) or "unknown resource"
    return f"{detector} on {resource}"


def _case_summary(observation: ObservationRecord) -> str:
    for key in ("summary", "description", "message"):
        value = observation.annotations.get(key)
        if value:
            return str(value)
    return f"{observation.status} {observation.severity} observation from {observation.source or 'unknown source'}"
