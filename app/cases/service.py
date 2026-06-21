"""Deterministic service boundary for case-grounded state.

This is the first dormant service slice: it proves observations can be applied
through a single owner that writes observations, aliases, projections, events,
and side-effect intents through a CaseStore. The proactive loop and
IncidentMemory are not wired to it yet.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, cast

from app.cases.models import (
    AliasType,
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    ObservationRecord,
    OperatorFeedback,
    OutboxIntent,
    TraceRecord,
    stable_json,
    utc_now,
)
from app.cases.policy import CasePolicy
from app.cases.store import CaseStore

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

    async def ack(self, case_id: str, *, operator: str) -> AtomicCaseProjection:
        case = await self._require_atomic_case(case_id)
        now = utc_now()
        case.acknowledged_by = operator
        case.acknowledged_at = now
        case.updated_at = now
        case.policy_version = self.policy.policy_version
        case = cast(AtomicCaseProjection, await self.store.upsert_case(case))
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="case_acknowledged",
                actor_type="operator",
                actor_id=operator,
                policy_version=self.policy.policy_version,
                payload={},
            )
        )
        return case

    async def suppress(
        self,
        case_id: str,
        *,
        reason: str,
        source: Literal["operator", "agent"],
        operator: str = "",
        ttl_seconds: int | None = None,
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
        await self.store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type="case_suppressed",
                actor_type="operator" if source == "operator" else "system",
                actor_id=operator,
                policy_version=self.policy.policy_version,
                payload={"reason": reason, "source": source, "suppressed_until": case.suppressed_until},
            )
        )
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
            if stored.feedback_id not in case.feedback_ids:
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
