"""Deterministic owner for meta-case grouping and child relationships."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, cast

from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseStatus,
    MetaCaseProjection,
    ObservationRecord,
    stable_json,
    utc_now,
)
from app.cases.policy import CasePolicy
from app.cases.service import observation_alias
from app.cases.store import CaseStore


@dataclass(slots=True)
class MetaCaseResult:
    action: str
    meta_case: MetaCaseProjection
    events: list[CaseEvent]


class CorrelationService:
    """Single owner for meta-case create/attach/detach decisions.

    The current implementation is deliberately deterministic and small. Future
    storm heuristics and agent proposals should call this service; they should
    not mutate child case relationships directly.
    """

    def __init__(self, store: CaseStore, *, policy: CasePolicy | None = None) -> None:
        self.store = store
        self.policy = policy or CasePolicy()

    async def correlate_observations(self, observations: Iterable[ObservationRecord]) -> MetaCaseResult | None:
        """Detect a simple same-scope notification storm and create a meta-case.

        This deterministic first pass intentionally prefers precision over
        cleverness: at least two firing observations must share a site, customer,
        service, or resource scope before a meta-case is proposed.
        """

        firing = [obs for obs in observations if obs.status == "firing"]
        if len(firing) < 2:
            return None
        scope_name, scope_value = _shared_scope(firing)
        if not scope_value:
            return None
        child_case_ids: list[str] = []
        for observation in firing:
            alias_type, alias_value = observation_alias(observation)
            case_id = await self.store.resolve_alias(alias_type, alias_value)
            if case_id and case_id not in child_case_ids:
                child_case_ids.append(case_id)
        if len(child_case_ids) < 2:
            return None
        event_type = _event_type_for_observations(firing)
        detector_names = sorted({obs.detector or obs.rule_id for obs in firing if obs.detector or obs.rule_id})
        return await self.create_meta_case(
            title=f"Correlated {scope_name} event: {scope_value}",
            summary=f"{len(firing)} firing observations share {scope_name}={scope_value}: {', '.join(detector_names[:5])}",
            event_type=event_type,
            correlation_reason=f"shared_{scope_name}",
            correlation_confidence=max(0.8, self.policy.storm_confidence_threshold),
            child_case_ids=child_case_ids,
            observations=firing,
        )

    async def create_meta_case(
        self,
        *,
        title: str,
        summary: str = "",
        event_fingerprint: str = "",
        event_type: str = "unknown",
        correlation_reason: str,
        correlation_confidence: float,
        child_case_ids: Iterable[str] = (),
        observations: Iterable[ObservationRecord] = (),
        actor_id: str = "",
    ) -> MetaCaseResult:
        observation_list = list(observations)
        fingerprint = event_fingerprint or event_fingerprint_from_parts(title, [obs.observation_id for obs in observation_list])
        status: CaseStatus = "active_event" if correlation_confidence >= self.policy.storm_confidence_threshold else "candidate_event"
        now = utc_now()
        meta = MetaCaseProjection(
            event_fingerprint=fingerprint,
            title=title,
            summary=summary,
            event_type=cast(Any, event_type),
            status=status,
            correlation_reason=correlation_reason,
            correlation_confidence=correlation_confidence,
            observation_ids=[obs.observation_id for obs in observation_list],
            notification_ids=[obs.source_event_id for obs in observation_list if obs.source_event_id],
            first_notification_at=min((obs.observed_at for obs in observation_list), default=""),
            last_notification_at=max((obs.observed_at for obs in observation_list), default=""),
            opened_at=now,
            updated_at=now,
            storm_started_at=min((obs.observed_at for obs in observation_list), default=""),
            storm_last_seen_at=max((obs.observed_at for obs in observation_list), default=""),
            policy_version=self.policy.policy_version,
        )
        meta = cast(MetaCaseProjection, await self.store.upsert_case(meta))
        events = [
            await self.store.append_event(
                CaseEvent(
                    meta_case_id=meta.case_id,
                    event_type="meta_case_created",
                    actor_type="system",
                    actor_id=actor_id,
                    policy_version=self.policy.policy_version,
                    payload={
                        "event_fingerprint": fingerprint,
                        "correlation_reason": correlation_reason,
                        "correlation_confidence": correlation_confidence,
                        "status": status,
                        "observation_ids": [obs.observation_id for obs in observation_list],
                    },
                )
            )
        ]
        for child_case_id in child_case_ids:
            attach = await self.attach_child(
                meta.case_id,
                child_case_id,
                reason=correlation_reason,
                confidence=correlation_confidence,
                actor_id=actor_id,
            )
            events.extend(attach.events)
        meta_case = await self._require_meta_case(meta.case_id)
        return MetaCaseResult("created", meta_case, events)

    async def attach_child(
        self,
        meta_case_id: str,
        child_case_id: str,
        *,
        reason: str,
        confidence: float,
        actor_id: str = "",
        independent_action_required: bool = False,
    ) -> MetaCaseResult:
        meta = await self._require_meta_case(meta_case_id)
        child = await self._require_atomic_case(child_case_id)
        if child.meta_case_id and child.meta_case_id != meta.case_id:
            other = await self.store.get_case(child.meta_case_id)
            if isinstance(other, MetaCaseProjection) and other.status not in {"split", "merged", "resolved", "closed"}:
                raise ValueError(f"child case {child.case_id} is already attached to active meta-case {child.meta_case_id}")
        if child.case_id not in meta.child_case_ids:
            meta.child_case_ids.append(child.case_id)
        if child.resource_id and child.resource_id not in meta.affected_entities:
            meta.affected_entities.append(child.resource_id)
        meta.last_child_update_at = utc_now()
        meta.updated_at = utc_now()
        meta.policy_version = self.policy.policy_version
        child.meta_case_id = meta.case_id
        child.covered_by_meta_case = not independent_action_required
        child.independent_action_required = independent_action_required
        child.updated_at = utc_now()
        child.policy_version = self.policy.policy_version
        meta = cast(MetaCaseProjection, await self.store.upsert_case(meta))
        await self.store.upsert_case(child)
        event = await self.store.append_event(
            CaseEvent(
                case_id=child.case_id,
                meta_case_id=meta.case_id,
                event_type="child_case_attached_to_meta_case",
                actor_type="operator" if actor_id else "system",
                actor_id=actor_id,
                policy_version=self.policy.policy_version,
                payload={
                    "reason": reason,
                    "confidence": confidence,
                    "covered_by_meta_case": child.covered_by_meta_case,
                    "independent_action_required": independent_action_required,
                },
            )
        )
        return MetaCaseResult("attached", meta, [event])

    async def detach_child(
        self,
        meta_case_id: str,
        child_case_id: str,
        *,
        reason: str,
        actor_id: str = "",
    ) -> MetaCaseResult:
        meta = await self._require_meta_case(meta_case_id)
        child = await self._require_atomic_case(child_case_id)
        meta.child_case_ids = [case_id for case_id in meta.child_case_ids if case_id != child.case_id]
        meta.last_child_update_at = utc_now()
        meta.updated_at = utc_now()
        meta.policy_version = self.policy.policy_version
        if child.meta_case_id == meta.case_id:
            child.meta_case_id = ""
            child.covered_by_meta_case = False
        child.updated_at = utc_now()
        child.policy_version = self.policy.policy_version
        meta = cast(MetaCaseProjection, await self.store.upsert_case(meta))
        await self.store.upsert_case(child)
        event = await self.store.append_event(
            CaseEvent(
                case_id=child.case_id,
                meta_case_id=meta.case_id,
                event_type="child_case_detached_from_meta_case",
                actor_type="operator" if actor_id else "system",
                actor_id=actor_id,
                policy_version=self.policy.policy_version,
                payload={"reason": reason},
            )
        )
        return MetaCaseResult("detached", meta, [event])

    async def mark_independent_action_required(
        self,
        child_case_id: str,
        *,
        required: bool = True,
        reason: str,
        actor_id: str = "",
    ) -> AtomicCaseProjection:
        child = await self._require_atomic_case(child_case_id)
        child.independent_action_required = required
        if required:
            child.covered_by_meta_case = False
        elif child.meta_case_id:
            child.covered_by_meta_case = True
        child.updated_at = utc_now()
        child.policy_version = self.policy.policy_version
        child = cast(AtomicCaseProjection, await self.store.upsert_case(child))
        await self.store.append_event(
            CaseEvent(
                case_id=child.case_id,
                meta_case_id=child.meta_case_id or None,
                event_type="child_case_independent_action_required_set",
                actor_type="operator" if actor_id else "system",
                actor_id=actor_id,
                policy_version=self.policy.policy_version,
                payload={"required": required, "reason": reason},
            )
        )
        return child

    async def _require_meta_case(self, meta_case_id: str) -> MetaCaseProjection:
        case = await self.store.get_case(meta_case_id)
        if not isinstance(case, MetaCaseProjection):
            raise KeyError(f"meta-case not found: {meta_case_id}")
        return case

    async def _require_atomic_case(self, case_id: str) -> AtomicCaseProjection:
        case = await self.store.get_case(case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found: {case_id}")
        return case


def event_fingerprint_from_parts(title: str, parts: Iterable[str]) -> str:
    payload = {"title": str(title or ""), "parts": sorted(str(part) for part in parts if str(part))}
    return hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()[:16]


def _shared_scope(observations: list[ObservationRecord]) -> tuple[str, str]:
    for field in ("site", "customer", "service", "resource", "entity"):
        values = {str(getattr(obs, field) or "") for obs in observations}
        values.discard("")
        if len(values) == 1:
            return field, next(iter(values))
    return "", ""


def _event_type_for_observations(observations: list[ObservationRecord]) -> str:
    rendered = " ".join(
        " ".join([obs.detector, obs.rule_id, str(obs.annotations.get("summary") or "")])
        for obs in observations
    ).lower()
    if "router" in rendered and ("down" in rendered or "unreachable" in rendered):
        return "router_down"
    if "link" in rendered and ("down" in rendered or "loss" in rendered):
        return "link_failure"
    if "monitor" in rendered or "scrape" in rendered:
        return "monitoring_failure"
    if "bgp" in rendered or "ospf" in rendered or "isis" in rendered or "reconvergence" in rendered:
        return "routing_reconvergence"
    if "power" in rendered:
        return "power_event"
    return "unknown"
