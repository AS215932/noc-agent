"""Typed records for the case-grounded NOC state machine.

These models are the durable boundary that the refactor will move toward:
raw notifications and scan results become :class:`ObservationRecord` objects,
case lifecycle changes become append-only :class:`CaseEvent` objects, and
external side effects are requested through :class:`OutboxIntent` rows.

The first implementation slice keeps these contracts dormant; later phases will
persist the same shapes in Postgres and wire the proactive/reactive paths
through the shared service layer.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


Severity = Literal["HIGH", "MEDIUM", "LOW", "UNKNOWN"]
CaseKind = Literal["atomic", "meta"]
ObservationStatus = Literal["firing", "clean", "resolved", "unknown"]
SourceHealth = Literal["healthy", "degraded", "unknown", "failed"]
CaseStatus = Literal[
    # LHP-v1 shared operational ledger statuses
    "open",
    "triaged",
    "context_requested",
    "handoff_requested",
    "handoff_in_progress",
    "verification_pending",
    "blocked",
    "failed",
    "needs_human",
    # existing reactive atomic statuses
    "investigating",
    "waiting_approval",
    "recovered_pending",
    "resolved",
    "expired",
    "linked",
    "closed",
    # meta-case statuses planned for storm correlation
    "candidate_event",
    "active_event",
    "investigating_event",
    "monitoring_recovery",
    "stabilized",
    "split",
    "merged",
]
ActorType = Literal["system", "operator", "monitor", "graph", "migration", "replay"]
AliasType = Literal[
    "hotspot_fp",
    "alert_fp",
    "source_fp",
    "source_event_id",
    "rule_entity",
    "issue_marker",
    "external_issue_id",
]
OutboxIntentType = Literal[
    "report",
    "meta_report",
    "handoff",
    "knowledge_candidate",
    "issue_comment",
    "discord_update",
    # LHP-v1 cross-loop event intents (dormant until handlers are wired)
    "engineering_handoff_requested",
    "engineering_handoff_updated",
    "engineering_handoff_verified",
    "engineering_handoff_resolved",
    "knowledge_context_requested",
    "knowledge_artifact_proposed",
    "verification_objective_created",
    "verification_objective_updated",
    "verification_passed",
    "verification_failed",
]
OutboxStatus = Literal["pending", "in_progress", "succeeded", "failed", "abandoned"]
TraceType = Literal[
    "cycle_snapshot",
    "gate_decision",
    "investigation",
    "report",
    "handoff",
    "knowledge_retrieval",
    "cycle_plan",
    "validated_intent",
    "replay",
]
FeedbackType = Literal[
    "operator_note",
    "operator_confirmed_diagnosis",
    "operator_rejected_diagnosis",
    "operator_confirmed_grouping",
    "operator_rejected_grouping",
    "operator_split",
    "operator_merge",
    "operator_resolution_label",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_json(value: Any) -> str:
    """Return a deterministic JSON representation for signatures/replay.

    Telemetry snapshots can contain nested dicts/lists and occasionally values
    that are not JSON-native. Convert unknown leaves to strings instead of
    raising so signature generation never breaks ingestion.
    """

    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_signal_signature(snapshot: dict[str, Any]) -> str:
    """Short, deterministic evidence hash used for change detection."""

    if not snapshot:
        return ""
    return hashlib.sha256(stable_json(snapshot).encode("utf-8")).hexdigest()[:16]


def _json_safe(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_safe(value.model_dump(mode="json"))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, list | tuple | set):
        return [_json_safe(child) for child in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


class ObservationRecord(BaseModel):
    """Normalized evidence from a webhook alert, scan result, metric, or log.

    A detector that did not run produces no observation. A clean observation is
    explicit (`status="clean"`) and is therefore distinguishable from absence.
    """

    model_config = ConfigDict(extra="forbid")

    observation_id: str = Field(default_factory=lambda: f"obs_{uuid4().hex[:12]}")
    source: str = ""
    source_event_id: str = ""
    source_fingerprint: str = ""
    dedup_key: str = ""
    detector: str = ""
    rule_id: str = ""
    entity: str = ""
    resource: str = ""
    interface: str = ""
    service: str = ""
    site: str = ""
    customer: str = ""
    severity: Severity = "UNKNOWN"
    status: ObservationStatus = "unknown"
    observed_at: str = Field(default_factory=utc_now)
    received_at: str = Field(default_factory=utc_now)
    scan_cycle_id: str = ""
    labels: dict[str, Any] = Field(default_factory=dict)
    annotations: dict[str, Any] = Field(default_factory=dict)
    signal_snapshot: dict[str, Any] = Field(default_factory=dict)
    signal_signature: str = ""
    source_health: SourceHealth = "unknown"
    observation_confidence: Literal["high", "medium", "low"] = "medium"
    payload_ref: str = ""
    schema_version: int = 1

    @model_validator(mode="after")
    def _fill_signature(self) -> "ObservationRecord":
        if not self.signal_signature:
            self.signal_signature = stable_signal_signature(self.signal_snapshot)
        return self

    @property
    def is_positive_clean(self) -> bool:
        return self.status in {"clean", "resolved"} and self.source_health not in {"degraded", "failed"}


class CaseEvent(BaseModel):
    """Append-only lifecycle/decision event for atomic cases or meta-cases."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex[:12]}")
    case_id: str | None = None
    meta_case_id: str | None = None
    event_type: str
    actor_type: ActorType = "system"
    actor_id: str = ""
    source: str = ""
    occurred_at: str = Field(default_factory=utc_now)
    observed_at: str = ""
    correlation_id: str = ""
    causation_id: str = ""
    policy_version: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    schema_version: int = 1


class CaseIdentityAlias(BaseModel):
    """Compatibility and drift mapping from external identities to case ids."""

    model_config = ConfigDict(extra="forbid")

    alias_id: str = Field(default_factory=lambda: f"alias_{uuid4().hex[:12]}")
    case_id: str
    alias_type: AliasType
    alias_value: str
    source: str = ""
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    created_at: str = Field(default_factory=utc_now)
    retired_at: str | None = None

    @model_validator(mode="after")
    def _normalize_alias(self) -> "CaseIdentityAlias":
        self.alias_value = str(self.alias_value or "").strip()
        return self


class AtomicCaseProjection(BaseModel):
    """Query projection for the authoritative atomic case record."""

    model_config = ConfigDict(extra="forbid")

    case_id: str = Field(default_factory=lambda: f"case_{uuid4().hex[:12]}")
    kind: Literal["atomic"] = "atomic"
    case_number: str = ""
    fingerprint: str = ""
    identity: dict[str, Any] = Field(default_factory=dict)
    resource_id: str = ""
    title: str = ""
    summary: str = ""
    origin: Literal["reactive", "proactive", "unknown"] = "unknown"
    rule_id: str = ""
    detector: str = ""
    site: str = ""
    customer: str = ""
    service: str = ""
    severity: Severity = "UNKNOWN"
    status: CaseStatus = "investigating"
    opened_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    resolved_at: str | None = None
    closed_at: str | None = None
    resolution_reason: str = ""
    last_seen: str = ""
    last_observed_unhealthy: str = ""
    last_observed_clean: str = ""
    last_evaluated_at: str = ""
    last_scan_cycle_id: str = ""
    signal_snapshot: dict[str, Any] = Field(default_factory=dict)
    signal_signature: str = ""
    previous_signal_signature: str = ""
    last_reported_at: str = ""
    last_reported_signature: str = ""
    last_reasserted_at: str = ""
    last_investigated_at: str = ""
    diagnosis_signature: str = ""
    last_diagnosis: dict[str, Any] = Field(default_factory=dict)
    recommendations: list[str] = Field(default_factory=list)
    investigation_status: str = ""
    investigation_error: str = ""
    issue_url: str = ""
    issue_id: str = ""
    handoff_status: str = ""
    last_handoff_at: str = ""
    acknowledged_by: str = ""
    acknowledged_at: str = ""
    snoozed_until: str = ""
    suppressed_until: str = ""
    suppression_reason: str = ""
    suppression_source: Literal["operator", "agent", ""] = ""
    meta_case_id: str = ""
    covered_by_meta_case: bool = False
    independent_action_required: bool = False
    trace_ids: list[str] = Field(default_factory=list)
    feedback_ids: list[str] = Field(default_factory=list)
    knowledge_citations: list[dict[str, Any]] = Field(default_factory=list)
    candidate_knowledge_update_ids: list[str] = Field(default_factory=list)
    policy_version: str = ""
    schema_version: int = 1

    @model_validator(mode="after")
    def _fill_atomic_signature(self) -> "AtomicCaseProjection":
        if not self.signal_signature:
            self.signal_signature = stable_signal_signature(self.signal_snapshot)
        return self


class MetaCaseProjection(BaseModel):
    """Query projection for a correlated notification storm / larger event."""

    model_config = ConfigDict(extra="forbid")

    case_id: str = Field(default_factory=lambda: f"meta_{uuid4().hex[:12]}")
    kind: Literal["meta"] = "meta"
    event_fingerprint: str = ""
    title: str = ""
    summary: str = ""
    event_type: Literal[
        "router_down",
        "link_failure",
        "routing_reconvergence",
        "monitoring_failure",
        "power_event",
        "unknown",
    ] = "unknown"
    status: CaseStatus = "candidate_event"
    suspected_primary_entity: str = ""
    suspected_site: str = ""
    suspected_customer: str = ""
    suspected_service: str = ""
    child_case_ids: list[str] = Field(default_factory=list)
    notification_ids: list[str] = Field(default_factory=list)
    observation_ids: list[str] = Field(default_factory=list)
    affected_entities: list[str] = Field(default_factory=list)
    affected_sites: list[str] = Field(default_factory=list)
    affected_customers: list[str] = Field(default_factory=list)
    affected_services: list[str] = Field(default_factory=list)
    affected_interfaces: list[str] = Field(default_factory=list)
    blast_radius_summary: str = ""
    correlation_reason: str = ""
    correlation_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    correlation_features: list[dict[str, Any]] = Field(default_factory=list)
    root_cause_hypothesis: str = ""
    supporting_evidence: list[str] = Field(default_factory=list)
    disconfirming_evidence: list[str] = Field(default_factory=list)
    opened_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    first_notification_at: str = ""
    last_notification_at: str = ""
    last_child_update_at: str = ""
    stabilized_at: str | None = None
    resolved_at: str | None = None
    closed_at: str | None = None
    resolution_reason: str = ""
    notification_count: int = 0
    child_case_count: int = 0
    peak_rate: float = 0.0
    storm_started_at: str = ""
    storm_last_seen_at: str = ""
    storm_quiet_since: str = ""
    stabilization_window_until: str = ""
    event_level_diagnosis: dict[str, Any] = Field(default_factory=dict)
    recommended_event_level_action: str = ""
    issue_url: str = ""
    issue_id: str = ""
    handoff_status: str = ""
    child_issue_urls: list[str] = Field(default_factory=list)
    trace_ids: list[str] = Field(default_factory=list)
    feedback_ids: list[str] = Field(default_factory=list)
    knowledge_citations: list[dict[str, Any]] = Field(default_factory=list)
    candidate_knowledge_update_ids: list[str] = Field(default_factory=list)
    final_root_cause_label: str = ""
    final_correlation_quality_label: str = ""
    policy_version: str = ""
    schema_version: int = 1

    @model_validator(mode="after")
    def _fill_counts(self) -> "MetaCaseProjection":
        self.child_case_count = len(self.child_case_ids)
        self.notification_count = max(self.notification_count, len(self.notification_ids))
        return self


class TraceRecord(BaseModel):
    """Replayable structured decision/tool/model trace."""

    model_config = ConfigDict(extra="forbid")

    trace_id: str = Field(default_factory=lambda: f"trace_{uuid4().hex[:12]}")
    cycle_id: str = ""
    case_id: str | None = None
    meta_case_id: str | None = None
    trace_type: TraceType
    policy_version: str = ""
    model_chain: list[str] = Field(default_factory=list)
    prompt_version: str = ""
    knowledge_export_version: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    schema_version: int = 1


class OperatorFeedback(BaseModel):
    """Operator judgment/correction attached to case/meta-case/trace ids."""

    model_config = ConfigDict(extra="forbid")

    feedback_id: str = Field(default_factory=lambda: f"fb_{uuid4().hex[:12]}")
    case_id: str | None = None
    meta_case_id: str | None = None
    trace_id: str | None = None
    actor_id: str = ""
    actor_role: str = "operator"
    feedback_type: FeedbackType
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    schema_version: int = 1

    @model_validator(mode="after")
    def _require_target(self) -> "OperatorFeedback":
        if not (self.case_id or self.meta_case_id or self.trace_id):
            raise ValueError("operator feedback requires a case_id, meta_case_id, or trace_id")
        return self


class OutboxIntent(BaseModel):
    """Crash-safe request for an external side effect.

    Services enqueue intents inside the same transaction as the case transition;
    workers execute them later and record the result back through the service.
    """

    model_config = ConfigDict(extra="forbid")

    outbox_id: str = Field(default_factory=lambda: f"out_{uuid4().hex[:12]}")
    intent_type: OutboxIntentType
    case_id: str | None = None
    meta_case_id: str | None = None
    idempotency_key: str
    state_signature: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    status: OutboxStatus = "pending"
    attempts: int = 0
    next_attempt_at: str = Field(default_factory=utc_now)
    created_at: str = Field(default_factory=utc_now)
    completed_at: str | None = None
    external_id: str = ""
    external_url: str = ""
    error: str = ""
    schema_version: int = 1

    @model_validator(mode="after")
    def _require_target(self) -> "OutboxIntent":
        if not (self.case_id or self.meta_case_id):
            raise ValueError("outbox intent must target a case_id or meta_case_id")
        self.idempotency_key = str(self.idempotency_key or "").strip()
        if not self.idempotency_key:
            raise ValueError("outbox intent requires an idempotency_key")
        return self
