from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal, TypeAlias, TypedDict
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


JsonPrimitive: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
JsonDict: TypeAlias = dict[str, JsonValue]


class EvidenceItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool: str
    summary: str
    direct_measurement: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)


class SpecialistFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    specialist: Literal["bgp", "security_firewall", "infrastructure"]
    summary: str
    assessment: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[EvidenceItem] = Field(default_factory=list)


class ChangeProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_id: str = Field(default_factory=lambda: str(uuid4()))
    incident_id: str
    resource_id: str
    assessment: str
    root_cause_hypothesis: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_refs: list[str] = Field(default_factory=list)
    drift_findings: list[str] = Field(default_factory=list)
    proposed_remediation: list[str] = Field(default_factory=list)
    validation_status: Literal["not_run", "validated", "needs_more_evidence"] = "not_run"
    human_review_rationale: str = ""


class ApprovalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str
    decision: Literal["approved", "rejected", "acknowledged"]
    operator: str
    comment: str = ""
    decided_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class IncidentSummary(BaseModel):
    model_config = ConfigDict(extra="allow")

    incident_id: str
    case_number: str = ""
    resource_id: str = ""
    title: str
    status: Literal[
        "running",
        "investigating",
        "waiting_approval",
        "recovered_pending",
        "resolved",
        "expired",
        "linked",
        "approved",
        "rejected",
        "finalized",
    ]
    chronic_instability: bool = False
    active_specialist: str | None = None
    proposal_count: int = 0
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class RetryCounts(TypedDict, total=False):
    triage: int
    specialist: int
    proposal: int


class WorkflowState(TypedDict, total=False):
    # Durable, JSON-safe workflow state only. Runtime dependencies, clients,
    # agents, toolsets, model objects, and file handles must never be added.
    incident_id: str
    case_number: str
    case_status: str
    case_event_count: int
    fingerprint: str
    thread_id: str
    current_step: str
    created_at: str
    updated_at: str
    normalized_alert: JsonDict
    resource_id: str
    correlation_key: str
    related_alerts: list[JsonDict]
    latest_event: JsonDict
    latest_transition: JsonDict | None
    case_context: JsonDict
    downstream_victims: list[str]
    needs_reassessment: bool
    incident_history: list[JsonDict]
    history_summary: JsonDict
    telemetry_cache: JsonDict
    active_specialist: Literal["bgp", "security_firewall", "infrastructure"]
    specialist_type: Literal["bgp", "security_firewall", "infrastructure"]
    routing_reason: str
    evidence_log: list[JsonDict]
    specialist_finding: JsonDict
    diagnostic_synthesis: JsonDict
    proposals: list[JsonDict]
    approval_state: str
    operator_decision: JsonDict | None
    chronic_instability: bool
    drift_findings: list[str]
    deduped: bool
    final_summary: JsonDict
    retry_counts: RetryCounts
    errors: list[JsonDict]
    perimeter_context_version: str
    manifest_hash: str


NOCState = WorkflowState


def json_safe_model_dump(model: BaseModel) -> JsonDict:
    return model.model_dump(mode="json")


def assert_json_serializable_state(state: dict[str, Any]) -> None:
    if "model_override" in state:
        raise TypeError("model_override is runtime-only and must not be persisted in WorkflowState")
    _assert_json_value(state, path="state")
    json.dumps(state)


def _assert_json_value(value: Any, *, path: str) -> None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} has non-string key {key!r}")
            _assert_json_value(item, path=f"{path}.{key}")
        return
    raise TypeError(f"{path} contains non-JSON value {type(value).__name__}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
