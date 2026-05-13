from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, TypedDict
from uuid import uuid4

from pydantic import BaseModel, Field


class EvidenceItem(BaseModel):
    tool: str
    summary: str
    direct_measurement: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)


class SpecialistFinding(BaseModel):
    specialist: Literal["bgp", "security_firewall", "infrastructure"]
    summary: str
    assessment: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[EvidenceItem] = Field(default_factory=list)


class ChangeProposal(BaseModel):
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
    incident_id: str
    decision: Literal["approved", "rejected", "acknowledged"]
    operator: str
    comment: str = ""
    decided_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class IncidentSummary(BaseModel):
    incident_id: str
    resource_id: str
    title: str
    status: Literal["running", "waiting_approval", "approved", "rejected", "finalized"]
    chronic_instability: bool = False
    active_specialist: str | None = None
    proposal_count: int = 0
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class NOCState(TypedDict, total=False):
    incident_id: str
    thread_id: str
    normalized_alert: dict[str, Any]
    resource_id: str
    correlation_key: str
    related_alerts: list[dict[str, Any]]
    incident_history: list[dict[str, Any]]
    telemetry_cache: dict[str, Any]
    active_specialist: str
    routing_reason: str
    evidence_log: list[dict[str, Any]]
    specialist_finding: dict[str, Any]
    legacy_action_plan: dict[str, Any]
    proposals: list[dict[str, Any]]
    approval_state: str
    operator_decision: dict[str, Any] | None
    chronic_instability: bool
    drift_findings: list[str]
    deduped: bool
    final_summary: dict[str, Any]
    model_override: Any
