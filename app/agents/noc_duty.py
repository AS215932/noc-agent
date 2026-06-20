"""Advisory NOC duty-officer planning contracts.

The duty officer is a proposal layer only. CaseService/CorrelationService remain
the law; outbox workers are the hands.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from app.cases.models import utc_now

DutyOfficerMode = Literal["disabled", "shadow", "advise", "guarded_act", "manual_only"]
ActionIntentType = Literal[
    "investigate",
    "report",
    "handoff",
    "suppress",
    "ack",
    "ask_operator",
    "attach_child_to_meta_case",
    "detach_child_from_meta_case",
    "merge_meta_cases",
    "split_meta_case",
    "mark_independent_action_required",
    "set_root_cause_hypothesis",
    "create_knowledge_candidate",
]
ValidationStatus = Literal["accepted", "rejected", "requires_operator_approval"]
Validator = Literal["CaseService", "CorrelationService", "KnowledgeAuthorityFilter", "PolicyGuard"]


class CycleSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cycle_id: str
    observed_at: str = Field(default_factory=utc_now)
    policy_version: str = ""
    knowledge_export_version: str = ""
    model_chain: list[str] = Field(default_factory=list)
    prompt_version: str = ""
    budget_state: dict[str, Any] = Field(default_factory=dict)
    new_observations: list[dict[str, Any]] = Field(default_factory=list)
    active_atomic_cases: list[dict[str, Any]] = Field(default_factory=list)
    active_meta_cases: list[dict[str, Any]] = Field(default_factory=list)
    recently_resolved_cases: list[dict[str, Any]] = Field(default_factory=list)
    suppressed_cases: list[dict[str, Any]] = Field(default_factory=list)
    retrieved_hyrule_knowledge: list[dict[str, Any]] = Field(default_factory=list)
    recent_operator_feedback: list[dict[str, Any]] = Field(default_factory=list)
    open_outbox_intents: list[dict[str, Any]] = Field(default_factory=list)
    uncertainty_flags: list[str] = Field(default_factory=list)


class ActionIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_id: str = Field(default_factory=lambda: f"intent_{uuid4().hex[:12]}")
    intent_type: ActionIntentType
    target_case_id: str | None = None
    target_meta_case_id: str | None = None
    rationale: str = ""
    evidence_refs: list[str] = Field(default_factory=list)
    observation_refs: list[str] = Field(default_factory=list)
    case_event_refs: list[str] = Field(default_factory=list)
    knowledge_citations: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    requires_operator_approval: bool = False
    idempotency_key: str = ""


class CyclePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cycle_id: str
    plan_id: str = Field(default_factory=lambda: f"plan_{uuid4().hex[:12]}")
    created_at: str = Field(default_factory=utc_now)
    deployment_mode: DutyOfficerMode = "shadow"
    agent_role_profile: str = "Senior Network Reliability Engineer"
    policy_version: str = ""
    knowledge_export_version: str = ""
    priority_order: list[str] = Field(default_factory=list)
    proposed_actions: list[ActionIntent] = Field(default_factory=list)
    storm_hypotheses: list[dict[str, Any]] = Field(default_factory=list)
    root_cause_hypotheses: list[dict[str, Any]] = Field(default_factory=list)
    operator_questions: list[str] = Field(default_factory=list)
    knowledge_candidate_recommendations: list[dict[str, Any]] = Field(default_factory=list)
    reasoning_summary: str = ""
    uncertainty_summary: str = ""
    knowledge_citations: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    token_cost: int = 0
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)


class ValidatedIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent_id: str
    validation_status: ValidationStatus
    rejection_reason: str = ""
    validator: Validator
    committed_case_event_id: str = ""
    outbox_id: str = ""
    created_trace_id: str = ""


class NOCDutyOfficerAgent:
    """Deterministic placeholder for shadow/advise plumbing.

    It produces typed proposals only; validators decide whether anything becomes
    case events or outbox intents. A future LLM-backed implementation should
    preserve this contract.
    """

    def __init__(self, *, mode: DutyOfficerMode = "shadow") -> None:
        self.mode = mode

    async def plan(self, snapshot: CycleSnapshot) -> CyclePlan:
        if self.mode == "disabled":
            return CyclePlan(
                cycle_id=snapshot.cycle_id,
                deployment_mode="disabled",
                policy_version=snapshot.policy_version,
                knowledge_export_version=snapshot.knowledge_export_version,
                reasoning_summary="Duty officer disabled; deterministic baseline only.",
                confidence=1.0,
            )
        priority_order = _priority_order(snapshot.active_meta_cases, snapshot.active_atomic_cases)
        actions = [
            ActionIntent(
                intent_type="investigate",
                target_case_id=case_id,
                rationale="Active case has no fresh investigation in the frozen snapshot.",
                confidence=0.5,
                requires_operator_approval=False,
                idempotency_key=f"agent-investigate:{snapshot.cycle_id}:{case_id}",
            )
            for case_id in priority_order[:3]
        ]
        return CyclePlan(
            cycle_id=snapshot.cycle_id,
            deployment_mode=self.mode,
            policy_version=snapshot.policy_version,
            knowledge_export_version=snapshot.knowledge_export_version,
            priority_order=priority_order,
            proposed_actions=actions,
            reasoning_summary="Deterministic placeholder plan generated from frozen case snapshot.",
            uncertainty_summary="No LLM reasoning used in this placeholder.",
            knowledge_citations=list(snapshot.retrieved_hyrule_knowledge),
            confidence=0.5 if actions else 1.0,
        )


def reject_intent(intent: ActionIntent, *, reason: str, validator: Validator = "PolicyGuard") -> ValidatedIntent:
    return ValidatedIntent(
        intent_id=intent.intent_id,
        validation_status="rejected",
        rejection_reason=reason,
        validator=validator,
    )


def _priority_order(meta_cases: list[dict[str, Any]], atomic_cases: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    for item in meta_cases:
        case_id = str(item.get("case_id") or "")
        if case_id:
            ordered.append(case_id)
    for item in sorted(atomic_cases, key=lambda case: _severity_rank(str(case.get("severity") or "")), reverse=True):
        case_id = str(item.get("case_id") or "")
        if case_id and case_id not in ordered:
            ordered.append(case_id)
    return ordered


def _severity_rank(severity: str) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(severity.upper(), 0)
