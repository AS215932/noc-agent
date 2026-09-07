"""Attention policy, independent of quiet persistent-card delivery timestamps."""
from datetime import datetime, timedelta, timezone
import os
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.cases.models import AtomicCaseProjection, Severity

SEVERITY_RANK = {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}

def attention_enabled() -> bool:
    """Creation requires the complete ownership gate; retained intents still drain."""
    return all(os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"} for name in (
        "NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED",
    ))


class AttentionDelivery(BaseModel):
    """Separate delivery projection; do not add fields to stored case JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    case_id: str
    generation: int = Field(ge=0)
    phase: Literal["firing", "recovered"]
    severity: Severity
    delivered_at: AwareDatetime
    sequence: int = Field(ge=1)


class AttentionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["new", "recurrence", "escalation", "recovery", "reminder"]
    generation: int
    phase: Literal["firing", "recovered"]
    severity: Severity
    expected_sequence: int
    idempotency_key: str


def _future(value: str, now: datetime) -> bool:
    if not value:
        return False
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.replace(tzinfo=timezone.utc) > now if stamp.tzinfo is None else stamp > now
    except ValueError:
        return False


def attention_due(case: AtomicCaseProjection, previous: AttentionDelivery | None, *,
                  now: datetime, reminder_seconds: int = 21600) -> AttentionRequest | None:
    """Recompute eligibility at enqueue and delivery; a quiet edit never resets it."""
    if previous is not None and previous.case_id != case.case_id:
        raise ValueError("attention projection belongs to another case")
    if case.status in {"closed", "expired", "linked", "recovered_pending"}:
        return None
    if case.covered_by_meta_case and not case.independent_action_required:
        return None
    if any(_future(value, now) for value in (case.suppressed_until, case.snoozed_until)):
        return None
    phase: Literal["firing", "recovered"] = "recovered" if case.status == "resolved" else "firing"
    kind: Literal["new", "recurrence", "escalation", "recovery", "reminder"] | None = None
    if phase == "recovered":
        if case.resolution_reason != "positive_clean_observation":
            return None
        if previous is not None and previous.phase == "firing":
            kind = "recovery"
    elif case.acknowledged_at or case.acknowledged_by:
        return None
    elif previous is None:
        kind = "new"
    elif previous.phase == "recovered":
        kind = "recurrence"
    else:
        if SEVERITY_RANK[case.severity] > SEVERITY_RANK[previous.severity]:
            kind = "escalation"
        elif case.report_generation != previous.generation:
            kind = "recurrence"
        elif case.severity == "HIGH" and now - previous.delivered_at >= timedelta(seconds=max(21600, reminder_seconds)):
            kind = "reminder"
    if kind is None:
        return None
    sequence = previous.sequence if previous else 0
    return AttentionRequest(
        kind=kind, generation=case.report_generation, phase=phase, severity=case.severity,
        expected_sequence=sequence,
        idempotency_key=f"attention:{case.case_id}:{case.report_generation}:{phase}:{case.severity}:{sequence}:{kind}",
    )
