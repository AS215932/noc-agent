from datetime import datetime, timedelta, timezone

from app.cases.attention import AttentionDelivery, attention_due
from app.cases.models import AtomicCaseProjection


START = datetime(2026, 9, 7, tzinfo=timezone.utc)


def delivered(case, **changes):
    return AttentionDelivery(case_id=case.case_id, generation=case.report_generation,
                             phase="firing", severity=case.severity, delivered_at=START,
                             sequence=1).model_copy(update=changes)


def test_quiet_updates_cannot_postpone_six_hour_attention():
    case = AtomicCaseProjection(severity="HIGH")
    prior = delivered(case)
    for hour in range(1, 7):
        now = START + timedelta(hours=hour)
        case.last_reported_at = now.isoformat()
        case.signal_signature = f"new-telemetry-{hour}"
        request = attention_due(case, prior, now=now)
        assert (request is not None) == (hour == 6)
    assert request.kind == "reminder"


def test_human_ack_stops_reminders_and_recovery_still_notifies():
    case = AtomicCaseProjection(severity="HIGH")
    prior = delivered(case)
    case.acknowledged_by = "operator"
    assert attention_due(case, prior, now=START + timedelta(hours=7)) is None
    case.status = "resolved"
    case.resolution_reason = "positive_clean_observation"
    assert attention_due(case, prior, now=START + timedelta(hours=7)).kind == "recovery"


def test_new_escalation_recurrence_and_duplicate_recovery():
    case = AtomicCaseProjection(severity="LOW")
    assert attention_due(case, None, now=START).kind == "new"
    prior = delivered(case)
    case.severity = "HIGH"
    assert attention_due(case, prior, now=START).kind == "escalation"
    recovered = delivered(case, phase="recovered")
    case.status = "resolved"
    case.resolution_reason = "positive_clean_observation"
    assert attention_due(case, recovered, now=START) is None
    case.status = "investigating"
    case.report_generation += 1
    assert attention_due(case, recovered, now=START).kind == "recurrence"


def test_ack_and_snooze_are_rechecked_without_changing_retry_identity():
    case = AtomicCaseProjection(severity="HIGH")
    prior = delivered(case)
    now = START + timedelta(hours=7)
    request = attention_due(case, prior, now=now)
    case.snoozed_until = (now + timedelta(hours=1)).isoformat()
    assert attention_due(case, prior, now=now) is None
    case.snoozed_until = ""
    assert attention_due(case, prior, now=now).idempotency_key == request.idempotency_key


def test_operator_resolution_does_not_claim_monitor_recovery():
    case = AtomicCaseProjection(severity="HIGH")
    prior = delivered(case)
    case.status = "resolved"
    for reason in ("", "operator_approved", "operator_rejected", "graph_update"):
        case.resolution_reason = reason
        assert attention_due(case, prior, now=START) is None
    case.resolution_reason = "positive_clean_observation"
    assert attention_due(case, prior, now=START).kind == "recovery"
