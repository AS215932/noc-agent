import pytest

from app.agents.noc_duty import ActionIntent
from app.agents.policy_guard import PolicyGuard
from app.cases import CaseService, CorrelationService, InMemoryCaseStore, ObservationRecord


@pytest.mark.asyncio
async def test_policy_guard_rejects_duplicate_handoff_when_issue_exists():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None
    await service.record_handoff_result(created.case.case_id, issue_url="https://github.invalid/issues/1")

    validated = await PolicyGuard(store).validate(ActionIntent(intent_type="handoff", target_case_id=created.case.case_id))

    assert validated.validation_status == "rejected"
    assert "issue_url" in validated.rejection_reason


@pytest.mark.asyncio
async def test_policy_guard_accepts_handoff_when_case_has_no_issue():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None

    validated = await PolicyGuard(store).validate(ActionIntent(intent_type="handoff", target_case_id=created.case.case_id))

    assert validated.validation_status == "accepted"


@pytest.mark.asyncio
async def test_policy_guard_rejects_silent_suppression_and_unsafe_grouping():
    store = InMemoryCaseStore()

    silent_suppress = await PolicyGuard(store).validate(ActionIntent(intent_type="suppress", target_case_id="case_1"))
    split = await PolicyGuard(store).validate(ActionIntent(intent_type="split_meta_case", target_meta_case_id="meta_1"))

    assert silent_suppress.validation_status == "rejected"
    assert split.validation_status == "rejected"


@pytest.mark.asyncio
async def test_policy_guard_rejects_attach_when_child_requires_independent_action():
    store = InMemoryCaseStore()
    service = CaseService(store)
    correlation = CorrelationService(store)
    child = await service.observe(ObservationRecord(source="proactive", rule_id="disk", resource="log", status="firing"))
    assert child.case is not None
    meta = await correlation.create_meta_case(title="event", correlation_reason="test", correlation_confidence=1.0)
    await correlation.mark_independent_action_required(child.case.case_id, reason="operator override")

    validated = await PolicyGuard(store).validate(
        ActionIntent(
            intent_type="attach_child_to_meta_case",
            target_case_id=child.case.case_id,
            target_meta_case_id=meta.meta_case.case_id,
        )
    )

    assert validated.validation_status == "rejected"
    assert "independent_action_required" in validated.rejection_reason
