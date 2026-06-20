import pytest
from pydantic import ValidationError

from app.agents.noc_duty import ActionIntent, CycleSnapshot, NOCDutyOfficerAgent, reject_intent


@pytest.mark.asyncio
async def test_duty_officer_placeholder_prioritizes_meta_then_severity():
    snapshot = CycleSnapshot(
        cycle_id="cyc_1",
        policy_version="policy_v1",
        knowledge_export_version="export_v1",
        active_meta_cases=[{"case_id": "meta_1", "severity": "LOW"}],
        active_atomic_cases=[
            {"case_id": "case_low", "severity": "LOW"},
            {"case_id": "case_high", "severity": "HIGH"},
        ],
        retrieved_hyrule_knowledge=[{"doc_id": "curated/runbook", "authoritative": True}],
    )

    plan = await NOCDutyOfficerAgent(mode="shadow").plan(snapshot)

    assert plan.deployment_mode == "shadow"
    assert plan.priority_order == ["meta_1", "case_high", "case_low"]
    assert [action.target_case_id for action in plan.proposed_actions] == ["meta_1", "case_high", "case_low"]
    assert plan.knowledge_citations == snapshot.retrieved_hyrule_knowledge


@pytest.mark.asyncio
async def test_duty_officer_disabled_mode_emits_no_actions():
    snapshot = CycleSnapshot(cycle_id="cyc_1", active_atomic_cases=[{"case_id": "case_1", "severity": "HIGH"}])

    plan = await NOCDutyOfficerAgent(mode="disabled").plan(snapshot)

    assert plan.deployment_mode == "disabled"
    assert plan.proposed_actions == []
    assert plan.confidence == 1.0


def test_action_intent_contract_rejects_bad_confidence():
    with pytest.raises(ValidationError):
        ActionIntent(intent_type="investigate", confidence=1.5)


def test_reject_intent_produces_validated_intent():
    intent = ActionIntent(intent_type="handoff", confidence=0.9)

    validated = reject_intent(intent, reason="handoff requires existing CaseService handoff intent")

    assert validated.intent_id == intent.intent_id
    assert validated.validation_status == "rejected"
    assert validated.validator == "PolicyGuard"
