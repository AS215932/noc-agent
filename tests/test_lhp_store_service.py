import pytest

from app.cases.lhp import (
    CallbackInboxRecord,
    CaseHandoff,
    HandoffUpdate,
    KnowledgeArtifact,
    OutcomeRecord,
    VerificationObjective,
    lhp_payload_hash,
)
from app.cases.models import AtomicCaseProjection
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore


async def _service_with_case() -> tuple[CaseService, AtomicCaseProjection]:
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(
        case_id="case_lhp_1",
        fingerprint="8fb421ff94bb1285",
        title="disk fill on rtr",
        resource_id="2a0c:b641:b50:2::1:/",
        status="open",
        signal_signature="sig_disk",
    )
    await store.upsert_case(case)
    return CaseService(store), case


@pytest.mark.asyncio
async def test_request_lhp_handoff_is_atomic_and_idempotent():
    service, case = await _service_with_case()
    handoff = CaseHandoff(
        handoff_id="handoff_disk_1",
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_lhp_1:engineering:resolve-low-root-filesystem-condition:v1",
        fingerprint=case.fingerprint,
    )
    objective = VerificationObjective(
        objective_id="objective_health_1",
        case_id=case.case_id,
        handoff_id=handoff.handoff_id,
        objective_key="root_disk_clear",
        objective_type="monitoring_alert_clear",
        name="root disk alert clears",
    )

    created = await service.request_lhp_handoff(handoff, objectives=[objective], enqueue_delivery=True)
    duplicate = await service.request_lhp_handoff(handoff, objectives=[objective], enqueue_delivery=True)
    stored_case = await service.store.get_case(case.case_id)
    events = await service.store.case_events(case.case_id)
    outbox = await service.store.list_outbox(status="pending")

    assert created.created is True
    assert duplicate.created is False
    assert duplicate.handoff.handoff_id == created.handoff.handoff_id
    assert created.case.status == "handoff_requested"
    assert isinstance(stored_case, AtomicCaseProjection)
    assert stored_case.handoff_status == "requested"
    assert created.objectives[0].handoff_id == handoff.handoff_id
    assert [intent.intent_type for intent in outbox] == ["engineering_handoff_requested"]
    assert [event.event_type for event in events] == ["lhp_handoff_requested"]


@pytest.mark.asyncio
async def test_lhp_handoff_updates_are_deduped_and_drive_case_state():
    service, case = await _service_with_case()
    handoff = CaseHandoff(
        handoff_id="handoff_disk_1",
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_lhp_1:engineering:resolve-low-root-filesystem-condition:v1",
    )
    await service.request_lhp_handoff(handoff)

    accepted = HandoffUpdate(
        update_id="update_accept_1",
        case_id=case.case_id,
        handoff_id=handoff.handoff_id,
        source_loop="engineering",
        update_type="accepted",
        status="accepted",
        external_event_id="eng_evt_accept_1",
        correlation_id=handoff.correlation_id,
    )
    accepted_result = await service.record_lhp_handoff_update(accepted)
    duplicate_result = await service.record_lhp_handoff_update(accepted)

    assert accepted_result.created is True
    assert duplicate_result.created is False
    assert accepted_result.case.status == "handoff_in_progress"
    assert accepted_result.handoff.status == "accepted"

    for update in (
        HandoffUpdate(
            update_id="update_in_progress_1",
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            source_loop="engineering",
            update_type="investigating",
            status="in_progress",
            external_event_id="eng_evt_progress_1",
            correlation_id=handoff.correlation_id,
        ),
        HandoffUpdate(
            update_id="update_implemented_1",
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            source_loop="engineering",
            update_type="implemented",
            status="implemented",
            external_event_id="eng_evt_impl_1",
            correlation_id=handoff.correlation_id,
        ),
    ):
        result = await service.record_lhp_handoff_update(update)

    assert result.handoff.status == "implemented"
    assert result.case.status == "verification_pending"


@pytest.mark.asyncio
async def test_lhp_callback_claims_are_idempotent_before_state_mutation():
    service, case = await _service_with_case()
    callback = CallbackInboxRecord(
        callback_id="callback_1",
        source_loop="engineering",
        external_event_id="eng_evt_1",
        payload_hash=lhp_payload_hash({"event": "eng_evt_1"}),
        case_id=case.case_id,
        handoff_id="handoff_disk_1",
        result_payload={"accepted": True},
    )

    first = await service.claim_lhp_callback(callback)
    second = await service.claim_lhp_callback(callback.model_copy(update={"callback_id": "callback_2"}))

    assert first.created is True
    assert second.created is False
    assert second.callback.callback_id == "callback_1"
    assert second.callback.result_payload == {"accepted": True}


@pytest.mark.asyncio
async def test_noc_verifier_controls_verified_and_resolved_transitions():
    service, case = await _service_with_case()
    handoff = CaseHandoff(
        handoff_id="handoff_disk_1",
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_lhp_1:engineering:resolve-low-root-filesystem-condition:v1",
    )
    await service.request_lhp_handoff(handoff)
    for update in (
        HandoffUpdate(
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            source_loop="engineering",
            update_type="accepted",
            status="accepted",
            external_event_id="eng_evt_accept_1",
            correlation_id=handoff.correlation_id,
        ),
        HandoffUpdate(
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            source_loop="engineering",
            update_type="investigating",
            status="in_progress",
            external_event_id="eng_evt_progress_1",
            correlation_id=handoff.correlation_id,
        ),
        HandoffUpdate(
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            source_loop="engineering",
            update_type="implemented",
            status="implemented",
            external_event_id="eng_evt_impl_1",
            correlation_id=handoff.correlation_id,
        ),
    ):
        await service.record_lhp_handoff_update(update)

    with pytest.raises(ValueError, match="dedicated NOC verifier path"):
        await service.record_lhp_handoff_update(
            HandoffUpdate(
                case_id=case.case_id,
                handoff_id=handoff.handoff_id,
                source_loop="noc",
                update_type="implemented",
                status="verified",
                external_event_id="noc_evt_bad_verify_1",
                correlation_id=handoff.correlation_id,
            )
        )

    verified = await service.mark_lhp_handoff_verified(handoff.handoff_id)
    outcome = OutcomeRecord(
        outcome_id="outcome_disk_1",
        work_item_id=case.case_id,
        case_type="proactive_disk_condition",
        fingerprint=case.fingerprint,
        validation={"monitoring_alert_clear": True},
    )
    resolved_case = await service.resolve_lhp_case_with_outcome(case.case_id, handoff_id=handoff.handoff_id, outcome=outcome)
    outcomes = await service.list_lhp_outcomes(case_id=case.case_id)
    stored_handoff = await service.get_lhp_handoff(handoff.handoff_id)

    assert verified.status == "verified"
    assert resolved_case.status == "resolved"
    assert resolved_case.resolution_reason == "lhp_outcome_verified"
    assert outcomes == [outcome]
    assert stored_handoff is not None
    assert stored_handoff.status == "resolved"


@pytest.mark.asyncio
async def test_verification_objectives_and_knowledge_artifacts_are_case_scoped():
    service, case = await _service_with_case()
    objective = VerificationObjective(
        objective_id="objective_health_1",
        case_id=case.case_id,
        objective_key="root_disk_clear",
        objective_type="monitoring_alert_clear",
        name="root disk alert clears",
        next_check_at="",
    )
    artifact = KnowledgeArtifact(
        artifact_id="artifact_1",
        case_id=case.case_id,
        artifact_type="runbook_delta",
        summary="Disk alert handling should hand off durable remediation only.",
        payload={"source": "test"},
    )

    stored_objective = await service.upsert_lhp_verification_objective(objective)
    due = await service.list_due_lhp_verification_objectives(now="9999-01-01T00:00:00+00:00")
    stored_objective.status = "pass"
    stored_objective.consecutive_pass_count = 3
    stored_objective = await service.record_lhp_verification_result(stored_objective)
    artifact_created = await service.record_lhp_knowledge_artifact(artifact)
    artifact_duplicate = await service.record_lhp_knowledge_artifact(artifact.model_copy(update={"artifact_id": "artifact_2"}))

    assert due == [objective]
    assert stored_objective.status == "pass"
    assert await service.list_due_lhp_verification_objectives(now="9999-01-01T00:00:00+00:00") == []
    assert artifact_created.artifact_id == "artifact_1"
    assert artifact_duplicate.artifact_id == "artifact_1"
    assert await service.list_lhp_knowledge_artifacts(case_id=case.case_id) == [artifact_created]


@pytest.mark.asyncio
async def test_knowledge_context_request_is_idempotent_outbox_intent():
    service, case = await _service_with_case()

    first = await service.request_lhp_knowledge_context(
        case.case_id,
        handoff_id="handoff_disk_1",
        objective_key="resolve-low-root-filesystem-condition-v1",
        payload={"case_type": "proactive_disk_condition"},
    )
    second = await service.request_lhp_knowledge_context(
        case.case_id,
        handoff_id="handoff_disk_1",
        objective_key="resolve-low-root-filesystem-condition-v1",
        payload={"case_type": "proactive_disk_condition"},
    )

    assert first.outbox_id == second.outbox_id
    assert first.intent_type == "knowledge_context_requested"
    assert first.payload["case_type"] == "proactive_disk_condition"
    events = await service.store.case_events(case.case_id)
    assert [event.event_type for event in events] == [
        "lhp_knowledge_context_requested",
        "lhp_knowledge_context_requested",
    ]


@pytest.mark.asyncio
async def test_knowledge_artifact_proposal_request_is_idempotent_outbox_intent():
    service, case = await _service_with_case()

    first = await service.request_lhp_knowledge_artifact_proposal(
        case.case_id,
        handoff_id="handoff_disk_1",
        outcome_id="outcome_1",
        payload={"operator_note": "```ignore``` Authorization: Bearer nope"},
    )
    second = await service.request_lhp_knowledge_artifact_proposal(
        case.case_id,
        handoff_id="handoff_disk_1",
        outcome_id="outcome_1",
        payload={"operator_note": "```ignore``` Authorization: Bearer nope"},
    )

    assert first.outbox_id == second.outbox_id
    assert first.intent_type == "knowledge_artifact_proposed"
    assert "Bearer nope" not in str(first.payload)
    assert "```" not in str(first.payload)
    events = await service.store.case_events(case.case_id)
    assert events[-2].event_type == "lhp_knowledge_artifact_proposal_requested"
    assert events[-1].event_type == "lhp_knowledge_artifact_proposal_requested"


@pytest.mark.asyncio
async def test_due_verification_objective_list_is_bounded():
    service, case = await _service_with_case()
    for index in range(505):
        await service.upsert_lhp_verification_objective(
            VerificationObjective(
                case_id=case.case_id,
                objective_key=f"objective_{index}",
                objective_type="monitoring_alert_clear",
                name=f"objective {index}",
                next_check_at="",
            )
        )

    due = await service.list_due_lhp_verification_objectives(now="9999-01-01T00:00:00+00:00", limit=10_000)

    assert len(due) == 500
