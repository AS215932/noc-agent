import pytest

from app.cases.lhp import CaseHandoff, VerificationObjective, build_lhp_approval_scope, lhp_payload_hash, lhp_payload_size
from app.cases.models import AtomicCaseProjection
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore
from app.proactive.lhp import build_disk_handoff_request, disk_resource_parts, is_disk_handoff_hotspot
from app.proactive.models import Hotspot, HotspotEvidence


def test_disk_handoff_request_sanitizes_and_marks_untrusted_payload():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="rtr:/",
        category="disk",
        severity="HIGH",
        title="Disk / low ```ignore previous```",
        resource="rtr",
        summary="Authorization: Bearer secret-token <script>",
        evidence=[HotspotEvidence(label="free ratio", value="5%", detail="```raw```")],
        recommended_checks=["du -sh /var/log ```rm -rf```"],
        warrants_change=True,
        change_rationale="password=secret rotate logs",
    )
    case = AtomicCaseProjection(case_id="case_1", fingerprint=hotspot.fingerprint(), status="open")

    request = build_disk_handoff_request(
        hotspot,
        case,
        cycle_id="cycle_1",
        suppression_entry={"reason": "Authorization: Bearer nope", "operator": "svag<admin>", "expires_at": 123.0},
        knowledge_context_enabled=True,
    )

    rendered = str(request.handoff.model_dump(mode="json"))
    assert "Bearer secret-token" not in rendered
    assert "Bearer nope" not in rendered
    assert "```" not in rendered
    assert "<script>" not in rendered
    assert request.handoff.payload["hotspot"]["untrusted_evidence"] is True
    assert request.handoff.payload["suppression"]["untrusted_evidence"] is True
    assert request.knowledge_payload["untrusted_evidence"] is True
    assert request.objectives[0].required_consecutive_passes == 3


def test_disk_helpers_identify_disk_hotspots_and_resource_parts():
    disk = Hotspot(
        rule_id="disk_fill",
        key="host:/var",
        category="disk",
        severity="HIGH",
        resource="host",
        warrants_change=True,
    )
    monitor_only = Hotspot(
        rule_id="disk_fill",
        key="host:/",
        category="disk",
        severity="MEDIUM",
        resource="host",
        warrants_change=False,
    )
    other = Hotspot(rule_id="bgp_risk", key="rtr:p1", category="bgp", severity="HIGH", resource="rtr")

    assert is_disk_handoff_hotspot(disk) is True
    assert is_disk_handoff_hotspot(monitor_only) is False
    assert is_disk_handoff_hotspot(other) is False
    assert disk_resource_parts(disk) == ("host", "/var")


def test_disk_handoff_idempotency_is_stable_within_case_occurrence():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="host:/",
        category="disk",
        severity="HIGH",
        resource="host",
        warrants_change=True,
    )
    case = AtomicCaseProjection(
        case_id="case_occurrence",
        fingerprint=hotspot.fingerprint(),
        opened_at="2026-07-01T00:00:00+00:00",
    )

    first = build_disk_handoff_request(hotspot, case, cycle_id="cycle_1")
    second = build_disk_handoff_request(hotspot, case, cycle_id="cycle_2")
    reopened = build_disk_handoff_request(
        hotspot,
        case.model_copy(update={"resolved_at": "2026-07-02T00:00:00+00:00"}),
        cycle_id="cycle_3",
    )

    assert first.handoff.idempotency_key == second.handoff.idempotency_key
    assert first.handoff.idempotency_key != reopened.handoff.idempotency_key
    assert ":v2:" in first.handoff.idempotency_key
    assert {item.objective_key for item in first.objectives}.isdisjoint(
        {item.objective_key for item in reopened.objectives}
    )


@pytest.mark.asyncio
async def test_cancelled_disk_occurrence_can_create_a_new_handoff():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="host:/",
        category="disk",
        severity="HIGH",
        resource="host",
        warrants_change=True,
    )
    case = AtomicCaseProjection(
        case_id="case_cancelled_occurrence",
        fingerprint=hotspot.fingerprint(),
        opened_at="2026-07-01T00:00:00+00:00",
    )
    store = InMemoryCaseStore()
    await store.upsert_case(case)
    service = CaseService(store)

    first = build_disk_handoff_request(hotspot, case, cycle_id="cycle_1")
    first_result = await service.request_lhp_handoff(first.handoff, objectives=first.objectives)
    await service.cancel_lhp_handoff(
        first_result.handoff.handoff_id,
        actor_id="operator",
        reason="monitor-only occurrence",
        external_event_id="cancel_disk_occurrence_1",
    )
    current_case = await store.get_case(case.case_id)
    assert isinstance(current_case, AtomicCaseProjection)

    retry = build_disk_handoff_request(hotspot, current_case, cycle_id="cycle_2")
    retry_result = await service.request_lhp_handoff(retry.handoff, objectives=retry.objectives)

    assert retry.handoff.idempotency_key != first.handoff.idempotency_key
    assert retry_result.created is True
    assert retry_result.handoff.handoff_id != first_result.handoff.handoff_id
    assert {item.objective_key for item in retry.objectives}.isdisjoint(
        {item.objective_key for item in first.objectives}
    )


def test_approval_scope_hashes_large_payload_instead_of_copying_it():
    large_payload = {
        "occurrence_id": "occurrence_large",
        "evidence": {f"sample_{index}": "x" * 900 for index in range(40)},
    }
    handoff = CaseHandoff(
        case_id="case_large",
        target_loop="engineering",
        objective="resolve disk condition",
        objective_key="resolve-disk-v1",
        idempotency_key="case_large:engineering:resolve-disk-v1:occurrence_large",
        payload=large_payload,
    )

    scope = build_lhp_approval_scope(handoff, [])

    assert lhp_payload_size(handoff.model_dump(mode="json")) > 30_000
    assert lhp_payload_size(scope) < 5_000
    assert scope["handoff"]["payload_hash"] == lhp_payload_hash(handoff.payload)
    assert "evidence" not in scope["handoff"]["payload"]


def test_approval_scope_bounds_legacy_objectives_and_binds_the_complete_set():
    handoff = CaseHandoff(
        handoff_id="handoff_legacy_many",
        case_id="case_legacy_many",
        target_loop="engineering",
        objective="resolve disk condition",
        objective_key="resolve-disk-many-v1",
        idempotency_key="case_legacy_many:engineering:resolve-disk-many:v1",
    )
    objectives = [
        VerificationObjective(
            case_id=handoff.case_id,
            handoff_id=handoff.handoff_id,
            objective_key=f"legacy_{index:02d}",
            objective_type="health_endpoint",
            name=f"legacy objective {index}",
            payload={"endpoint": f"/health/{index}"},
        )
        for index in range(25)
    ]

    original = build_lhp_approval_scope(handoff, objectives)
    objectives[-1].payload = {"endpoint": "/health/changed"}
    changed = build_lhp_approval_scope(handoff, objectives)

    assert len(original["verification_objectives"]) == 20
    assert original["verification_objective_count"] == 25
    assert original["verification_objectives_hash"] != changed["verification_objectives_hash"]
