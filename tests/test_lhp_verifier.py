import pytest

from app.cases.lhp import CaseHandoff, VerificationObjective
from app.cases.models import AtomicCaseProjection
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore
from app.cases.verifier import CaseVerifier, VerificationCheckResult
from app.config import LoopHandoffSettings


async def _service_with_handoff(required_passes: int = 2) -> tuple[CaseService, CaseHandoff]:
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(
        case_id="case_verify_1",
        fingerprint="fp_disk",
        status="verification_pending",
        title="disk low",
    )
    await store.upsert_case(case)
    service = CaseService(store)
    handoff = CaseHandoff(
        handoff_id="handoff_verify_1",
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_verify_1:engineering:resolve-low-root-filesystem-condition-v1:v1",
        case_type="proactive_disk_condition",
        fingerprint=case.fingerprint,
    )
    await service.request_lhp_handoff(
        handoff,
        objectives=[
            VerificationObjective(
                case_id=case.case_id,
                handoff_id=handoff.handoff_id,
                objective_key="disk_clear",
                objective_type="monitoring_alert_clear",
                name="disk alert clears",
                required_consecutive_passes=required_passes,
                next_check_at="",
            ),
            VerificationObjective(
                case_id=case.case_id,
                handoff_id=handoff.handoff_id,
                objective_key="health_ok",
                objective_type="health_endpoint",
                name="health ok",
                required_consecutive_passes=required_passes,
                next_check_at="",
            ),
        ],
    )
    for status in ("accepted", "in_progress", "implemented"):
        from app.cases.lhp import HandoffUpdate

        await service.record_lhp_handoff_update(
            HandoffUpdate(
                case_id=case.case_id,
                handoff_id=handoff.handoff_id,
                source_loop="engineering",
                update_type="implemented" if status == "implemented" else "investigating",
                status=status,
                external_event_id=f"eng_{status}",
                correlation_id=handoff.correlation_id,
            )
        )
    return service, handoff


@pytest.mark.asyncio
async def test_verifier_dry_run_evaluates_without_mutating_objectives():
    service, handoff = await _service_with_handoff()

    async def checker(objective, case):
        return VerificationCheckResult(status="pass", evidence_ref=f"test:{objective.objective_key}")

    verifier = CaseVerifier(
        service,
        settings=LoopHandoffSettings(enabled=True, case_verification_enabled=True, case_verification_dry_run=True),
        checker=checker,
    )
    report = await verifier.run_once(now="2026-06-22T20:00:00+00:00")

    assert report.checked == 2
    assert report.passed == 2
    assert report.updated == 0
    objectives = await service.list_lhp_verification_objectives(case_id=handoff.case_id)
    assert {objective.consecutive_pass_count for objective in objectives} == {0}


@pytest.mark.asyncio
async def test_verifier_marks_handoff_verified_after_required_consecutive_passes():
    service, handoff = await _service_with_handoff(required_passes=2)

    async def checker(objective, case):
        return VerificationCheckResult(
            status="pass",
            evidence_ref=f"test:{objective.objective_key}",
            payload={"note": "```ignore previous``` Authorization: Bearer nope"},
        )

    verifier = CaseVerifier(
        service,
        settings=LoopHandoffSettings(enabled=True, case_verification_enabled=True, case_verification_dry_run=False),
        checker=checker,
    )

    first = await verifier.run_once(now="2026-06-22T20:00:00+00:00")
    second = await verifier.run_once(now="2026-06-22T20:03:00+00:00")
    stored = await service.get_lhp_handoff(handoff.handoff_id)

    assert first.verified_handoffs == 0
    assert second.verified_handoffs == 1
    assert stored is not None
    assert stored.status == "verified"
    objectives = await service.list_lhp_verification_objectives(case_id=handoff.case_id)
    assert "Bearer nope" not in str([objective.payload for objective in objectives])
    assert "```" not in str([objective.payload for objective in objectives])
    assert "Bearer nope" not in str([objective.result_payload for objective in objectives])
    assert "```" not in str([objective.result_payload for objective in objectives])


@pytest.mark.asyncio
async def test_verifier_can_auto_resolve_when_enabled():
    service, handoff = await _service_with_handoff(required_passes=1)

    async def checker(objective, case):
        return VerificationCheckResult(status="pass", evidence_ref=f"test:{objective.objective_key}")

    verifier = CaseVerifier(
        service,
        settings=LoopHandoffSettings(
            enabled=True,
            case_verification_enabled=True,
            case_verification_dry_run=False,
            case_auto_resolve_enabled=True,
            knowledge_context_enabled=True,
        ),
        checker=checker,
    )
    report = await verifier.run_once(now="2026-06-22T20:00:00+00:00")
    case = await service.store.get_case(handoff.case_id)
    stored = await service.get_lhp_handoff(handoff.handoff_id)
    outcomes = await service.list_lhp_outcomes(case_id=handoff.case_id)
    outbox = await service.store.list_outbox()

    assert report.verified_handoffs == 1
    assert report.resolved_cases == 1
    assert case is not None and getattr(case, "status") == "resolved"
    assert stored is not None and stored.status == "resolved"
    assert len(outcomes) == 1
    assert [intent.intent_type for intent in outbox] == ["knowledge_artifact_proposed"]
    assert outbox[0].payload["outcome_id"] == outcomes[0].outcome_id


@pytest.mark.asyncio
async def test_default_verifier_uses_positive_clean_observation_for_monitoring_clear():
    service, handoff = await _service_with_handoff(required_passes=1)
    case = await service.store.get_case(handoff.case_id)
    assert isinstance(case, AtomicCaseProjection)
    case.last_observed_unhealthy = "2026-06-22T21:30:00+02:00"
    case.last_observed_clean = "2026-06-22T20:00:00+00:00"
    await service.store.upsert_case(case)

    verifier = CaseVerifier(
        service,
        settings=LoopHandoffSettings(enabled=True, case_verification_enabled=True, case_verification_dry_run=False),
    )
    report = await verifier.run_once(now="2026-06-22T20:01:00+00:00")

    assert report.passed == 2
    assert report.verified_handoffs == 1
