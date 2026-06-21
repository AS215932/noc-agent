import pytest

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord
from app.proactive.memory import MemoryPolicyEngine, ProactiveMemory, RealAlert, classify_observation
from app.proactive.models import CandidateLesson, Observation, utc_now


def test_policy_only_injects_validated_or_approved():
    engine = MemoryPolicyEngine()
    assert engine.can_inject("approved_policy")
    assert engine.can_inject("validated_advisory")
    assert not engine.can_inject("candidate")
    assert not engine.can_inject("deprecated")


def test_active_lessons_reads_only_lessons_dir(tmp_path):
    mem = ProactiveMemory(tmp_path)
    mem.ensure()
    (mem.lessons_dir / "bgp.md").write_text("Peer X needs a longer hold timer.")
    # A candidate proposal must NOT be injected.
    mem.propose_lesson(CandidateLesson(lesson_type="runbook", scope={"x": 1}, claim="do not inject me"))
    lessons = mem.active_lessons()
    assert lessons == ["Peer X needs a longer hold timer."]


def test_propose_lesson_dedupes_and_bumps_occurrences(tmp_path):
    mem = ProactiveMemory(tmp_path)
    a = mem.propose_lesson(CandidateLesson(lesson_type="scan_tuning", scope={"rule_id": "disk_fill"}, claim="c", evidence=["e1"]))
    b = mem.propose_lesson(CandidateLesson(lesson_type="scan_tuning", scope={"rule_id": "disk_fill"}, claim="c", evidence=["e2"]))
    assert a.occurrences == 1
    assert b.occurrences == 2
    assert set(b.evidence) == {"e1", "e2"}
    assert len(mem.proposals()) == 1  # one file, merged


def test_record_and_load_observations(tmp_path):
    mem = ProactiveMemory(tmp_path)
    mem.record_observation(Observation(rule_id="bgp_risk", resource="rtr", severity="MEDIUM"))
    rows = mem.load_observations()
    assert len(rows) == 1 and rows[0]["rule_id"] == "bgp_risk"


def test_classify_observation_confirmed_unconfirmed_pending():
    import time

    now = time.time()
    obs = {"created_at": _iso(now - 3 * 3600), "resource": "rtr"}  # 3h old
    real = [RealAlert(resource="rtr", ts=now - 2 * 3600, source="alertmanager")]
    assert classify_observation(obs, real, now, window_h=6, min_age_h=2) == "confirmed"

    # No matching real alert, window closed → unconfirmed
    assert classify_observation(obs, [], now, window_h=2, min_age_h=2) == "unconfirmed"

    # Too fresh → pending
    fresh = {"created_at": _iso(now - 600), "resource": "rtr"}
    assert classify_observation(fresh, real, now, window_h=6, min_age_h=2) == "pending"

    # A proactive "alert" must not count as confirmation (window closed → unconfirmed)
    proactive_only = [RealAlert(resource="rtr", ts=now - 2 * 3600, source="proactive")]
    assert classify_observation(obs, proactive_only, now, window_h=2, min_age_h=2) == "unconfirmed"


class _FakeCaseSource:
    def __init__(self, cases):
        self._cases = cases

    async def list_cases(self):
        return self._cases


@pytest.mark.asyncio
async def test_evaluate_outcomes_proposes_lesson_after_threshold(tmp_path):
    import time

    now = time.time()
    mem = ProactiveMemory(tmp_path)
    # Two predictions from the same rule, each followed by a real alert.
    for resource in ("rtr", "cr1-nl1"):
        mem.record_observation(
            Observation(rule_id="bgp_risk", resource=resource, severity="MEDIUM", created_at=_iso(now - 4 * 3600))
        )
    cases = [
        {"resource_id": "rtr", "updated_at": _iso(now - 3 * 3600), "latest_event": {"source": "alertmanager", "received_at": _iso(now - 3 * 3600)}},
        {"resource_id": "cr1-nl1", "updated_at": _iso(now - 3 * 3600), "latest_event": {"source": "icinga2", "received_at": _iso(now - 3 * 3600)}},
    ]
    proposed = await mem.evaluate_outcomes(_FakeCaseSource(cases), now=now, window_h=6, min_age_h=2, confirm_threshold=2)
    assert len(proposed) == 1
    assert proposed[0].scope == {"rule_id": "bgp_risk"}
    assert proposed[0].occurrences == 2
    # Observations are marked confirmed (idempotent on re-run).
    assert all(row.get("outcome") == "confirmed" for row in mem.load_observations())


@pytest.mark.asyncio
async def test_evaluate_outcomes_reads_case_service_cases(tmp_path):
    import time

    now = time.time()
    mem = ProactiveMemory(tmp_path)
    mem.record_observation(
        Observation(rule_id="bgp_risk", resource="rtr", severity="MEDIUM", created_at=_iso(now - 4 * 3600))
    )
    service = CaseService(InMemoryCaseStore())
    await service.observe(
        ObservationRecord(
            source="alertmanager",
            detector="BGPDown",
            resource="rtr",
            status="firing",
            severity="HIGH",
            observed_at=_iso(now - 3 * 3600),
        )
    )

    proposed = await mem.evaluate_outcomes(service, now=now, window_h=6, min_age_h=2, confirm_threshold=1)

    assert len(proposed) == 1
    assert proposed[0].scope == {"rule_id": "bgp_risk"}
    assert mem.load_observations()[0]["outcome"] == "confirmed"


def _iso(ts: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, timezone.utc).isoformat()
