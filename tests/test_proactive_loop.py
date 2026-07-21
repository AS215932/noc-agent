from dataclasses import replace

import pytest

from app.cases import CaseService, InMemoryCaseStore
from app.config import LoopHandoffSettings, ProactiveLoopSettings
from app.proactive import loop as loop_module
from app.proactive.loop import InvestigationOutcome, ProactiveLoop
from app.proactive.ledger import load_ledger, update_ledger


def _vector(*rows):
    return {"ok": True, "result": [{"metric": m, "value": v} for m, v in rows]}


class FakeMCPRuntime:
    def __init__(self, by_query):
        self.by_query = by_query

    async def call_tool(self, source, name, arguments):
        query = str(arguments.get("query", ""))
        for needle, response in self.by_query.items():
            if needle in query:
                return response
        return {"ok": True, "result": []}


def _runtime():
    return FakeMCPRuntime(
        {
            'state!="Established"': _vector(
                ({"instance": "[2a0c:b641:b50::a]:9342", "peer": "p1", "state": "Active"}, "1"),
            ),
            "node_filesystem_size_bytes": _vector(({"instance": "log:9100", "mountpoint": "/var"}, "0.05")),
            "predict_linear": _vector(({"instance": "log:9100", "mountpoint": "/var"}, "-1")),
        }
    )


def _monitor_only_disk_runtime():
    return FakeMCPRuntime(
        {
            'state!="Established"': {"ok": True, "result": []},
            "node_filesystem_size_bytes": _vector(({"instance": "log:9100", "mountpoint": "/var"}, "0.197")),
            "predict_linear": {"ok": True, "result": []},
        }
    )


def _settings(tmp_path, **overrides) -> ProactiveLoopSettings:
    base = ProactiveLoopSettings(
        enabled=True,
        shadow=True,
        severity_floor="MEDIUM",
        state_dir=str(tmp_path),
        memory_dir=str(tmp_path / "memory"),
    )
    return replace(base, **overrides)


def _disk_fingerprint() -> str:
    import hashlib

    return hashlib.sha256("disk_fill|log:/var".encode()).hexdigest()[:16]


class _Capture:
    def __init__(self):
        self.calls = []

    async def __call__(self, report, gate):
        self.calls.append((report, gate))


@pytest.mark.asyncio
async def test_disabled_loop_reports_disabled(tmp_path):
    lp = ProactiveLoop(_runtime(), settings=_settings(tmp_path, enabled=False), model_chain=lambda: ["m"])
    report = await lp.run_once()
    assert report.outcome == "disabled"


@pytest.mark.asyncio
async def test_shadow_cycle_scans_reports_but_does_not_investigate(tmp_path):
    cap = _Capture()
    called = []

    async def investigator(hotspot, decision):
        called.append(hotspot.key)
        return InvestigationOutcome(incident_id="x")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=cap,
        investigator=investigator,
        model_chain=lambda: ["m"],
    )
    report = await lp.run_once(deep=True)
    assert report.outcome == "scanned"
    assert report.hotspots  # disk + bgp found
    assert called == []  # shadow → no investigation despite an investigator
    assert cap.calls and cap.calls[0][1].max_investigations == 0
    assert load_ledger(tmp_path)["cycles"] == 1
    assert load_ledger(tmp_path)["investigations"] == 0


@pytest.mark.asyncio
async def test_autonomous_cycle_investigates_top_hotspot(tmp_path):
    seen = []

    async def investigator(hotspot, decision):
        seen.append(hotspot)
        return InvestigationOutcome(incident_id="inc1", cost_usd=0.5, handoff_url="https://gh/issue/1")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=False, max_investigations_per_cycle=1),
        reporter=_Capture(),
        investigator=investigator,
        model_chain=lambda: ["m"],
    )
    report = await lp.run_once(deep=True)
    assert report.outcome == "investigated"
    assert len(seen) == 1
    assert seen[0].severity == "HIGH"  # disk HIGH outranks bgp MEDIUM
    assert report.cost_usd == 0.5
    assert report.handoffs == ["https://gh/issue/1"]
    led = load_ledger(tmp_path)
    assert led["investigations"] == 1 and led["handoffs"] == 1


@pytest.mark.asyncio
async def test_lhp_disk_handoff_is_disabled_by_default(tmp_path):
    service = CaseService(InMemoryCaseStore())
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=service,
        loop_handoff_settings=LoopHandoffSettings(enabled=True, disk_alert_handoff_enabled=False),
    )

    await lp.run_once(deep=True)

    assert await service.list_lhp_handoffs() == []


@pytest.mark.asyncio
async def test_lhp_disk_handoff_is_created_once_for_fresh_disk_hotspot(tmp_path):
    service = CaseService(InMemoryCaseStore())
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=service,
        loop_handoff_settings=LoopHandoffSettings(enabled=True, disk_alert_handoff_enabled=True),
    )

    await lp.run_once(deep=True)
    await lp.run_once(deep=True)

    handoffs = await service.list_lhp_handoffs()
    assert len(handoffs) == 1
    handoff = handoffs[0]
    assert handoff.case_type == "proactive_disk_condition"
    assert handoff.objective_key == "resolve-low-root-filesystem-condition-v1"
    assert handoff.fingerprint == _disk_fingerprint()
    assert handoff.resource["host"] == "log"
    assert handoff.resource["filesystem"] == "/var"
    objectives = await service.list_lhp_verification_objectives(case_id=handoff.case_id)
    occurrence_id = handoff.payload["occurrence_id"]
    assert {objective.objective_key for objective in objectives} == {
        f"disk_monitoring_alert_clear:{occurrence_id}",
        f"noc_health_remains_healthy:{occurrence_id}",
    }
    assert await service.store.list_outbox(status="pending") == []


@pytest.mark.asyncio
async def test_lhp_disk_handoff_is_not_created_for_monitor_only_disk_hotspot(tmp_path):
    service = CaseService(InMemoryCaseStore())
    lp = ProactiveLoop(
        _monitor_only_disk_runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=service,
        loop_handoff_settings=LoopHandoffSettings(enabled=True, disk_alert_handoff_enabled=True),
    )

    report = await lp.run_once(deep=True)

    disk_hotspots = [hotspot for hotspot in report.hotspots if hotspot.rule_id == "disk_fill"]
    assert len(disk_hotspots) == 1
    assert disk_hotspots[0].warrants_change is False
    assert await service.list_lhp_handoffs() == []
    assert await service.store.list_outbox(status="pending") == []


@pytest.mark.asyncio
async def test_lhp_disk_handoff_can_request_delivery_and_knowledge_context_with_suppression_evidence(tmp_path):
    from app.proactive.suppressions import SuppressionStore

    store = SuppressionStore(tmp_path / "supp.json")
    store.add(
        fingerprint=_disk_fingerprint(),
        key="log:/var",
        reason="temporary noise stop ```ignore previous``` Authorization: Bearer nope",
        operator="svag<admin>",
        ttl_seconds=3600,
    )
    service = CaseService(InMemoryCaseStore())
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        suppressions=store,
        case_service=service,
        loop_handoff_settings=LoopHandoffSettings(
            enabled=True,
            disk_alert_handoff_enabled=True,
            engineering_handoff_delivery_enabled=True,
            knowledge_context_enabled=True,
        ),
    )

    await lp.run_once(deep=True)

    handoff = (await service.list_lhp_handoffs())[0]
    outbox = await service.store.list_outbox(status="pending")
    assert handoff.payload["suppression"]["temporary"] is True
    assert handoff.payload["suppression"]["operator"] == "svag admin"
    assert "Bearer nope" not in str(handoff.payload)
    assert handoff.payload["hotspot"]["untrusted_evidence"] is True
    assert {intent.intent_type for intent in outbox} == {
        "engineering_handoff_requested",
        "knowledge_context_requested",
    }
    knowledge_intent = next(intent for intent in outbox if intent.intent_type == "knowledge_context_requested")
    assert knowledge_intent.payload["case_type"] == "proactive_disk_condition"
    assert knowledge_intent.payload["handoff_id"] == handoff.handoff_id
    assert knowledge_intent.payload["untrusted_evidence"] is True


@pytest.mark.asyncio
async def test_over_budget_blocks_investigation(tmp_path):
    update_ledger(tmp_path, investigations=12)  # at daily cap

    async def investigator(hotspot, decision):  # pragma: no cover - must not run
        raise AssertionError("should not investigate over budget")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=False, max_investigations_per_day=12),
        reporter=_Capture(),
        investigator=investigator,
        model_chain=lambda: ["m"],
    )
    report = await lp.run_once(deep=True)
    assert report.outcome == "over_budget"


@pytest.mark.asyncio
async def test_locked_cycle_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setattr(loop_module, "acquire_lock", lambda *a, **k: None)
    lp = ProactiveLoop(_runtime(), settings=_settings(tmp_path), model_chain=lambda: ["m"])
    report = await lp.run_once()
    assert report.outcome == "locked"


@pytest.mark.asyncio
async def test_unchanged_hotspots_reported_once(tmp_path):
    cap = _Capture()
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=cap,
        model_chain=lambda: ["m"],
    )
    for _ in range(3):
        await lp.run_once(deep=True)  # identical hotspot set each cycle
    assert len(cap.calls) == 1  # posted once; repeats suppressed


@pytest.mark.asyncio
async def test_changed_hotspots_repost(tmp_path):
    cap = _Capture()
    runtime = _runtime()
    lp = ProactiveLoop(
        runtime,
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=cap,
        model_chain=lambda: ["m"],
    )
    await lp.run_once(deep=True)
    # bgp peer recovers → hotspot set changes
    runtime.by_query['state!="Established"'] = {"ok": True, "result": []}
    await lp.run_once(deep=True)
    await lp.run_once(deep=True)  # now stable again → suppressed
    assert len(cap.calls) == 2


@pytest.mark.asyncio
async def test_acked_hotspot_is_suppressed(tmp_path):
    from app.proactive.suppressions import SuppressionStore

    store = SuppressionStore(tmp_path / "supp.json")
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        suppressions=store,
    )
    r1 = await lp.run_once(deep=True)
    assert len(r1.hotspots) >= 1
    target = r1.hotspots[0]
    store.add(fingerprint=target.fingerprint(), key=target.key, operator="svag")
    r2 = await lp.run_once(deep=True)
    assert target.fingerprint() not in {h.fingerprint() for h in r2.hotspots}
    assert len(r2.hotspots) == len(r1.hotspots) - 1


@pytest.mark.asyncio
async def test_ack_by_short_prefix_suppresses(tmp_path):
    # The digest shows a 12-char ack id; acking by that prefix must suppress the
    # full-fingerprint hotspot (git short-SHA style).
    from app.proactive.suppressions import SuppressionStore

    store = SuppressionStore(tmp_path / "supp.json")
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        suppressions=store,
    )
    r1 = await lp.run_once(deep=True)
    target = r1.hotspots[0]
    store.add(fingerprint=target.fingerprint()[:12], key=target.key)  # short id, as shown
    r2 = await lp.run_once(deep=True)
    assert target.fingerprint() not in {h.fingerprint() for h in r2.hotspots}


@pytest.mark.asyncio
async def test_suppression_pruned_when_resolved(tmp_path):
    from app.proactive.suppressions import SuppressionStore

    store = SuppressionStore(tmp_path / "supp.json")
    store.add(fingerprint="deadbeef", key="not-firing")  # never appears in scan
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        suppressions=store,
    )
    await lp.run_once(deep=True)
    assert "deadbeef" not in store.active()  # resolved → pruned so it can re-alert later


def test_hotspot_field_shows_ack_id_and_case_link():
    from app.proactive.loop import _hotspot_field
    from app.proactive.models import Hotspot

    h = Hotspot(rule_id="disk_fill", key="rtr:/", severity="MEDIUM", title="Disk / low", summary="15% free", resource="rtr")
    field = _hotspot_field(h, case={"case_number": "NOC-20260617-007"}, public_url="https://noc.servify.network")
    assert f"ack id: `{h.fingerprint()[:12]}`" in field["value"]
    assert "[NOC-20260617-007](https://noc.servify.network/control/cases/NOC-20260617-007)" in field["value"]


@pytest.mark.asyncio
async def test_all_clear_posts_once(tmp_path):
    cap = _Capture()
    runtime = _runtime()
    lp = ProactiveLoop(
        runtime,
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=cap,
        model_chain=lambda: ["m"],
    )
    await lp.run_once(deep=True)  # hotspots present → post
    for needle in ('state!="Established"', "node_filesystem_size_bytes", "predict_linear"):
        runtime.by_query[needle] = {"ok": True, "result": []}  # everything resolves
    await lp.run_once(deep=True)  # all-clear → post once
    await lp.run_once(deep=True)  # still empty → silent
    assert len(cap.calls) == 2
    assert cap.calls[1][0].hotspots == []  # second call is the all-clear


@pytest.mark.asyncio
async def test_report_failure_is_retried_next_cycle(tmp_path):
    calls = []

    async def flaky(report, gate):
        calls.append(report)
        if len(calls) == 1:
            raise RuntimeError("discord down")  # first send fails

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=flaky,
        model_chain=lambda: ["m"],
    )
    await lp.run_once(deep=True)  # raises → de-dup state NOT committed
    await lp.run_once(deep=True)  # same set → retried (not suppressed), succeeds
    await lp.run_once(deep=True)  # now committed → suppressed
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_investigation_cooldown_skips_reinvestigation(tmp_path):
    # A persistent hotspot is diagnosed once; the next cycle it's on cooldown, so
    # the loop spends its budget on the *other* hotspot instead of re-running the
    # same one (which would force a duplicate digest post every cycle).
    seen = []

    async def investigator(hotspot, decision):
        seen.append(hotspot.fingerprint())
        return InvestigationOutcome(incident_id="inc")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(
            tmp_path, shadow=False, max_investigations_per_cycle=1, investigation_cooldown_s=3600
        ),
        reporter=_Capture(),
        investigator=investigator,
        model_chain=lambda: ["m"],
    )
    await lp.run_once(deep=True)  # investigates the top (disk HIGH)
    await lp.run_once(deep=True)  # disk on cooldown → investigates the other (bgp)
    r3 = await lp.run_once(deep=True)  # both on cooldown → nothing
    assert len(seen) == 2 and len(set(seen)) == 2  # each fingerprint investigated once
    assert r3.outcome == "scanned" and not r3.investigated


@pytest.mark.asyncio
async def test_timed_snooze_survives_flap_untimed_pruned(tmp_path):
    # A flapping hotspot (disk hovering at its threshold) sheds an untimed ack on
    # every brief all-clear and re-alerts; a *timed* snooze must persist instead.
    from app.proactive.suppressions import SuppressionStore

    store = SuppressionStore(tmp_path / "supp.json")
    runtime = _runtime()
    lp = ProactiveLoop(
        runtime,
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        suppressions=store,
    )
    r1 = await lp.run_once(deep=True)
    timed = r1.hotspots[0]
    store.add(fingerprint=timed.fingerprint(), key=timed.key, ttl_seconds=3600)  # snooze
    store.add(fingerprint="deadbeef", key="untimed")  # untimed ack, never firing
    # everything resolves (the flap to all-clear)
    for needle in ('state!="Established"', "node_filesystem_size_bytes", "predict_linear"):
        runtime.by_query[needle] = {"ok": True, "result": []}
    await lp.run_once(deep=True)
    active = store.active()
    assert timed.fingerprint() in active  # timed snooze survives the all-clear
    assert "deadbeef" not in active  # untimed ack pruned once it stops firing


@pytest.mark.asyncio
async def test_cheap_cycle_does_not_resolve_deep_hotspot(tmp_path):
    # The disk rule is deep-only; a cheap cycle that doesn't scan it must NOT
    # post a false all-clear — the deep hotspot is carried forward.
    cap = _Capture()
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=cap,
        model_chain=lambda: ["m"],
    )
    r1 = await lp.run_once(deep=True)  # deep: disk + bgp fire → post
    assert any(h.rule_id == "disk_fill" for h in r1.hotspots)
    r2 = await lp.run_once(deep=False)  # cheap-only: disk not scanned
    assert any(h.rule_id == "disk_fill" for h in r2.hotspots)  # carried, not resolved
    assert len(cap.calls) == 1  # unchanged set → no re-post, no all-clear


@pytest.mark.asyncio
async def test_degraded_scan_does_not_resolve(tmp_path):
    # A query failure (degraded) must not resolve anything → no false all-clear.
    cap = _Capture()
    runtime = _runtime()
    lp = ProactiveLoop(
        runtime,
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=cap,
        model_chain=lambda: ["m"],
    )
    r1 = await lp.run_once(deep=True)
    n1 = len(r1.hotspots)
    assert n1 >= 1

    class _Boom:
        async def call_tool(self, *a, **k):
            raise RuntimeError("prometheus down")

    lp.mcp_runtime = _Boom()
    r2 = await lp.run_once(deep=True)  # every query fails → degraded
    fps = {h.fingerprint() for h in r1.hotspots}
    assert {h.fingerprint() for h in r2.hotspots} >= fps
    r3 = await lp.run_once(deep=True)  # still degraded → merged set persisted, nothing erodes
    assert {h.fingerprint() for h in r3.hotspots} >= fps
    assert len(cap.calls) == 1  # nothing resolved across the streak → no all-clear


@pytest.mark.asyncio
async def test_case_service_shadow_observes_raw_not_carried_hotspots(tmp_path):
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=case_service,
    )

    deep_report = await lp.run_once(deep=True)
    disk = next(h for h in deep_report.hotspots if h.rule_id == "disk_fill")
    disk_case_id = await store.resolve_alias("source_fp", disk.fingerprint())
    assert disk_case_id is not None
    assert [event.event_type for event in await store.case_events(disk_case_id)] == [
        "case_created",
        "case_observed_unhealthy",
    ]

    cheap_report = await lp.run_once(deep=False)
    assert any(h.fingerprint() == disk.fingerprint() for h in cheap_report.hotspots)  # carried operationally
    # But the carried disk hotspot was not scanned in the cheap cycle, so it is
    # not recorded as a fresh unhealthy observation in the case-service shadow.
    assert [event.event_type for event in await store.case_events(disk_case_id)] == [
        "case_created",
        "case_observed_unhealthy",
    ]


@pytest.mark.asyncio
async def test_case_service_shadow_failure_is_nonfatal(tmp_path):
    class _BrokenCaseService:
        async def observe(self, observation):
            raise RuntimeError("shadow store unavailable")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=_BrokenCaseService(),
    )

    report = await lp.run_once(deep=True)

    assert report.outcome == "scanned"
    assert report.hotspots


@pytest.mark.asyncio
async def test_case_service_primary_observation_failure_fails_cycle(tmp_path):
    class _BrokenCaseService:
        async def observe(self, observation):
            raise RuntimeError("primary store unavailable")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=_BrokenCaseService(),
        case_service_primary=True,
    )

    report = await lp.run_once(deep=True)

    assert report.outcome == "error"
    assert report.errors


@pytest.mark.asyncio
async def test_deep_cycle_records_observations_and_journal(tmp_path):
    from app.proactive.memory import ProactiveMemory

    mem = ProactiveMemory(tmp_path / "memory")
    mem.ensure()

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        memory=mem,
    )
    report = await lp.run_once(deep=True)
    assert report.hotspots
    # one observation per hotspot, and a journal entry for the cycle
    assert len(mem.load_observations()) == len(report.hotspots)
    assert (mem.journal_dir / f"{report.cycle_id}.md").exists()


@pytest.mark.asyncio
async def test_case_service_primary_replaces_investigations_json_for_cooldown(tmp_path):
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    seen = []

    async def investigator(hotspot, decision):
        seen.append(hotspot.fingerprint())
        return InvestigationOutcome(incident_id="inc1")

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=False, max_investigations_per_cycle=2, investigation_cooldown_s=3600),
        reporter=_Capture(),
        investigator=investigator,
        model_chain=lambda: ["m"],
        case_service=case_service,
        case_service_primary=True,
    )

    await lp.run_once(deep=True)  # both current hotspots investigated and stamped on their cases
    await lp.run_once(deep=True)  # unchanged signatures are skipped by case state

    assert len(seen) == 2
    assert not (tmp_path / "investigations.json").exists()
    for fingerprint in seen:
        case_id = await store.resolve_alias("source_fp", fingerprint)
        assert case_id is not None
        case = await store.get_case(case_id)
        assert case is not None
        assert getattr(case, "last_investigated_at")


@pytest.mark.asyncio
async def test_case_service_primary_marks_cases_reported_after_successful_digest(tmp_path):
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True, report_reassert_s=99999),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        case_service=case_service,
        case_service_primary=True,
    )

    report = await lp.run_once(deep=True)

    assert report.hotspots
    for hotspot in report.hotspots:
        case_id = await store.resolve_alias("source_fp", hotspot.fingerprint())
        assert case_id is not None
        case = await store.get_case(case_id)
        assert case is not None
        assert getattr(case, "last_reported_at")
        assert getattr(case, "last_reported_signature")
