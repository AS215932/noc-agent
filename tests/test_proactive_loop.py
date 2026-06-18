from dataclasses import replace

import pytest

from app.config import ProactiveLoopSettings
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


def _settings(tmp_path, **overrides) -> ProactiveLoopSettings:
    base = ProactiveLoopSettings(
        enabled=True,
        shadow=True,
        severity_floor="MEDIUM",
        state_dir=str(tmp_path),
        memory_dir=str(tmp_path / "memory"),
    )
    return replace(base, **overrides)


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
async def test_deep_cycle_records_observations_and_journal(tmp_path):
    from app.proactive.memory import ProactiveMemory

    mem = ProactiveMemory(tmp_path / "memory")
    mem.ensure()

    class _IM:
        async def list_cases(self):
            return []

    lp = ProactiveLoop(
        _runtime(),
        settings=_settings(tmp_path, shadow=True),
        reporter=_Capture(),
        model_chain=lambda: ["m"],
        incident_memory=_IM(),
        memory=mem,
    )
    report = await lp.run_once(deep=True)
    assert report.hotspots
    # one observation per hotspot, and a journal entry for the cycle
    assert len(mem.load_observations()) == len(report.hotspots)
    assert (mem.journal_dir / f"{report.cycle_id}.md").exists()
