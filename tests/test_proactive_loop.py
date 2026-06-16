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
