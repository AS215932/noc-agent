import pytest

from app.config import ProactiveLoopSettings
from app.graph.routing import supervisor_route
from app.proactive.investigate import HeavyProbeFilteredRuntime, build_investigator
from app.proactive.models import Hotspot, hotspot_to_alert_payload


class _Tool:
    def __init__(self, name):
        self.name = name


def _hotspot(category="bgp", **kw):
    base = dict(
        rule_id="bgp_risk",
        key="rtr:peer1",
        category=category,
        severity="MEDIUM",
        score=240.0,
        title="BGP peer peer1 not Established",
        resource="rtr",
        summary="Peer peer1 on rtr is in state Active.",
        suggested_specialist="bgp",
    )
    base.update(kw)
    return Hotspot(**base)


# --- synthetic alert payload ---------------------------------------------


def test_payload_is_proactive_and_routes_to_bgp():
    payload = hotspot_to_alert_payload(_hotspot(category="bgp"))
    assert payload["source"] == "proactive"
    assert payload["status"] == "firing"
    # bgp keyword must steer the existing supervisor router to the bgp specialist
    routed = supervisor_route({"normalized_alert": payload})
    assert routed["active_specialist"] == "bgp"
    assert payload["proactive"]["fingerprint"]


def test_payload_infra_category_routes_to_infrastructure():
    payload = hotspot_to_alert_payload(
        _hotspot(
            category="disk",
            rule_id="disk_fill",
            title="Disk /var low",
            summary="log:/var has 5% free.",
            suggested_specialist="infrastructure",
        )
    )
    routed = supervisor_route({"normalized_alert": payload})
    assert routed["active_specialist"] == "infrastructure"


# --- heavy-probe filter ---------------------------------------------------


class _InnerRuntime:
    def __init__(self):
        self.calls = []

    def tools_for(self, specialist=None):
        return [_Tool("prometheus_query"), _Tool("tcpdump_capture"), _Tool("dns_probe_burst")]

    def toolsets_for(self, specialist=None):
        return ["inner-toolset"]

    async def call_tool(self, source, name, arguments):
        self.calls.append((source, name, arguments))
        return {"ok": True}


def test_filter_strips_heavy_tools_by_default():
    runtime = HeavyProbeFilteredRuntime(_InnerRuntime(), allow_heavy=False)
    names = {t.name for t in runtime.tools_for("bgp")}
    assert names == {"prometheus_query"}  # heavy probes removed


def test_filter_keeps_heavy_tools_when_allowed():
    runtime = HeavyProbeFilteredRuntime(_InnerRuntime(), allow_heavy=True)
    names = {t.name for t in runtime.tools_for("bgp")}
    assert "tcpdump_capture" in names
    assert runtime.toolsets_for("bgp") == ["inner-toolset"]


@pytest.mark.asyncio
async def test_filter_delegates_call_tool():
    inner = _InnerRuntime()
    runtime = HeavyProbeFilteredRuntime(inner, allow_heavy=False)
    await runtime.call_tool("hyrule", "prometheus_query", {"query": "up"})
    assert inner.calls == [("hyrule", "prometheus_query", {"query": "up"})]


# --- end-to-end investigator ---------------------------------------------


class _Intake:
    def __init__(self, should_investigate, case, action="opened"):
        self.should_investigate = should_investigate
        self.case = case
        self.action = action


@pytest.mark.asyncio
async def test_investigator_runs_graph_and_returns_outcome(monkeypatch):
    from app import graph_runtime
    import app.main as main

    captured = {}

    async def fake_intake(payload):
        captured["payload"] = payload
        return _Intake(True, {"incident_id": "INC-1"})

    async def fake_investigate(payload, model=None, case=None, *, mcp_runtime=None):
        captured["case"] = case
        captured["runtime"] = mcp_runtime

    monkeypatch.setattr(graph_runtime, "intake_alert", fake_intake)
    monkeypatch.setattr(main, "investigate_alert", fake_investigate)

    investigator = build_investigator(_InnerRuntime(), ProactiveLoopSettings(auto_heavy_probes=False))
    from app.proactive.models import DecisionContext

    outcome = await investigator(_hotspot(), DecisionContext())
    assert outcome is not None
    assert outcome.incident_id == "INC-1"
    assert captured["payload"]["source"] == "proactive"
    assert isinstance(captured["runtime"], HeavyProbeFilteredRuntime)


@pytest.mark.asyncio
async def test_investigator_skips_when_deduped(monkeypatch):
    from app import graph_runtime
    import app.main as main

    async def fake_intake(payload):
        return _Intake(False, None, action="suppressed_active")

    async def fail_investigate(*a, **k):  # pragma: no cover - must not be called
        raise AssertionError("should not investigate a deduped hotspot")

    monkeypatch.setattr(graph_runtime, "intake_alert", fake_intake)
    monkeypatch.setattr(main, "investigate_alert", fail_investigate)

    investigator = build_investigator(_InnerRuntime(), ProactiveLoopSettings())
    from app.proactive.models import DecisionContext

    assert await investigator(_hotspot(), DecisionContext()) is None
