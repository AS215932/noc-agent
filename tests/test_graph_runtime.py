import pytest
from pydantic_ai.models.test import TestModel

import app.graph_runtime as graph_runtime
from app.incident_memory import IncidentMemory


@pytest.mark.asyncio
async def test_graph_routes_bgp_alert_and_creates_pending_summary(monkeypatch):
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", IncidentMemory(redis_url=""))
    monkeypatch.setattr(graph_runtime, "_GRAPH", None)
    alert = {
        "status": "firing",
        "groupLabels": {"alertname": "BGP Peer Down", "host": "rtr"},
        "commonLabels": {"severity": "critical"},
        "alerts": [{"labels": {"alertname": "BGP Peer Down", "host": "rtr"}}],
    }

    plan, state = await graph_runtime.run_investigation_graph(alert, model=TestModel())

    assert plan.incident_summary is not None
    assert "diagnostic_synthesis" in state
    assert "action_plan" not in state
    assert state["active_specialist"] == "bgp"
    assert state["approval_state"] == "waiting_approval"
    assert "model_override" not in state
    assert "perimeter_context" not in state
    assert state["perimeter_context_version"]
    assert state["manifest_hash"]
    summary = await graph_runtime.summary_for(state["incident_id"])
    assert summary is not None
    assert summary["status"] == "waiting_approval"
    assert summary["thread_id"] == state["thread_id"]


@pytest.mark.asyncio
async def test_graph_resume_uses_stable_thread_id(monkeypatch):
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", IncidentMemory(redis_url=""))
    monkeypatch.setattr(graph_runtime, "_GRAPH", None)
    alert = {
        "status": "firing",
        "groupLabels": {"alertname": "BGP Peer Down", "host": "rtr"},
        "commonLabels": {"severity": "critical"},
        "alerts": [{"labels": {"alertname": "BGP Peer Down", "host": "rtr"}}],
    }

    _plan, state = await graph_runtime.run_investigation_graph(alert, model=TestModel())
    resumed = await graph_runtime.resume_investigation(
        state["incident_id"],
        {"decision": "acknowledged", "operator": "pytest", "comment": "ok"},
    )

    assert resumed is not None
    assert resumed["operator_decision"]["operator"] == "pytest"


@pytest.mark.asyncio
async def test_incident_memory_marks_chronic_after_four_distinct_events(monkeypatch):
    monkeypatch.setenv("NOC_ACTIVE_INCIDENT_SUPPRESSION_SECONDS", "0")
    memory = IncidentMemory(redis_url="")
    alert = {"groupLabels": {"alertname": "InstanceDown", "host": "noc"}}
    for idx in range(4):
        result = await memory.correlate(f"noc-{idx}", alert)
    assert result["chronic"] is False

    memory = IncidentMemory(redis_url="")
    last = None
    for _ in range(4):
        memory._local.active.clear()
        last = await memory.correlate("noc", alert)
    assert last is not None
    assert last["chronic"] is True


@pytest.mark.asyncio
async def test_record_operator_decision_updates_summary(monkeypatch):
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
    case_result = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "noc-agent-uptime", "host": "noc"},
            "alerts": [{"labels": {"alertname": "noc-agent-uptime", "host": "noc", "state": "WARNING"}}],
        }
    )
    incident_id = case_result.case["incident_id"]
    await memory.put_summary(
        incident_id,
        {
            "incident_id": incident_id,
            "case_number": case_result.case["case_number"],
            "status": "waiting_approval",
            "title": "test",
        },
    )

    updated = await graph_runtime.record_operator_decision(
        incident_id,
        {"incident_id": incident_id, "decision": "approved", "operator": "svag", "comment": "ok"},
    )
    case = await memory.get_case(incident_id)
    stored_summary = await memory.get_summary(incident_id)

    assert updated["status"] == "approved"
    assert updated["operator_decision"]["operator"] == "svag"
    assert stored_summary["status"] == "approved"
    assert stored_summary["incident_id"] == incident_id
    assert stored_summary["case_number"] == case_result.case["case_number"]
    assert case["status"] == "resolved"
    assert case["decision_status"] == "approved"


@pytest.mark.asyncio
async def test_inject_case_event_updates_existing_graph_state(monkeypatch):
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
    result = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "noc-agent-uptime", "host": "noc"},
            "alerts": [{"labels": {"alertname": "noc-agent-uptime", "host": "noc", "state": "WARNING"}}],
        }
    )
    case = await memory.set_case_thread(result.case["incident_id"], "thread-1")

    class FakeSnapshot:
        values = {
            "related_alerts": [{"status": "firing"}],
            "diagnostic_synthesis": {"incident_summary": "keep me"},
        }

    class FakeGraph:
        def __init__(self):
            self.update = None

        async def aget_state(self, config):
            return FakeSnapshot()

        async def aupdate_state(self, config, values, as_node=None):
            self.update = values

    graph = FakeGraph()
    monkeypatch.setitem(graph_runtime._THREAD_GRAPHS, "thread-1", graph)
    event = {"received_at": "2026-05-20T19:40:00Z", "state": "CRITICAL", "summary": "worse"}

    assert await graph_runtime.inject_case_event(case, event) is True
    assert graph.update["related_alerts"] == [{"status": "firing"}, event]
    assert "diagnostic_synthesis" not in graph.update
    assert graph.update["latest_event"] == event


@pytest.mark.asyncio
async def test_inject_case_event_without_thread_id_leaves_thread_graphs_unchanged(monkeypatch):
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
    original_graphs = {"thread-existing": object()}
    monkeypatch.setattr(graph_runtime, "_THREAD_GRAPHS", dict(original_graphs))

    result = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "noc-agent-uptime", "host": "noc"},
            "alerts": [{"labels": {"alertname": "noc-agent-uptime", "host": "noc", "state": "WARNING"}}],
        }
    )

    assert await graph_runtime.inject_case_event(result.case, {"state": "CRITICAL"}) is False
    assert graph_runtime._THREAD_GRAPHS == original_graphs


@pytest.mark.asyncio
async def test_inject_case_event_retries_without_as_node(monkeypatch):
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
    result = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "noc-agent-uptime", "host": "noc"},
            "alerts": [{"labels": {"alertname": "noc-agent-uptime", "host": "noc", "state": "WARNING"}}],
        }
    )
    case = await memory.set_case_thread(result.case["incident_id"], "thread-fallback")

    class EmptySnapshot:
        values = {}

    class FallbackGraph:
        def __init__(self):
            self.calls = []

        async def aget_state(self, config):
            return EmptySnapshot()

        async def aupdate_state(self, config, values, **kwargs):
            self.calls.append({"config": config, "values": values, "kwargs": kwargs})
            if len(self.calls) == 1:
                raise RuntimeError("as_node not supported")

    graph = FallbackGraph()
    monkeypatch.setattr(graph_runtime, "_THREAD_GRAPHS", {"thread-fallback": graph})

    assert await graph_runtime.inject_case_event(case, {"state": "CRITICAL"}) is True
    assert graph.calls[0]["kwargs"] == {"as_node": "intake_event_injection"}
    assert graph.calls[1]["kwargs"] == {}
