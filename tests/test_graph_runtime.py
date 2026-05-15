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

    assert plan.issue_summary
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
    await memory.put_summary("incident-1", {"incident_id": "incident-1", "status": "waiting_approval", "title": "test"})

    updated = await graph_runtime.record_operator_decision(
        "incident-1",
        {"incident_id": "incident-1", "decision": "approved", "operator": "svag", "comment": "ok"},
    )

    assert updated["status"] == "approved"
    assert updated["operator_decision"]["operator"] == "svag"
