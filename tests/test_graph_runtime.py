import pytest
from pydantic_ai.models.test import TestModel

import app.graph_runtime as graph_runtime
from app.agents.triage import DiagnosticSynthesis
from app.alert_utils import case_event_from_alert
from app.cases import CaseService, InMemoryCaseStore, ObservationRecord
from app.cases.graph_memory import CaseServiceGraphMemory
from app.graph.nodes import NodeRunner


_ALERT = {
    "status": "firing",
    "groupLabels": {"alertname": "BGP Peer Down", "host": "rtr"},
    "commonLabels": {"severity": "critical"},
    "alerts": [{"labels": {"alertname": "BGP Peer Down", "host": "rtr"}}],
}


async def _case_service_graph_case(alert: dict | None = None):
    alert = alert or _ALERT
    labels = {**alert.get("groupLabels", {}), **alert.get("commonLabels", {})}
    first = (alert.get("alerts") or [{}])[0]
    labels.update(first.get("labels", {}))
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source=str(alert.get("source") or "alertmanager"),
            detector=str(labels.get("alertname") or "alert"),
            resource=str(labels.get("host") or labels.get("instance") or "rtr"),
            status="firing",
            severity="HIGH",
            signal_snapshot={"summary": "BGP peer is down", "labels": labels},
        )
    )
    assert created.case is not None
    graph_memory = CaseServiceGraphMemory(store)
    graph_case = await graph_memory.get_case(created.case.case_id)
    assert graph_case is not None
    graph_case["latest_event"] = case_event_from_alert(alert)
    return store, service, graph_memory, graph_case


@pytest.mark.asyncio
async def test_graph_routes_bgp_alert_and_creates_pending_summary(monkeypatch):
    monkeypatch.setattr(graph_runtime, "_GRAPH", None)
    emitted = []
    monkeypatch.setattr(
        graph_runtime,
        "emit_state_trace",
        lambda state, *, phase: emitted.append((phase, state["incident_id"])) or 0,
    )
    graph_runtime._THREAD_GRAPHS.clear()
    _store, _service, graph_memory, graph_case = await _case_service_graph_case(_ALERT)

    plan, state = await graph_runtime.run_investigation_graph(
        _ALERT,
        model=TestModel(),
        case=graph_case,
        graph_memory=graph_memory,
    )

    assert plan.incident_summary is not None
    assert "diagnostic_synthesis" in state
    assert "action_plan" not in state
    assert state["active_specialist"] == "bgp"
    assert state["approval_state"] == "waiting_approval"
    assert "model_override" not in state
    assert "perimeter_context" not in state
    assert state["perimeter_context_version"]
    assert state["manifest_hash"]
    assert emitted == [("investigation", state["incident_id"])]
    summary = await graph_memory.get_summary(state["incident_id"])
    assert summary is not None
    assert summary["status"] == "waiting_approval"
    assert summary["thread_id"] == state["thread_id"]


@pytest.mark.asyncio
async def test_graph_resume_uses_stable_thread_id(monkeypatch):
    monkeypatch.setattr(graph_runtime, "_GRAPH", None)
    graph_runtime._THREAD_GRAPHS.clear()
    _store, _service, graph_memory, graph_case = await _case_service_graph_case(_ALERT)

    _plan, state = await graph_runtime.run_investigation_graph(
        _ALERT,
        model=TestModel(),
        case=graph_case,
        graph_memory=graph_memory,
    )
    resumed = await graph_runtime.resume_investigation(
        state["incident_id"],
        {"decision": "acknowledged", "operator": "pytest", "comment": "ok"},
        graph_memory=graph_memory,
    )

    assert resumed is not None
    assert resumed["operator_decision"]["operator"] == "pytest"


@pytest.mark.asyncio
async def test_graph_requires_explicit_case_and_memory():
    with pytest.raises(RuntimeError, match="explicit case"):
        await graph_runtime.run_investigation_graph(_ALERT, model=TestModel(), graph_memory=object())

    graph_case = {
        "incident_id": "case_123",
        "case_number": "NOC-123",
        "resource_id": "edge1",
        "status": "investigating",
        "thread_id": None,
        "source": "case_service",
    }
    with pytest.raises(RuntimeError, match="explicit graph memory"):
        await graph_runtime.run_investigation_graph(_ALERT, model=TestModel(), case=graph_case)


@pytest.mark.asyncio
async def test_graph_uses_case_service_memory_when_case_provided(monkeypatch):
    monkeypatch.setattr(graph_runtime, "_GRAPH", None)
    graph_runtime._THREAD_GRAPHS.clear()
    store, _service, graph_memory, graph_case = await _case_service_graph_case(_ALERT)

    _plan, state = await graph_runtime.run_investigation_graph(
        _ALERT,
        model=TestModel(),
        case=graph_case,
        graph_memory=graph_memory,
    )

    summary = await graph_memory.get_summary(graph_case["incident_id"])
    stored = await store.get_case(graph_case["incident_id"])
    event_types = [event.event_type for event in await store.case_events(graph_case["incident_id"])]
    assert summary is not None
    assert summary["status"] == "waiting_approval"
    assert summary["thread_id"] == state["thread_id"]
    assert "case_context" not in summary
    assert stored.last_diagnosis["graph_summary"]["incident_id"] == graph_case["incident_id"]
    assert "graph_summary_recorded" in event_types


@pytest.mark.asyncio
async def test_case_service_graph_memory_counts_repeated_case_events_as_history():
    store = InMemoryCaseStore()
    service = CaseService(store)
    for _ in range(4):
        await service.observe(
            ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
        )
    graph_memory = CaseServiceGraphMemory(store)

    history = await graph_memory.history_for("edge1")
    correlated = await graph_memory.correlate("edge1", {})

    assert len(history) == 4
    assert correlated["chronic"] is True


@pytest.mark.asyncio
async def test_case_service_graph_memory_links_child_to_parent_case():
    store = InMemoryCaseStore()
    service = CaseService(store)
    parent = await service.observe(
        ObservationRecord(source="alertmanager", detector="Power", resource="rack1", status="firing")
    )
    child = await service.observe(
        ObservationRecord(
            source="alertmanager",
            detector="BGP",
            resource="rtr1",
            status="firing",
            source_fingerprint="child-fp",
        )
    )
    assert parent.case is not None
    assert child.case is not None
    parent.case.case_number = "NOC-20260621-043"
    await store.upsert_case(parent.case)
    assert await store.resolve_alias("source_fp", "child-fp") == child.case.case_id
    graph_memory = CaseServiceGraphMemory(store)

    result = await graph_memory.link_to_parent_case(
        "child-fp",
        parent.case.case_number,
        "same rack power event",
        ["evt-1"],
    )

    stored_child = await store.get_case(child.case.case_id)
    parent_events = await store.case_events(parent.case.case_id)
    child_events = await store.case_events(child.case.case_id)
    repeated = await graph_memory.link_to_parent_case(
        child.case.case_id,
        parent.case.case_id,
        "same rack power event",
        ["evt-1"],
    )

    assert result["ok"] is True
    assert result["event_count"] == 2
    assert repeated["ok"] is True
    assert repeated["event_count"] == 0
    assert stored_child.status == "linked"
    assert stored_child.last_diagnosis["linked_parent_case"] == parent.case.case_id
    assert await store.resolve_alias("source_fp", "child-fp") == parent.case.case_id
    assert "linked_child_case" in [event.event_type for event in parent_events]
    assert len(await store.case_events(parent.case.case_id)) == len(parent_events)
    assert len(await store.case_events(child.case.case_id)) == len(child_events)


@pytest.mark.asyncio
async def test_case_service_graph_memory_link_rejects_missing_cases():
    graph_memory = CaseServiceGraphMemory(InMemoryCaseStore())

    result = await graph_memory.link_to_parent_case("missing-child", "missing-parent", "same event")

    assert result == {"ok": False, "error": "case_not_found"}


@pytest.mark.asyncio
async def test_case_service_graph_summary_cannot_resolve_case_without_operator_decision():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    graph_memory = CaseServiceGraphMemory(store)

    await graph_memory.put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "approved", "title": "model supplied approval"},
    )

    stored = await store.get_case(created.case.case_id)
    summary = await graph_memory.get_summary(created.case.case_id)
    assert stored.status == "investigating"
    assert summary["status"] == "approved"


@pytest.mark.asyncio
async def test_record_operator_decision_updates_case_service_summary(monkeypatch):
    emitted = []
    monkeypatch.setattr(
        graph_runtime,
        "emit_state_trace",
        lambda state, *, phase: emitted.append((phase, state["incident_id"])) or 0,
    )
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="icinga2", detector="noc-agent-uptime", resource="noc", status="firing")
    )
    assert created.case is not None
    graph_memory = CaseServiceGraphMemory(store)
    await graph_memory.put_summary(
        created.case.case_id,
        {
            "incident_id": created.case.case_id,
            "case_number": created.case.case_number,
            "status": "waiting_approval",
            "title": "test",
        },
    )

    updated = await graph_runtime.record_operator_decision(
        created.case.case_id,
        {"incident_id": created.case.case_id, "decision": "approved", "operator": "svag", "comment": "ok"},
        graph_memory=graph_memory,
    )
    case = await store.get_case(created.case.case_id)
    stored_summary = await graph_memory.get_summary(created.case.case_id)

    assert updated["status"] == "approved"
    assert updated["operator_decision"]["operator"] == "svag"
    assert stored_summary["status"] == "approved"
    assert stored_summary["incident_id"] == created.case.case_id
    assert stored_summary["case_number"] == created.case.case_number
    assert case.status == "resolved"
    assert emitted == [("resume", created.case.case_id)]


@pytest.mark.asyncio
async def test_approved_restart_proposal_executes_node_exporter_restart(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")

    class FakeMCPRuntime:
        def __init__(self):
            self.calls = []

        async def call_tool(self, source, name, arguments):
            self.calls.append((source, name, arguments))
            return {"ok": True, "tool": name}

    runtime = type("Runtime", (), {"mcp_runtime": FakeMCPRuntime()})()
    runner = NodeRunner(runtime)
    synthesis = DiagnosticSynthesis(incident_summary="node_exporter down", affected_objects=["cr1.nl1"])
    state = {
        "incident_id": "incident-1",
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "diagnostic_synthesis": synthesis.model_dump(mode="json"),
        "normalized_alert": {},
        "proposals": [
            {
                "proposed_remediation": ["Restart node_exporter on cr1.nl1"],
                "structured_actions": [
                    {
                        "action_id": "act-1",
                        "type": "restart_service",
                        "inputs": {"host": "cr1-nl1", "service": "node_exporter"},
                    }
                ],
            }
        ],
        "executed_actions": [],
    }

    update = await runner.execute_approved_remediation(state)

    assert runtime.mcp_runtime.calls == [
        (
            "hyrule",
            "os_service_restart",
            {
                "host": "cr1-nl1",
                "service": "node_exporter",
                "action_authorization": {
                    "action_id": "act-1",
                    "case_id": "incident-1",
                    "operator": "pytest",
                    "action_class": "restart_service",
                    "expiry": runtime.mcp_runtime.calls[0][2]["action_authorization"]["expiry"],
                },
            },
        )
    ]
    assert update["approval_state"] == "executed"


@pytest.mark.asyncio
async def test_text_only_proposal_does_not_execute_remediation(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")

    class FakeMCPRuntime:
        def __init__(self):
            self.calls = []

        async def call_tool(self, source, name, arguments):
            self.calls.append((source, name, arguments))
            return {"ok": True, "tool": name}

    runtime = type("Runtime", (), {"mcp_runtime": FakeMCPRuntime()})()
    runner = NodeRunner(runtime)
    synthesis = DiagnosticSynthesis(incident_summary="node_exporter down", affected_objects=["cr1.nl1"])
    state = {
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "diagnostic_synthesis": synthesis.model_dump(mode="json"),
        "normalized_alert": {},
        "proposals": [{"proposed_remediation": ["Restart node_exporter on cr1.nl1"]}],
        "executed_actions": [],
    }

    update = await runner.execute_approved_remediation(state)

    assert runtime.mcp_runtime.calls == []
    assert update["approval_state"] == "approved_no_executable_action"


@pytest.mark.asyncio
async def test_verification_retries_prometheus_before_escalating(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("app.graph.nodes.asyncio.sleep", fake_sleep)

    class FakeMCPRuntime:
        def __init__(self):
            self.calls = []

        async def call_tool(self, source, name, arguments):
            self.calls.append((source, name, arguments))
            return {"ok": True, "result": [{"metric": {"instance": arguments["query"]}, "value": "0"}]}

    runtime = type("Runtime", (), {"mcp_runtime": FakeMCPRuntime()})()
    runner = NodeRunner(runtime)
    state = {
        "approval_state": "executed",
        "executed_actions": [{"ok": True, "action": {"host": "cr1-nl1", "service": "node_exporter"}}],
        "verification_results": [],
    }

    update = await runner.verify_remediation(state)

    assert sleeps == [15, 15, 15]
    assert len(runtime.mcp_runtime.calls) == 3
    assert update["approval_state"] == "verification_failed"


@pytest.mark.asyncio
async def test_verification_stops_after_prometheus_recovery(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("app.graph.nodes.asyncio.sleep", fake_sleep)

    class FakeMCPRuntime:
        def __init__(self):
            self.calls = 0

        async def call_tool(self, source, name, arguments):
            self.calls += 1
            value = "1" if self.calls == 2 else "0"
            return {"ok": True, "result": [{"value": value}]}

    runtime = type("Runtime", (), {"mcp_runtime": FakeMCPRuntime()})()
    runner = NodeRunner(runtime)
    state = {
        "approval_state": "executed",
        "executed_actions": [{"ok": True, "action": {"host": "cr1-nl1", "service": "node_exporter"}}],
        "verification_results": [],
    }

    update = await runner.verify_remediation(state)

    assert sleeps == [15, 15]
    assert runtime.mcp_runtime.calls == 2
    assert update["approval_state"] == "verified"
