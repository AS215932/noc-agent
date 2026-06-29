from __future__ import annotations

import json

from app import agent_core_trace


def _state() -> dict[str, object]:
    return {
        "incident_id": "case_123",
        "case_number": "NOC-123",
        "thread_id": "thread-1",
        "handoff_id": "handoff_1",
        "objective_id": "objective_1",
        "resource_id": "rtr",
        "active_specialist": "bgp",
        "approval_state": "verified",
        "diagnostic_synthesis": {"incident_summary": "BGP peer down"},
        "proposals": [
            {"proposed_remediation": ["restart bird"], "structured_actions": []},
            {"proposed_remediation": ["open provider ticket"], "structured_actions": []},
        ],
        "evidence_log": [{"source": "prometheus", "summary": "session down"}],
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "executed_actions": [{"ok": True, "action": "noop", "handoff_id": "handoff_1"}],
        "verification_results": [{"ok": True, "check": "bgp", "objective_id": "objective_1"}],
    }


def test_disabled_by_default(monkeypatch, tmp_path):
    sink = tmp_path / "trace.jsonl"
    monkeypatch.delenv("HYRULE_NOC_AGENT_CORE_TRACE", raising=False)
    monkeypatch.setenv("HYRULE_NOC_AGENT_CORE_TRACE_PATH", str(sink))

    assert agent_core_trace.emit_state_trace(_state(), phase="investigation") == 0
    assert not sink.exists()


def test_emits_graph_proposal_evidence_and_decision_events(monkeypatch, tmp_path):
    sink = tmp_path / "trace.jsonl"
    monkeypatch.setenv("HYRULE_NOC_AGENT_CORE_TRACE", "1")
    monkeypatch.setenv("HYRULE_NOC_AGENT_CORE_TRACE_PATH", str(sink))

    count = agent_core_trace.emit_state_trace(_state(), phase="resume")

    assert count == 7
    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert [record["event_type"] for record in records] == [
        "noc_graph_summary",
        "noc_proposal",
        "noc_proposal",
        "noc_evidence_summary",
        "noc_operator_decision",
        "noc_executed_action",
        "noc_verification_result",
    ]
    assert {record["run_id"] for record in records} == {"case_123"}
    assert {record["graph_id"] for record in records} == {"noc-agent"}
    assert {record["case_id"] for record in records} == {"case_123"}
    assert records[0]["payload"]["proposal_count"] == 2
    assert records[4]["payload"]["operator_decision"]["operator"] == "pytest"
    assert records[5]["event_type"] == "noc_executed_action"
    assert records[5]["handoff_id"] == "handoff_1"
    assert records[5]["parent_event_id"] == records[4]["event_id"]
    assert records[5]["payload"]["untrusted_loop_text"] is True
    assert records[5]["payload"]["model_consumption_allowed"] is False
    assert records[6]["event_type"] == "noc_verification_result"
    assert records[6]["objective_id"] == "objective_1"
    assert records[6]["parent_event_id"] == records[4]["event_id"]


def test_emits_tuple_state_values_and_sanitizes_unknown_objects(monkeypatch, tmp_path):
    class Marker:
        def __str__(self) -> str:
            return "marker-as-text"

    sink = tmp_path / "trace.jsonl"
    state = _state()
    state["proposals"] = ({"proposed_remediation": (Marker(),)},)
    state["evidence_log"] = ({"source": Marker()},)
    monkeypatch.setenv("HYRULE_NOC_AGENT_CORE_TRACE", "1")
    monkeypatch.setenv("HYRULE_NOC_AGENT_CORE_TRACE_PATH", str(sink))

    count = agent_core_trace.emit_state_trace(state, phase="resume")

    assert count == 6
    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert records[0]["payload"]["proposal_count"] == 1
    assert records[0]["payload"]["evidence_count"] == 1
    assert records[1]["payload"]["proposal"]["proposed_remediation"] == ["marker-as-text"]
    assert records[2]["payload"]["evidence_log"] == [{"source": "marker-as-text"}]
