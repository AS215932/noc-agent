from __future__ import annotations

import json

from app import agent_core_trace


def _state() -> dict[str, object]:
    return {
        "incident_id": "case_123",
        "case_number": "NOC-123",
        "thread_id": "thread-1",
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
        "executed_actions": [{"ok": True, "action": "noop"}],
        "verification_results": [{"ok": True, "check": "bgp"}],
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

    assert count == 5
    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert [record["event_type"] for record in records] == [
        "noc_graph_summary",
        "noc_proposal",
        "noc_proposal",
        "noc_evidence_summary",
        "noc_operator_decision",
    ]
    assert {record["run_id"] for record in records} == {"case_123"}
    assert {record["graph_id"] for record in records} == {"noc-agent"}
    assert records[0]["payload"]["proposal_count"] == 2
    assert records[-1]["payload"]["operator_decision"]["operator"] == "pytest"


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

    assert count == 4
    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert records[0]["payload"]["proposal_count"] == 1
    assert records[0]["payload"]["evidence_count"] == 1
    assert records[1]["payload"]["proposal"]["proposed_remediation"] == ["marker-as-text"]
    assert records[2]["payload"]["evidence_log"] == [{"source": "marker-as-text"}]
