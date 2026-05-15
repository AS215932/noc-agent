from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from langgraph.types import Command

from app.agents.triage import DiagnosticSynthesis
from app.deps.runtime import RuntimeDeps
from app.graph.graph import build_graph
from app.graph.routing import resource_id_from_alert
from app.graph.state import WorkflowState, assert_json_serializable_state, utc_now
from app.incident_memory import IncidentMemory


INCIDENT_MEMORY = IncidentMemory()
_GRAPH = None
_THREAD_GRAPHS: dict[str, Any] = {}


async def run_investigation_graph(alert_payload: dict[str, Any], model=None, mcp_runtime=None) -> tuple[DiagnosticSynthesis, WorkflowState]:
    incident_id = str(uuid4())
    thread_id = str(uuid4())
    state: WorkflowState = {
        "incident_id": incident_id,
        "thread_id": thread_id,
        "current_step": "start",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "normalized_alert": _jsonish_dict(alert_payload),
        "resource_id": resource_id_from_alert(alert_payload),
        "related_alerts": [_jsonish_dict(alert_payload)],
        "incident_history": [],
        "history_summary": {},
        "telemetry_cache": {},
        "evidence_log": [],
        "proposals": [],
        "approval_state": "pending",
        "operator_decision": None,
        "drift_findings": [],
        "retry_counts": {},
        "errors": [],
    }
    assert_json_serializable_state(state)

    runtime = RuntimeDeps.build(
        incident_memory=INCIDENT_MEMORY,
        mcp_runtime=mcp_runtime,
        model_override=model,
    )
    graph = await _graph(runtime, cache=model is None and mcp_runtime is None)
    _THREAD_GRAPHS[thread_id] = graph
    result_state = await graph.ainvoke(
        state,
        {"configurable": {"thread_id": thread_id}},
    )
    result_state = {key: value for key, value in result_state.items() if key != "__interrupt__"}
    synthesis = DiagnosticSynthesis.model_validate(result_state["diagnostic_synthesis"])
    return synthesis, result_state


async def pending_summaries() -> list[dict[str, Any]]:
    return await INCIDENT_MEMORY.list_summaries()


async def summary_for(incident_id: str) -> dict[str, Any] | None:
    return await INCIDENT_MEMORY.get_summary(incident_id)


async def record_operator_decision(incident_id: str, decision: dict[str, Any]) -> dict[str, Any] | None:
    summary = await INCIDENT_MEMORY.get_summary(incident_id)
    if not summary:
        return None
    status = decision.get("decision", "acknowledged")
    summary.update(
        {
            "status": "approved" if status == "approved" else "rejected" if status == "rejected" else "finalized",
            "operator_decision": decision,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    await INCIDENT_MEMORY.put_summary(incident_id, summary)
    return summary


async def resume_investigation(incident_id: str, decision: dict[str, Any], mcp_runtime=None) -> WorkflowState | None:
    summary = await INCIDENT_MEMORY.get_summary(incident_id)
    if not summary or not summary.get("thread_id"):
        return None
    runtime = RuntimeDeps.build(incident_memory=INCIDENT_MEMORY, mcp_runtime=mcp_runtime)
    graph = _THREAD_GRAPHS.get(summary["thread_id"]) or await _graph(runtime, cache=mcp_runtime is None)
    state = await graph.ainvoke(
        Command(resume=decision),
        {"configurable": {"thread_id": summary["thread_id"]}},
    )
    return {key: value for key, value in state.items() if key != "__interrupt__"}


async def _graph(runtime: RuntimeDeps, *, cache: bool):
    global _GRAPH
    if cache and _GRAPH is not None:
        return _GRAPH
    graph = await build_graph(runtime)
    if cache:
        _GRAPH = graph
    return graph


def _jsonish_dict(value: dict[str, Any]) -> dict[str, Any]:
    import json

    return json.loads(json.dumps(value, default=str))
