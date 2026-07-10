from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from langgraph.types import Command

from app.agent_core_trace import emit_state_trace
from app.agents.triage import DiagnosticSynthesis
from app.alert_utils import case_event_from_alert
from app.deps.runtime import RuntimeDeps
from app.graph.graph import build_graph
from app.graph.routing import resource_id_from_alert
from app.graph.state import WorkflowState, assert_json_serializable_state, utc_now


_GRAPH = None
_THREAD_GRAPHS: dict[str, Any] = {}


def _require_graph_case(case: dict[str, Any] | None) -> dict[str, Any]:
    if not case:
        raise RuntimeError("Investigation graph requires an explicit case")
    return case


def _require_graph_memory(graph_memory=None):
    if graph_memory is None:
        raise RuntimeError("Investigation graph requires explicit graph memory")
    return graph_memory


async def run_investigation_graph(
    alert_payload: dict[str, Any],
    model=None,
    mcp_runtime=None,
    case: dict[str, Any] | None = None,
    *,
    graph_memory=None,
) -> tuple[DiagnosticSynthesis, WorkflowState]:
    case = _require_graph_case(case)
    memory = _require_graph_memory(graph_memory)
    incident_id = case["incident_id"]
    thread_id = case.get("thread_id") or str(uuid4())
    case_context = await memory.case_context(incident_id)
    latest_event = (
        case.get("latest_event")
        if case.get("latest_event")
        else case_context.get("event_timeline", [{}])[-1]
        if case_context.get("event_timeline")
        else case_event_from_alert(alert_payload)
    )
    state: WorkflowState = {
        "incident_id": incident_id,
        "case_number": case.get("case_number", ""),
        "case_status": case.get("status", ""),
        "case_event_count": int(case.get("event_count", 0)),
        "fingerprint": case.get("fingerprint", ""),
        "thread_id": thread_id,
        "current_step": "start",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "normalized_alert": _jsonish_dict(alert_payload),
        "resource_id": resource_id_from_alert(alert_payload),
        "related_alerts": [_jsonish_dict(alert_payload)],
        "latest_event": _jsonish_dict(latest_event),
        "latest_transition": case.get("latest_transition"),
        "case_context": _jsonish_dict(case_context),
        "downstream_victims": list(case.get("downstream_victims", [])),
        "needs_reassessment": bool(case.get("needs_reassessment", False)),
        "incident_history": [],
        "history_summary": {},
        "telemetry_cache": {},
        "evidence_log": [],
        "proposals": [],
        "executed_actions": [],
        "verification_results": [],
        "approval_state": "pending",
        "operator_decision": None,
        "drift_findings": [],
        "retry_counts": {},
        "errors": [],
    }
    assert_json_serializable_state(state)

    runtime = RuntimeDeps.build(
        graph_memory=memory,
        mcp_runtime=mcp_runtime,
        model_override=model,
    )
    graph = await _graph(runtime, cache=False)
    _THREAD_GRAPHS[thread_id] = graph
    await memory.set_case_thread(incident_id, thread_id)
    result_state = await graph.ainvoke(
        state,
        {"configurable": {"thread_id": thread_id}},
    )
    result_state = {key: value for key, value in result_state.items() if key != "__interrupt__"}
    synthesis = DiagnosticSynthesis.model_validate(result_state["diagnostic_synthesis"])
    grant_decision = result_state.get("operator_decision") or {}
    if isinstance(grant_decision, dict) and grant_decision.get("operator") == "standing-grant":
        # A standing grant skipped the interrupt, so this invocation ran all
        # the way through execution/verification — persist the terminal state
        # exactly like the manual record_operator_decision path instead of
        # stamping the case back to waiting_approval.
        await _persist_final_decision(memory, incident_id, grant_decision, result_state)
    else:
        await memory.update_case(
            incident_id,
            {
                "status": "waiting_approval",
                "thread_id": thread_id,
                "diagnostic_summary": synthesis.incident_summary,
                "case_context": await memory.case_context(incident_id),
            },
        )
    emit_state_trace(result_state, phase="investigation")
    return synthesis, result_state


async def record_operator_decision(
    incident_id: str,
    decision: dict[str, Any],
    mcp_runtime=None,
    *,
    graph_memory=None,
) -> dict[str, Any] | None:
    memory = _require_graph_memory(graph_memory)
    resolved = await memory.resolve_case_identifier(incident_id)
    incident_id = resolved or incident_id
    decision = {**decision, "incident_id": incident_id}
    summary = await memory.get_summary(incident_id)
    if not summary:
        return None
    status = decision.get("decision", "acknowledged")
    final_state = None
    if status == "approved" and summary.get("thread_id"):
        final_state = await resume_investigation(incident_id, decision, mcp_runtime=mcp_runtime, graph_memory=memory)
    if final_state is not None:
        summary = await _persist_final_decision(memory, incident_id, decision, final_state) or summary
    else:
        summary.update(
            {
                "status": "approved" if status == "approved" else "rejected" if status == "rejected" else "finalized",
                "operator_decision": decision,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await memory.put_summary(incident_id, summary)
        await memory.update_case(
            incident_id,
            {
                "status": "waiting_approval" if summary["status"] == "waiting_approval" else "resolved",
                "decision_status": summary["status"],
                "operator_decision": decision,
                "executed_actions": summary.get("executed_actions", []),
                "verification_results": summary.get("verification_results", []),
            },
        )
    emit_state_trace(final_state if final_state is not None else summary, phase="resume")
    return summary


async def _persist_final_decision(
    memory: Any,
    incident_id: str,
    decision: dict[str, Any],
    final_state: dict[str, Any],
) -> dict[str, Any] | None:
    """Persist a decided-and-executed graph outcome to the case/summary.

    Shared by the manual approval resume path and the standing-grant path (the
    latter never interrupts, so the initial invocation is the one that carries
    the terminal execution/verification state)."""
    summary = await memory.get_summary(incident_id)
    if not summary:
        return None
    summary.update(
        {
            "status": "approved",
            "operator_decision": decision,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    summary["approval_state"] = final_state.get("approval_state", summary["status"])
    summary["executed_actions"] = list(final_state.get("executed_actions", []))
    summary["verification_results"] = list(final_state.get("verification_results", []))
    if final_state.get("approval_state") == "verified":
        summary["status"] = "resolved"
    elif final_state.get("approval_state") in {"verification_failed", "execution_failed", "approved_no_executable_action"}:
        summary["status"] = "waiting_approval"
    await memory.put_summary(incident_id, summary)
    await memory.update_case(
        incident_id,
        {
            "status": "waiting_approval" if summary["status"] == "waiting_approval" else "resolved",
            "decision_status": summary["status"],
            "operator_decision": decision,
            "executed_actions": summary.get("executed_actions", []),
            "verification_results": summary.get("verification_results", []),
        },
    )
    return summary


async def resume_investigation(
    incident_id: str,
    decision: dict[str, Any],
    mcp_runtime=None,
    *,
    graph_memory=None,
) -> WorkflowState | None:
    memory = _require_graph_memory(graph_memory)
    resolved = await memory.resolve_case_identifier(incident_id)
    incident_id = resolved or incident_id
    summary = await memory.get_summary(incident_id)
    if not summary or not summary.get("thread_id"):
        return None
    runtime = RuntimeDeps.build(graph_memory=memory, mcp_runtime=mcp_runtime)
    graph = _THREAD_GRAPHS.get(summary["thread_id"]) or await _graph(
        runtime,
        cache=False,
    )
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
