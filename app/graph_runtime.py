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
from app.incident_memory import CaseIntakeResult, IncidentMemory, case_event_from_alert


INCIDENT_MEMORY = IncidentMemory()
_GRAPH = None
_THREAD_GRAPHS: dict[str, Any] = {}


async def intake_alert(alert_payload: dict[str, Any]) -> CaseIntakeResult:
    return await INCIDENT_MEMORY.intake_alert(alert_payload)


async def inject_case_event(case: dict[str, Any], event: dict[str, Any], *, incident_memory=None) -> bool:
    memory = incident_memory or INCIDENT_MEMORY
    thread_id = case.get("thread_id")
    if not thread_id:
        return False
    graph = _THREAD_GRAPHS.get(thread_id) or await _graph(
        RuntimeDeps.build(incident_memory=memory),
        cache=incident_memory is None,
    )
    context = await memory.case_context(case["incident_id"])
    config = {"configurable": {"thread_id": thread_id}}
    existing_values: dict[str, Any] = {}
    if hasattr(graph, "aget_state"):
        snapshot = await graph.aget_state(config)
        existing_values = dict(getattr(snapshot, "values", {}) or {})
    related_alerts = list(existing_values.get("related_alerts") or [])
    related_alerts.append(event)
    update = {
        "updated_at": utc_now(),
        "related_alerts": related_alerts,
        "case_event_count": case.get("event_count", 0),
        "latest_event": event,
        "latest_transition": case.get("latest_transition"),
        "downstream_victims": list(case.get("downstream_victims", [])),
        "case_status": case.get("status", ""),
        "case_context": context,
        "needs_reassessment": bool(case.get("needs_reassessment", False)),
    }
    try:
        await graph.aupdate_state(
            config,
            update,
            as_node="intake_event_injection",
        )
        return True
    except Exception:
        # Some LangGraph versions require as_node to be a compiled node. Retry
        # without as_node so production keeps preserving case context.
        await graph.aupdate_state(config, update)
        return True


async def link_to_parent_case(
    child_case_id: str,
    parent_case_id: str,
    reason: str,
    evidence_refs: list[str] | None = None,
) -> dict[str, Any]:
    result = await INCIDENT_MEMORY.link_to_parent_case(child_case_id, parent_case_id, reason, evidence_refs or [])
    parent = result.get("parent_case") if isinstance(result, dict) else None
    if parent:
        await inject_case_event(
            parent,
            {
                "received_at": utc_now(),
                "event_type": "linked_child_case",
                "child_case_id": child_case_id,
                "summary": reason,
                "evidence_refs": list(evidence_refs or []),
            },
        )
    return result


async def run_investigation_graph(
    alert_payload: dict[str, Any],
    model=None,
    mcp_runtime=None,
    case: dict[str, Any] | None = None,
    *,
    incident_memory=None,
) -> tuple[DiagnosticSynthesis, WorkflowState]:
    memory = incident_memory or INCIDENT_MEMORY
    if case is None:
        intake = await memory.intake_alert(alert_payload)
        case = intake.case
    incident_id = case["incident_id"] if case else str(uuid4())
    thread_id = case.get("thread_id") if case and case.get("thread_id") else str(uuid4())
    case_context = await memory.case_context(incident_id) if case else {}
    latest_event = (
        case.get("latest_event")
        if case and case.get("latest_event")
        else case_context.get("event_timeline", [{}])[-1]
        if case_context.get("event_timeline")
        else case_event_from_alert(alert_payload)
    )
    state: WorkflowState = {
        "incident_id": incident_id,
        "case_number": case.get("case_number", "") if case else "",
        "case_status": case.get("status", "") if case else "",
        "case_event_count": int(case.get("event_count", 0)) if case else 1,
        "fingerprint": case.get("fingerprint", "") if case else "",
        "thread_id": thread_id,
        "current_step": "start",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "normalized_alert": _jsonish_dict(alert_payload),
        "resource_id": resource_id_from_alert(alert_payload),
        "related_alerts": [_jsonish_dict(alert_payload)],
        "latest_event": _jsonish_dict(latest_event),
        "latest_transition": case.get("latest_transition") if case else None,
        "case_context": _jsonish_dict(case_context),
        "downstream_victims": list(case.get("downstream_victims", [])) if case else [],
        "needs_reassessment": bool(case.get("needs_reassessment", False)) if case else False,
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
        incident_memory=memory,
        mcp_runtime=mcp_runtime,
        model_override=model,
    )
    graph = await _graph(runtime, cache=model is None and mcp_runtime is None and incident_memory is None)
    _THREAD_GRAPHS[thread_id] = graph
    if case:
        await memory.set_case_thread(incident_id, thread_id)
    result_state = await graph.ainvoke(
        state,
        {"configurable": {"thread_id": thread_id}},
    )
    result_state = {key: value for key, value in result_state.items() if key != "__interrupt__"}
    synthesis = DiagnosticSynthesis.model_validate(result_state["diagnostic_synthesis"])
    if case:
        await memory.update_case(
            incident_id,
            {
                "status": "waiting_approval",
                "thread_id": thread_id,
                "diagnostic_summary": synthesis.incident_summary,
                "case_context": await memory.case_context(incident_id),
            },
        )
    return synthesis, result_state


async def pending_summaries() -> list[dict[str, Any]]:
    return await INCIDENT_MEMORY.list_summaries()


async def summary_for(incident_id: str) -> dict[str, Any] | None:
    resolved = await INCIDENT_MEMORY.resolve_case_identifier(incident_id)
    return await INCIDENT_MEMORY.get_summary(resolved or incident_id)


async def record_operator_decision(
    incident_id: str,
    decision: dict[str, Any],
    mcp_runtime=None,
    *,
    incident_memory=None,
) -> dict[str, Any] | None:
    memory = incident_memory or INCIDENT_MEMORY
    resolved = await memory.resolve_case_identifier(incident_id)
    incident_id = resolved or incident_id
    decision = {**decision, "incident_id": incident_id}
    summary = await memory.get_summary(incident_id)
    if not summary:
        return None
    status = decision.get("decision", "acknowledged")
    final_state = None
    if status == "approved" and summary.get("thread_id"):
        final_state = await resume_investigation(incident_id, decision, mcp_runtime=mcp_runtime, incident_memory=memory)
    summary.update(
        {
            "status": "approved" if status == "approved" else "rejected" if status == "rejected" else "finalized",
            "operator_decision": decision,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    if final_state is not None:
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
    incident_memory=None,
) -> WorkflowState | None:
    memory = incident_memory or INCIDENT_MEMORY
    resolved = await memory.resolve_case_identifier(incident_id)
    incident_id = resolved or incident_id
    summary = await memory.get_summary(incident_id)
    if not summary or not summary.get("thread_id"):
        return None
    runtime = RuntimeDeps.build(incident_memory=memory, mcp_runtime=mcp_runtime)
    graph = _THREAD_GRAPHS.get(summary["thread_id"]) or await _graph(
        runtime,
        cache=mcp_runtime is None and incident_memory is None,
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
