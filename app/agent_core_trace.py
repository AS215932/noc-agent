"""Best-effort agent-core TraceEvent emission for NOC graph runs."""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Mapping
from typing import Any

FLAG_ENV = "HYRULE_NOC_AGENT_CORE_TRACE"
_TRUTHY = {"1", "true", "yes", "on"}
GRAPH_ID = "noc-agent"


def enabled() -> bool:
    return os.environ.get(FLAG_ENV, "").strip().lower() in _TRUTHY


def emit_state_trace(state: Mapping[str, Any], *, phase: str) -> int:
    """Emit TraceEvents for a completed NOC graph state; return delivered count.

    Emission is strictly best-effort: missing ``agent-core``, collector failures, invalid
    payload shapes, and file/HTTP errors are swallowed so NOC remediation policy gates and
    graph execution can never be affected by observability delivery.
    """
    if not enabled():
        return 0
    try:
        sink_mod = importlib.import_module("agent_core.tracing.sink")
        sink = sink_mod.sink_from_env(FLAG_ENV)
        count = 0
        for event in _events_from_state(state, phase=phase):
            if sink.emit(event):
                count += 1
        return count
    except Exception:
        return 0


def _events_from_state(state: Mapping[str, Any], *, phase: str) -> list[Any]:
    tracing_mod = importlib.import_module("agent_core.contracts.tracing")
    TraceEvent = tracing_mod.TraceEvent
    run_id = _string_or_none(state.get("incident_id"))
    trace_id = _string_or_none(state.get("thread_id")) or run_id
    events: list[Any] = [
        TraceEvent(
            event_type="noc_graph_summary",
            graph_id=GRAPH_ID,
            node_id="graph_runtime",
            agent_role="noc_duty",
            environment="production",
            run_id=run_id,
            trace_id=trace_id,
            summary=_graph_summary(state, phase=phase),
            payload={
                "phase": phase,
                "incident_id": state.get("incident_id"),
                "case_number": state.get("case_number"),
                "resource_id": state.get("resource_id"),
                "case_status": state.get("case_status") or state.get("status"),
                "approval_state": state.get("approval_state"),
                "active_specialist": state.get("active_specialist"),
                "proposal_count": len(_listish(state.get("proposals"))),
                "evidence_count": len(_listish(state.get("evidence_log"))),
                "executed_action_count": len(_listish(state.get("executed_actions"))),
                "verification_result_count": len(_listish(state.get("verification_results"))),
            },
        )
    ]

    for index, proposal in enumerate(_listish(state.get("proposals")), start=1):
        safe_proposal = _jsonish(proposal)
        events.append(
            TraceEvent(
                event_type="noc_proposal",
                graph_id=GRAPH_ID,
                node_id="proposal_generation",
                agent_role="noc_duty",
                environment="production",
                run_id=run_id,
                trace_id=trace_id,
                summary=_proposal_summary(safe_proposal, index=index),
                payload={"phase": phase, "index": index, "proposal": safe_proposal},
            )
        )

    evidence = _listish(state.get("evidence_log"))
    if evidence:
        events.append(
            TraceEvent(
                event_type="noc_evidence_summary",
                graph_id=GRAPH_ID,
                node_id="evidence_collection",
                agent_role="noc_duty",
                environment="production",
                run_id=run_id,
                trace_id=trace_id,
                summary=f"captured {len(evidence)} evidence item(s)",
                payload={"phase": phase, "evidence_log": _jsonish(evidence)},
            )
        )

    operator_decision = state.get("operator_decision")
    if operator_decision:
        safe_decision = _jsonish(operator_decision)
        events.append(
            TraceEvent(
                event_type="noc_operator_decision",
                graph_id=GRAPH_ID,
                node_id="operator_decision",
                agent_role="operator",
                environment="production",
                run_id=run_id,
                trace_id=trace_id,
                summary=_operator_decision_summary(safe_decision, state),
                payload={
                    "phase": phase,
                    "operator_decision": safe_decision,
                    "approval_state": state.get("approval_state"),
                    "executed_actions": _jsonish(_listish(state.get("executed_actions"))),
                    "verification_results": _jsonish(_listish(state.get("verification_results"))),
                },
            )
        )

    return events


def _graph_summary(state: Mapping[str, Any], *, phase: str) -> str:
    synthesis = state.get("diagnostic_synthesis")
    if isinstance(synthesis, Mapping):
        summary = synthesis.get("incident_summary")
        if summary:
            return f"{phase}: {summary}"
    title = state.get("title") or state.get("diagnostic_summary")
    if title:
        return f"{phase}: {title}"
    incident = state.get("incident_id") or "incident"
    return f"{phase}: NOC graph completed for {incident}"


def _proposal_summary(proposal: Any, *, index: int) -> str:
    if isinstance(proposal, Mapping):
        action = proposal.get("proposed_remediation") or proposal.get("structured_actions")
        if action:
            return f"proposal {index}: {_short(action)}"
    return f"proposal {index}"


def _operator_decision_summary(decision: Any, state: Mapping[str, Any]) -> str:
    decision_value = decision.get("decision") if isinstance(decision, Mapping) else None
    approval_state = state.get("approval_state")
    if approval_state:
        return f"operator decision={decision_value or 'recorded'} approval_state={approval_state}"
    return f"operator decision={decision_value or 'recorded'}"


def _listish(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _jsonish(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonish(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonish(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def _short(value: Any) -> str:
    text = json.dumps(_jsonish(value), sort_keys=True)
    return text if len(text) <= 160 else text[:157] + "..."
