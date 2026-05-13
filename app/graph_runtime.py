from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from app.agent import ActionPlan, noc_triage_agent
from app.golden_state import drift_findings_for, load_supervisor_context
from app.incident_memory import IncidentMemory
from app.noc_state import ChangeProposal, EvidenceItem, IncidentSummary, NOCState, SpecialistFinding

try:  # pragma: no cover - import fallback is exercised only in stripped dev envs
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, StateGraph
except Exception:  # pragma: no cover
    InMemorySaver = None
    StateGraph = None
    START = END = None

try:  # pragma: no cover - depends on optional production Redis stack
    from langgraph.checkpoint.redis import RedisSaver
except Exception:  # pragma: no cover
    RedisSaver = None


INCIDENT_MEMORY = IncidentMemory()
_GRAPH = None


def resource_id_from_alert(alert_payload: dict[str, Any]) -> str:
    labels = _labels(alert_payload)
    host = labels.get("host") or labels.get("hostname")
    if host:
        return str(host)
    instance = labels.get("instance")
    if instance:
        return _instance_host(str(instance))
    return str(alert_payload.get("source") or "unknown")


async def run_investigation_graph(alert_payload: dict[str, Any], model=None) -> tuple[ActionPlan, NOCState]:
    state: NOCState = {
        "incident_id": str(uuid4()),
        "thread_id": str(uuid4()),
        "normalized_alert": alert_payload,
        "resource_id": resource_id_from_alert(alert_payload),
        "related_alerts": [alert_payload],
        "telemetry_cache": {},
        "evidence_log": [],
        "proposals": [],
        "approval_state": "pending",
        "operator_decision": None,
        "drift_findings": [],
    }

    graph = _graph()
    if graph is None:
        state = await _run_fallback(state, model=model)
    else:
        state = await graph.ainvoke(
            {**state, "model_override": model},
            {"configurable": {"thread_id": state["thread_id"]}},
        )

    plan = ActionPlan.model_validate(state["legacy_action_plan"])
    return plan, state


def pending_summaries() -> list[dict[str, Any]]:
    return INCIDENT_MEMORY.list_summaries()


def summary_for(incident_id: str) -> dict[str, Any] | None:
    return INCIDENT_MEMORY.get_summary(incident_id)


def record_operator_decision(incident_id: str, decision: dict[str, Any]) -> dict[str, Any] | None:
    summary = INCIDENT_MEMORY.get_summary(incident_id)
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
    INCIDENT_MEMORY.put_summary(incident_id, summary)
    return summary


def _graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    if StateGraph is None:
        return None

    workflow = StateGraph(NOCState)
    workflow.add_node("correlate_and_dedupe", _correlate_and_dedupe)
    workflow.add_node("recall_history", _recall_history)
    workflow.add_node("supervisor_route", _supervisor_route)
    workflow.add_node("run_specialist", _run_specialist)
    workflow.add_node("evidence_validation", _evidence_validation)
    workflow.add_node("golden_state_drift_check", _golden_state_drift_check)
    workflow.add_node("proposal_build", _proposal_build)
    workflow.add_node("approval_breakpoint", _approval_breakpoint)
    workflow.add_edge(START, "correlate_and_dedupe")
    workflow.add_edge("correlate_and_dedupe", "recall_history")
    workflow.add_edge("recall_history", "supervisor_route")
    workflow.add_edge("supervisor_route", "run_specialist")
    workflow.add_edge("run_specialist", "evidence_validation")
    workflow.add_edge("evidence_validation", "golden_state_drift_check")
    workflow.add_edge("golden_state_drift_check", "proposal_build")
    workflow.add_edge("proposal_build", "approval_breakpoint")
    workflow.add_edge("approval_breakpoint", END)

    checkpointer = _build_checkpointer()
    _GRAPH = workflow.compile(checkpointer=checkpointer)
    return _GRAPH


def _build_checkpointer():
    redis_url = os.getenv("NOC_REDIS_URL", "").strip()
    if redis_url and RedisSaver is not None:
        try:
            saver = RedisSaver.from_conn_string(redis_url)
            setup = getattr(saver, "setup", None)
            if callable(setup):
                setup()
            return saver
        except Exception:
            pass
    return InMemorySaver() if InMemorySaver is not None else None


async def _run_fallback(state: NOCState, model=None) -> NOCState:
    for node in (
        _correlate_and_dedupe,
        _recall_history,
        _supervisor_route,
    ):
        state.update(await _maybe_await(node(state)))
    state.update(await _run_specialist({**state, "model_override": model}))
    for node in (
        _evidence_validation,
        _golden_state_drift_check,
        _proposal_build,
        _approval_breakpoint,
    ):
        state.update(await _maybe_await(node(state)))
    return state


def _correlate_and_dedupe(state: NOCState) -> dict[str, Any]:
    result = INCIDENT_MEMORY.correlate(state["resource_id"], state["normalized_alert"])
    return {
        "deduped": result["deduped"],
        "correlation_key": state["resource_id"],
        "incident_history": result["history"],
        "chronic_instability": result["chronic"],
    }


def _recall_history(state: NOCState) -> dict[str, Any]:
    history = INCIDENT_MEMORY.history_for(state["resource_id"])
    return {"incident_history": history, "chronic_instability": len(history) > 3}


def _supervisor_route(state: NOCState) -> dict[str, Any]:
    labels = _labels(state["normalized_alert"])
    blob = " ".join(str(value) for value in [labels, state["normalized_alert"]]).lower()
    if any(token in blob for token in ("bgp", "peer", "route", "frr", "ospf")):
        specialist = "bgp"
        reason = "Routing keywords in the alert payload indicate a control-plane investigation."
    elif any(token in blob for token in ("pf", "nft", "firewall", "icmp unreachable", "drop", "blocked")):
        specialist = "security_firewall"
        reason = "Firewall or packet-filter symptoms dominate the alert payload."
    else:
        specialist = "infrastructure"
        reason = "The alert is host/service oriented rather than routing-specific."
    return {"active_specialist": specialist, "routing_reason": reason}


async def _run_specialist(state: NOCState) -> dict[str, Any]:
    prompt = (
        f"{load_supervisor_context()}\n\n"
        f"Active specialist: {state['active_specialist']}\n"
        f"Resource: {state['resource_id']}\n"
        f"Chronic instability: {state.get('chronic_instability', False)}\n"
        f"Investigate this normalized alert payload and return the structured action plan:\n"
        f"{state['normalized_alert']}"
    )
    model = state.get("model_override")
    result = await noc_triage_agent.run(prompt, model=model)
    plan = result.data if hasattr(result, "data") else result.output
    evidence = [
        EvidenceItem(
            tool=item if item else "agent_observation",
            summary=item if item else "Agent reported diagnostic evidence.",
            direct_measurement=_is_direct_measurement(item),
        )
        for item in plan.tools_used or plan.diagnostic_evidence
    ]
    finding = SpecialistFinding(
        specialist=state["active_specialist"],
        summary=plan.issue_summary,
        assessment=plan.root_cause_analysis,
        confidence=plan.confidence_score,
        evidence=evidence,
    )
    return {
        "legacy_action_plan": plan.model_dump(),
        "specialist_finding": finding.model_dump(),
        "evidence_log": [item.model_dump() for item in evidence],
    }


def _evidence_validation(state: NOCState) -> dict[str, Any]:
    finding = SpecialistFinding.model_validate(state["specialist_finding"])
    direct = any(item.direct_measurement for item in finding.evidence)
    confidence = finding.confidence
    if confidence > 0.8 and not direct:
        confidence = 0.8
    if not direct and confidence > 0.5:
        confidence = 0.5
    finding.confidence = confidence
    legacy = ActionPlan.model_validate(state["legacy_action_plan"])
    legacy.confidence_score = confidence
    return {
        "specialist_finding": finding.model_dump(),
        "legacy_action_plan": legacy.model_dump(),
    }


def _golden_state_drift_check(state: NOCState) -> dict[str, Any]:
    telemetry = dict(state.get("telemetry_cache") or {})
    findings = drift_findings_for(state["resource_id"], telemetry)
    return {"drift_findings": findings}


def _proposal_build(state: NOCState) -> dict[str, Any]:
    finding = SpecialistFinding.model_validate(state["specialist_finding"])
    legacy = ActionPlan.model_validate(state["legacy_action_plan"])
    proposal = ChangeProposal(
        incident_id=state["incident_id"],
        resource_id=state["resource_id"],
        assessment=finding.summary,
        root_cause_hypothesis=finding.assessment,
        confidence=finding.confidence,
        evidence_refs=[item.tool for item in finding.evidence],
        drift_findings=list(state.get("drift_findings", [])),
        proposed_remediation=list(legacy.automated_actions_proposed or legacy.operator_next_steps),
        validation_status="needs_more_evidence" if finding.confidence < 0.8 else "validated",
        human_review_rationale=legacy.human_escalation_reason or "Diagnostic tranche requires human review before any execution.",
    )
    return {"proposals": [proposal.model_dump()]}


def _approval_breakpoint(state: NOCState) -> dict[str, Any]:
    proposal_count = len(state.get("proposals", []))
    summary = IncidentSummary(
        incident_id=state["incident_id"],
        resource_id=state["resource_id"],
        title=ActionPlan.model_validate(state["legacy_action_plan"]).issue_summary,
        status="waiting_approval",
        chronic_instability=bool(state.get("chronic_instability", False)),
        active_specialist=state.get("active_specialist"),
        proposal_count=proposal_count,
    ).model_dump()
    summary["proposals"] = list(state.get("proposals", []))
    INCIDENT_MEMORY.put_summary(state["incident_id"], summary)
    return {"approval_state": "waiting_approval", "final_summary": summary}


def _labels(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels: dict[str, Any] = {}
    for key in ("groupLabels", "commonLabels"):
        if isinstance(alert_payload.get(key), dict):
            labels.update(alert_payload[key])
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alert_payload["alerts"][0], dict):
        nested = alert_payload["alerts"][0].get("labels")
        if isinstance(nested, dict):
            labels.update(nested)
    return labels


def _instance_host(instance: str) -> str:
    if instance.startswith("[") and "]" in instance:
        return instance[1:instance.index("]")]
    if instance.count(":") == 1:
        return instance.rsplit(":", 1)[0]
    return instance


def _is_direct_measurement(value: str) -> bool:
    lowered = str(value or "").lower()
    return any(token in lowered for token in ("tcpdump", "pflog", "nft_log", "ndp", "arp_state", "capture"))


async def _maybe_await(value):
    return await value if hasattr(value, "__await__") else value
