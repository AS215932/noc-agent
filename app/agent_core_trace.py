"""Best-effort agent-core TraceEvent emission for NOC graph runs."""

from __future__ import annotations

import hashlib
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
    summary_event = TraceEvent(
        **_correlation_fields(state),
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
    events: list[Any] = [summary_event]
    parent_event_id = summary_event.event_id

    for index, proposal in enumerate(_listish(state.get("proposals")), start=1):
        safe_proposal = _jsonish(proposal)
        events.append(
            TraceEvent(
                **_correlation_fields(state),
                event_type="noc_proposal",
                graph_id=GRAPH_ID,
                node_id="proposal_generation",
                agent_role="noc_duty",
                environment="production",
                run_id=run_id,
                trace_id=trace_id,
                parent_event_id=parent_event_id,
                summary=_proposal_summary(safe_proposal, index=index),
                payload={"phase": phase, "index": index, "proposal": safe_proposal},
            )
        )

    evidence = _listish(state.get("evidence_log"))
    if evidence:
        events.append(
            TraceEvent(
                **_correlation_fields(state),
                event_type="noc_evidence_summary",
                graph_id=GRAPH_ID,
                node_id="evidence_collection",
                agent_role="noc_duty",
                environment="production",
                run_id=run_id,
                trace_id=trace_id,
                parent_event_id=parent_event_id,
                summary=f"captured {len(evidence)} evidence item(s)",
                payload={"phase": phase, "evidence_log": _jsonish(evidence)},
            )
        )

    operator_decision = state.get("operator_decision")
    if operator_decision:
        safe_decision = _jsonish(operator_decision)
        decision_event = TraceEvent(
            **_correlation_fields(state),
            event_type="noc_operator_decision",
            graph_id=GRAPH_ID,
            node_id="operator_decision",
            agent_role="operator",
            environment="production",
            run_id=run_id,
            trace_id=trace_id,
            parent_event_id=parent_event_id,
            summary=_operator_decision_summary(safe_decision, state),
            payload={
                "phase": phase,
                "operator_decision": safe_decision,
                "approval_state": state.get("approval_state"),
                "executed_actions": _jsonish(_listish(state.get("executed_actions"))),
                "verification_results": _jsonish(_listish(state.get("verification_results"))),
            },
        )
        events.append(decision_event)
        parent_event_id = decision_event.event_id

    for index, action in enumerate(_listish(state.get("executed_actions")), start=1):
        safe_action = _jsonish(action)
        action_fields = _correlation_fields(state, item=safe_action)
        action_event = TraceEvent(
            **action_fields,
            event_type="noc_executed_action",
            graph_id=GRAPH_ID,
            node_id="remediation_execution",
            agent_role="noc_duty",
            environment="production",
            run_id=run_id,
            trace_id=trace_id,
            parent_event_id=parent_event_id,
            summary=_action_summary(safe_action, index=index),
            payload={
                "phase": phase,
                "index": index,
                "action": safe_action,
                "untrusted_loop_text": True,
                "model_consumption_allowed": False,
            },
        )
        events.append(action_event)

    for index, result in enumerate(_listish(state.get("verification_results")), start=1):
        safe_result = _jsonish(result)
        result_fields = _correlation_fields(state, item=safe_result)
        result_event = TraceEvent(
            **result_fields,
            event_type="noc_verification_result",
            graph_id=GRAPH_ID,
            node_id="verification",
            agent_role="noc_duty",
            environment="production",
            run_id=run_id,
            trace_id=trace_id,
            parent_event_id=parent_event_id,
            summary=_verification_summary(safe_result, index=index),
            payload={
                "phase": phase,
                "index": index,
                "verification_result": safe_result,
                "untrusted_loop_text": True,
                "model_consumption_allowed": False,
            },
        )
        events.append(result_event)

    envelope = _loop_decision_envelope(state, phase=phase, run_id=run_id, trace_id=trace_id)
    events.append(
        TraceEvent(
            **_correlation_fields(state),
            event_type="loop_decision_envelope",
            graph_id=GRAPH_ID,
            node_id="loop_decision_envelope",
            agent_role="noc_duty",
            environment="production",
            run_id=run_id,
            trace_id=trace_id,
            parent_event_id=parent_event_id,
            summary=f"NOC loop decision envelope for {run_id or 'incident'}",
            payload={
                "phase": phase,
                "loop_decision_envelope": envelope.model_dump(mode="json"),
            },
        )
    )

    return events


def _loop_decision_envelope(state: Mapping[str, Any], *, phase: str, run_id: str | None, trace_id: str | None) -> Any:
    contracts = importlib.import_module("agent_core.contracts")
    LoopDecisionEnvelope = getattr(contracts, "LoopDecisionEnvelope")
    GovernanceControls = getattr(contracts, "GovernanceControls")

    evidence_refs = _source_refs_from_evidence(_listish(state.get("evidence_log")))
    proposals = _listish(state.get("proposals"))
    executed_actions = _listish(state.get("executed_actions"))
    verification_results = _listish(state.get("verification_results"))
    operator_decision = state.get("operator_decision")
    safe_phase = _safe_token(phase, limit=64) or "unknown"
    safe_run_id = _safe_token(run_id)
    safe_trace_id = _safe_token(trace_id)
    case_id = _safe_token(state.get("case_id") or state.get("incident_id"))
    fingerprint = _safe_token(state.get("fingerprint") or state.get("resource_id") or case_id) or ""
    decision = _decision_from_state(state)
    proposed_action: dict[str, Any] = {
        "proposal_count": len(proposals),
        "executed_action_count": len(executed_actions),
        "verification_result_count": len(verification_results),
    }
    first_proposal = proposals[0] if proposals else None
    if isinstance(first_proposal, Mapping):
        proposed_action["first_proposal"] = _proposal_summary(_jsonish(first_proposal), index=1)
    if executed_actions:
        proposed_action["action_statuses"] = [
            _status_from_mapping(action) for action in executed_actions if isinstance(action, Mapping)
        ][:10]
    human_outcome = {}
    if isinstance(operator_decision, Mapping):
        human_outcome = {
            "decision": _string_or_none(operator_decision.get("decision")),
            "operator": _string_or_none(operator_decision.get("operator")),
            "status": _string_or_none(state.get("approval_state")),
        }
    return LoopDecisionEnvelope(
        envelope_id=f"ldec_noc_{_stable_hash([safe_run_id, safe_phase, fingerprint, decision])}",
        loop="noc",
        environment="production",
        graph_id=GRAPH_ID,
        node_id="graph_runtime",
        agent_role="noc_duty",
        run_id=safe_run_id,
        trace_id=safe_trace_id,
        input_event={
            "phase": safe_phase,
            "incident_id": _safe_token(state.get("incident_id")),
            "case_number": _safe_token(state.get("case_number"), limit=80),
            "resource_id": _safe_token(state.get("resource_id")),
            "approval_state": _safe_token(state.get("approval_state"), limit=80),
            "untrusted_loop_text": True,
            "model_consumption_allowed": False,
        },
        retrieved_context=evidence_refs,
        decision=decision,
        evidence_refs=evidence_refs,
        proposed_action=proposed_action,
        human_outcome={key: value for key, value in human_outcome.items() if value},
        governance=GovernanceControls(
            sensitivity_class="internal",
            approval_tier="operator" if proposals or executed_actions else "none",
            risk_class="medium",
            learning_allowed=True,
            never_learn=False,
            policy_ids=["noc-loop-decision.v1"],
            rationale="NOC graph decisions are emitted as sanitized loop decision envelopes for replay.",
        ),
        case_id=case_id,
        fingerprint=fingerprint,
        policy_version="noc-loop-decision.v1",
    )


def _decision_from_state(state: Mapping[str, Any]) -> str:
    if _listish(state.get("proposals")) or _listish(state.get("executed_actions")):
        return "draft"
    if str(state.get("approval_state") or "").lower() in {"waiting_approval", "pending"}:
        return "question"
    if _listish(state.get("evidence_log")):
        return "notify"
    return "stay_silent"


def _source_refs_from_evidence(evidence: list[Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for index, item in enumerate(evidence[:12], start=1):
        if isinstance(item, Mapping):
            ref = _safe_text(
                item.get("ref")
                or item.get("evidence_id")
                or item.get("source")
                or item.get("tool")
                or item.get("summary")
                or f"noc:evidence:{index}",
                limit=180,
            )
            kind = _safe_text(item.get("kind") or item.get("source") or "runtime_evidence", limit=64)
        else:
            ref = _safe_text(item, limit=180) or f"noc:evidence:{index}"
            kind = "runtime_evidence"
        if ref:
            refs.append({"kind": kind, "ref": ref})
    return refs


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


def _action_summary(action: Any, *, index: int) -> str:
    if isinstance(action, Mapping):
        name = _safe_text(
            action.get("action") or action.get("action_class") or action.get("command") or "action"
        )
        status = _safe_text(_status_from_mapping(action))
        return f"action {index}: {name} {status}"
    return f"action {index}"


def _verification_summary(result: Any, *, index: int) -> str:
    if isinstance(result, Mapping):
        check = _safe_text(
            result.get("check") or result.get("objective_id") or result.get("event") or "verification"
        )
        status = _safe_text(_status_from_mapping(result))
        return f"verification {index}: {check} {status}"
    return f"verification {index}"


def _correlation_fields(state: Mapping[str, Any], *, item: Any | None = None) -> dict[str, Any]:
    item_mapping = item if isinstance(item, Mapping) else {}
    case_id = _safe_token(
        item_mapping.get("case_id")
        if item_mapping.get("case_id") is not None
        else state.get("case_id") or state.get("incident_id")
    )
    handoff_id = _safe_token(
        item_mapping.get("handoff_id")
        if item_mapping.get("handoff_id") is not None
        else state.get("handoff_id")
    )
    objective_id = _safe_token(
        item_mapping.get("objective_id")
        if item_mapping.get("objective_id") is not None
        else state.get("objective_id")
    )
    links = []
    if case_id:
        links.append({"kind": "case", "label": "NOC case", "ref_id": case_id})
    if handoff_id:
        links.append({"kind": "handoff", "label": "Loop handoff", "ref_id": handoff_id})
    if objective_id:
        links.append({"kind": "verification", "label": "Verification objective", "ref_id": objective_id})
    return {"case_id": case_id, "handoff_id": handoff_id, "objective_id": objective_id, "links": links}


def _status_from_mapping(value: Mapping[str, Any]) -> str:
    if value.get("status"):
        return str(value["status"])
    ok = value.get("ok")
    if ok is True:
        return "ok"
    if ok is False:
        return "failed"
    return "recorded"


def _safe_text(value: Any, *, limit: int = 120) -> str:
    text = _string_or_none(value) or ""
    text = " ".join(text.split())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _safe_token(value: Any, *, limit: int = 180) -> str | None:
    text = _safe_text(value, limit=limit)
    if not text:
        return None
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:/@+-")
    return "".join(char if char in allowed else "_" for char in text)


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


def _stable_hash(parts: list[Any]) -> str:
    payload = json.dumps(_jsonish(parts), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
