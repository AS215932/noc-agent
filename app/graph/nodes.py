from __future__ import annotations

import asyncio
from typing import Any

from langgraph.types import interrupt
from pydantic_ai.toolsets import FunctionToolset

from app.agents.triage import DiagnosticEvidence, DiagnosticSynthesis, TriageAgentDeps, build_triage_agent
from app.deps.runtime import RuntimeDeps
from app.golden_state import drift_findings_for, load_supervisor_context
from app.graph.routing import is_direct_measurement, supervisor_route
from app.graph.state import (
    ChangeProposal,
    EvidenceItem,
    IncidentSummary,
    JsonValue,
    SpecialistFinding,
    WorkflowState,
    assert_json_serializable_state,
    json_safe_model_dump,
    utc_now,
)


class NodeRunner:
    def __init__(self, runtime: RuntimeDeps):
        self.runtime = runtime

    async def correlate_and_dedupe(self, state: WorkflowState) -> dict[str, Any]:
        result = await self.runtime.incident_memory.correlate(state["resource_id"], state["normalized_alert"])
        update = {
            "current_step": "correlate_and_dedupe",
            "updated_at": utc_now(),
            "deduped": result["deduped"],
            "correlation_key": state["resource_id"],
            "incident_history": result["history"],
            "history_summary": {"count": len(result["history"]), "chronic": result["chronic"]},
            "chronic_instability": result["chronic"],
        }
        assert_json_serializable_state(update)
        return update

    async def recall_history(self, state: WorkflowState) -> dict[str, Any]:
        history = await self.runtime.incident_memory.history_for(state["resource_id"])
        update = {
            "current_step": "recall_history",
            "updated_at": utc_now(),
            "incident_history": history,
            "history_summary": {"count": len(history), "chronic": len(history) > 3},
            "chronic_instability": len(history) > 3,
        }
        assert_json_serializable_state(update)
        return update

    async def supervisor_route(self, state: WorkflowState) -> dict[str, Any]:
        update = {"current_step": "supervisor_route", "updated_at": utc_now(), **supervisor_route(state)}
        assert_json_serializable_state(update)
        return update

    async def bgp_specialist(self, state: WorkflowState) -> dict[str, Any]:
        return await self._run_specialist(state, "bgp")

    async def firewall_specialist(self, state: WorkflowState) -> dict[str, Any]:
        return await self._run_specialist(state, "security_firewall")

    async def infrastructure_specialist(self, state: WorkflowState) -> dict[str, Any]:
        return await self._run_specialist(state, "infrastructure")

    async def _run_specialist(self, state: WorkflowState, specialist: str) -> dict[str, Any]:
        perimeter = self.runtime.perimeter_context.prompt_block() if self.runtime.perimeter_context else ""
        case_context = state.get("case_context") or {}
        prompt = (
            f"{load_supervisor_context()}\n\n"
            f"{perimeter}\n\n"
            f"Case context (dynamic intake state, not standalone evidence):\n{case_context}\n\n"
            "When proposing remediation, separate safe/automatable follow-up from high-impact actions that need approval. "
            "Mention chronic 7-day behavior when it is present in case context. "
            "If no remediation proposal is safe, explain exactly what evidence is missing.\n"
            f"Active specialist: {specialist}\n"
            f"Resource: {state['resource_id']}\n"
            f"Chronic instability: {state.get('chronic_instability', False)}\n"
            f"Investigate this normalized alert payload and return DiagnosticSynthesis:\n"
            f"{state['normalized_alert']}"
        )
        agent = build_triage_agent()
        toolsets = self.runtime.mcp_runtime.toolsets_for(specialist) if self.runtime.mcp_runtime is not None else []
        toolsets = list(toolsets)
        toolsets.append(self._case_link_toolset(state))
        result = await agent.run(
            prompt,
            model=self.runtime.model_override,
            deps=TriageAgentDeps(perimeter_context=perimeter),
            toolsets=toolsets,
        )
        synthesis = result.data if hasattr(result, "data") else result.output
        evidence = [_evidence_item(item) for item in synthesis.evidence_chain]
        finding = SpecialistFinding(
            specialist=specialist,
            summary=synthesis.incident_summary,
            assessment=_synthesis_assessment(synthesis),
            confidence=synthesis.confidence_score,
            evidence=evidence,
        )
        update = {
            "current_step": f"{specialist}_specialist",
            "updated_at": utc_now(),
            "active_specialist": specialist,
            "specialist_type": specialist,
            "diagnostic_synthesis": synthesis.model_dump(mode="json"),
            "specialist_finding": json_safe_model_dump(finding),
            "evidence_log": [json_safe_model_dump(item) for item in evidence],
            "perimeter_context_version": self.runtime.perimeter_context.schema_version if self.runtime.perimeter_context else "",
            "manifest_hash": self.runtime.perimeter_context.manifest_hash if self.runtime.perimeter_context else "",
        }
        assert_json_serializable_state(update)
        return update

    async def evidence_validation(self, state: WorkflowState) -> dict[str, Any]:
        finding = SpecialistFinding.model_validate(state["specialist_finding"])
        direct = any(item.direct_measurement for item in finding.evidence)
        confidence = finding.confidence
        if confidence > 0.8 and not direct:
            confidence = 0.8
        if not direct and confidence > 0.5:
            confidence = 0.5
        finding.confidence = confidence
        synthesis = DiagnosticSynthesis.model_validate(state["diagnostic_synthesis"])
        synthesis.confidence_score = confidence
        update = {
            "current_step": "evidence_validation",
            "updated_at": utc_now(),
            "specialist_finding": json_safe_model_dump(finding),
            "diagnostic_synthesis": synthesis.model_dump(mode="json"),
        }
        assert_json_serializable_state(update)
        return update

    async def golden_state_drift_check(self, state: WorkflowState) -> dict[str, Any]:
        telemetry = dict(state.get("telemetry_cache") or {})
        findings = drift_findings_for(state["resource_id"], telemetry)
        update = {"current_step": "golden_state_drift_check", "updated_at": utc_now(), "drift_findings": findings}
        assert_json_serializable_state(update)
        return update

    async def proposal_build(self, state: WorkflowState) -> dict[str, Any]:
        finding = SpecialistFinding.model_validate(state["specialist_finding"])
        synthesis = DiagnosticSynthesis.model_validate(state["diagnostic_synthesis"])
        remediation = synthesis.remediation_proposal
        proposed = remediation.proposed_actions if remediation is not None else synthesis.recommended_next_checks
        evidence_refs = remediation.evidence_refs if remediation is not None else [item.tool for item in finding.evidence]
        proposal = ChangeProposal(
            incident_id=state["incident_id"],
            resource_id=state["resource_id"],
            assessment=finding.summary,
            root_cause_hypothesis=finding.assessment,
            confidence=finding.confidence,
            evidence_refs=evidence_refs,
            drift_findings=list(state.get("drift_findings", [])),
            proposed_remediation=list(proposed),
            validation_status="needs_more_evidence" if finding.confidence < 0.8 else "validated",
            human_review_rationale=synthesis.human_escalation_reason or "Diagnostic tranche requires human review before any execution.",
        )
        update = {
            "current_step": "proposal_build",
            "updated_at": utc_now(),
            "proposals": [json_safe_model_dump(proposal)],
        }
        assert_json_serializable_state(update)
        return update

    async def prepare_approval(self, state: WorkflowState) -> dict[str, Any]:
        proposal_count = len(state.get("proposals", []))
        summary = IncidentSummary(
            incident_id=state["incident_id"],
            case_number=state.get("case_number", ""),
            resource_id=state["resource_id"],
            title=DiagnosticSynthesis.model_validate(state["diagnostic_synthesis"]).incident_summary,
            status="waiting_approval",
            chronic_instability=bool(state.get("chronic_instability", False)),
            active_specialist=state.get("active_specialist"),
            proposal_count=proposal_count,
        ).model_dump(mode="json")
        summary["thread_id"] = state["thread_id"]
        summary["proposals"] = list(state.get("proposals", []))
        summary["fingerprint"] = state.get("fingerprint", "")
        summary["event_count"] = state.get("case_event_count", 0)
        summary["case_context"] = state.get("case_context", {})
        await self.runtime.incident_memory.put_summary(state["incident_id"], summary)
        await self.runtime.incident_memory.update_case(
            state["incident_id"],
            {
                "status": "waiting_approval",
                "summary": summary,
                "thread_id": state["thread_id"],
                "diagnostic_summary": summary["title"],
            },
        )
        update = {
            "current_step": "prepare_approval",
            "updated_at": utc_now(),
            "approval_state": "waiting_approval",
            "final_summary": summary,
        }
        assert_json_serializable_state(update)
        return update

    async def execute_approved_remediation(self, state: WorkflowState) -> dict[str, Any]:
        decision = state.get("operator_decision") or {}
        if decision.get("decision") != "approved":
            update = {"current_step": "execute_approved_remediation", "updated_at": utc_now(), "approval_state": decision.get("decision", "acknowledged")}
            assert_json_serializable_state(update)
            return update

        actions = _approved_restart_actions(state)
        results = []
        for action in actions:
            try:
                result = await self.runtime.mcp_runtime.call_tool(
                    "hyrule",
                    "os_service_restart",
                    {"host": action["host"], "service": action["service"]},
                )
            except Exception as exc:
                result = {"ok": False, "error_type": type(exc).__name__, "sanitized_error": str(exc)}
            results.append({"action": action, "result": _jsonish(result), "ok": bool(isinstance(result, dict) and result.get("ok"))})

        if not actions:
            approval_state = "approved_no_executable_action"
        elif all(item["ok"] for item in results):
            approval_state = "executed"
        else:
            approval_state = "execution_failed"
        update = {
            "current_step": "execute_approved_remediation",
            "updated_at": utc_now(),
            "approval_state": approval_state,
            "executed_actions": list(state.get("executed_actions", [])) + results,
        }
        assert_json_serializable_state(update)
        return update

    async def verify_remediation(self, state: WorkflowState) -> dict[str, Any]:
        executed = [item for item in state.get("executed_actions", []) if item.get("ok")]
        results = []
        for item in executed:
            action = item.get("action") or {}
            host = str(action.get("host") or "")
            instance = _prometheus_instance_for(host)
            if not instance:
                results.append({"host": host, "ok": False, "error": "No Prometheus instance mapping for host", "attempts": []})
                continue
            query = f'up{{instance="{instance}"}}'
            attempts = []
            verified = False
            for attempt in range(1, 4):
                await asyncio.sleep(15)
                try:
                    response = await self.runtime.mcp_runtime.call_tool("hyrule", "prometheus_query", {"query": query})
                except Exception as exc:
                    response = {"ok": False, "error_type": type(exc).__name__, "sanitized_error": str(exc)}
                ok = _prometheus_up(response)
                attempts.append({"attempt": attempt, "query": query, "ok": ok, "response": _jsonish(response)})
                if ok:
                    verified = True
                    break
            results.append({"host": host, "instance": instance, "ok": verified, "attempts": attempts})

        if not executed:
            approval_state = state.get("approval_state", "approved_no_executable_action")
        elif all(item.get("ok") for item in results):
            approval_state = "verified"
        else:
            approval_state = "verification_failed"
        update = {
            "current_step": "verify_remediation",
            "updated_at": utc_now(),
            "approval_state": approval_state,
            "verification_results": list(state.get("verification_results", [])) + results,
        }
        assert_json_serializable_state(update)
        return update

    def _case_link_toolset(self, state: WorkflowState) -> FunctionToolset:
        async def link_to_parent_case(child_case_id: str, parent_case_id: str, reason: str, evidence_refs: list[str] | None = None) -> dict[str, Any]:
            """Link a downstream victim case to an upstream parent NOC case."""
            if child_case_id != state["incident_id"]:
                return {"ok": False, "error": "child_case_id must match the active case"}
            return await self.runtime.incident_memory.link_to_parent_case(
                child_case_id,
                parent_case_id,
                reason,
                evidence_refs or [],
            )

        return FunctionToolset([link_to_parent_case])

    async def approval_interrupt(self, state: WorkflowState) -> dict[str, Any]:
        decision = interrupt({"incident_id": state["incident_id"], "approval_state": "waiting_approval"})
        update = {
            "current_step": "approval_interrupt",
            "updated_at": utc_now(),
            "operator_decision": decision,
            "approval_state": decision.get("decision", "acknowledged") if isinstance(decision, dict) else "acknowledged",
        }
        assert_json_serializable_state(update)
        return update


def _evidence_item(item: DiagnosticEvidence) -> EvidenceItem:
    evidence_id = item.evidence_id or item.tool or "agent_observation"
    summary_parts = [
        f"{item.tool}({item.target})" if item.tool or item.target else evidence_id,
        item.interpretation or item.observed_value or "Agent reported diagnostic evidence.",
    ]
    return EvidenceItem(
        tool=evidence_id,
        summary=": ".join(part for part in summary_parts if part),
        direct_measurement=item.direct_measurement or is_direct_measurement(item.tool),
        payload=item.model_dump(mode="json"),
    )


def _synthesis_assessment(synthesis: DiagnosticSynthesis) -> str:
    sections: list[str] = []
    if synthesis.confirmed_facts:
        sections.append("Facts: " + "; ".join(fact.statement for fact in synthesis.confirmed_facts if fact.statement))
    if synthesis.deltas:
        sections.append(
            "Deltas: "
            + "; ".join(
                f"{delta.subject} {delta.attribute}: expected {delta.expected_value}, observed {delta.observed_value}"
                for delta in synthesis.deltas
                if delta.subject or delta.attribute or delta.expected_value or delta.observed_value
            )
        )
    if synthesis.hypotheses:
        sections.append("Hypotheses: " + "; ".join(h.statement for h in synthesis.hypotheses if h.statement))
    if synthesis.contradictions:
        sections.append("Contradictions: " + "; ".join(c.summary for c in synthesis.contradictions if c.summary))
    if synthesis.confidence_basis:
        sections.append("Confidence basis: " + synthesis.confidence_basis)
    return "\n".join(section for section in sections if section).strip() or synthesis.incident_summary


_ROUTER_ALIASES = {
    "cr1-nl1": "cr1-nl1",
    "cr1.nl1": "cr1-nl1",
    "cr1_nl1": "cr1-nl1",
    "cr1-de1": "cr1-de1",
    "cr1.de1": "cr1-de1",
    "cr1_de1": "cr1-de1",
}

_PROMETHEUS_NODE_INSTANCES = {
    "cr1-nl1": "[2a0c:b641:b50::a]:9100",
    "cr1-de1": "[2a0c:b641:b50::b]:9100",
}


def _approved_restart_actions(state: WorkflowState) -> list[dict[str, str]]:
    synthesis = DiagnosticSynthesis.model_validate(state["diagnostic_synthesis"])
    text = " ".join(
        [
            synthesis.incident_summary,
            " ".join(synthesis.affected_objects),
            _flatten_text(state.get("proposals", [])),
            _flatten_text(state.get("normalized_alert", {})),
        ]
    ).lower()
    if "restart" not in text or "node_exporter" not in text:
        return []
    hosts = []
    for alias, canonical in _ROUTER_ALIASES.items():
        if alias in text and canonical not in hosts:
            hosts.append(canonical)
    return [{"type": "restart_service", "host": host, "service": "node_exporter"} for host in hosts]


def _prometheus_instance_for(host: str) -> str | None:
    return _PROMETHEUS_NODE_INSTANCES.get(_ROUTER_ALIASES.get(host, host))


def _prometheus_up(response: Any) -> bool:
    if not isinstance(response, dict) or not response.get("ok", False):
        return False
    rows = response.get("result") or (response.get("data") or {}).get("result") or []
    for row in rows:
        value = row.get("value") if isinstance(row, dict) else None
        if str(value) == "1":
            return True
    return False


def _flatten_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(item) for item in value)
    return str(value)


def _jsonish(value: Any) -> JsonValue:
    import json

    return json.loads(json.dumps(value, default=str))
