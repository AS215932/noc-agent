from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from app.agents.triage import ActionPlan, TriageAgentDeps, build_triage_agent
from app.deps.runtime import RuntimeDeps
from app.golden_state import drift_findings_for, load_supervisor_context
from app.graph.routing import is_direct_measurement, supervisor_route
from app.graph.state import (
    ChangeProposal,
    EvidenceItem,
    IncidentSummary,
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
        prompt = (
            f"{load_supervisor_context()}\n\n"
            f"{perimeter}\n\n"
            f"Active specialist: {specialist}\n"
            f"Resource: {state['resource_id']}\n"
            f"Chronic instability: {state.get('chronic_instability', False)}\n"
            f"Investigate this normalized alert payload and return the structured action plan:\n"
            f"{state['normalized_alert']}"
        )
        agent = build_triage_agent()
        toolsets = self.runtime.mcp_runtime.toolsets_for(specialist) if self.runtime.mcp_runtime is not None else []
        result = await agent.run(
            prompt,
            model=self.runtime.model_override,
            deps=TriageAgentDeps(perimeter_context=perimeter),
            toolsets=toolsets,
        )
        plan = result.data if hasattr(result, "data") else result.output
        evidence = [
            EvidenceItem(
                tool=item if item else "agent_observation",
                summary=item if item else "Agent reported diagnostic evidence.",
                direct_measurement=is_direct_measurement(item),
            )
            for item in plan.tools_used or plan.diagnostic_evidence
        ]
        finding = SpecialistFinding(
            specialist=specialist,
            summary=plan.issue_summary,
            assessment=plan.root_cause_analysis,
            confidence=plan.confidence_score,
            evidence=evidence,
        )
        update = {
            "current_step": f"{specialist}_specialist",
            "updated_at": utc_now(),
            "active_specialist": specialist,
            "specialist_type": specialist,
            "action_plan": plan.model_dump(mode="json"),
            "legacy_action_plan": plan.model_dump(mode="json"),
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
        legacy = ActionPlan.model_validate(state["action_plan"])
        legacy.confidence_score = confidence
        update = {
            "current_step": "evidence_validation",
            "updated_at": utc_now(),
            "specialist_finding": json_safe_model_dump(finding),
            "action_plan": legacy.model_dump(mode="json"),
            "legacy_action_plan": legacy.model_dump(mode="json"),
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
        legacy = ActionPlan.model_validate(state["action_plan"])
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
            resource_id=state["resource_id"],
            title=ActionPlan.model_validate(state["action_plan"]).issue_summary,
            status="waiting_approval",
            chronic_instability=bool(state.get("chronic_instability", False)),
            active_specialist=state.get("active_specialist"),
            proposal_count=proposal_count,
        ).model_dump(mode="json")
        summary["thread_id"] = state["thread_id"]
        summary["proposals"] = list(state.get("proposals", []))
        await self.runtime.incident_memory.put_summary(state["incident_id"], summary)
        update = {
            "current_step": "prepare_approval",
            "updated_at": utc_now(),
            "approval_state": "waiting_approval",
            "final_summary": summary,
        }
        assert_json_serializable_state(update)
        return update

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
