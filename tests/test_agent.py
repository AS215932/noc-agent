import json

import pytest
from pydantic import ValidationError
from pydantic_ai.models.test import TestModel

from app.agent import (
    ConfirmedFact,
    DiagnosticEvidence,
    DiagnosticSynthesis,
    RemediationProposal,
    StateDelta,
    noc_triage_agent,
)


pytestmark = pytest.mark.asyncio


@pytest.fixture
def test_model():
    return TestModel()


@pytest.fixture
def mock_prometheus_alert():
    return {
        "status": "firing",
        "labels": {
            "alertname": "HighDiskUsage",
            "instance": "mon.as215932.net",
            "severity": "warning",
        },
        "annotations": {
            "description": "mon.as215932.net has > 90% disk usage.",
            "summary": "Disk usage critical",
        },
    }


def valid_synthesis_args(**overrides):
    base = {
        "read_only": True,
        "incident_summary": "node exporter unreachable on rtr",
        "affected_objects": ["rtr"],
        "intended_state": [
            {"subject": "rtr", "attribute": "node_exporter", "value": "scraped", "source": "manifest"}
        ],
        "observed_state": [
            {
                "subject": "rtr",
                "attribute": "node_exporter",
                "value": "down",
                "source": "mcp_telemetry",
                "evidence_refs": ["ev1"],
            }
        ],
        "deltas": [
            {
                "delta_id": "delta1",
                "subject": "rtr",
                "attribute": "node_exporter",
                "expected_value": "scraped",
                "observed_value": "down",
                "delta_type": "hard_failure",
                "impact": "host metrics unavailable",
                "evidence_refs": ["ev1"],
            }
        ],
        "evidence_chain": [
            {
                "evidence_id": "ev1",
                "tool": "prometheus_query",
                "target": "rtr",
                "observed_value": "up == 0",
                "expected_value": "up == 1",
                "interpretation": "Prometheus cannot scrape the router exporter.",
                "direct_measurement": True,
            }
        ],
        "confirmed_facts": [
            {
                "fact_id": "fact1",
                "statement": "Prometheus reports the router exporter as down.",
                "evidence_refs": ["ev1"],
            }
        ],
        "hypotheses": [],
        "contradictions": [],
        "confidence_score": 0.8,
        "confidence_basis": "Direct Prometheus telemetry shows the scrape is down.",
        "severity": "MEDIUM",
        "recommended_next_checks": ["Check exporter service logs on rtr."],
        "remediation_proposal": None,
        "executed_actions": [],
        "requires_human": True,
        "human_escalation_reason": "Read-only diagnostic tranche requires operator review.",
    }
    base.update(overrides)
    return base


async def test_agent_returns_diagnostic_synthesis(test_model, mock_prometheus_alert):
    prompt = f"Investigate this payload: {mock_prometheus_alert}"
    result = await noc_triage_agent.run(prompt, model=test_model)

    synthesis = result.data if hasattr(result, "data") else result.output
    assert isinstance(synthesis, DiagnosticSynthesis)
    assert synthesis.read_only is True
    assert synthesis.executed_actions == []


async def test_diagnostic_synthesis_schema_is_provider_safe():
    schema = DiagnosticSynthesis.model_json_schema()
    serialized = json.dumps(schema)

    assert "DiagnosticEvidence" in schema.get("$defs", {})
    assert "discriminator" not in serialized
    assert "oneOf" not in serialized
    assert "#/$defs/DiagnosticSynthesis" not in serialized
    assert "additionalProperties" in serialized


async def test_diagnostic_synthesis_validates_evidence_references():
    synthesis = DiagnosticSynthesis.model_validate(valid_synthesis_args())

    assert synthesis.evidence_chain[0].evidence_id == "ev1"


async def test_unknown_evidence_ids_are_rejected():
    payload = valid_synthesis_args(
        confirmed_facts=[{"fact_id": "fact1", "statement": "Unsupported fact.", "evidence_refs": ["missing"]}]
    )

    with pytest.raises(ValidationError, match="unknown evidence_id"):
        DiagnosticSynthesis.model_validate(payload)


async def test_confirmed_facts_and_deltas_require_evidence_refs():
    with pytest.raises(ValidationError, match="confirmed_fact"):
        DiagnosticSynthesis.model_validate(
            valid_synthesis_args(confirmed_facts=[{"fact_id": "fact1", "statement": "Unsupported fact."}])
        )

    with pytest.raises(ValidationError, match="delta"):
        DiagnosticSynthesis.model_validate(
            valid_synthesis_args(
                deltas=[
                    {
                        "delta_id": "delta1",
                        "subject": "rtr",
                        "attribute": "node_exporter",
                        "expected_value": "up",
                        "observed_value": "down",
                    }
                ]
            )
        )


async def test_read_only_contract_is_enforced():
    with pytest.raises(ValidationError, match="read_only"):
        DiagnosticSynthesis.model_validate(valid_synthesis_args(read_only=False))

    with pytest.raises(ValidationError, match="executed_actions"):
        DiagnosticSynthesis.model_validate(valid_synthesis_args(executed_actions=["restarted service"]))


async def test_remediation_proposal_requires_human_approval_and_evidence():
    with pytest.raises(ValidationError, match="approval_required"):
        DiagnosticSynthesis.model_validate(
            valid_synthesis_args(
                remediation_proposal={
                    "summary": "Restart exporter.",
                    "proposed_actions": ["Restart node_exporter."],
                    "risk": "Temporary metric gap.",
                    "rollback": "Restart again or revert unit change.",
                    "evidence_refs": ["ev1"],
                    "approval_required": False,
                }
            )
        )

    with pytest.raises(ValidationError, match="remediation_proposal"):
        DiagnosticSynthesis.model_validate(
            valid_synthesis_args(
                remediation_proposal={
                    "summary": "Restart exporter.",
                    "proposed_actions": ["Restart node_exporter."],
                    "approval_required": True,
                }
            )
        )


async def test_high_confidence_requires_direct_measurement():
    payload = valid_synthesis_args(
        confidence_score=0.9,
        evidence_chain=[
            {
                "evidence_id": "ev1",
                "tool": "alertmanager",
                "target": "rtr",
                "observed_value": "alert firing",
                "expected_value": "no alert",
                "interpretation": "Alert text only.",
                "direct_measurement": False,
            }
        ],
    )

    with pytest.raises(ValidationError, match="direct-measurement"):
        DiagnosticSynthesis.model_validate(payload)


async def test_agent_escalates_critical_incidents():
    payload = valid_synthesis_args(
        incident_summary="BGP session down with transit provider",
        confidence_score=0.9,
        severity="HIGH",
        human_escalation_reason="BGP route changes require manual verification.",
        remediation_proposal={
            "summary": "Prepare a BGP remediation proposal.",
            "proposed_actions": ["Review peer state and prepare policy-safe remediation."],
            "risk": "Route reachability may change.",
            "rollback": "Revert approved BGP policy change.",
            "evidence_refs": ["ev1"],
            "approval_required": True,
        },
    )

    result = await noc_triage_agent.run("BGP Alert", model=TestModel(custom_output_args=payload))
    synthesis = result.data if hasattr(result, "data") else result.output

    assert synthesis.requires_human is True
    assert "BGP" in synthesis.human_escalation_reason
    assert synthesis.remediation_proposal is not None


async def test_agent_invokes_prometheus_mcp_tool():
    """
    [TDD Goal] Test that the agent uses the `prometheus_query` tool to check for
    secondary metrics when a vague alert triggers it.
    """
    pass


async def test_agent_invokes_ssh_mcp_tool_for_remediation():
    """
    [TDD Goal] Remediation must remain proposal-only and approval-gated; this
    placeholder now tracks future approved executor integration, not supervisor
    tool execution.
    """
    pass
