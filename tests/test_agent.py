import pytest
from pydantic_ai import models
from pydantic_ai.models.test import TestModel
import json

from app.agent import noc_triage_agent, ActionPlan, DiagnosisResult

# Ensure pytest-asyncio runs the tests correctly
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
            "severity": "warning"
        },
        "annotations": {
            "description": "mon.as215932.net has > 90% disk usage.",
            "summary": "Disk usage critical"
        }
    }

async def test_agent_diagnoses_and_proposes_action(test_model, mock_prometheus_alert):
    """
    Test that the agent parses the alert prompt and returns structurally valid `ActionPlan`.
    This uses TestModel which validates that the prompt works and returns random valid Pydantic fields.
    """
    prompt = f"Investigate this payload: {mock_prometheus_alert}"
    result = await noc_triage_agent.run(prompt, model=test_model)
    
    plan = result.data if hasattr(result, 'data') else result.output
    assert isinstance(plan, ActionPlan)
    assert isinstance(plan.diagnosis, DiagnosisResult)
    assert hasattr(plan, "requires_human")
    assert hasattr(plan, "automated_actions_proposed")

async def test_action_plan_schema_is_provider_friendly():
    """
    Gemini tool schemas are picky; keep the triage output schema flat so the
    model can call diagnostic tools and still return structured output.
    """
    schema = ActionPlan.model_json_schema()
    serialized = json.dumps(schema)

    assert "$defs" not in schema
    assert "$ref" not in serialized
    assert "anyOf" not in serialized

async def test_action_plan_accepts_legacy_nested_diagnosis():
    plan = ActionPlan.model_validate({
        "diagnosis": {
            "issue_summary": "node exporter down",
            "root_cause_analysis": "Prometheus cannot scrape the target.",
            "confidence_score": 0.6,
            "severity": "HIGH",
        },
        "requires_human": True,
        "human_escalation_reason": "Needs host access.",
    })

    assert plan.diagnosis.issue_summary == "node exporter down"
    assert plan.issue_summary == "node exporter down"

# --- TDD / Future Capabilities Tests ---

async def test_agent_escalates_critical_incidents():
    """
    [TDD Goal] Simulate the LLM returning an ActionPlan indicating human escalation.
    We want to ensure that if `requires_human` is True, it triggers the alarm mechanism
    and stops automation execution.
    """
    plan_args = {
        "diagnosis": {
            "issue_summary": "BGP Session Down with transit provider",
            "root_cause_analysis": "Physical link cut or misconfiguration",
            "confidence_score": 0.9,
            "severity": "HIGH"
        },
        "requires_human": True,
        "automated_actions_proposed": [],
        "human_escalation_reason": "BGP route changes require manual verification."
    }

    result = await noc_triage_agent.run("BGP Alert", model=TestModel(custom_output_args=plan_args))
    plan = result.data if hasattr(result, 'data') else result.output
    
    # In future implementations, if this plan requires_human, we should short-circuit the execution
    # loop and ONLY dispatch a Discord notification.
    assert plan.requires_human is True
    assert "BGP" in plan.human_escalation_reason

async def test_agent_invokes_prometheus_mcp_tool():
    """
    [TDD Goal] Test that the agent uses the `prometheus_query` tool to check for 
    secondary metrics when a vague alert triggers it.
    """
    # This acts as our specification that we will attach a `prometheus_query` 
    # to the agent via Pydantic AI's dynamic tools from MCP.
    pass

async def test_agent_invokes_ssh_mcp_tool_for_remediation():
    """
    [TDD Goal] Test that when the agent is highly confident about an issue,
    such as a crashed web server or stuck cache, it autonomously invokes
    the `ssh_run_command` tool to resolve it.
    """
    # This acts as a specification that the MCP server's SSH tools
    # are correctly formatted and accessible for the agent.
    pass
