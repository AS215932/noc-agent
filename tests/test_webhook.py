import pytest
from fastapi import BackgroundTasks
from pydantic import ValidationError

from app.main import (
    AlertManagerPayload,
    _triage_fields,
    alertmanager_webhook,
    health_check,
    poll_mailbox,
)
from app.agent import ActionPlan

@pytest.fixture
def mock_alert_payload():
    return {
        "receiver": "webhook",
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "InstanceDown",
                    "instance": "rtr1.as215932.net:9100",
                    "job": "node",
                    "severity": "critical"
                },
                "annotations": {
                    "description": "rtr1.as215932.net:9100 of job node has been down for more than 5 minutes.",
                    "summary": "Instance rtr1.as215932.net:9100 down"
                },
                "startsAt": "2026-05-02T10:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z"
            }
        ],
        "groupLabels": {"alertname": "InstanceDown"},
        "commonLabels": {"alertname": "InstanceDown", "severity": "critical"},
        "commonAnnotations": {},
        "externalURL": "http://alertmanager:9093",
        "version": "4",
        "groupKey": "{}:{alertname=\"InstanceDown\"}",
        "truncatedAlerts": 0
    }

@pytest.mark.asyncio
async def test_health_check():
    """Test that the application starts and exposes the health check."""
    response = await health_check()
    assert response["status"] == "ok"

@pytest.mark.asyncio
async def test_alertmanager_webhook_accepted(mock_alert_payload, mocker):
    """
    Test that the webhook successfully parses standard AlertManager payloads
    and offloads the processing to a background task.
    """
    mocker.patch("app.main.investigate_alert") # Mock the background execution so we don't hit the real LLM API
    response = await alertmanager_webhook(
        AlertManagerPayload.model_validate(mock_alert_payload),
        BackgroundTasks(),
    )
    assert response["status"] == "accepted"

def test_alertmanager_webhook_invalid_payload():
    """
    Test that incomplete payloads are rejected with a 422 Unprocessable Entity.
    """
    with pytest.raises(ValidationError):
        AlertManagerPayload.model_validate({"receiver": "webhook"})

@pytest.mark.asyncio
async def test_mail_poll_accepted(mocker):
    mocker.patch("app.main.process_mailbox_once")
    response = await poll_mailbox(BackgroundTasks())
    assert response["status"] == "accepted"

# --- TDD / Future Capabilities Tests ---

@pytest.mark.asyncio
async def test_webhook_triggers_discord_notification(mocker, mock_alert_payload):
    """
    [TDD Goal] In the future, once the investigation background task runs,
    it should send a summary to a specified Discord Webhook URL.
    This test serves as a design driver for creating the `discord.py` integration.
    """
    mock_discord = mocker.patch("app.main.send_discord_notification", return_value=None)
    
    # We call the investigation function directly to avoid asyncio background_tasks complications
    from app.main import investigate_alert
    
    # Use TestModel so we don't actually hit the LLM
    from pydantic_ai.models.test import TestModel
    from app.agent import noc_triage_agent
    
    # Run the test directly overriding the model logic for the scope of the method call
    await investigate_alert(mock_alert_payload, model=TestModel())

    mock_discord.assert_called_once()
    args, kwargs = mock_discord.call_args
    assert "Detailed Report:" in kwargs["title"]

def test_triage_fields_turn_internal_schema_failure_into_operator_guidance(mock_alert_payload):
    plan = ActionPlan.model_validate({
        "issue_summary": "node_exporter unreachable on rtr1",
        "root_cause_analysis": "Prometheus has stopped scraping node_exporter.",
        "confidence_score": 0.6,
        "severity": "HIGH",
        "requires_human": True,
        "human_escalation_reason": "Unable to execute diagnostic SSH commands or Prometheus queries due to an internal system schema limitation.",
    })

    fields = _triage_fields(plan, mock_alert_payload)
    action_plan = next(field["value"] for field in fields if field["name"] == "Action Plan")

    assert "internal system schema limitation" not in action_plan
    assert "Live diagnostics were not completed" in action_plan
    assert "up{instance=\"rtr1.as215932.net:9100\"}" in action_plan
    assert "systemctl status node_exporter" in action_plan

@pytest.mark.asyncio
async def test_agent_uses_mcp_tools(mocker):
    """
    [TDD Goal] The agent needs to be able to connect to hyrule-mcp via its command,
    and expose the hyrule-mcp tools to the Pydantic AI agent before running it.
    """
    # TODO: Implement MCP Context load in main.py and mock the load function
    pass
