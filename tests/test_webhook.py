import pytest
from app.main import app
from fastapi.testclient import TestClient

client = TestClient(app)

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

def test_health_check():
    """Test that the application starts and exposes the health check."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_alertmanager_webhook_accepted(mock_alert_payload, mocker):
    """
    Test that the webhook successfully parses standard AlertManager payloads
    and offloads the processing to a background task.
    """
    mocker.patch("app.main.investigate_alert") # Mock the background execution so we don't hit the real LLM API
    response = client.post("/webhook/alertmanager", json=mock_alert_payload)
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"

def test_alertmanager_webhook_invalid_payload():
    """
    Test that incomplete payloads are rejected with a 422 Unprocessable Entity.
    """
    response = client.post("/webhook/alertmanager", json={"receiver": "webhook"})
    assert response.status_code == 422

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
    assert "NOC Triage:" in kwargs["title"]

@pytest.mark.asyncio
async def test_webhook_triggers_prometheus_mcp_client(mocker):
    """
    [TDD Goal] The background task should eventually initialize the MCP client
    and expose the hyrule-mcp tools to the Pydantic AI agent before running it.
    """
    # TODO: Implement MCP Context load in main.py and mock the load function
    pass
