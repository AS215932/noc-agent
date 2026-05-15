import pytest
from fastapi import BackgroundTasks
from pydantic import ValidationError

from app.main import (
    AlertManagerPayload,
    IcingaNotification,
    _embedded_discord_bot_enabled,
    _release_mail_poller_lock,
    _try_acquire_mail_poller_lock,
    _triage_fields,
    alertmanager_webhook,
    health_config,
    health_check,
    health_mail,
    health_mcp,
    poll_mailbox,
    icinga_webhook,
)
from app.agent import DiagnosticSynthesis
from app.mcp_runtime import MCPRuntime
from fastapi import Response, status

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
async def test_health_mcp_reports_degraded_when_tools_missing(mocker):
    mocker.patch("app.main.mcp_runtime", MCPRuntime(owner="test"))
    http_response = Response()

    response = await health_mcp(http_response)

    assert response["status"] == "degraded"
    assert http_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


@pytest.mark.asyncio
async def test_health_mcp_requires_registered_tools(mocker):
    runtime = MCPRuntime(owner="test")
    runtime.states["hyrule"].ready = True
    runtime.states["hyrule"].tool_count = 0
    runtime.states["xo"].ready = True
    runtime.states["xo"].tool_count = 3
    mocker.patch("app.main.mcp_runtime", runtime)
    http_response = Response()

    response = await health_mcp(http_response)

    assert response["status"] == "degraded"
    assert response["hyrule"] is False
    assert response["xo"] is True
    assert response["hyrule_tool_count"] == 0
    assert http_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


def test_embedded_discord_bot_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_START_EMBEDDED_BOT", raising=False)

    assert _embedded_discord_bot_enabled() is False


def test_embedded_discord_bot_can_be_enabled(monkeypatch):
    monkeypatch.setenv("NOC_AGENT_START_EMBEDDED_BOT", "1")

    assert _embedded_discord_bot_enabled() is True

@pytest.mark.asyncio
async def test_health_config_reports_missing_mail_password(monkeypatch):
    for name in (
        "GEMINI_API_KEY",
        "DISCORD_WEBHOOK_URL",
        "HYRULE_MCP_CMD",
        "XO_MCP_CMD",
        "XO_TOKEN",
        "ICINGA_API_USER",
        "ICINGA_API_PASSWORD",
    ):
        monkeypatch.setenv(name, "set")
    monkeypatch.delenv("MAIL_IMAP_PASSWORD", raising=False)
    http_response = Response()

    response = await health_config(http_response)

    assert response["status"] == "degraded"
    assert "MAIL_IMAP_PASSWORD" in response["missing"]
    assert response["mail_polling"] == "disabled"
    assert http_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

@pytest.mark.asyncio
async def test_health_mail_checks_connection(mocker):
    mocker.patch("app.main.check_mailbox_connection", return_value={"status": "ok"})
    http_response = Response()

    response = await health_mail(http_response)

    assert response["status"] == "ok"
    assert http_response.status_code == status.HTTP_200_OK

@pytest.mark.asyncio
async def test_health_mail_reports_failures(mocker):
    mocker.patch("app.main.check_mailbox_connection", side_effect=RuntimeError("bad imap"))
    http_response = Response()

    response = await health_mail(http_response)

    assert response["status"] == "degraded"
    assert "infrastructure issue" in response["error"]
    assert "bad imap" not in response["error"]
    assert http_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

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

@pytest.mark.asyncio
async def test_alertmanager_webhook_ignores_recovery(mock_alert_payload, mocker):
    background_tasks = mocker.Mock()
    mock_alert_payload["status"] = "resolved"
    mock_alert_payload["alerts"][0]["status"] = "resolved"

    response = await alertmanager_webhook(
        AlertManagerPayload.model_validate(mock_alert_payload),
        background_tasks,
    )

    assert response["status"] == "ignored"
    background_tasks.add_task.assert_not_called()


@pytest.mark.asyncio
async def test_icinga_webhook_ignores_recovery(mocker):
    background_tasks = mocker.Mock()
    notification = IcingaNotification(
        host_name="vault",
        service_name="disk /",
        check_command="disk",
        state="OK",
        state_type="RECOVERY",
        output="disk recovered",
    )

    response = await icinga_webhook(notification, background_tasks)

    assert response["status"] == "ignored"
    background_tasks.add_task.assert_not_called()

def test_alertmanager_webhook_invalid_payload():
    """
    Test that incomplete payloads are rejected with a 422 Unprocessable Entity.
    """
    with pytest.raises(ValidationError):
        AlertManagerPayload.model_validate({"receiver": "webhook"})

def test_mail_poller_lock_allows_only_one_owner(tmp_path, monkeypatch):
    import app.main as main

    monkeypatch.setattr(main, "MAIL_POLLER_LOCK_PATH", str(tmp_path / "mail-poller.lock"))
    first_lock = _try_acquire_mail_poller_lock()
    try:
        assert first_lock is not None
        assert _try_acquire_mail_poller_lock() is None
    finally:
        _release_mail_poller_lock(first_lock)

    second_lock = _try_acquire_mail_poller_lock()
    try:
        assert second_lock is not None
    finally:
        _release_mail_poller_lock(second_lock)

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
    mock_start_discord = mocker.patch("app.discord.send_discord_notification", return_value=None)
    mock_detail_discord = mocker.patch("app.main.send_discord_notification", return_value=None)
    
    # We call the investigation function directly to avoid asyncio background_tasks complications
    from app.main import investigate_alert
    
    # Use TestModel so we don't actually hit the LLM
    from pydantic_ai.models.test import TestModel
    from app.agent import noc_triage_agent
    
    # Run the test directly overriding the model logic for the scope of the method call
    await investigate_alert(mock_alert_payload, model=TestModel())

    mock_detail_discord.assert_called_once()
    start_titles = [call.kwargs["title"] for call in mock_start_discord.call_args_list]
    detail_titles = [mock_detail_discord.call_args.kwargs["title"]]
    assert any(title.startswith("⏳ Starting: NOC Triage:") for title in start_titles)
    assert detail_titles[0].startswith("Detailed Report:")
    titles = start_titles + detail_titles
    assert not any("Finished" in title for title in titles)

def test_triage_fields_turn_internal_schema_failure_into_operator_guidance(mock_alert_payload):
    plan = DiagnosticSynthesis.model_validate({
        "read_only": True,
        "incident_summary": "node_exporter unreachable on rtr1",
        "confidence_basis": "Prometheus has stopped scraping node_exporter.",
        "confidence_score": 0.6,
        "severity": "HIGH",
        "requires_human": True,
        "human_escalation_reason": "Unable to execute diagnostic SSH commands or Prometheus queries due to an internal system schema limitation.",
        "executed_actions": [],
    })

    fields = _triage_fields(plan, mock_alert_payload)
    action_plan = next(field["value"] for field in fields if field["name"] == "Next Checks / Proposal")

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
