import pytest
from fastapi import Response, status
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from app.agent import DiagnosticSynthesis
from app.main import health_model, investigate_alert, metrics
from app.model_config import load_model_config
from app.safe_errors import classify_exception


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
                    "severity": "critical",
                },
                "annotations": {"summary": "Instance rtr1.as215932.net:9100 down"},
                "startsAt": "2026-05-02T10:00:00Z",
            }
        ],
        "groupLabels": {"alertname": "InstanceDown"},
        "commonLabels": {"alertname": "InstanceDown", "severity": "critical"},
        "commonAnnotations": {},
        "externalURL": "http://alertmanager:9093",
        "version": "4",
        "groupKey": "{}:{alertname=\"InstanceDown\"}",
        "truncatedAlerts": 0,
    }


def _plan_args() -> dict:
    return {
        "read_only": True,
        "incident_summary": "Fallback model completed triage",
        "confidence_score": 0.8,
        "confidence_basis": "The fallback model produced a safe diagnosis.",
        "severity": "MEDIUM",
        "requires_human": True,
        "human_escalation_reason": "Fallback triage should be reviewed by an operator.",
        "recommended_next_checks": ["Review the alert in Prometheus."],
        "executed_actions": [],
    }


def test_model_config_reports_missing_credentials(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.setenv("AGENT_FALLBACK_MODELS", "anthropic:claude-sonnet-4-5")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    config = load_model_config()

    assert config.configured_models == [
        "google-gla:gemini-3.1-pro-preview",
        "anthropic:claude-sonnet-4-5",
    ]
    assert any("GOOGLE_API_KEY" in item for item in config.missing_credentials)
    assert any("ANTHROPIC_API_KEY" in item for item in config.missing_credentials)


def test_model_config_builds_active_fallback_chain(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.setenv("AGENT_FALLBACK_MODELS", "anthropic:claude-sonnet-4-5")
    monkeypatch.setenv("GEMINI_API_KEY", "test-google-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic-key")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    config = load_model_config()

    assert config.active_model_chain == [
        "google-gla:gemini-3.1-pro-preview",
        "anthropic:claude-sonnet-4-5",
    ]
    assert config.missing_credentials == []


@pytest.mark.asyncio
async def test_fallback_model_recovers_from_model_http_429():
    async def fail_with_quota(*_args):
        raise ModelHTTPError(
            429,
            "gemini-3.1-pro-preview",
            body={"error": {"message": "You exceeded your current quota"}},
        )

    fallback_model = FallbackModel(
        FunctionModel(fail_with_quota, model_name="gemini-3.1-pro-preview"),
        TestModel(custom_output_args=_plan_args(), model_name="fallback-test"),
    )
    agent = Agent(fallback_model, output_type=DiagnosticSynthesis)

    result = await agent.run("Investigate alert")
    plan = result.data if hasattr(result, "data") else result.output

    assert plan.incident_summary == "Fallback model completed triage"


@pytest.mark.asyncio
async def test_investigate_alert_sanitizes_provider_failure(mock_alert_payload, mocker):
    async def fail_with_quota(*_args):
        raise ModelHTTPError(
            429,
            "gemini-3.1-pro-preview",
            body={
                "error": {
                    "message": "You exceeded your current quota",
                    "details": [{"quotaValue": "250", "metadata": {"secret": "do-not-show"}}],
                }
            },
        )

    send_discord = mocker.patch("app.discord.send_discord_notification")

    await investigate_alert(mock_alert_payload, model=FunctionModel(fail_with_quota, model_name="gemini-3.1-pro-preview"))

    descriptions = [call.kwargs.get("description", "") for call in send_discord.call_args_list]
    combined = "\n".join(descriptions)
    assert "configured AI model quota is exhausted" in combined
    assert "quotaValue" not in combined
    assert "do-not-show" not in combined
    assert "generativelanguage.googleapis.com" not in combined


def test_safe_error_classifies_quota_without_leaking_body():
    exc = ModelHTTPError(
        429,
        "gemini-3.1-pro-preview",
        body={"error": {"message": "You exceeded your current quota", "url": "https://ai.google.dev/secret"}},
    )

    safe = classify_exception(exc)

    assert safe.category == "quota_exhausted"
    assert "quota" in safe.public_message
    assert "https://ai.google.dev/secret" not in safe.public_message


@pytest.mark.asyncio
async def test_health_model_is_degraded_for_missing_model_credentials(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.setenv("AGENT_FALLBACK_MODELS", "anthropic:claude-sonnet-4-5")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_QUOTA_PROJECT_ID", raising=False)
    response = Response()

    health = await health_model(response)

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert health["status"] == "degraded"
    assert health["quota_monitoring"] == "not_configured"
    assert "test-google-key" not in str(health)


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_model_metrics():
    response = await metrics()
    body = response.body.decode()

    assert "noc_agent_model_run_attempts_total" in body
    assert response.media_type.startswith("text/plain")
