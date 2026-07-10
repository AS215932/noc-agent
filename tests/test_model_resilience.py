import copy

import pytest
from fastapi import Response, status
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.models.test import TestModel

from app.agent import DiagnosticSynthesis
from app.cases import CaseService, InMemoryCaseStore
from app.cases.graph_memory import CaseServiceGraphMemory
from app.cases.notifications import observations_from_alertmanager
from app.main import health_model, investigate_alert, metrics
from app.model_config import build_agent_model, load_model_config
from app.model_metrics import STATE
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


@pytest.fixture
def model_runtime_state_guard():
    original_state = copy.deepcopy(STATE)
    try:
        yield
    finally:
        for attr, value in vars(original_state).items():
            setattr(STATE, attr, value)
        for attr in list(vars(STATE).keys()):
            if attr not in vars(original_state):
                delattr(STATE, attr)


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


def test_default_model_config_uses_openrouter_chain(monkeypatch):
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)
    monkeypatch.delenv("NOC_AGENT_CONFIG", raising=False)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")

    config = load_model_config()

    assert config.configured_models == [
        "openrouter:z-ai/glm-5.2",
        "venice:zai-org-glm-5-2",
        "openrouter:deepseek/deepseek-v4-flash",
        "venice:deepseek-v4-flash",
    ]
    assert config.unsupported_models == []
    assert config.missing_credentials == []


def test_openrouter_missing_key_is_reported_without_marking_model_unsupported(monkeypatch):
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    config = load_model_config()

    assert config.unsupported_models == []
    assert any("OPENROUTER_API_KEY" in item for item in config.missing_credentials)


def test_model_env_overrides_config_file(monkeypatch, tmp_path):
    config_path = tmp_path / "noc-agent.toml"
    config_path.write_text(
        '[model]\nprimary = "openrouter:deepseek/deepseek-v4-pro"\nfallbacks = ["openrouter:anthropic/claude-sonnet-4.6"]\n'
    )
    monkeypatch.setenv("NOC_AGENT_CONFIG", str(config_path))
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    config = load_model_config()

    assert config.configured_models == ["google-gla:gemini-3.1-pro-preview"]
    assert config.missing_credentials == []


def test_model_config_parse_error_degrades_health_without_leaking_secret(monkeypatch, tmp_path):
    config_path = tmp_path / "bad.toml"
    config_path.write_text('[model]\nprimary = "openrouter:deepseek/deepseek-v4-pro"\nsecret = "do-not-leak"\ninvalid = [')
    monkeypatch.setenv("NOC_AGENT_CONFIG", str(config_path))
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.delenv("AGENT_MODEL", raising=False)
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)

    config = load_model_config()

    assert config.config_errors
    assert "do-not-leak" not in str(config.config_errors)


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


def test_venice_missing_key_is_reported_without_marking_model_unsupported(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL", "openrouter:z-ai/glm-5.2")
    monkeypatch.setenv("AGENT_FALLBACK_MODELS", "venice:zai-org-glm-5-2")
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.delenv("VENICE_API_KEY", raising=False)

    config = load_model_config()

    assert config.unsupported_models == []
    assert any("VENICE_API_KEY" in item for item in config.missing_credentials)
    assert config.active_model_chain == ["openrouter:z-ai/glm-5.2"]
    assert config.provider_api_key_envs["venice"] == "VENICE_API_KEY"


def test_venice_fallback_builds_openai_compatible_model(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL", "openrouter:z-ai/glm-5.2")
    monkeypatch.setenv("AGENT_FALLBACK_MODELS", "venice:zai-org-glm-5-2")
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")
    monkeypatch.delenv("VENICE_BASE_URL", raising=False)

    config = load_model_config()
    assert config.active_model_chain == [
        "openrouter:z-ai/glm-5.2",
        "venice:zai-org-glm-5-2",
    ]

    model = build_agent_model()
    assert isinstance(model, FallbackModel)
    venice_model = model.models[-1]
    assert isinstance(venice_model, OpenAIChatModel)
    assert venice_model.model_name == "zai-org-glm-5-2"
    assert str(venice_model.client.base_url).startswith("https://api.venice.ai/api/v1")


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
    store = InMemoryCaseStore()
    service = CaseService(store)
    observation = observations_from_alertmanager({**mock_alert_payload, "source": "alertmanager"})[0]
    observed = await service.observe(observation)
    assert observed.case is not None
    graph_memory = CaseServiceGraphMemory(store)
    graph_case = await graph_memory.get_case(observed.case.case_id)
    assert graph_case is not None

    await investigate_alert(
        mock_alert_payload,
        model=FunctionModel(fail_with_quota, model_name="gemini-3.1-pro-preview"),
        case=graph_case,
        graph_memory=graph_memory,
    )

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
async def test_health_model_is_degraded_for_recent_runtime_failure_when_config_is_healthy(monkeypatch, model_runtime_state_guard):
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_QUOTA_PROJECT_ID", raising=False)
    monkeypatch.setattr("app.model_metrics.time.time", lambda: 180.0)

    STATE.last_failure_at = 123.0
    STATE.last_failure_category = "unknown_infrastructure"
    STATE.last_failure_model = "unknown"
    STATE.last_success_at = None
    STATE.last_success_model = None

    response = Response()
    health = await health_model(response)

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert health["status"] == "degraded"
    assert health["quota_monitoring"] == "not_configured"
    assert health["last_failure_category"] == "unknown_infrastructure"
    assert health["last_failure_at"] == 123.0
    assert health["last_failure_model"] == "unknown"


@pytest.mark.asyncio
async def test_health_model_ignores_stale_runtime_failure_when_config_is_healthy(monkeypatch, model_runtime_state_guard):
    monkeypatch.setenv("AGENT_MODEL", "google-gla:gemini-3.1-pro-preview")
    monkeypatch.delenv("AGENT_FALLBACK_MODELS", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_QUOTA_PROJECT_ID", raising=False)
    monkeypatch.setattr("app.model_metrics.time.time", lambda: 1000.0)

    STATE.last_failure_at = 123.0
    STATE.last_failure_category = "unknown_infrastructure"
    STATE.last_failure_model = "unknown"
    STATE.last_success_at = None
    STATE.last_success_model = None

    response = Response()
    health = await health_model(response)

    assert response.status_code == status.HTTP_200_OK
    assert health["status"] == "ok"
    assert health["quota_monitoring"] == "not_configured"
    assert health["last_failure_category"] == "unknown_infrastructure"
    assert health["last_failure_at"] == 123.0
    assert health["last_failure_model"] == "unknown"


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_model_metrics():
    response = await metrics()
    body = response.body.decode()

    assert "noc_agent_model_run_attempts_total" in body
    assert "noc_agent_openrouter_credit_probe_ok" in body
    assert response.media_type.startswith("text/plain")
