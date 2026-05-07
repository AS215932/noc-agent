import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic_ai.exceptions import ModelHTTPError


SECRET_FIELD_PATTERN = re.compile(
    r"(?i)(api[_-]?key|token|password|secret)(['\"]?\s*[:=]\s*['\"]?)[^,'\"\s}]+"
)
SECRET_QUERY_PATTERNS = [
    re.compile(r"(?i)(x-goog-api-key=)[^&\s]+"),
    re.compile(r"(?i)(key=)[^&\s]+"),
]


@dataclass(frozen=True)
class SafeError:
    category: str
    public_message: str
    operator_next_steps: list[str]
    provider: str = "unknown"
    model_name: str = "unknown"

    def discord_description(self, context: str) -> str:
        steps = "\n".join(f"- {step}" for step in self.operator_next_steps)
        return (
            f"{context} could not complete because an infrastructure dependency failed.\n\n"
            f"{self.public_message}\n\n"
            "Operator next steps:\n"
            f"{steps}"
        )


def classify_exception(exc: BaseException) -> SafeError:
    relevant = _most_relevant_exception(exc)
    model_name = str(getattr(relevant, "model_name", "unknown") or "unknown")
    provider = _provider_from_model(model_name)
    status_code = getattr(relevant, "status_code", None)
    body_text = _safe_text(getattr(relevant, "body", None))
    message = f"{type(relevant).__name__}: {_safe_text(relevant)} {body_text}".lower()

    if isinstance(relevant, ModelHTTPError):
        if status_code == 429:
            if any(word in message for word in ("quota", "resource_exhausted", "exceeded")):
                return _quota_exhausted(provider, model_name)
            return _rate_limited(provider, model_name)
        if status_code in {401, 403}:
            if "quota" in message or "resource_exhausted" in message:
                return _quota_exhausted(provider, model_name)
            return _auth_failed(provider, model_name)
        if status_code in {408, 504}:
            return _timeout(provider, model_name)
        if isinstance(status_code, int) and status_code >= 500:
            return _provider_unavailable(provider, model_name)

    if isinstance(relevant, (TimeoutError, httpx.TimeoutException)):
        return _timeout(provider, model_name)

    if isinstance(relevant, httpx.HTTPStatusError):
        code = relevant.response.status_code
        if code == 429:
            return _rate_limited(provider, model_name)
        if code in {401, 403}:
            return _auth_failed(provider, model_name)
        if code >= 500:
            return _provider_unavailable(provider, model_name)

    if "quota" in message or "resource_exhausted" in message:
        return _quota_exhausted(provider, model_name)
    if "rate limit" in message or "429" in message:
        return _rate_limited(provider, model_name)
    if "unauthorized" in message or "forbidden" in message or "api key" in message:
        return _auth_failed(provider, model_name)
    if "mcp" in message or "client session" in message or "tool execution" in message:
        return _mcp_unavailable()
    if "timeout" in message or "timed out" in message:
        return _timeout(provider, model_name)

    return SafeError(
        category="unknown_infrastructure",
        provider=provider,
        model_name=model_name,
        public_message="The agent hit an unexpected infrastructure issue. Raw backend details were withheld.",
        operator_next_steps=[
            "Check `/health/model`, `/health/mcp`, and recent `noc-agent` logs.",
            "Run the alert triage manually if the incident may be customer-impacting.",
        ],
    )


def safe_health_error(exc: BaseException) -> str:
    safe = classify_exception(exc)
    return safe.public_message


def log_exception(event: str, exc: BaseException, **fields: Any) -> None:
    relevant = _most_relevant_exception(exc)
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "level": "error",
        "event": event,
        "service": "noc-agent",
        "error": {
            "type": type(relevant).__name__,
            "message": _redact(_truncate(_safe_text(relevant), 1200)),
        },
        **fields,
    }
    print(json.dumps(payload, separators=(",", ":"), default=str), file=sys.stdout, flush=True)


def _most_relevant_exception(exc: BaseException) -> BaseException:
    if isinstance(exc, BaseExceptionGroup) and exc.exceptions:
        for child in exc.exceptions:
            relevant = _most_relevant_exception(child)
            if isinstance(relevant, ModelHTTPError):
                return relevant
        return _most_relevant_exception(exc.exceptions[0])
    return exc


def _safe_text(value: Any) -> str:
    return _redact(_truncate(str(value or ""), 2000))


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def _redact(value: str) -> str:
    redacted = SECRET_FIELD_PATTERN.sub(r"\1\2[REDACTED]", value)
    for pattern in SECRET_QUERY_PATTERNS:
        redacted = pattern.sub(r"\1[REDACTED]", redacted)
    return redacted


def _provider_from_model(model_name: str) -> str:
    if ":" in model_name:
        return model_name.split(":", 1)[0]
    if model_name.startswith("gemini"):
        return "google-gla"
    return "unknown"


def _quota_exhausted(provider: str, model_name: str) -> SafeError:
    return SafeError(
        category="quota_exhausted",
        provider=provider,
        model_name=model_name,
        public_message="The configured AI model quota is exhausted or too low for this run.",
        operator_next_steps=[
            "Check `/health/model` and the Gemini quota dashboard.",
            "Verify fallback model credentials are configured.",
            "Escalate the original alert to a human operator if it is still firing.",
        ],
    )


def _rate_limited(provider: str, model_name: str) -> SafeError:
    return SafeError(
        category="rate_limited",
        provider=provider,
        model_name=model_name,
        public_message="The AI provider is rate limiting the agent right now.",
        operator_next_steps=[
            "Wait briefly or use the configured fallback model.",
            "Review recent alert volume and model usage metrics.",
        ],
    )


def _auth_failed(provider: str, model_name: str) -> SafeError:
    return SafeError(
        category="auth_failed",
        provider=provider,
        model_name=model_name,
        public_message="The agent could not authenticate with an AI provider.",
        operator_next_steps=[
            "Check required provider API keys in `/health/model`.",
            "Redeploy `noc-agent` after fixing the secret configuration.",
        ],
    )


def _provider_unavailable(provider: str, model_name: str) -> SafeError:
    return SafeError(
        category="provider_unavailable",
        provider=provider,
        model_name=model_name,
        public_message="The AI provider backend is currently unavailable or returning errors.",
        operator_next_steps=[
            "Confirm fallback providers are healthy.",
            "Retry triage after provider status recovers if no fallback succeeds.",
        ],
    )


def _timeout(provider: str, model_name: str) -> SafeError:
    return SafeError(
        category="timeout",
        provider=provider,
        model_name=model_name,
        public_message="The agent timed out while waiting on an infrastructure dependency.",
        operator_next_steps=[
            "Check provider latency, MCP health, and network reachability from the NOC VM.",
            "Continue manual investigation for active customer-impacting alerts.",
        ],
    )


def _mcp_unavailable() -> SafeError:
    return SafeError(
        category="mcp_unavailable",
        provider="mcp",
        model_name="mcp",
        public_message="The agent could not use one of its diagnostic tool backends.",
        operator_next_steps=[
            "Check `/health/mcp` and the MCP child process logs.",
            "Run Prometheus or SSH diagnostics manually until MCP recovers.",
        ],
    )
