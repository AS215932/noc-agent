import time
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from app.safe_errors import SafeError


MODEL_RUN_ATTEMPTS = Counter(
    "noc_agent_model_run_attempts_total",
    "NOC Agent model run attempts.",
    ["agent"],
)
MODEL_RUN_SUCCESSES = Counter(
    "noc_agent_model_run_successes_total",
    "NOC Agent model run successes.",
    ["agent", "model"],
)
MODEL_RUN_FAILURES = Counter(
    "noc_agent_model_run_failures_total",
    "NOC Agent model run failures by safe category.",
    ["agent", "category", "provider", "model"],
)
MODEL_FALLBACK_ATTEMPTS = Counter(
    "noc_agent_model_fallback_attempts_total",
    "Model fallback attempts.",
    ["from_model", "category"],
)
MODEL_FALLBACK_SUCCESSES = Counter(
    "noc_agent_model_fallback_successes_total",
    "Model fallback successes.",
    ["from_model", "to_model"],
)
DISCORD_SANITIZED_FAILURES = Counter(
    "noc_agent_sanitized_discord_failures_total",
    "Sanitized infrastructure failures sent to Discord.",
    ["category"],
)
MODEL_RUN_DURATION = Histogram(
    "noc_agent_model_run_duration_seconds",
    "End-to-end agent run duration.",
    ["agent", "outcome"],
)
MODEL_REQUEST_DURATION = Histogram(
    "noc_agent_model_request_duration_seconds",
    "Approximate model request duration for successful runs.",
    ["agent", "model"],
)
MODEL_LAST_SUCCESS = Gauge(
    "noc_agent_model_last_success_timestamp_seconds",
    "Unix timestamp for the last successful model run.",
    ["agent", "model"],
)
MODEL_LAST_FAILURE = Gauge(
    "noc_agent_model_last_failure_timestamp_seconds",
    "Unix timestamp for the last failed model run.",
    ["agent", "category"],
)
MODEL_DEGRADED = Gauge(
    "noc_agent_model_degraded",
    "Whether model access is degraded.",
)
MODEL_USAGE = Counter(
    "noc_agent_model_usage_total",
    "PydanticAI usage counters by agent and selected model.",
    ["agent", "model", "kind"],
)

_fallback_failures: ContextVar[list[str]] = ContextVar("fallback_failures", default=[])
RECENT_RUNTIME_FAILURE_WINDOW_SECONDS = 600.0


@dataclass
class ModelRuntimeState:
    configured_models: list[str] = field(default_factory=list)
    missing_credentials: list[str] = field(default_factory=list)
    unsupported_models: list[str] = field(default_factory=list)
    active_model_chain: list[str] = field(default_factory=list)
    last_success_at: float | None = None
    last_success_model: str | None = None
    last_failure_at: float | None = None
    last_failure_category: str | None = None
    last_failure_model: str | None = None

    def health(self) -> dict[str, Any]:
        status = "degraded" if (self.missing_credentials or self.unsupported_models) else "ok"
        if self._has_recent_runtime_failure():
            status = "degraded"
        return {"status": status, **asdict(self)}

    def _has_recent_runtime_failure(self) -> bool:
        if not self.last_failure_category or self.last_failure_at is None:
            return False
        if self.last_success_at is not None and self.last_success_at > self.last_failure_at:
            return False
        return (time.time() - self.last_failure_at) <= RECENT_RUNTIME_FAILURE_WINDOW_SECONDS


STATE = ModelRuntimeState()


def set_model_config(
    *,
    configured_models: list[str],
    missing_credentials: list[str],
    unsupported_models: list[str],
    active_model_chain: list[str],
) -> None:
    STATE.configured_models = configured_models
    STATE.missing_credentials = missing_credentials
    STATE.unsupported_models = unsupported_models
    STATE.active_model_chain = active_model_chain
    MODEL_DEGRADED.set(1 if missing_credentials or unsupported_models else 0)


def start_run(agent: str) -> float:
    MODEL_RUN_ATTEMPTS.labels(agent=agent).inc()
    _fallback_failures.set([])
    return time.perf_counter()


def record_fallback_attempt(from_model: str, category: str) -> None:
    MODEL_FALLBACK_ATTEMPTS.labels(from_model=from_model or "unknown", category=category).inc()
    failures = list(_fallback_failures.get())
    failures.append(from_model or "unknown")
    _fallback_failures.set(failures)


def record_success(agent: str, started_at: float, result: Any) -> str:
    model = selected_model_name(result)
    now = time.time()
    duration = max(0.0, time.perf_counter() - started_at)
    STATE.last_success_at = now
    STATE.last_success_model = model
    STATE.last_failure_category = None
    MODEL_DEGRADED.set(1 if STATE.missing_credentials or STATE.unsupported_models else 0)
    MODEL_RUN_SUCCESSES.labels(agent=agent, model=model).inc()
    MODEL_RUN_DURATION.labels(agent=agent, outcome="success").observe(duration)
    MODEL_REQUEST_DURATION.labels(agent=agent, model=model).observe(duration)
    MODEL_LAST_SUCCESS.labels(agent=agent, model=model).set(now)
    _record_usage(agent, model, result)
    for failed_model in _fallback_failures.get():
        if failed_model != model:
            MODEL_FALLBACK_SUCCESSES.labels(from_model=failed_model, to_model=model).inc()
    _fallback_failures.set([])
    return model


def record_failure(agent: str, started_at: float, safe: SafeError) -> None:
    now = time.time()
    duration = max(0.0, time.perf_counter() - started_at)
    STATE.last_failure_at = now
    STATE.last_failure_category = safe.category
    STATE.last_failure_model = safe.model_name
    MODEL_DEGRADED.set(1)
    MODEL_RUN_FAILURES.labels(
        agent=agent,
        category=safe.category,
        provider=safe.provider,
        model=safe.model_name,
    ).inc()
    MODEL_RUN_DURATION.labels(agent=agent, outcome="failure").observe(duration)
    MODEL_LAST_FAILURE.labels(agent=agent, category=safe.category).set(now)
    _fallback_failures.set([])


def record_sanitized_discord_failure(category: str) -> None:
    DISCORD_SANITIZED_FAILURES.labels(category=category or "unknown_infrastructure").inc()


def selected_model_name(result: Any) -> str:
    try:
        for message in reversed(result.new_messages()):
            model_name = getattr(message, "model_name", None)
            if model_name:
                return str(model_name)
    except Exception:
        pass
    return "unknown"


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


def _record_usage(agent: str, model: str, result: Any) -> None:
    usage_fn = getattr(result, "usage", None)
    if not callable(usage_fn):
        return
    usage = usage_fn()
    for name in ("requests", "input_tokens", "output_tokens", "total_tokens"):
        value = getattr(usage, name, 0) or 0
        if value:
            MODEL_USAGE.labels(agent=agent, model=model, kind=name).inc(value)
