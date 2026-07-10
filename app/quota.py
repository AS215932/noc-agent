import math
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from prometheus_client import Gauge

from app.config import NocAgentSettings, load_settings


DEFAULT_GEMINI_QUOTA_METRIC = "generativelanguage.googleapis.com/generate_requests_per_model_per_day"
DEFAULT_GEMINI_QUOTA_SERVICE = "generativelanguage.googleapis.com"
DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE = "serviceruntime.googleapis.com/quota/rate/net_usage"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
VENICE_DEFAULT_BASE_URL = "https://api.venice.ai/api/v1"

GEMINI_QUOTA_CONFIGURED = Gauge(
    "noc_agent_gemini_quota_configured",
    "Whether Gemini quota monitoring is configured.",
)
GEMINI_QUOTA_PROBE_OK = Gauge(
    "noc_agent_gemini_quota_probe_ok",
    "Whether the last Gemini quota probe succeeded.",
)
GEMINI_QUOTA_USAGE = Gauge(
    "noc_agent_gemini_quota_usage",
    "Latest Gemini quota usage value from Cloud Monitoring.",
)
GEMINI_QUOTA_REMAINING = Gauge(
    "noc_agent_gemini_quota_remaining",
    "Latest Gemini quota remaining estimate; -1 when unknown.",
)

OPENROUTER_KEY_CONFIGURED = Gauge(
    "noc_agent_openrouter_key_configured",
    "Whether OpenRouter API key monitoring is configured.",
)
OPENROUTER_CREDIT_PROBE_OK = Gauge(
    "noc_agent_openrouter_credit_probe_ok",
    "Whether the last OpenRouter credit probe succeeded.",
)
OPENROUTER_LIMIT_REMAINING = Gauge(
    "noc_agent_openrouter_limit_remaining_usd",
    "OpenRouter API key remaining credit limit in USD; -1 when unknown or unlimited.",
)
OPENROUTER_LIMIT = Gauge(
    "noc_agent_openrouter_limit_usd",
    "OpenRouter API key credit limit in USD; -1 when unknown or unlimited.",
)
OPENROUTER_USAGE_TOTAL = Gauge(
    "noc_agent_openrouter_usage_total_usd",
    "OpenRouter API key all-time usage in USD.",
)
OPENROUTER_USAGE_DAILY = Gauge(
    "noc_agent_openrouter_usage_daily_usd",
    "OpenRouter API key daily usage in USD.",
)
OPENROUTER_USAGE_WEEKLY = Gauge(
    "noc_agent_openrouter_usage_weekly_usd",
    "OpenRouter API key weekly usage in USD.",
)
OPENROUTER_USAGE_MONTHLY = Gauge(
    "noc_agent_openrouter_usage_monthly_usd",
    "OpenRouter API key monthly usage in USD.",
)
OPENROUTER_ACCOUNT_TOTAL_CREDITS = Gauge(
    "noc_agent_openrouter_account_total_credits_usd",
    "OpenRouter account total purchased credits in USD; -1 when unknown.",
)
OPENROUTER_ACCOUNT_TOTAL_USAGE = Gauge(
    "noc_agent_openrouter_account_total_usage_usd",
    "OpenRouter account total used credits in USD; -1 when unknown.",
)
OPENROUTER_ACCOUNT_REMAINING_CREDITS = Gauge(
    "noc_agent_openrouter_account_remaining_credits_usd",
    "OpenRouter account remaining credits in USD; -1 when unknown.",
)


@dataclass(frozen=True)
class QuotaStatus:
    status: str
    message: str
    metric: str | None = None
    project_id: str | None = None
    usage: float | None = None
    remaining: float | None = None

    def health_value(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "metric": self.metric,
            "project_id": self.project_id,
            "usage": self.usage,
            "remaining": self.remaining,
        }


@dataclass(frozen=True)
class OpenRouterKeyStatus:
    status: str
    message: str
    label: str | None = None
    limit: float | None = None
    limit_remaining: float | None = None
    limit_reset: str | None = None
    usage: float | None = None
    usage_daily: float | None = None
    usage_weekly: float | None = None
    usage_monthly: float | None = None
    is_free_tier: bool | None = None

    def health_value(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "label": self.label,
            "limit": self.limit,
            "limit_remaining": self.limit_remaining,
            "limit_reset": self.limit_reset,
            "usage": self.usage,
            "usage_daily": self.usage_daily,
            "usage_weekly": self.usage_weekly,
            "usage_monthly": self.usage_monthly,
            "is_free_tier": self.is_free_tier,
        }


@dataclass(frozen=True)
class OpenRouterAccountCreditStatus:
    status: str
    message: str
    total_credits: float | None = None
    total_usage: float | None = None
    remaining: float | None = None

    def health_value(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "total_credits": self.total_credits,
            "total_usage": self.total_usage,
            "remaining": self.remaining,
        }


@dataclass(frozen=True)
class ProviderCreditStatus:
    status: str
    message: str
    providers: dict[str, dict[str, Any]] = field(default_factory=dict)

    def health_value(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "providers": self.providers,
        }


@dataclass(frozen=True)
class VeniceKeyStatus:
    status: str
    message: str
    balance_usd: float | None = None
    balance_diem: float | None = None
    access_permitted: bool | None = None
    api_tier: str | None = None
    next_epoch_begins: str | None = None

    def health_value(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "balance_usd": self.balance_usd,
            "balance_diem": self.balance_diem,
            "access_permitted": self.access_permitted,
            "api_tier": self.api_tier,
            "next_epoch_begins": self.next_epoch_begins,
        }


_OPENROUTER_CACHE: tuple[float, str, ProviderCreditStatus] | None = None
_VENICE_CACHE: tuple[float, str, ProviderCreditStatus] | None = None


def check_model_provider_credits(config: Any | None = None) -> ProviderCreditStatus:
    settings = load_settings()
    if config is None:
        try:
            from app.model_config import load_model_config

            config = load_model_config()
        except Exception:
            config = None
    models = list(getattr(config, "active_model_chain", None) or getattr(config, "configured_models", None) or [])
    if not models:
        models = [settings.model.primary, *settings.model.fallbacks]

    providers: dict[str, dict[str, Any]] = {}
    statuses: list[str] = []
    if any(_provider_from_model(model) == "openrouter" for model in models):
        openrouter = check_openrouter_credits(settings)
        providers.update(openrouter.providers)
        statuses.append(openrouter.status)

    if any(_provider_from_model(model) in {"google", "google-gla", "google-vertex"} for model in models):
        gemini = check_gemini_quota()
        providers["google"] = gemini.health_value()
        statuses.append(gemini.status)

    if any(_provider_from_model(model) == "venice" for model in models):
        venice = check_venice_credits()
        providers.update(venice.providers)
        statuses.append(venice.status)

    if not providers:
        return ProviderCreditStatus(status="ok", message="No model providers require credit monitoring.")

    if any(item == "degraded" for item in statuses):
        status = "degraded"
    elif statuses and all(item == "not_configured" for item in statuses):
        status = "not_configured"
    else:
        status = "ok"
    return ProviderCreditStatus(status=status, message="Model provider credit probes completed.", providers=providers)


def check_openrouter_credits(settings: NocAgentSettings | None = None) -> ProviderCreditStatus:
    settings = settings or load_settings()
    openrouter = settings.providers.openrouter
    if not openrouter.credit_monitoring_enabled:
        status = ProviderCreditStatus(
            status="not_configured",
            message="OpenRouter credit monitoring is disabled.",
            providers={"openrouter": {"status": "not_configured", "message": "disabled"}},
        )
        _publish_openrouter(OpenRouterKeyStatus(status="not_configured", message="disabled"), None)
        return status

    api_key = os.getenv(openrouter.api_key_env) or os.getenv("OPENROUTER_API_KEY")
    management_key = os.getenv(openrouter.management_api_key_env) or os.getenv("OPENROUTER_MANAGEMENT_API_KEY")
    cache_key = f"{openrouter.api_key_env}:{bool(api_key)}:{openrouter.management_api_key_env}:{bool(management_key)}"
    global _OPENROUTER_CACHE
    now = time.monotonic()
    if _OPENROUTER_CACHE is not None:
        cached_at, cached_key, cached = _OPENROUTER_CACHE
        if cached_key == cache_key and now - cached_at <= openrouter.credit_probe_cache_seconds:
            return cached

    if not api_key:
        key_status = OpenRouterKeyStatus(
            status="not_configured",
            message=f"{openrouter.api_key_env} is not configured.",
        )
        account_status = OpenRouterAccountCreditStatus(
            status="not_configured",
            message=f"{openrouter.management_api_key_env} is not configured.",
        )
        result = _openrouter_result(key_status, account_status)
        _publish_openrouter(key_status, account_status)
        _OPENROUTER_CACHE = (now, cache_key, result)
        return result

    key_status = _check_openrouter_key(api_key, settings)
    account_status = _check_openrouter_account(management_key, settings) if management_key else OpenRouterAccountCreditStatus(
        status="not_configured",
        message=f"{openrouter.management_api_key_env} is not configured.",
    )
    result = _openrouter_result(key_status, account_status)
    _publish_openrouter(key_status, account_status)
    _OPENROUTER_CACHE = (now, cache_key, result)
    return result


def _check_openrouter_key(api_key: str, settings: NocAgentSettings) -> OpenRouterKeyStatus:
    try:
        data = _openrouter_get("/key", api_key, settings)
        key = data.get("data", {}) if isinstance(data, dict) else {}
        limit = _maybe_float(key.get("limit"))
        remaining = _maybe_float(key.get("limit_remaining"))
        critical_remaining, warn_remaining = _openrouter_key_thresholds(limit, settings)
        status = "ok"
        message = "OpenRouter key usage query completed."
        if remaining is not None and remaining <= critical_remaining:
            status = "degraded"
            message = "OpenRouter key remaining credit limit is at or below the critical threshold."
        elif remaining is not None and remaining <= warn_remaining:
            status = "degraded"
            message = "OpenRouter key remaining credit limit is at or below the warning threshold."
        return OpenRouterKeyStatus(
            status=status,
            message=message,
            label=str(key.get("label")) if key.get("label") is not None else None,
            limit=limit,
            limit_remaining=remaining,
            limit_reset=str(key.get("limit_reset")) if key.get("limit_reset") is not None else None,
            usage=_maybe_float(key.get("usage")),
            usage_daily=_maybe_float(key.get("usage_daily")),
            usage_weekly=_maybe_float(key.get("usage_weekly")),
            usage_monthly=_maybe_float(key.get("usage_monthly")),
            is_free_tier=key.get("is_free_tier") if isinstance(key.get("is_free_tier"), bool) else None,
        )
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code in {401, 403}:
            return OpenRouterKeyStatus(status="degraded", message="OpenRouter key query was rejected by authentication/authorization.")
        return OpenRouterKeyStatus(status="degraded", message=f"OpenRouter key query failed safely: HTTP {code}")
    except (httpx.TimeoutException, TimeoutError):
        return OpenRouterKeyStatus(status="degraded", message="OpenRouter key query timed out safely.")
    except Exception:
        return OpenRouterKeyStatus(status="degraded", message="OpenRouter key query failed safely.")


def _openrouter_key_thresholds(limit: float | None, settings: NocAgentSettings) -> tuple[float, float]:
    critical = _safe_nonnegative_float(settings.providers.openrouter.critical_remaining_usd)
    warn = _safe_nonnegative_float(settings.providers.openrouter.warn_remaining_usd)
    if limit is not None and math.isfinite(limit) and limit > 0:
        critical = min(critical, limit * 0.05)
        warn = min(warn, limit * 0.20)
    return critical, warn


def _safe_nonnegative_float(value: float) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0:
        return 0.0
    return value


def _check_openrouter_account(management_key: str, settings: NocAgentSettings) -> OpenRouterAccountCreditStatus:
    try:
        data = _openrouter_get("/credits", management_key, settings)
        account = data.get("data", {}) if isinstance(data, dict) else {}
        total_credits = _maybe_float(account.get("total_credits"))
        total_usage = _maybe_float(account.get("total_usage"))
        remaining = total_credits - total_usage if total_credits is not None and total_usage is not None else None
        return OpenRouterAccountCreditStatus(
            status="ok",
            message="OpenRouter account credit query completed.",
            total_credits=total_credits,
            total_usage=total_usage,
            remaining=remaining,
        )
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code in {401, 403}:
            return OpenRouterAccountCreditStatus(
                status="degraded",
                message="OpenRouter account credit query was rejected by authentication/authorization.",
            )
        return OpenRouterAccountCreditStatus(status="degraded", message=f"OpenRouter account credit query failed safely: HTTP {code}")
    except (httpx.TimeoutException, TimeoutError):
        return OpenRouterAccountCreditStatus(status="degraded", message="OpenRouter account credit query timed out safely.")
    except Exception:
        return OpenRouterAccountCreditStatus(
            status="degraded",
            message="OpenRouter account credit query failed safely.",
        )


def _openrouter_get(path: str, api_key: str, settings: NocAgentSettings) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {api_key}"}
    if settings.providers.openrouter.app_url:
        headers["HTTP-Referer"] = settings.providers.openrouter.app_url
    if settings.providers.openrouter.app_title:
        headers["X-Title"] = settings.providers.openrouter.app_title
    response = httpx.get(
        f"{OPENROUTER_BASE_URL}{path}",
        headers=headers,
        timeout=settings.providers.openrouter.credit_probe_timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}


def _openrouter_result(key: OpenRouterKeyStatus, account: OpenRouterAccountCreditStatus | None) -> ProviderCreditStatus:
    if key.status == "degraded" or (account and account.status == "degraded"):
        status = "degraded"
    elif key.status == "not_configured":
        status = "not_configured"
    else:
        status = "ok"
    provider = {
        "status": status,
        "key": key.health_value(),
        "account": account.health_value() if account else None,
    }
    return ProviderCreditStatus(
        status=status,
        message="OpenRouter credit probe completed.",
        providers={"openrouter": provider},
    )


def check_venice_credits() -> ProviderCreditStatus:
    api_key = os.getenv("VENICE_API_KEY")
    cache_key = f"venice:{bool(api_key)}"
    global _VENICE_CACHE
    now = time.monotonic()
    if _VENICE_CACHE is not None:
        cached_at, cached_key, cached = _VENICE_CACHE
        if cached_key == cache_key and now - cached_at <= _venice_env_float("VENICE_CREDIT_PROBE_CACHE_SECONDS", 60.0):
            return cached

    if not api_key:
        key_status = VeniceKeyStatus(status="not_configured", message="VENICE_API_KEY is not configured.")
    else:
        key_status = _check_venice_key(api_key)

    result = ProviderCreditStatus(
        status=key_status.status,
        message="Venice credit probe completed.",
        providers={"venice": {"status": key_status.status, "key": key_status.health_value()}},
    )
    _VENICE_CACHE = (now, cache_key, result)
    return result


def _check_venice_key(api_key: str) -> VeniceKeyStatus:
    try:
        base = os.getenv("VENICE_BASE_URL", VENICE_DEFAULT_BASE_URL).rstrip("/")
        response = httpx.get(
            f"{base}/api_keys/rate_limits",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=_venice_env_float("VENICE_CREDIT_PROBE_TIMEOUT_SECONDS", 5.0),
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        balances = data.get("balances") if isinstance(data.get("balances"), dict) else {}
        balance_usd = _maybe_float(balances.get("USD"))
        balance_diem = _maybe_float(balances.get("DIEM"))
        access = data.get("accessPermitted") if isinstance(data.get("accessPermitted"), bool) else None
        tier = data.get("apiTier")
        tier_id = str(tier.get("id")) if isinstance(tier, dict) and tier.get("id") is not None else None
        parts = [value for value in (balance_usd, balance_diem) if value is not None]
        remaining = sum(parts) if parts else None
        critical = _venice_env_float("VENICE_CRITICAL_REMAINING_USD", 1.0)
        warn = _venice_env_float("VENICE_WARN_REMAINING_USD", 5.0)
        status = "ok"
        message = "Venice key rate-limit query completed."
        if access is False:
            status = "degraded"
            message = "Venice API key is not permitted to consume inference APIs."
        elif remaining is not None and remaining <= critical:
            status = "degraded"
            message = "Venice remaining balance is at or below the critical threshold."
        elif remaining is not None and remaining <= warn:
            status = "degraded"
            message = "Venice remaining balance is at or below the warning threshold."
        return VeniceKeyStatus(
            status=status,
            message=message,
            balance_usd=balance_usd,
            balance_diem=balance_diem,
            access_permitted=access,
            api_tier=tier_id,
            next_epoch_begins=str(data.get("nextEpochBegins")) if data.get("nextEpochBegins") is not None else None,
        )
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code in {401, 403}:
            return VeniceKeyStatus(status="degraded", message="Venice key query was rejected by authentication/authorization.")
        return VeniceKeyStatus(status="degraded", message=f"Venice key query failed safely: HTTP {code}")
    except (httpx.TimeoutException, TimeoutError):
        return VeniceKeyStatus(status="degraded", message="Venice key query timed out safely.")
    except Exception:
        return VeniceKeyStatus(status="degraded", message="Venice key query failed safely.")


def _venice_env_float(env_name: str, default: float) -> float:
    raw = os.getenv(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if not math.isfinite(value) or value < 0:
        return default
    return value


def check_gemini_quota() -> QuotaStatus:
    project_id = os.getenv("GEMINI_QUOTA_PROJECT_ID", "").strip()
    quota_metric = os.getenv("GEMINI_QUOTA_METRIC", DEFAULT_GEMINI_QUOTA_METRIC).strip()
    service = os.getenv("GEMINI_QUOTA_SERVICE", DEFAULT_GEMINI_QUOTA_SERVICE).strip()
    usage_metric_type = os.getenv("GEMINI_QUOTA_USAGE_METRIC_TYPE", DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE).strip()
    if not project_id:
        status = QuotaStatus(
            status="not_configured",
            message="GEMINI_QUOTA_PROJECT_ID is not configured.",
            metric=quota_metric,
        )
        _publish(status)
        return status

    try:
        from google.cloud import monitoring_v3  # type: ignore[import-not-found]
    except Exception:
        status = QuotaStatus(
            status="degraded",
            message="google-cloud-monitoring is not installed; configure Cloud Monitoring alerts or add the optional library.",
            metric=quota_metric,
            project_id=project_id,
        )
        _publish(status)
        return status

    try:
        client = monitoring_v3.MetricServiceClient()
        now = datetime.now(UTC)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": now,
                "start_time": now - timedelta(hours=24),
            }
        )
        request = monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project_id}",
            filter=_build_usage_filter(
                usage_metric_type=usage_metric_type,
                service=service,
                quota_metric=quota_metric,
            ),
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        )
        usage = 0.0
        for series in client.list_time_series(request=request):
            if not series.points:
                continue
            value = series.points[0].value
            usage += float(value.double_value or value.int64_value or 0)

        daily_limit = _float_env("GEMINI_QUOTA_DAILY_LIMIT")
        remaining = daily_limit - usage if daily_limit is not None else None
        critical = _float_env("GEMINI_QUOTA_CRITICAL_REMAINING", default=0)
        warning = _float_env("GEMINI_QUOTA_WARN_REMAINING", default=25)
        status = "ok"
        if remaining is not None and remaining <= critical:
            status = "degraded"
        elif remaining is not None and remaining <= warning:
            status = "degraded"

        status_value = QuotaStatus(
            status=status,
            message="Gemini quota metric query completed.",
            metric=quota_metric,
            project_id=project_id,
            usage=usage,
            remaining=remaining,
        )
        _publish(status_value)
        return status_value
    except Exception as exc:
        status_value = QuotaStatus(
            status="degraded",
            message=f"Gemini quota metric query failed safely: {type(exc).__name__}",
            metric=quota_metric,
            project_id=project_id,
        )
        _publish(status_value)
        return status_value


def _build_usage_filter(*, usage_metric_type: str, service: str, quota_metric: str) -> str:
    return " ".join(
        [
            f'metric.type = "{_monitoring_filter_value(usage_metric_type)}"',
            'resource.type = "consumer_quota"',
            f'resource.label.service = "{_monitoring_filter_value(service)}"',
            f'metric.label.quota_metric = "{_monitoring_filter_value(quota_metric)}"',
        ]
    )


def _monitoring_filter_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _float_env(name: str, default: float | None = None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _provider_from_model(model_name: str) -> str:
    if ":" in model_name:
        return model_name.split(":", 1)[0]
    if model_name.startswith("gemini"):
        return "google-gla"
    return "unknown"


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _publish(status: QuotaStatus) -> None:
    configured = 1 if status.project_id else 0
    GEMINI_QUOTA_CONFIGURED.set(configured)
    GEMINI_QUOTA_PROBE_OK.set(1 if status.status in {"ok", "not_configured"} else 0)
    GEMINI_QUOTA_USAGE.set(status.usage if status.usage is not None else -1)
    GEMINI_QUOTA_REMAINING.set(status.remaining if status.remaining is not None else -1)


def _publish_openrouter(key: OpenRouterKeyStatus, account: OpenRouterAccountCreditStatus | None) -> None:
    OPENROUTER_KEY_CONFIGURED.set(0 if key.status == "not_configured" else 1)
    OPENROUTER_CREDIT_PROBE_OK.set(1 if key.status in {"ok", "not_configured"} and (account is None or account.status in {"ok", "not_configured"}) else 0)
    OPENROUTER_LIMIT_REMAINING.set(key.limit_remaining if key.limit_remaining is not None else -1)
    OPENROUTER_LIMIT.set(key.limit if key.limit is not None else -1)
    OPENROUTER_USAGE_TOTAL.set(key.usage if key.usage is not None else -1)
    OPENROUTER_USAGE_DAILY.set(key.usage_daily if key.usage_daily is not None else -1)
    OPENROUTER_USAGE_WEEKLY.set(key.usage_weekly if key.usage_weekly is not None else -1)
    OPENROUTER_USAGE_MONTHLY.set(key.usage_monthly if key.usage_monthly is not None else -1)
    OPENROUTER_ACCOUNT_TOTAL_CREDITS.set(account.total_credits if account and account.total_credits is not None else -1)
    OPENROUTER_ACCOUNT_TOTAL_USAGE.set(account.total_usage if account and account.total_usage is not None else -1)
    OPENROUTER_ACCOUNT_REMAINING_CREDITS.set(account.remaining if account and account.remaining is not None else -1)
