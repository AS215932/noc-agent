import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from prometheus_client import Gauge


DEFAULT_GEMINI_QUOTA_METRIC = "generativelanguage.googleapis.com/generate_requests_per_model_per_day"
DEFAULT_GEMINI_QUOTA_SERVICE = "generativelanguage.googleapis.com"
DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE = "serviceruntime.googleapis.com/quota/rate/net_usage"

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


def _publish(status: QuotaStatus) -> None:
    configured = 1 if status.project_id else 0
    GEMINI_QUOTA_CONFIGURED.set(configured)
    GEMINI_QUOTA_PROBE_OK.set(1 if status.status in {"ok", "not_configured"} else 0)
    GEMINI_QUOTA_USAGE.set(status.usage if status.usage is not None else -1)
    GEMINI_QUOTA_REMAINING.set(status.remaining if status.remaining is not None else -1)
