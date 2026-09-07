"""Delivery health independent of HTTP intake and model availability."""

from datetime import datetime, timezone
import time

from app.cases.models import OutboxIntent
from app.cases.runtime import CaseServiceRuntime


def delivery_health(
    runtime: CaseServiceRuntime,
    outstanding: list[OutboxIntent],
    *,
    enabled: bool,
    running: bool,
    stale_after_s: int = 300,
    now: float | None = None,
) -> dict:
    now = time.time() if now is None else now
    threshold = max(30, stale_after_s)
    completed = getattr(runtime, "outbox_last_completed_at", 0.0)
    baseline = completed or getattr(runtime, "started_at", 0.0)
    heartbeat_age = max(0.0, now - baseline)
    oldest = 0.0
    undated = 0
    reports = 0
    for intent in outstanding:
        if getattr(intent, "intent_type", "") != "report":
            continue
        reports += 1
        try:
            created = datetime.fromisoformat(intent.created_at.replace("Z", "+00:00"))
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            oldest = max(oldest, now - created.timestamp())
        except (ValueError, TypeError, AttributeError):
            undated += 1
    reasons = []
    if enabled:
        if not running:
            reasons.append("worker_not_running")
        if heartbeat_age > threshold:
            reasons.append("worker_stale")
        if reports and oldest > threshold:
            reasons.append("reports_overdue")
        if undated:
            reasons.append("report_timestamp_invalid")
    return {
        "status": "degraded" if reasons else "ok",
        "reasons": reasons,
        "last_started_at": getattr(runtime, "outbox_last_started_at", 0.0),
        "last_completed_at": completed,
        "heartbeat_age_seconds": heartbeat_age,
        "outstanding_reports": reports,
        "oldest_report_age_seconds": max(0.0, oldest),
        "stale_after_seconds": threshold,
    }
