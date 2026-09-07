"""Delivery health independent of HTTP intake and model availability."""

import time

from app.cases.store import OutboxHealth
from app.cases.runtime import CaseServiceRuntime


def delivery_health(
    runtime: CaseServiceRuntime,
    outstanding: OutboxHealth,
    *,
    enabled: bool,
    running: bool,
    stale_after_s: int = 300,
    worker_interval_s: int = 30,
    now: float | None = None,
    monotonic_now: float | None = None,
) -> dict:
    now = time.time() if now is None else now
    monotonic_now = time.monotonic() if monotonic_now is None else monotonic_now
    threshold = max(30, stale_after_s)
    heartbeat_threshold = max(threshold, 2 * max(1, worker_interval_s) + 30)
    completed = getattr(runtime, "outbox_last_completed_at", 0.0)
    baseline = float(getattr(runtime, "started_monotonic", monotonic_now))
    completion_clock = getattr(runtime, "outbox_last_completed_monotonic", None)
    if completion_clock is not None:
        baseline = float(completion_clock)
    heartbeat_age = max(0.0, monotonic_now - baseline)
    oldest = max(0.0, now - outstanding.oldest_report_timestamp) if outstanding.oldest_report_timestamp is not None else 0.0
    reports = outstanding.outstanding_reports
    undated = outstanding.invalid_report_timestamps
    reasons = []
    if enabled:
        if not running:
            reasons.append("worker_not_running")
        if heartbeat_age > heartbeat_threshold:
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
        "heartbeat_stale_after_seconds": heartbeat_threshold,
    }
