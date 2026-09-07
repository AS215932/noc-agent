from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import Response

from app.cases import CaseService, InMemoryCaseStore, OutboxIntent
from app.cases.health import delivery_health
from app.cases.store import OutboxHealth
from app.cases.runtime import CaseServiceRuntime, process_case_outbox_once


def runtime():
    store = InMemoryCaseStore()
    return CaseServiceRuntime(service=CaseService(store), store=store, started_at=1000, started_monotonic=1000)


def test_idle_worker_has_startup_grace_but_not_unlimited_grace():
    state = runtime()
    assert delivery_health(state, OutboxHealth(), enabled=True, running=True, now=1100, monotonic_now=1100)["status"] == "ok"
    health = delivery_health(state, OutboxHealth(), enabled=True, running=True, now=1400, monotonic_now=1400)
    assert health["reasons"] == ["worker_stale"]
    state.outbox_last_completed_at = 1399
    state.outbox_last_completed_monotonic = 1399
    assert delivery_health(state, OutboxHealth(), enabled=True, running=True, now=1400, monotonic_now=1400)["status"] == "ok"
    assert delivery_health(state, OutboxHealth(), enabled=True, running=False, now=1400, monotonic_now=1400)["reasons"] == ["worker_not_running"]
    assert delivery_health(state, OutboxHealth(), enabled=False, running=False, now=9000, monotonic_now=9000)["status"] == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "failed", "in_progress"])
async def test_old_undelivered_report_detected_despite_fresh_heartbeat(status):
    state = runtime()
    state.outbox_last_completed_at = 1999
    state.outbox_last_completed_monotonic = 1999
    intent = OutboxIntent(case_id="test-case", intent_type="report", idempotency_key="old", status=status,
                          created_at=datetime.fromtimestamp(1000, timezone.utc).isoformat())
    await state.store.enqueue_outbox(intent)
    health = delivery_health(state, await state.store.outbox_health(), enabled=True, running=True, now=2000, monotonic_now=2000)
    assert health["reasons"] == ["reports_overdue"]
    assert health["outstanding_reports"] == 1
    assert health["oldest_report_age_seconds"] == 1000


@pytest.mark.asyncio
async def test_idle_tick_updates_heartbeat_and_store_failure_does_not(monkeypatch):
    state = runtime()
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "0")
    await process_case_outbox_once(state)
    completed = state.outbox_last_completed_at
    assert completed >= state.outbox_last_started_at > 0
    monkeypatch.setattr(state.store, "list_outbox", AsyncMock(side_effect=RuntimeError("database unavailable")))
    with pytest.raises(RuntimeError):
        await process_case_outbox_once(state)
    assert state.outbox_last_completed_at == completed


@pytest.mark.asyncio
async def test_health_endpoint_detects_stuck_running_task(monkeypatch):
    import app.main as main

    state = runtime()
    monkeypatch.setattr(main, "case_service_runtime", state)
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")

    class RunningTask:
        def done(self):
            return False

    monkeypatch.setattr(main, "case_outbox_task", RunningTask())
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["outbox_worker"]["running"] is True
    assert "worker_stale" in result["delivery"]["reasons"]


@pytest.mark.asyncio
async def test_health_endpoint_store_error_is_degraded_and_sanitized(monkeypatch):
    import app.main as main

    state = runtime()
    monkeypatch.setattr(main, "case_service_runtime", state)
    monkeypatch.setattr(state.store, "outbox_health", AsyncMock(side_effect=RuntimeError("private database detail")))
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["status"] == "degraded"
    assert "private database detail" not in str(result)


def test_long_poll_interval_does_not_mask_report_age_or_false_alarm_heartbeat():
    state = runtime()
    state.outbox_last_completed_at = 1000
    state.outbox_last_completed_monotonic = 1000
    health = delivery_health(state, OutboxHealth(), enabled=True, running=True, now=1500, monotonic_now=1500, worker_interval_s=600)
    assert health["status"] == "ok"
    health = delivery_health(state, OutboxHealth(outstanding_reports=1, oldest_report_timestamp=1000),
                             enabled=True, running=True, now=1500, monotonic_now=1500, worker_interval_s=600)
    assert health["reasons"] == ["reports_overdue"]
    assert health["stale_after_seconds"] == 300
    assert health["heartbeat_stale_after_seconds"] == 1230


@pytest.mark.asyncio
async def test_enabled_worker_without_runtime_is_degraded(monkeypatch):
    import app.main as main

    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setattr(main, "case_service_runtime", None)
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["delivery"]["reasons"] == ["worker_runtime_unavailable"]


@pytest.mark.asyncio
async def test_health_endpoint_never_materializes_outbox_payloads(monkeypatch):
    import app.main as main

    state = runtime()
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "0")
    monkeypatch.setattr(main, "case_service_runtime", state)
    monkeypatch.setattr(state.store, "list_outbox", AsyncMock(side_effect=AssertionError("unbounded lookup")))
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 200
    assert result["outbox"] == {"pending": 0, "failed": 0}
    state.store.list_outbox.assert_not_called()


@pytest.mark.parametrize("wall_now", [-10000, 10000000000])
def test_wall_clock_jumps_do_not_change_worker_staleness(wall_now):
    state = runtime()
    state.outbox_last_completed_at = 1200
    state.outbox_last_completed_monotonic = 1200
    healthy = delivery_health(state, OutboxHealth(), enabled=True, running=True,
                              now=wall_now, monotonic_now=1300)
    assert healthy["heartbeat_age_seconds"] == 100
    assert healthy["status"] == "ok"
    stale = delivery_health(state, OutboxHealth(), enabled=True, running=True,
                            now=wall_now, monotonic_now=1600)
    assert stale["heartbeat_age_seconds"] == 400
    assert stale["reasons"] == ["worker_stale"]
