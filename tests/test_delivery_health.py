from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import Response

from app.cases import CaseService, InMemoryCaseStore, OutboxIntent
from app.cases.health import delivery_health
from app.cases.runtime import CaseServiceRuntime, process_case_outbox_once


def runtime():
    store = InMemoryCaseStore()
    return CaseServiceRuntime(service=CaseService(store), store=store, started_at=1000)


def test_idle_worker_has_startup_grace_but_not_unlimited_grace():
    state = runtime()
    assert delivery_health(state, [], enabled=True, running=True, now=1100)["status"] == "ok"
    health = delivery_health(state, [], enabled=True, running=True, now=1400)
    assert health["reasons"] == ["worker_stale"]
    state.outbox_last_completed_at = 1399
    assert delivery_health(state, [], enabled=True, running=True, now=1400)["status"] == "ok"
    assert delivery_health(state, [], enabled=True, running=False, now=1400)["reasons"] == ["worker_not_running"]
    assert delivery_health(state, [], enabled=False, running=False, now=9000)["status"] == "ok"


@pytest.mark.parametrize("status", ["pending", "failed", "in_progress"])
def test_old_undelivered_report_detected_despite_fresh_heartbeat(status):
    state = runtime()
    state.outbox_last_completed_at = 1999
    intent = OutboxIntent(case_id="test-case", intent_type="report", idempotency_key="old", status=status,
                          created_at=datetime.fromtimestamp(1000, timezone.utc).isoformat())
    health = delivery_health(state, [intent], enabled=True, running=True, now=2000)
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
    monkeypatch.setattr(state.store, "list_outbox", AsyncMock(side_effect=RuntimeError("private database detail")))
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["status"] == "degraded"
    assert "private database detail" not in str(result)
