import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from app.cases import AtomicCaseProjection, CaseService, InMemoryCaseStore
from app.cases.handlers import build_report_handler
from app.cases.runtime import CaseServiceRuntime, enqueue_due_case_reminders, process_case_outbox_once


async def add_case(runtime, case_id, **changes):
    case = AtomicCaseProjection(
        case_id=case_id, severity="HIGH",
        last_reported_at=(datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
    ).model_copy(update=changes)
    case.last_reported_signature = runtime.service.report_state_signature(case)
    return await runtime.store.upsert_case(case)


def make_runtime(store=None):
    store = store or InMemoryCaseStore()
    return CaseServiceRuntime(store=store, service=CaseService(store))


@pytest.mark.asyncio
async def test_scheduler_is_disabled_with_reactive_reporting(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_REPORT", raising=False)
    runtime = make_runtime()
    await add_case(runtime, "a")
    runtime.store.list_reminder_candidates = AsyncMock(side_effect=AssertionError("unexpected scan"))
    assert await enqueue_due_case_reminders(runtime) == 0
    runtime.store.list_reminder_candidates.assert_not_called()


@pytest.mark.asyncio
async def test_scan_pages_by_stable_id_and_wraps_without_starvation(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    runtime = make_runtime()
    for case_id in ["a", "b", "c", "d", "e"]:
        await add_case(runtime, case_id)
    await runtime.service.ack("b", operator="oncall")
    assert await enqueue_due_case_reminders(runtime, batch_size=2) == 1
    assert runtime.reminder_cursor == "b"
    a = await runtime.store.get_case("a")
    a.updated_at = "2099-01-01T00:00:00+00:00"
    await runtime.store.upsert_case(a)
    assert await enqueue_due_case_reminders(runtime, batch_size=2) == 2
    assert runtime.reminder_cursor == "d"
    assert await enqueue_due_case_reminders(runtime, batch_size=2) == 1
    assert runtime.reminder_cursor == ""
    assert {item.case_id for item in await runtime.store.list_outbox()} == {"a", "c", "d", "e"}


@pytest.mark.asyncio
async def test_restart_and_parallel_scans_reuse_durable_pending_identity(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    first = make_runtime()
    await add_case(first, "a")
    restarted = make_runtime(first.store)
    await asyncio.gather(enqueue_due_case_reminders(first), enqueue_due_case_reminders(restarted))
    assert len(await first.store.list_outbox()) == 1
    await enqueue_due_case_reminders(make_runtime(first.store))
    assert len(await first.store.list_outbox()) == 1


@pytest.mark.asyncio
async def test_worker_delivers_without_another_monitor_observation(monkeypatch):
    import app.cases.handlers as handlers

    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    runtime = make_runtime()
    await add_case(runtime, "due")
    await add_case(runtime, "ack", acknowledged_by="oncall")
    await add_case(runtime, "warning", severity="MEDIUM")
    await add_case(runtime, "resolved", status="resolved")
    await add_case(runtime, "never-reported", last_reported_at="")
    notifier = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "build_default_outbox_handlers", lambda service, **kwargs: {
        "report": build_report_handler(service, notifier=notifier, reminder_notifier=notifier),
    })
    result = await process_case_outbox_once(runtime)
    assert result.succeeded == 1
    notifier.assert_awaited_once()
    assert (await runtime.store.get_case("due")).last_reasserted_at
    assert (await process_case_outbox_once(runtime)).processed == 0


@pytest.mark.asyncio
async def test_scan_failure_does_not_block_existing_delivery(monkeypatch):
    import app.cases.handlers as handlers

    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    runtime = make_runtime()
    case = await add_case(runtime, "due")
    await runtime.service.request_report(case)
    runtime.store.list_reminder_candidates = AsyncMock(side_effect=RuntimeError("scan unavailable"))
    notifier = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "build_default_outbox_handlers", lambda service, **kwargs: {
        "report": build_report_handler(service, notifier=notifier, reminder_notifier=notifier),
    })
    assert (await process_case_outbox_once(runtime)).succeeded == 1
    notifier.assert_awaited_once()
