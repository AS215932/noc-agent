from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from app.cases.attention import AttentionDelivery
from app.cases.attention_scheduler import enqueue_attention
from app.cases.handlers import build_report_handler
from app.cases.models import AtomicCaseProjection
from app.cases.outbox import OutboxProcessor
from app.cases.runtime import CaseServiceRuntime, enqueue_due_case_reminders
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_flag_rollback_drains_attention_before_resuming_legacy_clock(monkeypatch):
    for flag in ("NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED"):
        monkeypatch.setenv(flag, "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    runtime = CaseServiceRuntime(service=service, store=store)
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    case.last_reported_at = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    case.last_reported_signature = service.report_state_signature(case)
    await store.upsert_case(case)
    store._attention[case.case_id] = AttentionDelivery(case_id=case.case_id, severity="HIGH", phase="firing",
        generation=0, sequence=1, delivered_at=datetime.now(timezone.utc) - timedelta(hours=7))
    # A legacy request can already exist when configuration changes. Put it
    # first in the queue to prove delivery-time checks, not just scheduler order.
    await service.request_report(case)
    await enqueue_attention(store, case)
    monkeypatch.setenv("NOC_CASE_ATTENTION_ENABLED", "0")
    assert await enqueue_due_case_reminders(runtime) == 0
    attention_send, legacy_send = AsyncMock(return_value=True), AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_report_handler(service,
        notifier=attention_send, reminder_notifier=legacy_send)})
    assert (await processor.process_pending()).succeeded == 2
    attention_send.assert_awaited_once()
    legacy_send.assert_not_awaited()
    assert await enqueue_due_case_reminders(runtime) == 0
    assert (await store.get_case(case.case_id)).last_reported_at == case.last_reported_at
    # Legacy reminders resume after the actual last attention delivery interval.
    attention = await store.get_attention(case.case_id)
    store._attention[case.case_id] = attention.model_copy(update={
        "delivered_at": datetime.now(timezone.utc) - timedelta(hours=7),
    })
    assert await enqueue_due_case_reminders(runtime) == 1
    assert (await processor.process_pending()).succeeded == 1
    legacy_send.assert_awaited_once()
    assert await enqueue_due_case_reminders(runtime) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_delivery", [False, True])
async def test_inflight_legacy_reminder_fences_concurrent_attention(monkeypatch, previous_delivery):
    import asyncio

    monkeypatch.setenv("NOC_CASE_ATTENTION_ENABLED", "0")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    case.last_reported_at = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    case.last_reported_signature = service.report_state_signature(case)
    await store.upsert_case(case)
    if previous_delivery:
        store._attention[case.case_id] = AttentionDelivery(case_id=case.case_id, severity="HIGH", phase="firing",
            generation=0, sequence=1, delivered_at=datetime.now(timezone.utc) - timedelta(hours=9))
    legacy = await service.request_report(case)
    started, finish = asyncio.Event(), asyncio.Event()

    async def legacy_send(**kwargs):
        started.set()
        await finish.wait()
        return True

    attention_send = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_report_handler(service,
        notifier=attention_send, reminder_notifier=legacy_send)}, retry_backoff_s=0)
    task = asyncio.create_task(processor.process_intent(legacy))
    await asyncio.wait_for(started.wait(), timeout=2)
    try:
        # Another worker may still use enabled configuration while this worker
        # rolls back. Enqueue after the legacy send's eligibility check.
        attention = await enqueue_attention(store, case)
        assert attention is not None
        assert (await processor.process_intent(attention)).failed == 1
        attention_send.assert_not_awaited()
    finally:
        finish.set()
        result = await task
    assert result.succeeded == 1
    delivery = await store.get_attention(case.case_id)
    assert delivery.sequence == (2 if previous_delivery else 1)
    assert datetime.now(timezone.utc) - delivery.delivered_at < timedelta(seconds=5)
    assert (await processor.process_pending()).succeeded == 1
    attention_send.assert_not_awaited()
    assert await enqueue_attention(store, await store.get_case(case.case_id)) is None
    assert case.case_id not in store._attention_leases


@pytest.mark.asyncio
async def test_failed_legacy_reminder_releases_shared_lease_without_clock(monkeypatch):
    monkeypatch.setenv("NOC_CASE_ATTENTION_ENABLED", "0")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "alertmanager"})
    case.last_reported_at = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    case.last_reported_signature = service.report_state_signature(case)
    await store.upsert_case(case)
    legacy = await service.request_report(case)
    send = AsyncMock(return_value=False)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, reminder_notifier=send)}, retry_backoff_s=0)
    assert (await processor.process_intent(legacy)).failed == 1
    assert await store.get_attention(case.case_id) is None
    assert case.case_id not in store._attention_leases
    send.return_value = True
    assert (await processor.process_pending()).succeeded == 1
    assert (await store.get_attention(case.case_id)).sequence == 1
