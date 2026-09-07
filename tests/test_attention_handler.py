import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from app.cases.attention_handler import build_attention_handler
from app.cases.attention_scheduler import enqueue_attention
from app.cases.models import AtomicCaseProjection
from app.cases.outbox import OutboxProcessor
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_failed_send_retries_without_advancing_attention_clock():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    intent = await enqueue_attention(store, case)
    sender = AsyncMock(return_value=False)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)}, retry_backoff_s=0)
    assert (await processor.process_intent(intent)).failed == 1
    assert await store.get_attention(case.case_id) is None
    assert case.case_id not in store._attention_leases
    sender.return_value = True
    assert (await processor.process_pending()).succeeded == 1
    delivered = await store.get_attention(case.case_id)
    assert delivered.sequence == 1
    assert case.case_id not in store._attention_leases
    assert (await processor.process_pending()).processed == 0
    assert sender.await_count == 2
    # Quiet telemetry does not change the attention delivery time or identity.
    case.signal_signature = "updated telemetry"
    case.last_reported_at = datetime.now(timezone.utc).isoformat()
    await store.upsert_case(case)
    assert await enqueue_attention(store, case) is None
    assert await store.get_attention(case.case_id) == delivered


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["ack", "recovery", "suppression", "generation"])
async def test_state_change_between_enqueue_and_claim_prevents_stale_send(monkeypatch, transition):
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "alertmanager"})
    await store.upsert_case(case)
    intent = await enqueue_attention(store, case)
    original_claim = store.claim_attention

    async def claim_then_change(*args, **kwargs):
        lease = await original_claim(*args, **kwargs)
        if transition == "ack":
            case.acknowledged_by = "operator"
        elif transition == "recovery":
            case.status = "resolved"
        elif transition == "suppression":
            case.suppressed_until = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        else:
            case.report_generation += 1
        await store.upsert_case(case)
        return lease

    monkeypatch.setattr(store, "claim_attention", claim_then_change)
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    assert (await processor.process_intent(intent)).succeeded == 1
    sender.assert_not_awaited()
    assert await store.get_attention(case.case_id) is None
    assert case.case_id not in store._attention_leases
    stored, = await store.list_outbox(status="succeeded")
    assert stored.payload["notification_suppressed"] == "attention_no_longer_due"


@pytest.mark.asyncio
async def test_recovery_supersedes_queued_reminder_and_delivers_once():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    await enqueue_attention(store, case)
    assert (await processor.process_pending()).succeeded == 1
    delivered = await store.get_attention(case.case_id)
    store._attention[case.case_id] = delivered.model_copy(update={
        "delivered_at": datetime.now(timezone.utc) - timedelta(hours=7),
    })
    reminder = await enqueue_attention(store, case)
    assert reminder.payload["attention_request"]["kind"] == "reminder"
    case.status = "resolved"
    await store.upsert_case(case)
    recovery = await enqueue_attention(store, case)
    assert recovery.payload["attention_request"]["kind"] == "recovery"
    assert (await processor.process_pending()).succeeded == 2
    assert [call.args[1].kind for call in sender.await_args_list] == ["new", "recovery"]
    assert (await store.get_attention(case.case_id)).phase == "recovered"
    assert await enqueue_attention(store, case) is None


@pytest.mark.asyncio
async def test_temporary_suppression_can_reopen_same_undelivered_generation():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    original = await enqueue_attention(store, case)
    case.snoozed_until = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    await store.upsert_case(case)
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    assert (await processor.process_pending()).succeeded == 1
    sender.assert_not_awaited()
    case.snoozed_until = ""
    await store.upsert_case(case)
    reopened = await enqueue_attention(store, case)
    assert reopened.outbox_id == original.outbox_id and reopened.status == "pending"
    assert (await processor.process_pending()).succeeded == 1
    sender.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_send_releases_lease_without_recording_delivery():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    intent = await enqueue_attention(store, case)
    started = asyncio.Event()

    async def sender(*_args):
        started.set()
        await asyncio.Event().wait()
        return True

    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    task = asyncio.create_task(processor.process_intent(intent))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await store.get_attention(case.case_id) is None
    assert case.case_id not in store._attention_leases
