from datetime import datetime, timedelta, timezone

import pytest

from app.cases import InMemoryCaseStore, OutboxHandlerResult, OutboxIntent, OutboxProcessor


@pytest.mark.asyncio
async def test_outbox_processor_executes_pending_intents_once():
    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(
        OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="report:case_1:sig")
    )
    seen = []

    async def handle_report(row: OutboxIntent):
        seen.append(row.outbox_id)
        return OutboxHandlerResult(external_id="discord-1", external_url="https://discord.invalid/msg/1")

    report = await OutboxProcessor(store, {"report": handle_report}).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert seen == [intent.outbox_id]
    stored = (await store.list_outbox())[0]
    assert stored.status == "succeeded"
    assert stored.attempts == 1
    assert stored.external_id == "discord-1"
    assert stored.external_url == "https://discord.invalid/msg/1"

    second = await OutboxProcessor(store, {"report": handle_report}).process_pending()
    assert second.processed == 0
    assert seen == [intent.outbox_id]


@pytest.mark.asyncio
async def test_outbox_processor_marks_failures_retryable():
    store = InMemoryCaseStore()
    await store.enqueue_outbox(OutboxIntent(case_id="case_1", intent_type="handoff", idempotency_key="handoff:case_1"))

    async def broken(row: OutboxIntent):
        raise RuntimeError("github unavailable")

    report = await OutboxProcessor(store, {"handoff": broken}, retry_backoff_s=5).process_pending()

    assert report.processed == 1
    assert report.failed == 1
    stored = (await store.list_outbox())[0]
    assert stored.status == "failed"
    assert stored.attempts == 1
    assert "github unavailable" in stored.error
    assert stored.next_attempt_at


@pytest.mark.asyncio
async def test_outbox_processor_preserves_concurrent_abandonment():
    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(
        OutboxIntent(case_id="case_1", intent_type="handoff", idempotency_key="handoff:cancelled")
    )

    async def cancelled_while_handler_finishes(row: OutboxIntent):
        abandoned = row.model_copy(
            update={"status": "abandoned", "completed_at": "2026-07-21T20:00:00+00:00", "error": "cancelled"}
        )
        assert await store.update_outbox_if_status(abandoned, expected_status="in_progress") is not None
        return OutboxHandlerResult(external_id="stale-result", external_url="https://example.invalid/stale")

    report = await OutboxProcessor(store, {"handoff": cancelled_while_handler_finishes}).process_pending()

    assert report.processed == 1
    assert report.succeeded == 0
    assert report.skipped == 1
    stored = next(row for row in await store.list_outbox() if row.outbox_id == intent.outbox_id)
    assert stored.status == "abandoned"
    assert stored.error == "cancelled"
    assert stored.external_id == ""
    assert stored.external_url == ""


@pytest.mark.asyncio
async def test_outbox_processor_skips_unknown_intent_types_without_claiming():
    store = InMemoryCaseStore()
    await store.enqueue_outbox(OutboxIntent(case_id="case_1", intent_type="knowledge_candidate", idempotency_key="kc:1"))

    report = await OutboxProcessor(store, {}).process_pending()

    assert report.skipped == 1
    stored = (await store.list_outbox())[0]
    assert stored.status == "pending"
    assert stored.attempts == 0


@pytest.mark.asyncio
async def test_outbox_processor_retries_failed_intents_when_due():
    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="report:retry"))
    failed = intent.model_copy(update={"status": "failed", "next_attempt_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()})
    await store.update_outbox(failed)
    seen = []

    async def handle(row: OutboxIntent):
        seen.append(row.attempts)
        return None

    report = await OutboxProcessor(store, {"report": handle}).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert seen == [1]
    stored = (await store.list_outbox())[0]
    assert stored.status == "succeeded"


@pytest.mark.asyncio
async def test_outbox_processor_waits_for_failed_retry_backoff():
    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="report:later"))
    failed = intent.model_copy(update={"status": "failed", "next_attempt_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()})
    await store.update_outbox(failed)

    async def handle(row: OutboxIntent):  # pragma: no cover - must not run
        raise AssertionError("not due")

    report = await OutboxProcessor(store, {"report": handle}).process_pending()

    assert report.processed == 0
    stored = (await store.list_outbox())[0]
    assert stored.status == "failed"


@pytest.mark.asyncio
async def test_outbox_processor_respects_limit():
    store = InMemoryCaseStore()
    await store.enqueue_outbox(OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="report:1"))
    await store.enqueue_outbox(OutboxIntent(case_id="case_2", intent_type="report", idempotency_key="report:2"))

    async def handle(row: OutboxIntent):
        return None

    report = await OutboxProcessor(store, {"report": handle}).process_pending(limit=1)

    assert report.processed == 1
    rows = await store.list_outbox()
    assert [row.status for row in rows].count("succeeded") == 1
    assert [row.status for row in rows].count("pending") == 1


@pytest.mark.asyncio
async def test_cancelled_notification_is_recovered_after_lease_expires():
    import asyncio
    from unittest.mock import AsyncMock

    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(OutboxIntent(
        case_id="case_1", intent_type="report", idempotency_key="report:cancelled",
    ))
    entered = asyncio.Event()
    async def hung(row):
        entered.set()
        await asyncio.Event().wait()
    task = asyncio.create_task(OutboxProcessor(store, {"report": hung}).process_intent(intent))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    claimed = (await store.list_outbox())[0]
    assert claimed.status == "in_progress" and claimed.claim_token and claimed.claim_expires_at
    retry = AsyncMock(return_value=OutboxHandlerResult(external_id="delivered"))
    processor = OutboxProcessor(store, {"report": retry})
    assert (await processor.process_pending()).processed == 0
    claimed.claim_expires_at = "2000-01-01T00:00:00+00:00"
    await store.update_outbox(claimed)
    assert (await processor.process_pending()).succeeded == 1
    retry.assert_awaited_once()
    recovered = (await store.list_outbox())[0]
    assert recovered.claim_token != claimed.claim_token
    assert recovered.external_id == "delivered"


@pytest.mark.asyncio
@pytest.mark.parametrize("old_raises", [False, True])
async def test_expired_claim_cannot_overwrite_new_worker_result(old_raises):
    import asyncio

    store = InMemoryCaseStore()
    intent = await store.enqueue_outbox(OutboxIntent(
        case_id="case_1", intent_type="report", idempotency_key="report:stale-worker",
    ))
    entered, release = asyncio.Event(), asyncio.Event()
    async def old(row):
        entered.set()
        await release.wait()
        if old_raises:
            raise RuntimeError("stale failure")
        return OutboxHandlerResult(external_id="stale")
    async def new(row):
        return OutboxHandlerResult(external_id="current")
    task = asyncio.create_task(OutboxProcessor(store, {"report": old}).process_intent(intent))
    await asyncio.wait_for(entered.wait(), 2)
    try:
        claim = (await store.list_outbox())[0]
        claim.claim_expires_at = "2000-01-01T00:00:00+00:00"
        await store.update_outbox(claim)
        assert (await OutboxProcessor(store, {"report": new}).process_pending()).succeeded == 1
    finally:
        release.set()
        old_result = await asyncio.wait_for(task, 2)
    assert old_result.skipped == 1
    final = (await store.list_outbox())[0]
    assert final.status == "succeeded" and final.external_id == "current"
    assert not final.error


@pytest.mark.asyncio
async def test_legacy_report_claim_recovers_without_replaying_other_side_effects():
    from unittest.mock import AsyncMock

    store = InMemoryCaseStore()
    for kind in ["report", "handoff"]:
        await store.enqueue_outbox(OutboxIntent(
            case_id="case_1", intent_type=kind, idempotency_key=kind,
            status="in_progress", created_at="2000-01-01T00:00:00+00:00",
        ))
    handler = AsyncMock(return_value=None)
    assert (await OutboxProcessor(store, {"report": handler, "handoff": handler}).process_pending()).succeeded == 1
    handler.assert_awaited_once()
    rows = {row.intent_type: row for row in await store.list_outbox()}
    assert rows["report"].status == "succeeded"
    assert rows["handoff"].status == "in_progress"
