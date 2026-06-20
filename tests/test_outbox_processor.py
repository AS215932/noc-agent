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
