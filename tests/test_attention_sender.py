from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.case_cards import deliver_case_card
from app.cases.attention import AttentionDelivery, attention_due
from app.cases.attention_handler import build_attention_handler
from app.cases.attention_scheduler import enqueue_attention
from app.cases.attention_sender import build_attention_sender
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.outbox import OutboxProcessor
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_initial_attention_reuses_existing_facts_card(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    case = AtomicCaseProjection(severity="HIGH", summary="Disk space low", identity={"source": "icinga2"})
    assert await deliver_case_card(destination="test-channel", case_id=case.case_id, payload={"description": "initial facts"},
                                    create=create, edit=edit, revision=1)

    async def notify(**card):
        return await deliver_case_card(destination="test-channel", case_id=card.pop("case_id"),
                                        revision=card.pop("revision"), payload=card, create=create, edit=edit)

    request = attention_due(case, None, now=datetime.now(timezone.utc))
    intent = OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key=request.idempotency_key)
    sender = build_attention_sender(notifier=notify)
    assert await sender(case, request, intent) is True
    # A lost database completion and process restart must not create a second
    # initial card. The second sender has no in-process message cache.
    assert await build_attention_sender(notifier=notify)(case, request, intent) is True
    create.assert_awaited_once()
    edit.assert_awaited_once_with(123)


@pytest.mark.asyncio
async def test_lifecycle_attention_retries_reuse_one_event_message(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    create, edit = AsyncMock(return_value=321), AsyncMock(return_value=True)
    case = AtomicCaseProjection(severity="HIGH", summary="Disk [probe] <value>", identity={"source": "icinga2"})
    previous = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="LOW",
                                  delivered_at=datetime.now(timezone.utc), sequence=1)
    request = attention_due(case, previous, now=datetime.now(timezone.utc))
    assert request.kind == "escalation"
    intent = OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key=request.idempotency_key)
    observed = []

    async def notify(**card):
        observed.append(dict(card))
        return await deliver_case_card(destination="test-channel", case_id=card.pop("case_id"),
                                        revision=card.pop("revision"), payload=card, create=create, edit=edit)

    assert await build_attention_sender(notifier=notify)(case, request, intent) is True
    assert await build_attention_sender(notifier=notify)(case, request, intent) is True
    assert observed[0]["case_id"] == observed[1]["case_id"] == request.idempotency_key
    assert observed[0]["case_id"] != case.case_id
    assert observed[0]["description"] == "Disk probe value"
    create.assert_awaited_once()
    edit.assert_not_awaited()


@pytest.mark.asyncio
async def test_verbosity_suppressed_attention_reopens_only_when_eligible(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="LOW", identity={"source": "icinga2"})
    await store.upsert_case(case)
    original = await enqueue_attention(store, case)
    notify = AsyncMock(return_value=True)
    handler = build_attention_handler(store, sender=build_attention_sender(notifier=notify))
    processor = OutboxProcessor(store, {"report": handler})
    assert (await processor.process_pending()).succeeded == 1
    assert await store.get_attention(case.case_id) is None
    notify.assert_not_awaited()
    assert (await enqueue_attention(store, case)).status == "succeeded"
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    reopened = await enqueue_attention(store, case)
    assert reopened.outbox_id == original.outbox_id and reopened.status == "pending"
    assert (await processor.process_pending()).succeeded == 1
    notify.assert_awaited_once()
