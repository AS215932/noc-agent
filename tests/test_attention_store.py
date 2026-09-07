import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app.cases.attention import AttentionDelivery
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.store import InMemoryCaseStore
from app.cases.outbox import OutboxHandlerResult, OutboxProcessor


@pytest.mark.asyncio
async def test_attention_lease_excludes_competitors_and_fences_expired_owner():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    intents = [OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key=f"lease-{i}",
                            status="in_progress", claim_token=f"claim-{i}") for i in range(2)]
    for intent in intents:
        await store.enqueue_outbox(intent)
    leases = await asyncio.gather(*[store.claim_attention(intent, expected_sequence=0) for intent in intents])
    assert sum(token is not None for token in leases) == 1
    winner = next(i for i, token in enumerate(leases) if token is not None)
    loser = 1 - winner
    old = leases[winner]
    # Simulate elapsed lease time without slowing the unit suite.
    value = store._attention_leases[case.case_id]
    store._attention_leases[case.case_id] = (*value[:3], datetime.now(timezone.utc) - timedelta(seconds=1))
    new = await store.claim_attention(intents[loser], expected_sequence=0)
    assert new is not None and new != old
    await store.release_attention(case.case_id, old)
    assert await store.claim_attention(intents[winner], expected_sequence=0) is None
    delivery = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
                                 delivered_at=datetime.now(timezone.utc), sequence=1)
    assert await store.complete_attention(intents[winner].model_copy(update={"status": "succeeded"}), delivery,
        expected_sequence=0, expected_claim_token=intents[winner].claim_token, lease_token=old) is None
    assert await store.complete_attention(intents[loser].model_copy(update={"status": "succeeded"}), delivery,
        expected_sequence=0, expected_claim_token=intents[loser].claim_token, lease_token=new) is not None
    assert case.case_id not in store._attention_leases
    assert await store.claim_attention(intents[winner], expected_sequence=0) is None


@pytest.mark.asyncio
async def test_attention_commit_is_atomic_and_fenced_by_claim_and_sequence():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    intent = OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key="attention-one",
                          status="in_progress", claim_token="winner")
    await store.enqueue_outbox(intent)
    lease = await store.claim_attention(intent, expected_sequence=0)
    completed = intent.model_copy(update={"status": "succeeded"})
    delivered = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
                                  delivered_at=datetime.now(timezone.utc), sequence=1)
    assert await store.complete_attention(completed, delivered, expected_sequence=0, expected_claim_token="stale", lease_token=lease) is None
    assert await store.get_attention(case.case_id) is None
    assert len(await store.list_outbox(status="in_progress")) == 1
    results = await asyncio.gather(*[
        store.complete_attention(completed, delivered, expected_sequence=0, expected_claim_token="winner", lease_token=lease)
        for _ in range(2)
    ])
    assert sum(result is not None for result in results) == 1
    assert await store.get_attention(case.case_id) == delivered
    assert len(await store.list_outbox(status="succeeded")) == 1
    # A different outbox row based on the old sequence cannot move attention
    # backward, and cannot be marked successful by the losing completion.
    competing = intent.model_copy(update={"outbox_id": "competing", "idempotency_key": "attention-two"})
    await store.enqueue_outbox(competing)
    assert await store.complete_attention(competing.model_copy(update={"status": "succeeded"}), delivered,
                                          expected_sequence=0, expected_claim_token="winner", lease_token=lease) is None
    assert len(await store.list_outbox(status="in_progress")) == 1
    assert await store.get_attention(case.case_id) == delivered


@pytest.mark.asyncio
async def test_quiet_case_write_does_not_mutate_attention_projection():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    original = case.model_dump()
    intent = OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key="attention",
                          status="in_progress", claim_token="claim")
    await store.enqueue_outbox(intent)
    lease = await store.claim_attention(intent, expected_sequence=0)
    delivered = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
                                  delivered_at=datetime.now(timezone.utc), sequence=1)
    await store.complete_attention(intent.model_copy(update={"status": "succeeded"}), delivered,
                                   expected_sequence=0, expected_claim_token="claim", lease_token=lease)
    assert (await store.get_case(case.case_id)).model_dump() == original
    case.last_reported_at = datetime.now(timezone.utc).isoformat()
    await store.upsert_case(case)
    assert await store.get_attention(case.case_id) == delivered


@pytest.mark.asyncio
async def test_processor_commits_successful_attention_and_does_not_replay_it():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    await store.enqueue_outbox(OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key="attention"))
    calls = []

    async def send(intent):
        calls.append(intent.outbox_id)
        lease = await store.claim_attention(intent, expected_sequence=0)
        return OutboxHandlerResult(attention_lease_token=lease, attention_delivery=AttentionDelivery(
            case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
            delivered_at=datetime.now(timezone.utc), sequence=1))

    processor = OutboxProcessor(store, {"report": send})
    assert (await processor.process_pending()).succeeded == 1
    assert (await store.get_attention(case.case_id)).sequence == 1
    assert (await processor.process_pending()).processed == 0
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_failed_attention_send_cannot_advance_delivery_clock():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    await store.enqueue_outbox(OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key="attention"))

    async def send(_intent):
        raise ConnectionError("notification unavailable")

    assert (await OutboxProcessor(store, {"report": send}).process_pending()).failed == 1
    assert await store.get_attention(case.case_id) is None
    assert len(await store.list_outbox(status="failed")) == 1
