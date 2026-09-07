from datetime import datetime, timezone

import pytest

from app.cases.attention_scheduler import enqueue_attention_batch
from app.cases.models import AtomicCaseProjection
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_repeated_poll_and_restart_preserve_one_attention_intent_per_case():
    store = InMemoryCaseStore()
    cases = [AtomicCaseProjection(case_id=f"case-{number}", severity="HIGH", identity={"source": "icinga2"})
             for number in range(3)]
    for case in cases:
        await store.upsert_case(case)
    cursor, count = await enqueue_attention_batch(store, limit=2)
    assert (cursor, count) == ("case-1", 2)
    # Reordering recent telemetry must not starve the later ID.
    cases[0].updated_at = datetime.now(timezone.utc).isoformat()
    cases[0].signal_signature = "quiet update"
    await store.upsert_case(cases[0])
    assert await enqueue_attention_batch(store, after_case_id=cursor, limit=2) == ("", 1)
    # A fresh scheduler starts from the beginning after a restart.
    await enqueue_attention_batch(store)
    rows = await store.list_outbox()
    assert len(rows) == 3
    assert all(row.payload["attention_request"]["kind"] == "new" for row in rows)


@pytest.mark.asyncio
async def test_manual_and_acknowledged_cases_do_not_enqueue_attention():
    store = InMemoryCaseStore()
    await store.upsert_case(AtomicCaseProjection(severity="HIGH", identity={"source": "manual"}))
    await store.upsert_case(AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"}, acknowledged_by="operator"))
    assert await enqueue_attention_batch(store) == ("", 0)
    assert await store.list_outbox() == []
