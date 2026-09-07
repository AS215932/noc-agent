"""Real transaction checks through an explicitly configured local test socket."""
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
from uuid import uuid4
from unittest.mock import AsyncMock

import asyncpg
import pytest

from app.cases.attention import AttentionDelivery
from app.cases.attention_scheduler import enqueue_attention_batch
from app.cases.models import AtomicCaseProjection, MetaCaseProjection, OutboxIntent
from app.cases.correlation import CorrelationService
from app.cases.postgres import PostgresCaseStore
from app.cases.service import CaseService


SOCKET = os.getenv("NOC_TEST_ATTENTION_PG_SOCKET", "")
pytestmark = pytest.mark.skipif(not SOCKET, reason="isolated PostgreSQL Unix socket not configured")


@pytest.mark.asyncio
async def test_atomic_attention_rollback_concurrency_and_restart(monkeypatch):
    assert SOCKET.startswith("/tmp/as215932-attention-pg-")
    assert (Path(SOCKET) / ".s.PGSQL.5432").is_socket()
    schema = "attention_test_" + uuid4().hex
    admin = await asyncpg.connect(host=SOCKET, user="postgres", database="postgres")
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    pool = None
    try:
        pool = await asyncpg.create_pool(host=SOCKET, user="postgres", database="postgres",
                                        min_size=1, max_size=3, server_settings={"search_path": schema})
        store = PostgresCaseStore(pool)
        await store.setup()
        case = AtomicCaseProjection(severity="HIGH")
        await store.upsert_case(case)
        intent = OutboxIntent(case_id=case.case_id, intent_type="report", idempotency_key="attention",
                              status="in_progress", claim_token="owner")
        await store.enqueue_outbox(intent)
        leases = await asyncio.gather(*[store.claim_attention(intent, expected_sequence=0) for _ in range(2)])
        assert sum(value is not None for value in leases) == 1
        old_lease = next(value for value in leases if value is not None)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE case_attention_lease SET expires_at=clock_timestamp()-interval '1 second'")
        lease = await store.claim_attention(intent, expected_sequence=0)
        assert lease is not None and lease != old_lease
        await store.release_attention(case.case_id, old_lease)
        assert await store.claim_attention(intent, expected_sequence=0) is None
        completed = intent.model_copy(update={"status": "succeeded"})
        delivery = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
                                     delivered_at=datetime.now(timezone.utc), sequence=1)
        assert await store.complete_attention(completed, delivery, expected_sequence=0,
                                              expected_claim_token="owner", lease_token=old_lease) is None
        # Fail the second write after the attention projection was inserted.
        # PostgreSQL must roll back both changes, not leave a false delivery.
        async with pool.acquire() as conn:
            await conn.execute("""CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'test completion failure'; END $$;
                CREATE TRIGGER reject_completion BEFORE UPDATE ON side_effect_outbox
                FOR EACH ROW WHEN (NEW.status='succeeded') EXECUTE FUNCTION reject_completion();""")
        with pytest.raises(asyncpg.RaiseError):
            await store.complete_attention(completed, delivery, expected_sequence=0, expected_claim_token="owner", lease_token=lease)
        assert await store.get_attention(case.case_id) is None
        assert len(await store.list_outbox(status="in_progress")) == 1
        async with pool.acquire() as conn:
            await conn.execute("DROP TRIGGER reject_completion ON side_effect_outbox")
        assert await store.complete_attention(completed, delivery, expected_sequence=0, expected_claim_token="stale", lease_token=lease) is None
        results = await asyncio.gather(*[
            store.complete_attention(completed, delivery, expected_sequence=0, expected_claim_token="owner", lease_token=lease)
            for _ in range(2)
        ])
        assert sum(result is not None for result in results) == 1
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM case_attention_lease") == 0
        assert (await store.get_case(case.case_id)).model_dump() == case.model_dump()
        await pool.close()
        pool = await asyncpg.create_pool(host=SOCKET, user="postgres", database="postgres", min_size=1, max_size=1,
                                        server_settings={"search_path": schema})
        restarted = PostgresCaseStore(pool)
        assert await restarted.get_attention(case.case_id) == delivery
        assert len(await restarted.list_outbox(status="succeeded")) == 1
        # The outer case transaction must reuse its connection for nested store
        # calls even with a one-connection pool, and roll back a failed event.
        service = CaseService(restarted)
        append_event = restarted.append_event
        monkeypatch.setattr(restarted, "append_event", AsyncMock(side_effect=RuntimeError("event write failed")))
        with pytest.raises(RuntimeError, match="event write failed"):
            await asyncio.wait_for(service.ack(case_id=case.case_id, operator="not-committed"), timeout=2)
        assert not (await restarted.get_case(case.case_id)).acknowledged_by
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM case_acknowledgement_scope") == 0
        monkeypatch.setattr(restarted, "append_event", append_event)
        await asyncio.wait_for(asyncio.gather(
            service.ack(case_id=case.case_id, operator="oncall"),
            service.mark_reported(case_id=case.case_id, state_signature="current-facts"),
        ), timeout=2)
        current = await restarted.get_case(case.case_id)
        assert current.acknowledged_by == "oncall"
        assert current.last_reported_signature == "current-facts"
        assert await restarted.acknowledgement_scope(case.case_id, current.acknowledged_at) == "HIGH"
        assert await restarted.acknowledgement_scope(case.case_id, "stale-ack") is None
        # Stable keyset pagination must survive changes to recent telemetry and
        # skip retired/manual cases without excluding a pending recovery.
        for number, status, source in [
            (0, "investigating", "icinga2"),
            (1, "closed", "icinga2"),
            (2, "investigating", "manual"),
            (3, "investigating", "alertmanager"),
            (4, "resolved", "icinga2"),
        ]:
            await restarted.upsert_case(AtomicCaseProjection(
                case_id=f"page-{number}", status=status, severity="HIGH", identity={"source": source},
            ))
        cursor, count = await enqueue_attention_batch(restarted, limit=2)
        assert (cursor, count) == ("page-3", 2)
        first = await restarted.get_case("page-0")
        first.updated_at = datetime.now(timezone.utc).isoformat()
        first.signal_signature = "new quiet telemetry"
        await restarted.upsert_case(first)
        remaining = await restarted.list_attention_candidates(after_case_id=cursor, limit=2)
        assert [item.case_id for item in remaining] == ["page-4"]
        assert await enqueue_attention_batch(restarted, after_case_id=cursor, limit=2) == ("", 0)
        await enqueue_attention_batch(restarted)
        assert len(await restarted.list_outbox(status="pending")) == 2
        assert await restarted.has_pending_attention("page-0")
        assert not await restarted.has_pending_attention("page-4")
        assert not await restarted.has_pending_attention(case.case_id)
        await pool.close()
        pool = await asyncpg.create_pool(host=SOCKET, user="postgres", database="postgres", min_size=1, max_size=3,
                                        server_settings={"search_path": schema})
        restarted = PostgresCaseStore(pool)
        assert await restarted.acknowledgement_scope(case.case_id, current.acknowledged_at) == "HIGH"
        meta = MetaCaseProjection()
        await restarted.upsert_case(meta)
        correlation = CorrelationService(restarted)
        await asyncio.wait_for(asyncio.gather(
            correlation.attach_child(meta.case_id, "page-0", reason="shared event", confidence=1),
            correlation.attach_child(meta.case_id, "page-3", reason="shared event", confidence=1),
            CaseService(restarted).ack("page-0", operator="oncall"),
        ), timeout=3)
        assert set((await restarted.get_case(meta.case_id)).child_case_ids) == {"page-0", "page-3"}
        assert (await restarted.get_case("page-0")).acknowledged_by == "oncall"
    finally:
        if pool is not None:
            await pool.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()
