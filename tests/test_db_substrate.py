import os

import pytest

from app.cases.models import AtomicCaseProjection, MetaCaseProjection, ObservationRecord
from app.cases.postgres import PostgresCaseStore, _case_from_payload, _load_asyncpg
from app.db.config import DatabaseSettings, load_database_settings
from app.db.schema import SCHEMA_STATEMENTS, schema_sql


def test_database_settings_loads_env_and_fails_loud_when_required(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.setenv("NOC_REQUIRE_POSTGRES", "true")

    settings = load_database_settings()

    assert settings.require_postgres is True
    assert settings.enabled is False
    with pytest.raises(RuntimeError):
        settings.assert_ready_for_production()

    monkeypatch.setenv("NOC_DATABASE_URL", "postgresql://noc@example.invalid/noc")
    settings = load_database_settings()
    assert settings.enabled is True
    settings.assert_ready_for_production()


def test_database_settings_prefers_noc_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://generic/example")
    monkeypatch.setenv("NOC_DATABASE_URL", "postgresql://noc/example")

    assert load_database_settings().url == "postgresql://noc/example"


def test_case_schema_contains_authoritative_tables_and_uniqueness_guards():
    sql = schema_sql().lower()

    for table in (
        "observation_inbox",
        "observations",
        "cases",
        "case_events",
        "case_identity_aliases",
        "side_effect_outbox",
        "meta_case_correlation_evidence",
        "traces",
        "operator_feedback",
        "case_handoffs",
        "handoff_updates",
        "verification_objectives",
        "knowledge_artifacts",
        "outcome_records",
        "callback_inbox",
        "handoff_transport_deliveries",
    ):
        assert f"create table if not exists {table}" in sql
    assert "case_identity_aliases_active_unique_idx" in sql
    assert "cases_active_atomic_fingerprint_idx" in sql
    assert "side_effect_outbox" in sql and "idempotency_key text not null unique" in sql
    assert "case_handoffs_active_unique_idx" in sql
    assert "handoff_updates_external_event_unique_idx" in sql
    assert "max_attempts integer not null default 10" in sql
    assert "unique(source_loop, external_event_id)" in sql
    assert "unique(case_id, objective_key)" in sql
    assert "unique(case_id, artifact_type, version)" in sql
    assert len(SCHEMA_STATEMENTS) >= 40


def test_postgres_payload_rehydrates_atomic_or_meta_projection():
    atomic = AtomicCaseProjection(case_id="case_1", fingerprint="fp")
    meta = MetaCaseProjection(case_id="meta_1", event_fingerprint="event_fp")

    assert _case_from_payload(atomic.model_dump(mode="json")).case_id == "case_1"
    rehydrated_meta = _case_from_payload(meta.model_dump(mode="json"))
    assert isinstance(rehydrated_meta, MetaCaseProjection)
    assert rehydrated_meta.event_fingerprint == "event_fp"


@pytest.mark.asyncio
async def test_postgres_observation_insert_refreshes_payload_on_dedup_key():
    class _Conn:
        def __init__(self):
            self.queries = []

        async def fetchrow(self, query, *args):
            self.queries.append(query)
            return {"payload": args[13]}

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Conn()
    store = PostgresCaseStore(_Pool(conn))
    observation = ObservationRecord(source="alertmanager", status="firing", dedup_key="alertmanager:event:firing")

    stored = await store.put_observation(observation)

    assert stored.dedup_key == observation.dedup_key
    assert "ON CONFLICT (dedup_key) WHERE dedup_key <> ''" in conn.queries[0]
    assert "signal_signature = EXCLUDED.signal_signature" in conn.queries[0]
    assert "payload = jsonb_set(EXCLUDED.payload" in conn.queries[0]


def test_postgres_store_reports_missing_asyncpg_as_optional_dependency():
    try:
        import asyncpg
    except Exception:
        with pytest.raises(RuntimeError):
            _load_asyncpg()
    else:
        assert _load_asyncpg().__name__ == "asyncpg"


@pytest.mark.asyncio
async def test_postgres_handoff_delivery_guard_uses_cross_process_advisory_lock():
    class _Transaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Conn:
        def __init__(self):
            self.queries = []

        def transaction(self):
            return _Transaction()

        async def execute(self, query, *args):
            self.queries.append((query, args))

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Conn()
    store = PostgresCaseStore(_Pool(conn))

    async with store.handoff_delivery_guard("handoff_1"):
        pass

    queries = [query for query, _args in conn.queries]
    assert "SET LOCAL lock_timeout = '0'" in queries
    assert "SET LOCAL statement_timeout = '0'" in queries
    assert any("pg_advisory_xact_lock" in query for query in queries)


def test_database_settings_defaults_do_not_require_postgres(monkeypatch):
    for key in list(os.environ):
        if key.startswith("NOC_DATABASE") or key in {"DATABASE_URL", "NOC_REQUIRE_POSTGRES"}:
            monkeypatch.delenv(key, raising=False)

    settings = load_database_settings()

    assert settings.enabled is False
    settings.assert_ready_for_production()
