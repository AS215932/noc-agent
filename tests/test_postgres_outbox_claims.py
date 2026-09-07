"""Optional real-PostgreSQL check, using an isolated local test container only."""
import asyncio
import json
import os
import re
import subprocess

import pytest

from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.postgres import PostgresCaseStore
from app.db.schema import SCHEMA_STATEMENTS

CONTAINER = os.getenv("NOC_TEST_POSTGRES_CONTAINER", "")
pytestmark = pytest.mark.skipif(not CONTAINER, reason="isolated PostgreSQL test container not configured")


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql(query):
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-Atq", "-v", "ON_ERROR_STOP=1"],
        input=query, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


class Connection:
    async def fetch(self, query, *args):
        bound = re.sub(r"\$(\d+)", lambda match: sql_literal(args[int(match[1]) - 1]), query)
        output = await asyncio.to_thread(sql, bound)
        return [{"payload": json.loads(line)} for line in output.splitlines() if line]

    async def fetchrow(self, query, *args):
        bound = re.sub(r"\$(\d+)", lambda match: sql_literal(args[int(match[1]) - 1]), query)
        output = await asyncio.to_thread(sql, bound)
        return {"payload": json.loads(output)} if output else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Pool:
    def acquire(self):
        return Connection()


@pytest.mark.asyncio
async def test_postgres_reminder_candidate_pagination():
    assert CONTAINER.startswith("as215932-noc-outbox-test")
    network = subprocess.check_output(["docker", "inspect", "-f", "{{.HostConfig.NetworkMode}}", CONTAINER], text=True).strip()
    assert network == "none"
    sql("CREATE TABLE IF NOT EXISTS cases (case_id text PRIMARY KEY);")
    sql("ALTER TABLE cases ADD COLUMN IF NOT EXISTS kind text; ALTER TABLE cases ADD COLUMN IF NOT EXISTS payload jsonb;")
    cases = [AtomicCaseProjection(case_id="scheduler-" + suffix, severity=severity, last_reported_at=reported)
             for suffix, severity, reported in [
                 ("a", "HIGH", "2026-09-07T00:00:00+00:00"),
                 ("b", "MEDIUM", "2026-09-07T00:00:00+00:00"),
                 ("c", "HIGH", "2026-09-07T00:00:00+00:00"),
                 ("d", "HIGH", ""),
                 ("e", "HIGH", "2026-09-07T00:00:00+00:00"),
             ]]
    try:
        for case in cases:
            sql("INSERT INTO cases(case_id,kind,payload) VALUES (" + ",".join(map(sql_literal, [
                case.case_id, case.kind, case.model_dump_json(),
            ])) + ");")
        store = PostgresCaseStore(Pool())
        first = await store.list_reminder_candidates(after_case_id="scheduler-", limit=2)
        assert [case.case_id for case in first] == ["scheduler-a", "scheduler-c"]
        second = await store.list_reminder_candidates(after_case_id=first[-1].case_id, limit=2)
        assert [case.case_id for case in second] == ["scheduler-e"]
        assert await store.list_reminder_candidates(after_case_id=second[-1].case_id) == []
        assert await store.list_reminder_candidates(limit=0) == []
    finally:
        for case in cases:
            sql("DELETE FROM cases WHERE case_id=" + sql_literal(case.case_id) + ";")


@pytest.mark.asyncio
async def test_postgres_claim_fencing_and_legacy_payload():
    # Require an explicitly named, network-isolated disposable test container.
    assert CONTAINER.startswith("as215932-noc-outbox-test")
    network = subprocess.check_output(["docker", "inspect", "-f", "{{.HostConfig.NetworkMode}}", CONTAINER], text=True).strip()
    assert network == "none"
    sql("CREATE TABLE IF NOT EXISTS cases (case_id text PRIMARY KEY);")
    statement = next(x for x in SCHEMA_STATEMENTS if "CREATE TABLE IF NOT EXISTS side_effect_outbox (" in x)
    sql(statement + ";")
    store = PostgresCaseStore(Pool())
    for token in ["", "old-claim"]:
        intent = OutboxIntent(
            case_id="test-case", intent_type="report", idempotency_key="fencing:" + token,
            status="in_progress", claim_token=token,
        )
        payload = intent.model_dump(mode="json")
        if not token:
            payload.pop("claim_token")
            payload.pop("claim_expires_at")
        sql("INSERT INTO side_effect_outbox (outbox_id,intent_type,idempotency_key,status,payload) VALUES ("
            + ",".join(map(sql_literal, [intent.outbox_id, "report", intent.idempotency_key, intent.status, json.dumps(payload)]))
            + ");")
        try:
            found = await store.get_outbox_by_key(intent.idempotency_key)
            assert found is not None and found.outbox_id == intent.outbox_id
            assert await store.get_outbox_by_key(intent.idempotency_key + ":missing") is None
            recovered = intent.model_copy(update={"claim_token": "new-claim"})
            assert await store.update_outbox_if_status(
                recovered, expected_status="in_progress", expected_claim_token="incorrect-token",
            ) is None
            assert await store.update_outbox_if_status(
                recovered, expected_status="in_progress", expected_claim_token=token,
            ) is not None
            stale = intent.model_copy(update={"status": "succeeded", "external_id": "stale"})
            assert await store.update_outbox_if_status(
                stale, expected_status="in_progress", expected_claim_token=token,
            ) is None
            current = recovered.model_copy(update={"status": "succeeded", "external_id": "current"})
            saved = await store.update_outbox_if_status(
                current, expected_status="in_progress", expected_claim_token="new-claim",
            )
            assert saved is not None and saved.external_id == "current"
        finally:
            sql("DELETE FROM side_effect_outbox WHERE outbox_id=" + sql_literal(intent.outbox_id) + ";")


@pytest.mark.asyncio
async def test_postgres_outbox_health_aggregates_without_reading_payloads():
    assert CONTAINER.startswith("as215932-noc-outbox-test")
    network = subprocess.check_output(["docker", "inspect", "-f", "{{.HostConfig.NetworkMode}}", CONTAINER], text=True).strip()
    assert network == "none"
    sql("CREATE TABLE IF NOT EXISTS cases (case_id text PRIMARY KEY);")
    sql(next(x for x in SCHEMA_STATEMENTS if "CREATE TABLE IF NOT EXISTS side_effect_outbox (" in x) + ";")
    # Invalid model payloads deliberately prove this reads typed columns only.
    for suffix, status, kind, created in [
        ("a", "pending", "report", "2026-09-07T00:00:00+00:00"),
        ("b", "failed", "report", "2026-09-07T01:00:00+00:00"),
        ("c", "in_progress", "report", "2026-09-07T02:00:00+00:00"),
        ("d", "pending", "handoff", "2020-01-01T00:00:00+00:00"),
        ("e", "succeeded", "report", "2020-01-01T00:00:00+00:00"),
        ("f", "pending", "report", ""),
        ("g", "failed", "report", "not-a-timestamp"),
        ("h", "pending", "report", "2026-02-30T00:00:00Z"),
        ("i", "pending", "report", "infinity"),
    ]:
        key = "health:" + suffix
        sql("INSERT INTO side_effect_outbox(outbox_id,idempotency_key,status,intent_type,created_at,payload) VALUES ("
            + ",".join(map(sql_literal, [key, key, status, kind, created, "{}"])) + ");")
    try:
        index = next(x for x in SCHEMA_STATEMENTS if "CREATE INDEX IF NOT EXISTS side_effect_outbox_health_idx" in x)
        sql(index + ";")
        sql("INSERT INTO side_effect_outbox(outbox_id,idempotency_key,status,intent_type,created_at,payload) "
            "SELECT 'health:history-' || n, 'health:history-' || n, 'succeeded', 'report', "
            "'2020-01-01T00:00:00Z', '{}'::jsonb FROM generate_series(1,10000) n;")
        sql("ANALYZE side_effect_outbox;")
        queries = []

        class CapturedConnection(Connection):
            async def fetchrow(self, query, *args):
                queries.append(query)
                return await super().fetchrow(query, *args)

        class CapturedPool:
            def acquire(self):
                return CapturedConnection()

        health = await PostgresCaseStore(CapturedPool()).outbox_health()
        plan = json.loads(sql("EXPLAIN (FORMAT JSON) " + queries[-1]))
        assert "side_effect_outbox_health_idx" in str(plan)

        assert (health.pending, health.failed, health.in_progress) == (5, 2, 1)
        assert health.outstanding_reports == 7
        assert health.invalid_report_timestamps == 4
        assert health.oldest_report_timestamp == 1788739200
    finally:
        sql("DELETE FROM side_effect_outbox WHERE outbox_id LIKE 'health:%';")
