"""Optional real-PostgreSQL check, using an isolated local test container only."""
import asyncio
import json
import os
import re
import subprocess

import pytest

from app.cases.models import OutboxIntent
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
