import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.cases.models import OutboxIntent
from app.cases.report_spool import replay_reports, retain_report, spool_directory, spool_stats
from app.cases.store import InMemoryCaseStore


class RejectedReference(Exception):
    sqlstate = "23503"


@pytest.fixture(autouse=True)
def private_spool(monkeypatch, tmp_path):
    monkeypatch.setenv("NOC_REPORT_SPOOL_DIR", str(tmp_path / "spool"))


def report(key="terminal:case:1"):
    return OutboxIntent(case_id="case_test", intent_type="report", idempotency_key=key,
                        payload={"card_update": {"description": "retained result"}, "card_revision": 1})


@pytest.mark.asyncio
async def test_private_retention_survives_store_failure_and_replays():
    intent = report()
    await retain_report(intent)
    path, = spool_directory().glob("*.json")
    assert path.stat().st_mode & 0o777 == 0o600
    assert spool_directory().stat().st_mode & 0o777 == 0o700
    failed = SimpleNamespace(enqueue_outbox=AsyncMock(side_effect=ConnectionError("offline")))
    with pytest.raises(ConnectionError):
        await replay_reports(failed)
    assert path.exists()
    # Replay discovers the durable file without any retained process state.
    store = InMemoryCaseStore()
    assert await replay_reports(store) == 1
    saved = await store.get_outbox_by_key(intent.idempotency_key)
    assert saved.payload == intent.payload
    assert not path.exists()


@pytest.mark.asyncio
async def test_lost_database_ack_and_concurrent_replay_remain_idempotent():
    intent = report()
    await retain_report(intent)
    store = InMemoryCaseStore()

    async def commit_then_disconnect(item):
        await store.enqueue_outbox(item)
        raise ConnectionError("ack lost")

    with pytest.raises(ConnectionError):
        await replay_reports(SimpleNamespace(enqueue_outbox=commit_then_disconnect))
    assert (await spool_stats())["pending"] == 1
    await asyncio.gather(replay_reports(store), replay_reports(store))
    assert (await store.outbox_health()).pending == 1
    assert (await spool_stats())["pending"] == 0


@pytest.mark.asyncio
async def test_corrupt_record_preserved_and_replay_is_bounded():
    for number in range(3):
        await retain_report(report(f"terminal:{number}"))
    bad = spool_directory() / "000-corrupt.json"
    bad.write_text("broken record")
    store = InMemoryCaseStore()
    assert await replay_reports(store, limit=2) == 1
    assert bad.with_suffix(".invalid").read_text() == "broken record"
    stats = await spool_stats()
    assert stats["invalid"] == 1
    assert stats["pending"] == 2


@pytest.mark.asyncio
async def test_owned_enqueue_failure_retains_terminal_without_channel_fallback(monkeypatch):
    from app.cases.reporting import send_investigation_card

    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    notifier = AsyncMock()
    runtime = SimpleNamespace(store=SimpleNamespace(get_case=AsyncMock(side_effect=ConnectionError("offline"))))
    assert not await send_investigation_card(runtime=runtime, case_id="case_test", notifier=notifier,
                                             description="investigation result")
    notifier.assert_not_awaited()
    store = InMemoryCaseStore()
    assert await replay_reports(store) == 1
    assert (await store.outbox_health()).pending == 1


@pytest.mark.asyncio
async def test_retention_failure_is_not_reported_as_success(monkeypatch):
    from app.cases.reporting import send_investigation_card

    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    monkeypatch.setattr("app.cases.report_spool._retain", lambda _: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        await send_investigation_card(runtime=None, case_id="case_test", description="result")


@pytest.mark.asyncio
async def test_oldest_retained_timestamp_is_visible():
    await retain_report(report())
    path, = spool_directory().glob("*.json")
    os.utime(path, (100, 100))
    assert (await spool_stats())["oldest_retained_at"] == 100


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_retained_backlog_degrades_health_even_with_healthy_worker(monkeypatch, corrupt):
    from fastapi import Response
    import app.main as main
    from app.cases import CaseService
    from app.cases.runtime import CaseServiceRuntime

    store = InMemoryCaseStore()
    state = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", state)
    monkeypatch.setattr(main, "case_outbox_task", SimpleNamespace(done=lambda: False))
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    await retain_report(report())
    path, = spool_directory().glob("*.json")
    if corrupt:
        path.write_text("corrupt")
        await replay_reports(store)
    else:
        os.utime(path, (100, 100))
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["delivery"]["reasons"] == ["retained_reports_invalid" if corrupt else "retained_reports_overdue"]


@pytest.mark.asyncio
async def test_worker_replays_retained_intent_before_processing(monkeypatch):
    from app.cases import CaseService
    from app.cases.runtime import CaseServiceRuntime, process_case_outbox_once
    from app.cases.outbox import OutboxProcessReport

    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    store = InMemoryCaseStore()
    state = CaseServiceRuntime(store=store, service=CaseService(store))
    intent = report()
    await retain_report(intent)

    async def process(_self, **_kwargs):
        assert await store.get_outbox_by_key(intent.idempotency_key) is not None
        return OutboxProcessReport()

    monkeypatch.setattr("app.cases.runtime.OutboxProcessor.process_pending", process)
    await process_case_outbox_once(state)
    assert (await spool_stats())["pending"] == 0


@pytest.mark.asyncio
async def test_rejected_record_does_not_block_later_records():
    for number in range(3):
        await retain_report(report(f"report-{number}"))
    first = sorted(spool_directory().glob("*.json"))[0]
    rejected = OutboxIntent.model_validate_json(first.read_text()).idempotency_key
    store = InMemoryCaseStore()

    async def enqueue(item):
        if item.idempotency_key == rejected:
            raise RejectedReference("case reference unavailable")
        return await store.enqueue_outbox(item)

    assert await replay_reports(SimpleNamespace(enqueue_outbox=enqueue)) == 2
    assert first.with_suffix(".invalid").exists()
    assert (await spool_stats())["invalid"] == 1
    assert (await store.outbox_health()).pending == 2


@pytest.mark.asyncio
async def test_full_rejected_prefix_cannot_starve_later_report():
    for number in range(101):
        await retain_report(report(f"prefix-{number}"))
    paths = sorted(spool_directory().glob("*.json"))
    allowed = OutboxIntent.model_validate_json(paths[-1].read_text()).idempotency_key
    store = InMemoryCaseStore()

    async def enqueue(item):
        if item.idempotency_key != allowed:
            raise RejectedReference("missing restored case")
        return await store.enqueue_outbox(item)

    replay_store = SimpleNamespace(enqueue_outbox=enqueue)
    assert await replay_reports(replay_store) == 0
    assert (await spool_stats())["invalid"] == 100
    assert await replay_reports(replay_store) == 1
    assert await store.get_outbox_by_key(allowed) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ConnectionError, TimeoutError])
async def test_store_wide_failure_stops_after_first_attempt(error):
    for number in range(3):
        await retain_report(report(f"offline-{number}"))
    enqueue = AsyncMock(side_effect=error("offline"))
    with pytest.raises(error):
        await replay_reports(SimpleNamespace(enqueue_outbox=enqueue))
    assert enqueue.await_count == 1
    assert (await spool_stats())["pending"] == 3


@pytest.mark.asyncio
async def test_database_outage_still_exposes_local_retention(monkeypatch):
    from fastapi import Response
    import app.main as main
    from app.cases import CaseService
    from app.cases.runtime import CaseServiceRuntime

    store = InMemoryCaseStore()
    monkeypatch.setattr(main, "case_service_runtime", CaseServiceRuntime(store=store, service=CaseService(store)))
    monkeypatch.setattr(store, "outbox_health", AsyncMock(side_effect=ConnectionError("private database detail")))
    await retain_report(report())
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["report_spool"]["pending"] == 1
    assert result["report_spool"]["oldest_retained_at"] is not None
    assert "private database detail" not in str(result)


@pytest.mark.asyncio
async def test_spool_read_failure_is_visible_without_hiding_database_health(monkeypatch):
    from fastapi import Response
    import app.main as main
    from app.cases import CaseService
    from app.cases.runtime import CaseServiceRuntime

    store = InMemoryCaseStore()
    monkeypatch.setattr(main, "case_service_runtime", CaseServiceRuntime(store=store, service=CaseService(store)))
    monkeypatch.setattr("app.cases.report_spool.spool_stats", AsyncMock(side_effect=OSError("private path")))
    response = Response()
    result = await main.health_cases(response)
    assert response.status_code == 503
    assert result["outbox"] == {"pending": 0, "failed": 0}
    assert "report_spool_unavailable" in result["delivery"]["reasons"]
    assert "private path" not in str(result)
