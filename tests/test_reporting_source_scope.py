from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.cases.models import AtomicCaseProjection
from app.cases.reporting import send_investigation_card


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["manual", "proactive", "icinga2", "alertmanager"])
@pytest.mark.parametrize("failure", ["lookup", "enqueue"])
async def test_only_reactive_store_failures_use_retained_ownership(monkeypatch, tmp_path, source, failure):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    monkeypatch.setenv("NOC_REPORT_SPOOL_DIR", str(tmp_path))
    store = SimpleNamespace(
        get_case=AsyncMock(return_value=AtomicCaseProjection(case_id="case_test", identity={"source": source})),
        enqueue_outbox=AsyncMock(side_effect=ConnectionError("store unavailable")),
    )
    if failure == "lookup":
        store.get_case.side_effect = ConnectionError("store unavailable")
    notify = AsyncMock(return_value=True)
    delivered = await send_investigation_card(runtime=SimpleNamespace(store=store), case_id="case_test",
        source=source, notifier=notify, title="Investigation result", description="Result")
    reactive = source in {"icinga2", "alertmanager"}
    assert delivered is not reactive
    assert notify.await_count == (0 if reactive else 1)
    assert len(list(tmp_path.glob("*.json"))) == int(reactive)


@pytest.mark.asyncio
async def test_source_can_be_inferred_from_loaded_case_before_enqueue_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    monkeypatch.setenv("NOC_REPORT_SPOOL_DIR", str(tmp_path))
    store = SimpleNamespace(
        get_case=AsyncMock(return_value=AtomicCaseProjection(case_id="case_test", identity={"source": "icinga2"})),
        enqueue_outbox=AsyncMock(side_effect=ConnectionError("store unavailable")),
    )
    notify = AsyncMock(return_value=True)
    assert not await send_investigation_card(runtime=SimpleNamespace(store=store), case_id="case_test",
        notifier=notify, title="Result", description="Result")
    notify.assert_not_awaited()
    assert len(list(tmp_path.glob("*.json"))) == 1
