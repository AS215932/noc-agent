from unittest.mock import AsyncMock

import pytest

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor
from app.cases.handlers import build_report_handler
from app.discord import Verbosity


@pytest.mark.asyncio
@pytest.mark.parametrize("severity", ["LOW", "MEDIUM"])
async def test_terminal_retry_reopens_now_eligible_facts_without_new_observation(monkeypatch, severity):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(source="alertmanager", detector="Disk", resource="rtr",
                                                      status="firing", severity=severity))
    case = observed.case
    notify = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)}, retry_backoff_s=0)
    initial = await service.request_report(case)
    assert (await processor.process_intent(initial)).succeeded == 1
    notify.assert_not_awaited()
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=case.case_id, intent_type="report", idempotency_key="terminal-after-policy-change",
        payload={"card_update": {"title": "Investigation result", "description": "Diagnosis", "color": 0,
                                 "level": int(Verbosity.ERROR)}},
    ))
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    assert (await processor.process_intent(terminal)).failed == 1
    reopened = await store.get_outbox_by_key(initial.idempotency_key)
    assert reopened.status == "pending"
    # Only the durable worker runs from here; no new monitor observation.
    assert (await processor.process_pending()).succeeded == 2
    assert notify.await_count == 2
    assert (await store.get_outbox_by_key(terminal.idempotency_key)).payload["card_update_delivered"] is True
    assert (await store.get_case(case.case_id)).last_reported_at
