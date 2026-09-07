from unittest.mock import AsyncMock

import pytest
from fastapi import BackgroundTasks

from app.case_cards import deliver_case_card
from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor
from app.cases.handlers import build_report_handler
from app.cases.runtime import CaseServiceRuntime
from app.discord import Verbosity


@pytest.fixture
def owned_cards(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("NOC_AUTO_ACK_ON_INVESTIGATION", "0")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")


@pytest.mark.asyncio
async def test_initial_facts_arrive_before_model_failure_and_share_one_card(monkeypatch, tmp_path, owned_cards):
    import app.main as main

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    runtime = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", runtime)
    created, edited = [], []

    async def fail_model(*args, **kwargs):
        assert len(created) == 1
        assert "4% free" in created[0]["description"]
        raise RuntimeError("model unavailable")

    monkeypatch.setattr(main, "run_investigation_graph", AsyncMock(side_effect=fail_model))

    async def notify(case_id, revision=None, **payload):
        async def create():
            created.append(payload)
            return 123

        async def edit(message_id):
            assert message_id == 123
            edited.append(payload)
            return True

        return await deliver_case_card(destination="test", case_id=case_id, payload=payload,
                                       revision=revision, create=create, edit=edit)

    monkeypatch.setattr(main, "send_case_notification", notify)
    payload = {"source": "alertmanager", "status": "firing", "alerts": [{
        "status": "firing", "fingerprint": "disk-rtr",
        "labels": {"alertname": "DiskLow", "instance": "rtr", "severity": "critical"},
        "annotations": {"summary": "Router root filesystem has 4% free"},
    }]}
    background = BackgroundTasks()
    result = await main._case_service_reactive_primary_response(payload, background, label="Alert")
    # Durable intake can acknowledge before Discord I/O or model execution.
    assert not created
    assert not edited
    case = await store.get_case(result["incident_id"])
    assert not case.last_reported_at
    main.run_investigation_graph.assert_not_called()
    await background()
    main.run_investigation_graph.assert_awaited_once()
    assert len(created) == 1
    assert len(edited) == 1
    assert (await store.get_case(case.case_id)).last_reported_at
    assert "Starting investigation" not in edited[0]["description"]


@pytest.mark.asyncio
async def test_terminal_update_waits_for_failed_initial_delivery(monkeypatch, owned_cards):
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="alertmanager", detector="DiskLow", resource="rtr", severity="HIGH", status="firing",
    ))
    await service.request_report(observed.case, payload={"title": "Initial facts"})
    await store.enqueue_outbox(OutboxIntent(
        case_id=observed.case.case_id, intent_type="report", idempotency_key="terminal",
        payload={"card_update": {"title": "Investigation unavailable", "description": "Dependency failed",
                                 "color": 0, "level": int(Verbosity.ERROR)}},
    ))
    sent = []
    available = False

    async def notify(**kwargs):
        sent.append(kwargs["title"])
        return available

    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)}, retry_backoff_s=0)
    failed = await processor.process_pending()
    assert failed.failed == 2
    assert sent == ["Initial facts"]
    assert not (await store.get_case(observed.case.case_id)).last_reported_at
    available = True
    retried = await processor.process_pending()
    assert retried.succeeded == 2
    assert sent == ["Initial facts", "Initial facts", "Investigation unavailable"]


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["proactive", "manual"])
async def test_other_card_does_not_require_reactive_initial_report(owned_cards, source):
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(source=source, detector="DiskLow", resource="rtr", status="firing"))
    notifier = AsyncMock(return_value=True)
    intent = OutboxIntent(case_id=observed.case.case_id, intent_type="report", idempotency_key="proactive",
                          payload={"card_update": {"title": "Proactive result", "description": "Finding",
                                                   "color": 0, "level": int(Verbosity.INFO)}})
    await build_report_handler(service, notifier=notifier)(intent)
    notifier.assert_awaited_once()
