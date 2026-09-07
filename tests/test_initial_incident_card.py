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
async def test_initial_store_failure_does_not_abort_later_investigation(monkeypatch, owned_cards):
    import app.main as main

    store = InMemoryCaseStore()
    runtime = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", runtime)
    observation = ObservationRecord(source="alertmanager", detector="DiskLow", resource="rtr",
                                    severity="HIGH", status="firing")
    observed = await runtime.service.observe(observation)
    background = BackgroundTasks()
    await main._maybe_request_reactive_case_report(observation, observed, background_tasks=background)
    # Simulate a database failure at the actual claim boundary, after enqueue.
    monkeypatch.setattr(store, "update_outbox_if_status", AsyncMock(side_effect=RuntimeError("store unavailable")))
    investigation = AsyncMock()
    background.add_task(investigation)
    await background()
    investigation.assert_awaited_once()
    pending = await store.list_outbox(status="pending")
    assert len(pending) == 1
    assert pending[0].intent_type == "report"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"report_generation": 1},
    {"severity": "HIGH"},
    {"signal_signature": "changed-signal"},
])
async def test_prior_report_does_not_allow_current_facts_to_be_overtaken(owned_cards, change):
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="alertmanager", detector="DiskLow", resource="rtr", severity="MEDIUM", status="firing",
    ))
    prior = observed.case
    await service.mark_reported(prior.case_id, state_signature=service.report_state_signature(prior))
    current = (await store.get_case(prior.case_id)).model_copy(update=change)
    await store.upsert_case(current)
    initial = await service.request_report(current, payload={"title": "Current facts"})
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=current.case_id, intent_type="report", idempotency_key="new-terminal",
        payload={"card_update": {"title": "Terminal", "description": "Model result",
                                 "color": 0, "level": int(Verbosity.WARNING)}},
    ))
    sent = []
    available = False

    async def notify(**kwargs):
        sent.append(kwargs["title"])
        return available

    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)}, retry_backoff_s=0)
    result = await processor.process_pending()
    assert result.failed == 2
    assert sent == ["Current facts"]
    assert (await store.get_case(current.case_id)).last_reported_signature != initial.state_signature
    available = True
    result = await processor.process_pending()
    assert result.succeeded == 2
    assert sent == ["Current facts", "Current facts", "Terminal"]
    assert (await store.get_outbox_by_key(terminal.idempotency_key)).status == "succeeded"


@pytest.mark.asyncio
async def test_superseded_legacy_initial_card_allows_newer_terminal_edit(monkeypatch, tmp_path, owned_cards):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="icinga2", detector="DiskLow", resource="rtr", severity="HIGH", status="firing",
    ))
    created, edited = [], []

    async def notify(case_id, revision=None, **payload):
        async def create():
            created.append(payload["title"])
            return 321

        async def edit(message_id):
            assert message_id == 321
            edited.append(payload["title"])
            return True

        return await deliver_case_card(destination="test", case_id=case_id, payload=payload,
                                       revision=revision, create=create, edit=edit)

    await notify(case_id=observed.case.case_id, title="Legacy card", revision=20)
    initial = await service.request_report(observed.case, payload={"title": "Old initial facts", "card_revision": 10})
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)})
    assert (await processor.process_intent(initial)).succeeded == 1
    assert (await store.get_outbox_by_key(initial.idempotency_key)).payload["notification_superseded"]
    assert not (await store.get_case(observed.case.case_id)).last_reported_at
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=observed.case.case_id, intent_type="report", idempotency_key="legacy-terminal",
        payload={"card_revision": 30, "card_update": {"title": "New terminal", "description": "Result",
                                                     "color": 0, "level": int(Verbosity.ERROR)}},
    ))
    assert (await processor.process_intent(terminal)).succeeded == 1
    assert created == ["Legacy card"]
    assert edited == ["New terminal"]
    # Superseded facts establish card existence, not a delivered-facts timestamp.
    assert not (await store.get_case(observed.case.case_id)).last_reported_at
    assert await store.get_outbox_by_key("missing") is None
