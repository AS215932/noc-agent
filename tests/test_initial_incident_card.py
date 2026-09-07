from unittest.mock import AsyncMock

import pytest
from fastapi import BackgroundTasks

from app.case_cards import CardNotFound, deliver_case_card
from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor
from app.cases.handlers import build_report_handler
from app.cases.runtime import CaseServiceRuntime
from app.discord import Verbosity


@pytest.fixture
def owned_cards(monkeypatch, tmp_path):
    monkeypatch.setenv("NOC_REPORT_SPOOL_DIR", str(tmp_path / "report-spool"))
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
@pytest.mark.parametrize("deleted", [False, True])
async def test_superseded_legacy_initial_card_allows_newer_terminal_edit(monkeypatch, tmp_path, owned_cards, deleted):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="icinga2", detector="DiskLow", resource="rtr", severity="HIGH", status="firing",
    ))
    created, edited = [], []
    exists = True

    async def notify(case_id, revision=None, force_refresh=False, **payload):
        async def create():
            nonlocal exists
            exists = True
            created.append(payload["title"])
            return 321

        async def edit(message_id):
            assert message_id == 321
            if not exists:
                raise CardNotFound
            edited.append(payload["title"])
            return True

        return await deliver_case_card(destination="test", case_id=case_id, payload=payload,
                                       revision=revision, force_refresh=force_refresh, create=create, edit=edit)

    await notify(case_id=observed.case.case_id, title="Legacy card", revision=20)
    exists = not deleted
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
    assert created == (["Legacy card", "Old initial facts"] if deleted else ["Legacy card"])
    assert edited == (["New terminal"] if deleted else ["Old initial facts", "New terminal"])
    assert (await store.get_case(observed.case.case_id)).last_reported_at
    assert await store.get_outbox_by_key("missing") is None


@pytest.mark.asyncio
async def test_missing_report_after_handoff_is_enqueued_and_eventually_unblocks(owned_cards, monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(source="icinga2", detector="Disk", resource="rtr", severity="HIGH", status="firing"))
    await service.mark_reported(observed.case.case_id, state_signature=service.report_state_signature(observed.case))
    case = await store.get_case(observed.case.case_id)
    case.issue_url = "https://github.com/example/example/issues/1"
    await store.upsert_case(case)
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=case.case_id, intent_type="report", idempotency_key="handoff-terminal",
        payload={"card_revision": 10, "card_update": {"title": "Result", "description": "Model result", "color": 0, "level": int(Verbosity.ERROR)}},
    ))
    created, edited = [], []

    async def notifier(case_id, revision=None, **payload):
        async def create():
            created.append(payload["title"])
            return 123

        async def edit(message_id):
            edited.append(payload["title"])
            return True

        return await deliver_case_card(destination="handoff-test", case_id=case_id, revision=revision,
                                       payload=payload, create=create, edit=edit)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}, retry_backoff_s=0)
    assert (await processor.process_intent(terminal)).failed == 1
    prerequisite = await store.get_outbox_by_key(f"report:{case.case_id}:{service.report_state_signature(case)}")
    assert prerequisite is not None and prerequisite.status == "pending"
    assert (await processor.process_intent(prerequisite)).succeeded == 1
    assert (await processor.process_pending()).succeeded == 1
    assert prerequisite.payload["card_revision"] == 10
    assert len(created) == 1
    assert edited == ["Result"]


@pytest.mark.asyncio
async def test_group_only_attempts_selected_facts_before_triage(monkeypatch, owned_cards):
    import app.main as main

    store = InMemoryCaseStore()
    state = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", state)
    sent = []

    async def notify(**kwargs):
        sent.append(kwargs["case_id"])
        assert len(sent) == 1, "Unrelated Discord sends must not precede triage"
        return True

    async def investigate(*args, **kwargs):
        assert sent == [kwargs["case"]["incident_id"]]

    monkeypatch.setattr(main, "send_case_notification", notify)
    model = AsyncMock(side_effect=investigate)
    monkeypatch.setattr(main, "investigate_alert", model)
    payload = {"source": "alertmanager", "status": "firing", "alerts": [
        {"status": "firing", "fingerprint": f"group-{i}",
         "labels": {"alertname": "Disk", "instance": f"host-{i}", "severity": "critical"},
         "annotations": {"summary": "Disk almost full"}} for i in range(3)
    ]}
    background = BackgroundTasks()
    await main._case_service_reactive_primary_response(payload, background, label="Alert")
    await background()
    model.assert_awaited_once()
    assert len(await store.list_outbox(status="pending")) == 2


@pytest.mark.asyncio
async def test_force_refresh_checks_deleted_card_even_with_identical_cached_payload(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    create = AsyncMock(return_value=123)
    edit = AsyncMock(side_effect=CardNotFound)
    kwargs = dict(destination="test", case_id="deleted", payload={"facts": "critical disk"}, create=create, edit=edit)
    assert await deliver_case_card(**kwargs, revision=1)
    assert await deliver_case_card(**kwargs, revision=2, force_refresh=True)
    edit.assert_awaited_once_with(123)
    assert create.await_count == 2


@pytest.mark.asyncio
async def test_report_scheduling_failure_does_not_strand_investigation_claim(monkeypatch, owned_cards):
    import app.main as main

    store = InMemoryCaseStore()
    state = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", state)
    original = main._maybe_request_reactive_case_report

    async def fail_selected(*args, **kwargs):
        if kwargs.get("background_tasks") is not None:
            raise RuntimeError("temporary store failure")
        return await original(*args, **kwargs)

    monkeypatch.setattr(main, "_maybe_request_reactive_case_report", fail_selected)
    payload = {"source": "alertmanager", "status": "firing", "alerts": [{
        "status": "firing", "fingerprint": "schedule-failure",
        "labels": {"alertname": "Disk", "instance": "rtr", "severity": "critical"},
    }]}
    with pytest.raises(RuntimeError):
        await main._case_service_reactive_primary_response(payload, BackgroundTasks(), label="Alert")
    case = (await store.list_cases())[0]
    assert not case.last_investigated_at
    assert state.service.should_investigate(case)
    monkeypatch.setattr(main, "_maybe_request_reactive_case_report", original)
    monkeypatch.setattr(main, "send_case_notification", AsyncMock(return_value=True))
    investigate = AsyncMock()
    monkeypatch.setattr(main, "investigate_alert", investigate)
    retry = BackgroundTasks()
    await main._case_service_reactive_primary_response(payload, retry, label="Alert")
    await retry()
    investigate.assert_awaited_once()


@pytest.mark.asyncio
async def test_persistent_store_outage_never_sends_terminal_directly(monkeypatch, owned_cards):
    import app.main as main

    store = InMemoryCaseStore()
    state = CaseServiceRuntime(store=store, service=CaseService(store))
    monkeypatch.setattr(main, "case_service_runtime", state)
    notifier = AsyncMock(return_value=True)
    monkeypatch.setattr(main, "send_case_notification", notifier)
    model = AsyncMock(side_effect=RuntimeError("model unavailable"))
    monkeypatch.setattr(main, "run_investigation_graph", model)
    payload = {"source": "alertmanager", "status": "firing", "alerts": [{
        "status": "firing", "fingerprint": "persistent-failure",
        "labels": {"alertname": "Disk", "instance": "rtr", "severity": "critical"},
    }]}
    tasks = BackgroundTasks()
    await main._case_service_reactive_primary_response(payload, tasks, label="Alert")
    monkeypatch.setattr(store, "update_outbox_if_status", AsyncMock(side_effect=RuntimeError("store unavailable")))
    monkeypatch.setattr(store, "get_case", AsyncMock(side_effect=RuntimeError("store unavailable")))
    await tasks()
    model.assert_awaited_once()
    notifier.assert_not_awaited()
    assert len(await store.list_outbox(status="pending")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("severity", ["LOW", "MEDIUM"])
async def test_eligible_terminal_error_includes_lower_severity_facts(monkeypatch, tmp_path, owned_cards, severity):
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="alertmanager", detector="Disk", resource="rtr", status="firing", severity=severity,
    ))
    case = observed.case
    case.summary = "Router filesystem is low"
    await store.upsert_case(case)
    await service.request_report(case)
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=case.case_id, intent_type="report", idempotency_key="eligible-error",
        payload={"card_update": {"title": "Investigation failed", "description": "Model unavailable",
                                 "color": 0, "level": int(Verbosity.ERROR)}},
    ))
    created = []

    async def notify(case_id, revision=None, **payload):
        async def create():
            created.append(payload)
            return 123
        return await deliver_case_card(destination="verbosity", case_id=case_id, revision=revision, payload=payload,
                                       create=create, edit=AsyncMock(return_value=True))

    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)})
    assert (await processor.process_pending()).succeeded == 2
    assert len(created) == 1
    assert created[0]["level"] == Verbosity.ERROR
    assert created[0]["description"].splitlines() == ["Router filesystem is low", "", "Model unavailable"]
    assert (await store.get_outbox_by_key(terminal.idempotency_key)).payload["card_update_delivered"]


@pytest.mark.asyncio
async def test_normally_reported_card_deleted_during_investigation_retains_facts(monkeypatch, tmp_path, owned_cards):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="alertmanager", detector="Disk", resource="rtr", status="firing", severity="HIGH",
    ))
    case = observed.case
    case.summary = "Router has 4% free"
    await store.upsert_case(case)
    created = []
    exists = False

    async def notify(case_id, revision=None, force_refresh=False, **payload):
        async def create():
            nonlocal exists
            exists = True
            created.append(payload)
            return 123

        async def edit(message_id):
            if not exists:
                raise CardNotFound
            return True

        return await deliver_case_card(destination="normal-deletion", case_id=case_id, payload=payload,
                                       revision=revision, force_refresh=force_refresh, create=create, edit=edit)

    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)})
    initial = await service.request_report(case, payload={"card_revision": 10})
    assert (await processor.process_intent(initial)).succeeded == 1
    assert (await store.get_case(case.case_id)).last_reported_signature == service.report_state_signature(case)
    exists = False
    terminal = await store.enqueue_outbox(OutboxIntent(
        case_id=case.case_id, intent_type="report", idempotency_key="normal-deletion-terminal",
        payload={"card_revision": 20, "card_update": {"title": "Investigation failed",
            "description": "Model unavailable", "color": 0, "level": int(Verbosity.ERROR)}},
    ))
    assert (await processor.process_intent(terminal)).succeeded == 1
    assert len(created) == 2
    assert "Router has 4% free" in created[-1]["description"]
    assert "Model unavailable" in created[-1]["description"]
