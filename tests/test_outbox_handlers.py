import asyncio

import pytest

from app.cases import CaseHandoff, CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor, VerificationObjective
from app.cases.handlers import build_default_outbox_handlers, build_engineering_lhp_handoff_handler, build_handoff_handler, build_report_handler
from app.config import LoopHandoffSettings
from app.proactive.handoff import GitHubHandoff


class FakeGitHub:
    def __init__(self):
        self.created = []
        self.comments = []
        self.search_returns_existing = False

    async def request(self, method, path, *, params=None, json=None):
        if method == "GET" and path == "/search/issues":
            if self.search_returns_existing:
                return 200, {"items": [self.created[0]]}
            return 200, {"items": []}
        if method == "POST" and path.endswith("/issues"):
            issue = {
                "number": 202,
                "html_url": "https://github.com/AS215932/network-operations/issues/202",
                "body": json["body"],
                "title": json["title"],
                "labels": json.get("labels", []),
            }
            self.created.append(issue)
            self.search_returns_existing = True
            return 201, issue
        if method == "POST" and path.endswith("/comments"):
            self.comments.append((path, json["body"]))
            return 201, {}
        raise AssertionError(f"unexpected request {method} {path}")


@pytest.mark.asyncio
async def test_report_handler_sends_notification_and_marks_case_reported():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="proactive",
            rule_id="disk_fill",
            resource="rtr1:/var",
            status="firing",
            severity="HIGH",
            annotations={"summary": "/var has 5% free"},
            signal_snapshot={"summary": "/var has 5% free"},
        )
    )
    assert created.case is not None
    state_signature = service.report_state_signature(created.case)
    intent = await service.request_report(created.case, state_signature=state_signature)
    sent = []

    async def notifier(**kwargs):
        sent.append(kwargs)

    report = await OutboxProcessor(
        store,
        {"report": build_report_handler(service, notifier=notifier, control_public_url="https://noc.example")},
    ).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert sent
    assert sent[0]["case_id"] == created.case.case_id
    assert "NOC case" in sent[0]["title"]
    stored_case = await store.get_case(created.case.case_id)
    assert stored_case is not None
    assert getattr(stored_case, "last_reported_signature") == state_signature
    stored_intent = (await store.list_outbox())[0]
    assert stored_intent.outbox_id == intent.outbox_id
    assert stored_intent.status == "succeeded"
    assert stored_intent.external_url.endswith(f"/control/cases/{created.case.case_number or created.case.case_id}")


@pytest.mark.asyncio
async def test_handoff_handler_creates_issue_and_records_case_result():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="proactive",
            rule_id="bgp_risk",
            resource="rtr1:peer1",
            status="firing",
            severity="HIGH",
            annotations={"summary": "peer flapping"},
            signal_snapshot={"summary": "peer flapping"},
        )
    )
    assert created.case is not None
    await service.record_investigation_result(
        created.case.case_id,
        diagnosis={"summary": "BGP peer flap likely needs policy/timer coordination"},
        recommendations=["check peer timer config"],
    )
    intent = await service.handoff_intent(created.case.case_id, payload={"body": "Please review peer policy."})
    assert intent is not None
    fake = FakeGitHub()
    handoff = GitHubHandoff(repo="AS215932/network-operations", token="t", requester=fake.request)

    report = await OutboxProcessor(
        store,
        {"handoff": build_handoff_handler(service, handoff_client=handoff, control_public_url="https://noc.example")},
    ).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert len(fake.created) == 1
    assert f"noc-case-id:{created.case.case_id}" in fake.created[0]["body"]
    assert "Please review peer policy." in fake.created[0]["body"]
    stored_case = await store.get_case(created.case.case_id)
    assert stored_case is not None
    assert getattr(stored_case, "issue_url") == "https://github.com/AS215932/network-operations/issues/202"
    assert getattr(stored_case, "issue_id") == "202"
    events = [event.event_type for event in await store.case_events(created.case.case_id)]
    assert "handoff_created_issue" in events


@pytest.mark.asyncio
async def test_engineering_lhp_handoff_handler_creates_candidate_issue_and_delivery_record():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="proactive",
            rule_id="disk_fill",
            resource="rtr:/",
            status="firing",
            severity="HIGH",
            annotations={"summary": "root disk low"},
            signal_snapshot={"summary": "root disk low"},
        )
    )
    assert created.case is not None
    handoff = CaseHandoff(
        handoff_id="handoff_disk_1",
        case_id=created.case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key=f"{created.case.case_id}:engineering:resolve-low-root-filesystem-condition-v1:v1",
        case_type="proactive_disk_condition",
        fingerprint=created.case.fingerprint,
        resource={"host": "rtr", "filesystem": "/"},
        constraints=["do_not_make_suppression_permanent_without_separate_approval"],
        acceptance_criteria=["monitoring alert clears"],
    )
    await service.request_lhp_handoff(
        handoff,
        objectives=[
            VerificationObjective(
                case_id=created.case.case_id,
                handoff_id=handoff.handoff_id,
                objective_key="disk_clear",
                objective_type="monitoring_alert_clear",
                name="disk clear",
            )
        ],
        enqueue_delivery=True,
    )
    fake = FakeGitHub()
    gh = GitHubHandoff(repo="AS215932/network-operations", token="t", requester=fake.request)

    report = await OutboxProcessor(
        store,
        {
            "engineering_handoff_requested": build_engineering_lhp_handoff_handler(
                service,
                handoff_client=gh,
                control_public_url="https://noc.example",
            )
        },
    ).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert len(fake.created) == 1
    issue = fake.created[0]
    assert "loop:candidate" in issue["labels"]
    assert "loop:approved" not in issue["labels"]
    assert {"noc", "engineering-handoff", "monitoring", "disk"}.issubset(set(issue["labels"]))
    assert "noc-lhp-handoff-id:handoff_disk_1" in issue["body"]
    assert f"noc-case-id:{created.case.case_id}" in issue["body"]
    assert "loop:approved" in issue["body"]
    deliveries = getattr(store, "_handoff_deliveries")
    delivery = next(iter(deliveries.values()))
    assert delivery.status == "succeeded"
    assert delivery.external_url.endswith("/issues/202")


@pytest.mark.asyncio
async def test_engineering_lhp_handoff_handler_skips_cancelled_handoff():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing")
    )
    assert created.case is not None
    handoff = CaseHandoff(
        handoff_id="handoff_cancelled_delivery",
        case_id=created.case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="cancelled-delivery:v1",
    )
    await service.request_lhp_handoff(handoff)
    await service.cancel_lhp_handoff(
        handoff.handoff_id,
        actor_id="operator",
        reason="stale handoff",
        external_event_id="cancelled-delivery-event",
    )
    fake = FakeGitHub()
    gh = GitHubHandoff(repo="AS215932/network-operations", token="t", requester=fake.request)
    handler = build_engineering_lhp_handoff_handler(service, handoff_client=gh)

    result = await handler(
        OutboxIntent(
            case_id=created.case.case_id,
            intent_type="engineering_handoff_requested",
            idempotency_key="in-flight-cancelled-delivery",
            payload={"handoff_id": handoff.handoff_id},
        )
    )

    assert result is not None
    assert result.payload_updates["delivery_skipped"] is True
    assert result.payload_updates["terminal_status"] == "cancelled"
    assert fake.created == []


@pytest.mark.asyncio
async def test_engineering_lhp_delivery_serializes_concurrent_cancellation():
    class BlockingGitHub(FakeGitHub):
        def __init__(self):
            super().__init__()
            self.delivery_started = asyncio.Event()
            self.release_delivery = asyncio.Event()

        async def request(self, method, path, *, params=None, json=None):
            if method == "GET" and path == "/search/issues":
                self.delivery_started.set()
                await self.release_delivery.wait()
            return await super().request(method, path, params=params, json=json)

    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing"))
    assert created.case is not None
    handoff = CaseHandoff(
        handoff_id="handoff_delivery_cancel_race",
        case_id=created.case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="delivery-cancel-race:v1",
    )
    requested = await service.request_lhp_handoff(handoff, enqueue_delivery=True)
    assert requested.outbox_intent is not None
    fake = BlockingGitHub()
    gh = GitHubHandoff(repo="AS215932/network-operations", token="t", requester=fake.request)
    handler = build_engineering_lhp_handoff_handler(service, handoff_client=gh)

    delivery_task = asyncio.create_task(handler(requested.outbox_intent))
    await fake.delivery_started.wait()
    cancellation_task = asyncio.create_task(
        service.cancel_lhp_handoff(
            handoff.handoff_id,
            actor_id="operator",
            reason="cancel while delivery is running",
            external_event_id="cancel_delivery_race_1",
        )
    )
    await asyncio.sleep(0)
    assert cancellation_task.done() is False

    fake.release_delivery.set()
    delivery_result = await delivery_task
    cancellation_result = await cancellation_task

    assert delivery_result.external_url.endswith("/issues/202")
    assert cancellation_result.handoff.status == "cancelled"
    assert len(fake.created) == 1


@pytest.mark.asyncio
async def test_handoff_handler_reuses_existing_case_issue():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing")
    )
    assert created.case is not None
    await service.record_handoff_result(created.case.case_id, issue_url="https://github.com/o/r/issues/1", issue_id="1")
    # Simulates an old queued handoff intent racing with the already-stamped case.
    intent = await store.enqueue_outbox(
        OutboxIntent(
            case_id=created.case.case_id,
            intent_type="handoff",
            idempotency_key="handoff:already-stamped",
        )
    )
    fake = FakeGitHub()
    handoff = GitHubHandoff(repo="o/r", token="t", requester=fake.request)

    report = await OutboxProcessor(store, {"handoff": build_handoff_handler(service, handoff_client=handoff)}).process_pending()

    assert report.succeeded == 1
    assert fake.created == []
    stored_intent = next(row for row in await store.list_outbox() if row.outbox_id == intent.outbox_id)
    assert stored_intent.external_url == "https://github.com/o/r/issues/1"


@pytest.mark.asyncio
async def test_default_handlers_include_knowledge_candidate_and_handoff_only_when_configured(tmp_path):
    store = InMemoryCaseStore()
    service = CaseService(store)

    assert set(build_default_outbox_handlers(service)) == {"report"}
    assert set(build_default_outbox_handlers(service, knowledge_candidate_dir=tmp_path)) == {
        "report",
        "knowledge_candidate",
    }
    handoff = GitHubHandoff(repo="o/r", token="t", requester=FakeGitHub().request)
    assert set(build_default_outbox_handlers(service, knowledge_candidate_dir=tmp_path, handoff_client=handoff)) == {
        "report",
        "knowledge_candidate",
        "handoff",
    }
    assert set(
        build_default_outbox_handlers(
            service,
            knowledge_candidate_dir=tmp_path,
            handoff_client=handoff,
            engineering_handoff_client=handoff,
        )
    ) == {"report", "knowledge_candidate", "handoff", "engineering_handoff_requested"}
    assert set(
        build_default_outbox_handlers(
            service,
            loop_handoff_settings=LoopHandoffSettings(enabled=True, knowledge_context_enabled=True, knowledge_candidate_dir=str(tmp_path)),
        )
    ) == {"report", "knowledge_context_requested", "knowledge_artifact_proposed"}

@pytest.mark.asyncio
async def test_report_outbox_retries_failed_delivery_before_marking_reported(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    from app.case_cards import deliver_case_card

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/",
        status="firing", severity="HIGH",
    ))
    assert created.case is not None
    signature = service.report_state_signature(created.case)
    await service.request_report(created.case, state_signature=signature)
    create = AsyncMock(side_effect=[TimeoutError("temporary"), 123])
    edit = AsyncMock(return_value=True)

    async def notifier(**kwargs):
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"description": kwargs["description"]}, create=create, edit=edit,
        )

    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}, retry_backoff_s=0)
    first = await processor.process_pending()
    assert first.failed == 1 and first.succeeded == 0
    case = await store.get_case(created.case.case_id)
    assert not case.last_reported_signature
    intent = (await store.list_outbox())[0]
    assert intent.status == "failed" and intent.next_attempt_at
    second = await processor.process_pending()
    assert second.succeeded == 1
    case = await store.get_case(created.case.case_id)
    assert case.last_reported_signature == signature
    assert (await store.list_outbox())[0].status == "succeeded"
    assert create.await_count == 2

@pytest.mark.asyncio
@pytest.mark.parametrize("severity,verbosity", [("LOW", "WARNING"), ("HIGH", "ERROR")])
async def test_verbosity_filtered_reports_complete_without_retry_or_report_stamp(monkeypatch, severity, verbosity):
    from unittest.mock import AsyncMock
    monkeypatch.setenv("LOG_LEVEL_DISCORD", verbosity)
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity=severity,
    ))
    await service.request_report(created.case)
    notifier = AsyncMock(return_value=False)
    result = await OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}).process_pending()
    assert result.succeeded == 1 and result.failed == 0
    notifier.assert_not_called()
    intent = (await store.list_outbox())[0]
    assert intent.payload["notification_suppressed"] == "verbosity"
    assert not (await store.get_case(created.case.case_id)).last_reported_signature


@pytest.mark.asyncio
async def test_failed_terminal_card_edit_retries_from_outbox(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from app.case_cards import deliver_case_card
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    create = AsyncMock(return_value=123)
    edit = AsyncMock(side_effect=[TimeoutError("temporary"), True])
    async def notifier(**kwargs):
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"title": kwargs["title"]}, revision=kwargs.get("revision"),
            create=create, edit=edit,
        )
    assert await notifier(case_id=created.case.case_id, title="Starting investigation")
    assert not await send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id, notifier=notifier,
        title="Investigation unavailable", description="Dependency failed", color=0, level=Verbosity.ERROR,
    )
    failed = await store.list_outbox(status="failed")
    assert len(failed) == 1
    failed[0].next_attempt_at = "2000-01-01T00:00:00+00:00"
    await store.update_outbox(failed[0])
    result = await OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}).process_pending()
    assert result.succeeded == 1
    assert edit.await_count == 2
    create.assert_awaited_once()
    assert (await store.list_outbox())[0].payload["card_update_delivered"] is True


@pytest.mark.asyncio
async def test_old_intake_report_cannot_overwrite_newer_terminal_card(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from app.case_cards import deliver_case_card
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    intake = await service.request_report(created.case)
    intake.created_at = "2000-01-01T00:00:00+00:00"
    await store.update_outbox_if_status(intake, expected_status="pending")
    sent = []
    async def notifier(**kwargs):
        async def create():
            sent.append(kwargs["title"])
            return 123
        async def edit(message_id):
            sent.append(kwargs["title"])
            return True
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"title": kwargs["title"]}, revision=kwargs.get("revision"),
            create=create, edit=edit,
        )
    assert await send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id, notifier=notifier,
        title="Investigation unavailable", description="Dependency failed", color=0, level=Verbosity.ERROR,
    )
    result = await OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}).process_pending()
    assert result.succeeded == 1
    assert sent == ["Investigation unavailable"]


@pytest.mark.asyncio
async def test_lowering_verbosity_requeues_same_report_identity(monkeypatch):
    from unittest.mock import AsyncMock

    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    original = await service.request_report(created.case)
    notifier = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
    assert (await processor.process_pending()).succeeded == 1
    same_policy = await service.request_report(created.case)
    assert same_policy.status == "succeeded"
    assert (await processor.process_pending()).processed == 0
    notifier.assert_not_called()

    monkeypatch.setenv("LOG_LEVEL_DISCORD", "WARNING")
    reopened = await service.request_report(created.case)
    assert reopened.outbox_id == original.outbox_id
    assert reopened.status == "pending"
    assert reopened.completed_at is None
    assert reopened.created_at == original.created_at
    assert "notification_suppressed" not in reopened.payload
    assert (await processor.process_pending()).succeeded == 1
    notifier.assert_awaited_once()
    assert len(await store.list_outbox()) == 1
    assert (await store.get_case(created.case.case_id)).last_reported_signature


@pytest.mark.asyncio
@pytest.mark.parametrize("immediate_delivery", [False, True])
async def test_failure_metric_recorded_once_by_delivery_outbox(monkeypatch, immediate_delivery):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    metric = Mock()
    monkeypatch.setattr("app.cases.reporting.record_sanitized_discord_failure", metric)
    monkeypatch.setattr("app.cases.handlers.record_sanitized_discord_failure", metric)
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    notifier = AsyncMock(side_effect=[immediate_delivery, True])
    assert await send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id, notifier=notifier,
        safe_category="infrastructure", title="Unavailable", description="Dependency failed", level=Verbosity.ERROR,
    ) is immediate_delivery
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
    if immediate_delivery:
        metric.assert_called_once_with("infrastructure")
        assert (await processor.process_pending()).processed == 0
    else:
        metric.assert_not_called()
        failed = (await store.list_outbox())[0]
        failed.next_attempt_at = "2000-01-01T00:00:00+00:00"
        await store.update_outbox(failed)
        assert (await processor.process_pending()).succeeded == 1
    metric.assert_called_once_with("infrastructure")
    assert (await processor.process_pending()).processed == 0
    metric.assert_called_once_with("infrastructure")


@pytest.mark.asyncio
async def test_superseded_failure_is_not_counted_as_delivered(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock
    from app.case_cards import deliver_case_card
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    metric = Mock()
    monkeypatch.setattr("app.cases.reporting.record_sanitized_discord_failure", metric)
    monkeypatch.setattr("app.cases.handlers.record_sanitized_discord_failure", metric)
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    await send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id, notifier=AsyncMock(return_value=False),
        safe_category="infrastructure", title="Unavailable", description="Dependency failed", level=Verbosity.ERROR,
    )
    intent = (await store.list_outbox())[0]
    intent.next_attempt_at = "2000-01-01T00:00:00+00:00"
    await store.update_outbox(intent)
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)

    async def notifier(**kwargs):
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"title": kwargs["title"]}, revision=kwargs["revision"], create=create, edit=edit,
        )

    await notifier(case_id=created.case.case_id, title="Recovered", revision=intent.payload["card_revision"] + 1)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
    assert (await processor.process_pending()).succeeded == 1
    metric.assert_not_called()
    edit.assert_not_called()
    assert (await store.list_outbox())[0].payload["notification_superseded"] is True


@pytest.mark.asyncio
async def test_reopened_intake_does_not_overwrite_later_failure(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app.case_cards import deliver_case_card
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "ERROR")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    original = await service.request_report(created.case)
    sent = []
    async def notifier(**kwargs):
        async def create():
            sent.append(kwargs["title"])
            return 123
        async def edit(message_id):
            sent.append(kwargs["title"])
            return True
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"title": kwargs["title"]}, revision=kwargs["revision"], create=create, edit=edit,
        )
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
    assert (await processor.process_pending()).succeeded == 1
    metric = Mock()
    monkeypatch.setattr("app.cases.handlers.record_sanitized_discord_failure", metric)
    assert await send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id,
        notifier=notifier, title="Investigation unavailable", description="Dependency failed",
        safe_category="infrastructure", level=Verbosity.ERROR,
    )
    metric.assert_called_once_with("infrastructure")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "WARNING")
    reopened = await service.request_report(created.case)
    assert reopened.created_at == original.created_at
    assert (await processor.process_pending()).succeeded == 1
    assert sent == ["Investigation unavailable"]


@pytest.mark.asyncio
async def test_immediate_failure_metric_survives_newer_card(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock
    from app.case_cards import deliver_case_card
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    intake = await service.request_report(created.case)
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    async def notifier(**kwargs):
        return await deliver_case_card(
            destination="bot:999:42", case_id=kwargs["case_id"],
            payload={"title": kwargs["title"]}, revision=kwargs["revision"], create=create, edit=edit,
        )
    metric = Mock()
    monkeypatch.setattr("app.cases.handlers.record_sanitized_discord_failure", metric)
    runtime = SimpleNamespace(store=store, service=service)
    assert await send_investigation_card(
        runtime=runtime, case_id=created.case.case_id, notifier=notifier,
        safe_category="infrastructure", title="Unavailable", description="Dependency failed", level=Verbosity.ERROR,
    )
    # Immediate delivery must leave unrelated pending work to its normal worker.
    assert (await store.list_outbox(status="pending"))[0].outbox_id == intake.outbox_id
    assert await send_investigation_card(
        runtime=runtime, case_id=created.case.case_id, notifier=notifier,
        title="Recovered", description="Investigation complete", level=Verbosity.INFO,
    )
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
    assert (await processor.process_pending()).succeeded == 1
    metric.assert_called_once_with("infrastructure")
    create.assert_awaited_once()
    edit.assert_awaited_once()


@pytest.mark.asyncio
async def test_immediate_delivery_and_worker_share_one_claim(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock
    from app.cases.reporting import send_investigation_card
    from app.discord import Verbosity

    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(
        source="icinga2", rule_id="disk", resource="rtr:/", status="firing", severity="HIGH",
    ))
    entered, release = asyncio.Event(), asyncio.Event()
    async def transport(**kwargs):
        entered.set()
        await release.wait()
        return True
    notifier = AsyncMock(side_effect=transport)
    metric = Mock()
    monkeypatch.setattr("app.cases.handlers.record_sanitized_discord_failure", metric)
    task = asyncio.create_task(send_investigation_card(
        runtime=SimpleNamespace(store=store, service=service), case_id=created.case.case_id,
        notifier=notifier, safe_category="infrastructure",
        title="Unavailable", description="Dependency failed", level=Verbosity.ERROR,
    ))
    await asyncio.wait_for(entered.wait(), timeout=2)
    try:
        processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)})
        assert (await processor.process_pending()).processed == 0
    finally:
        release.set()
        assert await asyncio.wait_for(task, timeout=2)
    notifier.assert_awaited_once()
    metric.assert_called_once_with("infrastructure")
    assert (await store.list_outbox())[0].status == "succeeded"
