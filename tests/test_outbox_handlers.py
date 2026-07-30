import asyncio

import pytest
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from app.cases import CaseHandoff, CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor, VerificationObjective
from app.cases.handlers import build_default_outbox_handlers, build_discord_reminder_handler, build_engineering_lhp_handoff_handler, build_handoff_handler, build_report_handler
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
        return SimpleNamespace(message_id="discord-123", channel_id="noc-channel", action="created")

    report = await OutboxProcessor(
        store,
        {"report": build_report_handler(service, notifier=notifier, control_public_url="https://noc.example")},
    ).process_pending()

    assert report.processed == 1
    assert report.succeeded == 1
    assert sent
    assert sent[0]["case_id"] == created.case.case_id
    assert sent[0]["route"] == "network"
    assert sent[0]["message_id"] == ""
    assert "NOC case" in sent[0]["title"]
    stored_case = await store.get_case(created.case.case_id)
    assert stored_case is not None
    assert getattr(stored_case, "last_reported_signature") == state_signature
    assert getattr(stored_case, "discord_message_id") == "discord-123"
    assert getattr(stored_case, "discord_channel_id") == "noc-channel"
    stored_intent = (await store.list_outbox())[0]
    assert stored_intent.outbox_id == intent.outbox_id
    assert stored_intent.status == "succeeded"
    assert stored_intent.external_url.endswith(f"/control/cases/{created.case.case_number or created.case.case_id}")
    assert stored_intent.external_id == "discord-123"


@pytest.mark.asyncio
async def test_unacknowledged_critical_gets_one_six_hour_reminder():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="alertmanager",
            rule_id="BGPSessionDown",
            resource="cr1:peer",
            status="firing",
            severity="HIGH",
            notification_route="network",
            signal_snapshot={"state": "down"},
        )
    )
    assert created.case is not None
    case = created.case.model_copy(deep=True)
    now = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)
    case.last_reported_at = (now - timedelta(hours=7)).isoformat()
    case.discord_message_id = "card-1"
    await store.upsert_case(case)

    intents = await service.request_due_critical_reminders(now=now)
    duplicate = await service.request_due_critical_reminders(now=now)
    assert len(intents) == 1
    assert duplicate[0].outbox_id == intents[0].outbox_id

    sent = []

    async def notifier(**kwargs):
        sent.append(kwargs)
        return SimpleNamespace(message_id="reminder-1", channel_id="noc", action="created")

    report = await OutboxProcessor(
        store,
        {"discord_update": build_discord_reminder_handler(service, notifier=notifier)},
    ).process_pending()

    assert report.succeeded == 1
    assert len(sent) == 1
    assert sent[0]["route"] == "network"
    stored = await store.get_case(case.case_id)
    assert stored is not None and getattr(stored, "discord_last_reminder_at")


@pytest.mark.asyncio
async def test_acknowledgement_updates_existing_card_and_stops_reminders():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="alertmanager",
            rule_id="BGPSessionDown",
            resource="cr1:peer",
            status="firing",
            severity="HIGH",
            notification_route="network",
            signal_snapshot={"state": "down"},
        )
    )
    assert created.case is not None
    case = created.case.model_copy(deep=True)
    case.discord_message_id = "card-1"
    case.last_reported_at = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    await store.upsert_case(case)

    acked = await service.ack(case.case_id, operator="alice")
    reminders = await service.request_due_critical_reminders()

    assert acked.acknowledged_by == "alice"
    assert reminders == []
    pending = await store.list_outbox(status="pending")
    assert len(pending) == 1
    assert pending[0].intent_type == "report"
    assert pending[0].payload["schema"] == "case_acknowledgement_v1"


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

    base_handlers = {"report", "discord_update"}
    assert set(build_default_outbox_handlers(service)) == base_handlers
    assert set(build_default_outbox_handlers(service, knowledge_candidate_dir=tmp_path)) == {
        *base_handlers,
        "knowledge_candidate",
    }
    handoff = GitHubHandoff(repo="o/r", token="t", requester=FakeGitHub().request)
    assert set(build_default_outbox_handlers(service, knowledge_candidate_dir=tmp_path, handoff_client=handoff)) == {
        *base_handlers,
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
    ) == {*base_handlers, "knowledge_candidate", "handoff", "engineering_handoff_requested"}
    assert set(
        build_default_outbox_handlers(
            service,
            loop_handoff_settings=LoopHandoffSettings(enabled=True, knowledge_context_enabled=True, knowledge_candidate_dir=str(tmp_path)),
        )
    ) == {*base_handlers, "knowledge_context_requested", "knowledge_artifact_proposed"}
