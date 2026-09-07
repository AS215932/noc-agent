from datetime import datetime, timedelta, timezone

import pytest

from app.cases import AtomicCaseProjection, CaseService, InMemoryCaseStore, ObservationRecord, OutboxProcessor
from app.cases.handlers import build_report_handler


async def reported_case(store, service):
    result = await service.observe(ObservationRecord(
        source="proactive", rule_id="disk", resource="rtr", status="firing", severity="HIGH",
    ))
    case = result.case
    assert isinstance(case, AtomicCaseProjection)
    case.last_reported_at = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    case.last_reported_signature = service.report_state_signature(case)
    return await store.upsert_case(case)


@pytest.mark.asyncio
async def test_unchanged_critical_reminder_six_hour_boundary():
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    last = datetime.fromisoformat(case.last_reported_at)
    assert not service.should_report(case, now=last + timedelta(hours=6, microseconds=-1))
    assert service.should_report(case, now=last + timedelta(hours=6))


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"severity": "MEDIUM"}, {"severity": "LOW"}, {"severity": "UNKNOWN"},
    {"acknowledged_by": "operator"}, {"acknowledged_at": "2026-09-07T00:00:00+00:00"},
    {"status": "resolved"}, {"status": "closed"}, {"status": "expired"},
    {"status": "linked"}, {"status": "recovered_pending"},
    {"covered_by_meta_case": True},
    {"snoozed_until": "2099-01-01T00:00:00+00:00"},
    {"suppressed_until": "2099-01-01T00:00:00+00:00"},
])
async def test_unchanged_non_actionable_cases_do_not_repeat(changes):
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    case = case.model_copy(update=changes)
    case.last_reported_signature = service.report_state_signature(case)
    assert not service.should_report(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["ack", "resolve", "downgrade", "snooze", "new_report"])
async def test_queued_reminder_rechecks_live_case_before_delivery(change):
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    intent = await service.request_report(case)
    if change == "ack":
        await service.ack(case.case_id, operator="oncall")
    else:
        if change == "resolve":
            case.status = "resolved"
        elif change == "downgrade":
            case.severity = "MEDIUM"
        elif change == "snooze":
            case.snoozed_until = "2099-01-01T00:00:00+00:00"
        else:
            case.last_reported_at = datetime.now(timezone.utc).isoformat()
        await store.upsert_case(case)

    async def unexpected(**kwargs):
        pytest.fail("stale reminder must not notify")

    processor = OutboxProcessor(store, {"report": build_report_handler(
        service, notifier=unexpected, reminder_notifier=unexpected,
    )})
    result = await processor.process_pending()
    assert result.succeeded == 1
    saved = next(item for item in await store.list_outbox() if item.outbox_id == intent.outbox_id)
    assert saved.payload["notification_suppressed"] == "reminder_no_longer_due"


@pytest.mark.asyncio
async def test_reminder_identity_retries_until_delivery_then_advances():
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    first = await service.request_report(case)
    duplicate = await service.request_report(case)
    assert first.outbox_id == duplicate.outbox_id
    sent = []

    async def reminder(**kwargs):
        sent.append(kwargs)
        return len(sent) > 1

    async def card(**kwargs):
        pytest.fail("reminders must create a notification, not silently edit the card")

    processor = OutboxProcessor(store, {"report": build_report_handler(
        service, notifier=card, reminder_notifier=reminder,
    )}, retry_backoff_s=0)
    failed = await processor.process_pending()
    assert failed.failed == 1
    unchanged = await store.get_case(case.case_id)
    assert unchanged.last_reported_at == case.last_reported_at
    assert (await service.request_report(unchanged)).outbox_id == first.outbox_id
    delivered = await processor.process_pending()
    assert delivered.succeeded == 1
    reported = await store.get_case(case.case_id)
    assert reported.last_reasserted_at
    assert not service.should_report(reported)
    assert (await service.request_report(reported)).outbox_id != first.outbox_id
    assert len(sent) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("recurrence", [False, True])
async def test_acknowledgement_does_not_cover_escalation_or_recurrence(recurrence):
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    case.status = "resolved" if recurrence else "investigating"
    case.severity = "HIGH" if recurrence else "MEDIUM"
    await store.upsert_case(case)
    await service.ack(case.case_id, operator="oncall")
    result = await service.observe(ObservationRecord(
        source="proactive", rule_id="disk", resource="rtr", status="firing", severity="HIGH",
    ))
    assert result.case.case_id == case.case_id
    assert not result.case.acknowledged_at
    assert not result.case.acknowledged_by
    assert result.case.report_generation == case.report_generation + 1
    assert service.should_report(result.case)


@pytest.mark.asyncio
async def test_identical_observation_preserves_acknowledgement():
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    await service.ack(case.case_id, operator="oncall")
    result = await service.observe(ObservationRecord(
        source="proactive", rule_id="disk", resource="rtr", status="firing", severity="HIGH",
    ))
    assert result.case.acknowledged_by == "oncall"
    assert not service.should_remind(result.case)


@pytest.mark.asyncio
async def test_snoozed_queued_reminder_can_deliver_after_snooze_expires():
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = await reported_case(store, service)
    intent = await service.request_report(case)
    case.snoozed_until = "2099-01-01T00:00:00+00:00"
    await store.upsert_case(case)
    calls = []

    async def notify(**kwargs):
        calls.append(kwargs)
        return True

    processor = OutboxProcessor(store, {"report": build_report_handler(
        service, notifier=notify, reminder_notifier=notify,
    )})
    assert (await processor.process_pending()).succeeded == 1
    assert not calls
    case.snoozed_until = "2020-01-01T00:00:00+00:00"
    await store.upsert_case(case)
    reopened = await service.request_report(case)
    assert reopened.outbox_id == intent.outbox_id
    assert reopened.status == "pending"
    assert (await processor.process_pending()).succeeded == 1
    assert len(calls) == 1
