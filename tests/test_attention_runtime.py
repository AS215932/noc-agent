from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from app.cases.handlers import build_report_handler
from app.cases.models import AtomicCaseProjection
from app.cases.runtime import CaseServiceRuntime, enqueue_due_case_reminders, process_case_outbox_once
from app.cases.service import CaseService
from app.cases.policy import CasePolicy
from app.cases.attention import AttentionDelivery
from app.cases.attention_scheduler import enqueue_attention
from app.cases.outbox import OutboxProcessor
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_enabled_worker_uses_attention_clock_while_quiet_reports_leave_it_unchanged(monkeypatch):
    for flag in ("NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED"):
        monkeypatch.setenv(flag, "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    runtime = CaseServiceRuntime(service=service, store=store)
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    notify = AsyncMock(return_value=True)
    monkeypatch.setattr("app.cases.handlers.build_default_outbox_handlers",
        lambda service, **kwargs: {"report": build_report_handler(service, notifier=notify)})
    assert (await process_case_outbox_once(runtime)).succeeded == 1
    assert notify.call_args.kwargs["case_id"] == case.case_id
    first = await store.get_attention(case.case_id)
    store._attention[case.case_id] = first.model_copy(update={
        "delivered_at": datetime.now(timezone.utc) - timedelta(hours=7),
    })
    # Even freshly updated card facts cannot postpone a due attention reminder.
    case.last_reported_at = datetime.now(timezone.utc).isoformat()
    case.last_reported_signature = service.report_state_signature(case)
    await store.upsert_case(case)
    assert service.should_remind(case) is False  # Legacy path is inhibited.
    assert (await process_case_outbox_once(runtime)).succeeded == 1
    assert "Unacknowledged critical" in notify.call_args.kwargs["title"]
    attention = await store.get_attention(case.case_id)
    assert attention.sequence == 2
    case.signal_signature = "new quiet telemetry"
    await store.upsert_case(case)
    await service.request_report(case)
    assert (await process_case_outbox_once(runtime)).succeeded == 1
    assert notify.call_args.kwargs["case_id"] == case.case_id
    assert await store.get_attention(case.case_id) == attention
    assert (await process_case_outbox_once(runtime)).processed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled", ["NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED"])
async def test_attention_creation_requires_all_ownership_gates(monkeypatch, disabled):
    for flag in ("NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED"):
        monkeypatch.setenv(flag, "0" if flag == disabled else "1")
    store = InMemoryCaseStore()
    runtime = CaseServiceRuntime(service=CaseService(store), store=store)
    await store.upsert_case(AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"}))
    assert await enqueue_due_case_reminders(runtime) == 0
    assert await store.list_outbox() == []


@pytest.mark.asyncio
async def test_daily_policy_applies_to_scheduler_and_previously_queued_delivery(monkeypatch):
    for flag in ("NOC_CASE_ATTENTION_ENABLED", "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED"):
        monkeypatch.setenv(flag, "1")
    store = InMemoryCaseStore()
    service = CaseService(store, policy=CasePolicy(report_reassert_s=86400))
    runtime = CaseServiceRuntime(service=service, store=store)
    case = AtomicCaseProjection(severity="HIGH", identity={"source": "icinga2"})
    await store.upsert_case(case)
    store._attention[case.case_id] = AttentionDelivery(case_id=case.case_id, generation=0, phase="firing", severity="HIGH",
        delivered_at=datetime.now(timezone.utc) - timedelta(hours=7), sequence=1)
    assert await enqueue_due_case_reminders(runtime) == 0
    # An old six-hour policy may already have queued this request. Delivery must
    # honor the newly configured daily interval independently of the scheduler.
    queued = await enqueue_attention(store, case)
    assert queued is not None
    notify = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notify)})
    assert (await processor.process_pending()).succeeded == 1
    notify.assert_not_awaited()
    previous = await store.get_attention(case.case_id)
    store._attention[case.case_id] = previous.model_copy(update={
        "delivered_at": datetime.now(timezone.utc) - timedelta(hours=25),
    })
    assert await enqueue_due_case_reminders(runtime) == 1
    assert (await processor.process_pending()).succeeded == 1
    notify.assert_awaited_once()
