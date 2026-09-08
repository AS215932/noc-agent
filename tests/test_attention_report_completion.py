import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.cases.attention_scheduler import enqueue_attention
from app.cases.handlers import build_report_handler
from app.cases.models import AtomicCaseProjection
from app.cases.outbox import OutboxProcessor
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
@pytest.mark.parametrize('transition', ['ack', 'recovery'])
async def test_initial_facts_bridge_before_first_scan_preserves_concurrent_state(monkeypatch, transition):
    for flag in ('NOC_CASE_ATTENTION_ENABLED', 'NOC_CASESERVICE_REACTIVE_REPORT', 'NOC_CASE_OUTBOX_ENABLED'):
        monkeypatch.setenv(flag, '1')
    monkeypatch.setenv('LOG_LEVEL_DISCORD', 'INFO')
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity='HIGH', identity={'source': 'icinga2'})
    await store.upsert_case(case)
    initial = await service.request_report(case)

    async def notify(**kwargs):
        current = await store.get_case(case.case_id)
        if transition == 'ack':
            current.acknowledged_by = 'operator'
            current.acknowledged_at = datetime.now(timezone.utc).isoformat()
        else:
            current.status = 'resolved'
            current.severity = 'LOW'
            current.resolution_reason = 'positive_clean_observation'
        await store.upsert_case(current)
        return True

    processor = OutboxProcessor(store, {'report': build_report_handler(service, notifier=notify)})
    assert (await processor.process_intent(initial)).succeeded == 1
    firing = await store.get_attention(case.case_id)
    assert firing.phase == 'firing' and firing.severity == 'HIGH'
    current = await store.get_case(case.case_id)
    assert current.last_reported_at
    if transition == 'ack':
        assert current.acknowledged_by == 'operator'
        assert await enqueue_attention(store, current) is None
        current.status = 'resolved'
        current.severity = 'LOW'
        current.resolution_reason = 'positive_clean_observation'
        await store.upsert_case(current)
    else:
        assert current.status == 'resolved'
    recovery = await enqueue_attention(store, current)
    assert recovery.payload['attention_request']['kind'] == 'recovery'
    assert recovery.payload['attention_request']['severity'] == 'HIGH'
    assert len([event for event in await store.case_events(case.case_id) if event.event_type == 'case_reported']) == 1


@pytest.mark.asyncio
async def test_cancel_before_guarded_completion_does_not_commit_report_clock(monkeypatch):
    for flag in ('NOC_CASE_ATTENTION_ENABLED', 'NOC_CASESERVICE_REACTIVE_REPORT', 'NOC_CASE_OUTBOX_ENABLED'):
        monkeypatch.setenv(flag, '1')
    monkeypatch.setenv('LOG_LEVEL_DISCORD', 'INFO')
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity='HIGH', identity={'source': 'icinga2'})
    await store.upsert_case(case)
    initial = await service.request_report(case)
    notify = AsyncMock(return_value=True)
    original_complete = store.complete_attention
    completion = None

    async def cancel_at_commit(*args, **kwargs):
        nonlocal completion
        completion = (args, kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(store, 'complete_attention', cancel_at_commit)
    processor = OutboxProcessor(store, {'report': build_report_handler(service, notifier=notify)})
    with pytest.raises(asyncio.CancelledError):
        await processor.process_intent(initial)
    notify.assert_awaited_once()
    assert not (await store.get_case(case.case_id)).last_reported_at
    assert await store.get_attention(case.case_id) is None
    assert not [e for e in await store.case_events(case.case_id) if e.event_type == 'case_reported']
    args, kwargs = completion
    # A guarded successful completion advances all projections together; its
    # replay cannot create another event or update either timestamp twice.
    assert await original_complete(*args, **kwargs) is not None
    assert await original_complete(*args, **kwargs) is None
    assert (await store.get_case(case.case_id)).last_reported_at
    assert (await store.get_attention(case.case_id)).sequence == 1
    assert len([e for e in await store.case_events(case.case_id) if e.event_type == 'case_reported']) == 1
