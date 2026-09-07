import asyncio

import pytest

from app.cases.models import ObservationRecord
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["ack", "suppress", "reported"])
async def test_observation_refreshes_snapshot_after_concurrent_control_write(monkeypatch, transition):
    store = InMemoryCaseStore()
    service = CaseService(store)
    observation = ObservationRecord(source="icinga2", detector="Disk", resource="rtr", severity="HIGH", status="firing")
    created = await service.observe(observation)
    read = asyncio.Event()
    resume = asyncio.Event()
    original_get = store.get_case
    observer = None

    async def stale_get(case_id):
        snapshot = await original_get(case_id)
        if asyncio.current_task() is observer and not read.is_set():
            read.set()
            await resume.wait()
        return snapshot

    monkeypatch.setattr(store, "get_case", stale_get)
    observer = asyncio.create_task(service.observe(observation.model_copy(update={"observation_id": "race", "signal_signature": "fresh"})))
    try:
        await asyncio.wait_for(read.wait(), timeout=1)
        if transition == "ack":
            await service.ack(case_id=created.case.case_id, operator="oncall")
        elif transition == "suppress":
            await service.suppress(case_id=created.case.case_id, reason="maintenance", source="operator", operator="oncall")
        else:
            await service.mark_reported(case_id=created.case.case_id, state_signature="delivered-facts")
        resume.set()
        updated = (await asyncio.wait_for(observer, timeout=1)).case
    finally:
        resume.set()
        if not observer.done():
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
    assert updated.signal_signature == "fresh"
    if transition == "ack":
        assert updated.acknowledged_by == "oncall" and updated.acknowledged_at
    elif transition == "suppress":
        assert updated.suppression_reason == "maintenance" and updated.suppressed_until
    else:
        assert updated.last_reported_signature == "delivered-facts" and updated.last_reported_at
