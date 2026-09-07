import asyncio
from types import SimpleNamespace

import pytest

from app.cases.models import ObservationRecord
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore
from app.cases.graph_memory import CaseServiceGraphMemory
from app.cases.correlation import CorrelationService
from app.cases.models import AtomicCaseProjection, MetaCaseProjection


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


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["put_summary", "update_case"])
async def test_graph_write_refreshes_after_concurrent_ack(monkeypatch, method):
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    graph = CaseServiceGraphMemory(store)
    read, resume = asyncio.Event(), asyncio.Event()
    original_get = store.get_case
    graph_task = None
    reads = 0

    async def snapshot_get(case_id):
        nonlocal reads
        snapshot = await original_get(case_id)
        if asyncio.current_task() is graph_task:
            reads += 1
            # Identifier resolution reads once; the second read is the actual
            # projection that an unguarded graph writer would later overwrite.
            if reads == 2:
                read.set()
                await resume.wait()
        return snapshot

    monkeypatch.setattr(store, "get_case", snapshot_get)
    graph_task = asyncio.create_task(getattr(graph, method)(case.case_id, {"title": "fresh", "diagnostic_summary": "fresh"}))
    try:
        await asyncio.wait_for(read.wait(), timeout=1)
        await service.ack(case.case_id, operator="oncall")
        resume.set()
        await asyncio.wait_for(graph_task, timeout=1)
    finally:
        resume.set()
        if not graph_task.done():
            graph_task.cancel()
            await asyncio.gather(graph_task, return_exceptions=True)
    current = await store.get_case(case.case_id)
    assert current.acknowledged_by == "oncall"
    assert current.summary == "fresh"


@pytest.mark.asyncio
async def test_concurrent_child_attachments_preserve_parent_and_ack(monkeypatch):
    store = InMemoryCaseStore()
    service = CaseService(store)
    correlation = CorrelationService(store)
    meta = MetaCaseProjection()
    children = [AtomicCaseProjection(severity="HIGH") for _ in range(2)]
    for case in [meta, *children]:
        await store.upsert_case(case)
    original_upsert = store.upsert_case

    async def yield_before_write(case):
        await asyncio.sleep(0)
        return await original_upsert(case)

    monkeypatch.setattr(store, "upsert_case", yield_before_write)
    await asyncio.wait_for(asyncio.gather(
        *(correlation.attach_child(meta.case_id, child.case_id, reason="shared incident", confidence=1) for child in children),
        service.ack(children[0].case_id, operator="oncall"),
    ), timeout=2)
    parent = await store.get_case(meta.case_id)
    assert set(parent.child_case_ids) == {case.case_id for case in children}
    for child in children:
        current = await store.get_case(child.case_id)
        assert current.meta_case_id == meta.case_id
        assert current.covered_by_meta_case
    assert (await store.get_case(children[0].case_id)).acknowledged_by == "oncall"


@pytest.mark.asyncio
async def test_control_state_write_serializes_with_ack(monkeypatch):
    import app.main as main
    store = InMemoryCaseStore()
    service = CaseService(store)
    case = AtomicCaseProjection(severity="HIGH")
    await store.upsert_case(case)
    monkeypatch.setattr(main, "case_service_runtime", SimpleNamespace(store=store, service=service))
    read, resume = asyncio.Event(), asyncio.Event()
    original_get = store.get_case
    control = None

    async def paused_get(case_id):
        snapshot = await original_get(case_id)
        if asyncio.current_task() is control and not read.is_set():
            read.set()
            await resume.wait()
        return snapshot

    monkeypatch.setattr(store, "get_case", paused_get)
    request = SimpleNamespace(decision="approved", operator="oncall", model_dump=lambda: {"decision": "approved"})
    control = asyncio.create_task(main._apply_case_service_primary_decision_state(case, request, {"status": "waiting_approval"}))
    ack = None
    try:
        await asyncio.wait_for(read.wait(), timeout=1)
        ack = asyncio.create_task(service.ack(case.case_id, operator="oncall"))
        await asyncio.sleep(0)
        assert not ack.done()
        resume.set()
        await asyncio.wait_for(asyncio.gather(control, ack), timeout=1)
    finally:
        resume.set()
        tasks = [task for task in (control, ack) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    current = await store.get_case(case.case_id)
    assert current.status == "waiting_approval" and current.acknowledged_by == "oncall"
