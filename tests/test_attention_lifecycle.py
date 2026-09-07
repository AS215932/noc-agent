from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.cases.attention_handler import build_attention_handler
from app.cases.attention_scheduler import enqueue_attention
from app.cases.models import ObservationRecord
from app.cases.outbox import OutboxProcessor
from app.cases.service import CaseService
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_human_ack_survives_quiet_updates_but_not_escalation_or_recurrence():
    store = InMemoryCaseStore()
    service = CaseService(store)
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})

    async def observe(severity, status="firing", signal=""):
        result = await service.observe(ObservationRecord(source="icinga2", detector="Disk", resource="rtr",
            severity=severity, status=status, source_health="healthy", signal_signature=signal))
        await enqueue_attention(store, result.case)
        await processor.process_pending()
        return result.case

    case = await observe("LOW")
    await service.ack(case.case_id, operator="oncall")
    quiet = await observe("LOW", signal="fresh telemetry")
    assert quiet.acknowledged_by == "oncall"
    assert sender.await_count == 1
    medium = await observe("MEDIUM")
    assert not medium.acknowledged_by
    assert medium.report_generation == case.report_generation + 1
    await service.ack(case.case_id, operator="oncall")
    critical = await observe("HIGH")
    assert not critical.acknowledged_by
    await service.ack(case.case_id, operator="oncall")
    recovered = await observe("HIGH", status="clean")
    assert recovered.status == "resolved"
    reopened = await observe("HIGH")
    assert not reopened.acknowledged_by and reopened.status == "investigating"
    assert [call.args[1].kind for call in sender.await_args_list] == [
        "new", "escalation", "escalation", "recovery", "recurrence",
    ]


@pytest.mark.asyncio
async def test_automatic_icinga_ownership_does_not_acknowledge_case_attention(monkeypatch):
    import app.main as main
    monkeypatch.setenv("NOC_AUTO_ACK_ON_INVESTIGATION", "1")
    monkeypatch.delenv("HYRULE_MCP_ACTION_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("NOC_APPROVAL_SIGNING_SECRET", raising=False)
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(source="icinga2", detector="Disk", resource="rtr", severity="HIGH", status="firing"))
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    await enqueue_attention(store, observed.case)
    await processor.process_pending()
    previous = await store.get_attention(observed.case.case_id)
    store._attention[observed.case.case_id] = previous.model_copy(update={
        "delivered_at": datetime.now(timezone.utc) - timedelta(hours=7),
    })
    mcp = SimpleNamespace(call_tool=AsyncMock(return_value={"ok": True}))
    await main._take_ownership_ack({"source": "icinga2", "groupLabels": {"host": "rtr", "service": "disk"}},
                                   {"incident_id": observed.case.case_id}, mcp)
    mcp.call_tool.assert_awaited_once()
    assert mcp.call_tool.call_args.args[2]["notify"] is False
    current = await store.get_case(observed.case.case_id)
    assert not current.acknowledged_at and not current.acknowledged_by
    reminder = await enqueue_attention(store, current)
    assert reminder.payload["attention_request"]["kind"] == "reminder"


@pytest.mark.asyncio
@pytest.mark.parametrize("covered", ["MEDIUM", "HIGH"])
async def test_acknowledgement_covers_lower_severity_rebounds(covered):
    store = InMemoryCaseStore()
    service = CaseService(store)

    async def observe(severity):
        return (await service.observe(ObservationRecord(source="icinga2", detector="Disk", resource="rtr",
            severity=severity, status="firing"))).case

    case = await observe(covered)
    acknowledged = await service.ack(case.case_id, operator="oncall")
    for severity in ("LOW", "MEDIUM", covered):
        rebound = await observe(severity)
        assert rebound.acknowledged_at == acknowledged.acknowledged_at
        assert rebound.acknowledged_by == "oncall"
        assert rebound.report_generation == acknowledged.report_generation
    assert await store.acknowledgement_scope(case.case_id, acknowledged.acknowledged_at) == covered
    assert await store.acknowledgement_scope(case.case_id, "another-ack") is None
    if covered == "MEDIUM":
        escalated = await observe("HIGH")
        assert not escalated.acknowledged_by
        assert escalated.report_generation == acknowledged.report_generation + 1
    # A later explicit acknowledgement at LOW changes the coverage intentionally.
    await observe("LOW")
    acknowledged_again = await service.ack(case.case_id, operator="oncall")
    assert await store.acknowledgement_scope(case.case_id, acknowledged_again.acknowledged_at) == "LOW"
    assert not (await observe("MEDIUM")).acknowledged_by


@pytest.mark.asyncio
async def test_unacknowledged_rebounds_do_not_create_recurrence():
    store = InMemoryCaseStore()
    service = CaseService(store)
    sender = AsyncMock(return_value=True)
    processor = OutboxProcessor(store, {"report": build_attention_handler(store, sender=sender)})
    for severity in ("HIGH", "LOW", "MEDIUM", "HIGH"):
        case = (await service.observe(ObservationRecord(source="icinga2", detector="Disk", resource="rtr",
            severity=severity, status="firing"))).case
        await enqueue_attention(store, case)
        await processor.process_pending()
        assert case.report_generation == 0
    assert [call.args[1].kind for call in sender.await_args_list] == ["new"]
