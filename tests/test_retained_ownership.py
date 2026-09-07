from unittest.mock import AsyncMock

from fastapi import Response
import pytest

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxProcessor
from app.cases.handlers import build_report_handler
from app.cases.report_spool import replay_reports
from app.cases.reporting import send_investigation_card
from app.discord import Verbosity


@pytest.mark.asyncio
async def test_retained_owned_terminal_keeps_facts_order_and_sanitization_after_flag_change(monkeypatch, tmp_path):
    monkeypatch.setenv("NOC_REPORT_SPOOL_DIR", str(tmp_path))
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "1")
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "1")
    monkeypatch.setenv("LOG_LEVEL_DISCORD", "INFO")
    store = InMemoryCaseStore()
    service = CaseService(store)
    observed = await service.observe(ObservationRecord(
        source="icinga2", detector="Disk", resource="rtr", status="firing", severity="HIGH",
        annotations={"summary": "Disk [probe] <value> \n `sample`"},
    ))
    case = observed.case
    case.summary = "Disk [probe] <value> \n `sample`"
    await store.upsert_case(case)
    notifier = AsyncMock(return_value=False)
    processor = OutboxProcessor(store, {"report": build_report_handler(service, notifier=notifier)}, retry_backoff_s=0)
    initial = await service.request_report(case)
    assert (await processor.process_intent(initial)).failed == 1
    # Store-unavailable creation must retain ownership in the file itself.
    await send_investigation_card(runtime=None, case_id=case.case_id, notifier=notifier,
        title="Investigation result", description="Diagnosis retained", level=Verbosity.ERROR)
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_REPORT", "0")
    assert await replay_reports(store) == 1
    pending = await store.list_outbox(status="pending")
    assert pending[0].payload["reactive_owned_card"] is True
    notifier.reset_mock()
    notifier.return_value = True
    # Pending terminal precedes failed facts in the worker's queue; it must wait.
    await processor.process_pending()
    assert notifier.await_count == 1
    assert "Diagnosis retained" not in notifier.call_args.kwargs["description"]
    await processor.process_pending()
    assert notifier.await_count == 2
    description = notifier.call_args.kwargs["description"]
    assert "Disk" in description and "Diagnosis retained" in description
    assert all(char not in description for char in "[]<>`")
    assert "Disk probe value sample" in description


@pytest.mark.asyncio
@pytest.mark.parametrize("condition,reason", [
    ("overdue", "retained_reports_overdue"),
    ("invalid", "retained_reports_invalid"),
    ("limited", "report_spool_scan_limited"),
    ("unavailable", "report_spool_unavailable"),
    ("fresh", "worker_runtime_unavailable"),
])
async def test_disabled_runtime_reports_retained_delivery_failures(monkeypatch, condition, reason):
    import app.main as main
    monkeypatch.setattr(main, "case_service_runtime", None)
    monkeypatch.setenv("NOC_CASE_OUTBOX_ENABLED", "0")
    stats = {"scan_limited": condition == "limited", "invalid": int(condition == "invalid"),
             "pending": int(condition in {"overdue", "fresh"}),
             "oldest_retained_at": 100 if condition == "overdue" else None}
    reader = AsyncMock(return_value=stats)
    if condition == "unavailable":
        reader.side_effect = OSError("local read failure")
    monkeypatch.setattr("app.cases.report_spool.spool_stats", reader)
    response = Response()
    body = await main.health_cases(response)
    assert response.status_code == 503
    assert body["status"] == "degraded"
    assert reason in body["delivery"]["reasons"]
