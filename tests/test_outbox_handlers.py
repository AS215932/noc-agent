import pytest

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxProcessor
from app.cases.handlers import build_default_outbox_handlers, build_report_handler


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
async def test_default_handlers_include_knowledge_candidate_only_when_configured(tmp_path):
    store = InMemoryCaseStore()
    service = CaseService(store)

    assert set(build_default_outbox_handlers(service)) == {"report"}
    assert set(build_default_outbox_handlers(service, knowledge_candidate_dir=tmp_path)) == {
        "report",
        "knowledge_candidate",
    }
