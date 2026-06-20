import json

import pytest

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord, OutboxIntent, OutboxProcessor
from app.knowledge import build_knowledge_candidate_handler


@pytest.mark.asyncio
async def test_knowledge_candidate_outbox_handler_writes_review_gated_event(tmp_path):
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing"))
    assert created.case is not None
    await service.observe(ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="resolved"))
    intent = await store.enqueue_outbox(
        OutboxIntent(
            case_id=created.case.case_id,
            intent_type="knowledge_candidate",
            idempotency_key="knowledge_candidate:case_1",
            payload={"lessons": ["Disk fill resolved after cleanup; review threshold guidance."]},
        )
    )

    processor = OutboxProcessor(store, {"knowledge_candidate": build_knowledge_candidate_handler(store, tmp_path)})
    report = await processor.process_pending()

    assert report.succeeded == 1
    completed = (await store.list_outbox())[0]
    assert completed.outbox_id == intent.outbox_id
    assert completed.status == "succeeded"
    assert completed.external_url
    event = json.loads((tmp_path / f"{completed.external_id}.json").read_text(encoding="utf-8"))
    assert event["event_type"] == "lesson_candidate"
    assert event["status"] == "proposed"
    assert event["authority_tier"] == "A4"
    assert event["promotion"]["review_required"] is True
    assert event["source"]["case_id"] == created.case.case_id
