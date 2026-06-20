"""Outbox handlers for review-gated hyrule-knowledge candidates."""

from __future__ import annotations

from pathlib import Path

from app.cases.models import AtomicCaseProjection, MetaCaseProjection, OutboxIntent
from app.cases.outbox import OutboxHandlerResult
from app.cases.store import CaseStore
from app.knowledge.promotion import build_lesson_candidate_event, write_lesson_candidate_event


def build_knowledge_candidate_handler(store: CaseStore, output_dir: str | Path):
    destination = Path(output_dir)

    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        target_id = intent.case_id or intent.meta_case_id
        if not target_id:
            raise ValueError("knowledge candidate intent requires case_id or meta_case_id")
        case = await store.get_case(target_id)
        if not isinstance(case, AtomicCaseProjection | MetaCaseProjection):
            raise KeyError(f"case not found for knowledge candidate: {target_id}")
        events = await store.case_events(target_id)
        event = build_lesson_candidate_event(
            case,
            case_events=events,
            lessons=[str(item) for item in intent.payload.get("lessons", [])],
            target=str(intent.payload.get("target") or "okf/observed/noc-agent"),
        )
        path = write_lesson_candidate_event(destination, event)
        return OutboxHandlerResult(external_id=event["id"], external_url=str(path), payload_updates={"learning_event_id": event["id"]})

    return handle
