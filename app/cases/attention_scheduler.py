"""Durable attention enqueueing, independent of quiet report signatures."""
from datetime import datetime, timezone

from app.cases.attention import attention_due
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.store import CaseStore


async def enqueue_attention(store: CaseStore, case: AtomicCaseProjection, *, now: datetime | None = None,
                            reminder_seconds: int = 21600) -> OutboxIntent | None:
    if case.identity.get("source") not in {"alertmanager", "icinga2"}:
        return None
    previous = await store.get_attention(case.case_id)
    request = attention_due(case, previous, now=now or datetime.now(timezone.utc), reminder_seconds=reminder_seconds)
    if request is None:
        return None
    intent = await store.enqueue_outbox(OutboxIntent(
        case_id=case.case_id, intent_type="report", idempotency_key=request.idempotency_key,
        payload={"attention_request": request.model_dump(mode="json")},
    ))
    suppressed = intent.payload.get("notification_suppressed")
    eligible_again = suppressed == "attention_no_longer_due"
    if suppressed == "attention_verbosity":
        from app.cases.attention_sender import attention_level
        from app.discord import get_verbosity
        eligible_again = attention_level(request) >= get_verbosity()
    if intent.status == "succeeded" and eligible_again:
        # Temporary acknowledgement/suppression must not consume an undelivered
        # generation forever when the same request later becomes eligible.
        retry = intent.model_copy(deep=True)
        retry.status = "pending"
        retry.completed_at = None
        retry.next_attempt_at = datetime.now(timezone.utc).isoformat()
        retry.payload.pop("notification_suppressed", None)
        updated = await store.update_outbox_if_status(retry, expected_status="succeeded",
                                                       expected_claim_token=intent.claim_token)
        return updated or await store.enqueue_outbox(intent)
    return intent


async def enqueue_attention_batch(store: CaseStore, *, after_case_id: str = "", limit: int = 100,
                                  now: datetime | None = None, reminder_seconds: int = 21600) -> tuple[str, int]:
    limit = max(1, min(limit, 1000))
    now = now or datetime.now(timezone.utc)
    cases = await store.list_attention_candidates(after_case_id=after_case_id, limit=limit, now=now,
                                                   reminder_seconds=reminder_seconds)
    count = 0
    for case in cases:
        if await enqueue_attention(store, case, now=now, reminder_seconds=reminder_seconds) is not None:
            count += 1
    return (cases[-1].case_id if len(cases) == limit else "", count)
