"""Side-effect outbox worker primitives.

State transitions enqueue outbox intents. Workers execute the external side
effect later and update the row, making crashes/retries idempotent by
`idempotency_key`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from app.cases.models import OutboxIntent, utc_now
from app.cases.store import CaseStore
from app.model_metrics import record_case_service_outbox_processed


@dataclass(frozen=True, slots=True)
class OutboxHandlerResult:
    external_id: str = ""
    external_url: str = ""
    payload_updates: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxProcessReport:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0


OutboxHandler = Callable[[OutboxIntent], Awaitable[OutboxHandlerResult | None]]


class OutboxProcessor:
    def __init__(self, store: CaseStore, handlers: dict[str, OutboxHandler], *, retry_backoff_s: int = 60) -> None:
        self.store = store
        self.handlers = handlers
        self.retry_backoff_s = retry_backoff_s

    async def process_pending(self, *, limit: int = 10) -> OutboxProcessReport:
        candidates = await self._due_intents()
        processed = succeeded = failed = skipped = 0
        for intent in candidates[: max(0, limit)]:
            handler = self.handlers.get(intent.intent_type)
            if handler is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                continue
            claimed = intent.model_copy(deep=True)
            claimed.status = "in_progress"
            claimed.attempts += 1
            stored_claim = await self.store.update_outbox_if_status(claimed, expected_status=intent.status)
            if stored_claim is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                continue
            processed += 1
            claimed = stored_claim
            try:
                result = await handler(claimed)
            except Exception as exc:
                errored = claimed.model_copy(deep=True)
                errored.status = "failed"
                errored.error = f"{type(exc).__name__}: {exc}"
                errored.next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=self.retry_backoff_s)).isoformat()
                stored_error = await self.store.update_outbox_if_status(errored, expected_status="in_progress")
                if stored_error is None:
                    skipped += 1
                    record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                else:
                    failed += 1
                    record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="failed")
                continue
            completed = claimed.model_copy(deep=True)
            completed.status = "succeeded"
            completed.completed_at = utc_now()
            completed.error = ""
            if result is not None:
                completed.external_id = result.external_id
                completed.external_url = result.external_url
                if result.payload_updates:
                    completed.payload.update(result.payload_updates)
            stored_completion = await self.store.update_outbox_if_status(completed, expected_status="in_progress")
            if stored_completion is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
            else:
                succeeded += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="succeeded")
        return OutboxProcessReport(processed=processed, succeeded=succeeded, failed=failed, skipped=skipped)

    async def _due_intents(self) -> list[OutboxIntent]:
        pending = await self.store.list_outbox(status="pending")
        failed = await self.store.list_outbox(status="failed")
        now = datetime.now(timezone.utc)
        due_failed = [intent for intent in failed if _is_due(intent, now=now)]
        return [*pending, *due_failed]


def _is_due(intent: OutboxIntent, *, now: datetime) -> bool:
    if not intent.next_attempt_at:
        return True
    try:
        due_at = datetime.fromisoformat(str(intent.next_attempt_at).replace("Z", "+00:00"))
    except ValueError:
        return True
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=timezone.utc)
    return due_at <= now
