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
        pending = await self.store.list_outbox(status="pending")
        processed = succeeded = failed = skipped = 0
        for intent in pending[: max(0, limit)]:
            handler = self.handlers.get(intent.intent_type)
            if handler is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                continue
            processed += 1
            claimed = intent.model_copy(deep=True)
            claimed.status = "in_progress"
            claimed.attempts += 1
            await self.store.update_outbox(claimed)
            try:
                result = await handler(claimed)
            except Exception as exc:
                failed += 1
                errored = claimed.model_copy(deep=True)
                errored.status = "failed"
                errored.error = f"{type(exc).__name__}: {exc}"
                errored.next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=self.retry_backoff_s)).isoformat()
                await self.store.update_outbox(errored)
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="failed")
                continue
            succeeded += 1
            completed = claimed.model_copy(deep=True)
            completed.status = "succeeded"
            completed.completed_at = utc_now()
            completed.error = ""
            if result is not None:
                completed.external_id = result.external_id
                completed.external_url = result.external_url
                if result.payload_updates:
                    completed.payload.update(result.payload_updates)
            await self.store.update_outbox(completed)
            record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="succeeded")
        return OutboxProcessReport(processed=processed, succeeded=succeeded, failed=failed, skipped=skipped)
