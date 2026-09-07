"""Side-effect outbox worker primitives.

State transitions enqueue outbox intents. Workers execute the external side
effect later and update the row, making crashes/retries idempotent by
`idempotency_key`.
"""

from __future__ import annotations

import asyncio

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from app.cases.models import OutboxIntent, utc_now
from app.cases.attention import AttentionDelivery
from app.cases.store import CaseStore
from app.model_metrics import record_case_service_outbox_processed, record_sanitized_discord_failure


@dataclass(frozen=True, slots=True)
class OutboxHandlerResult:
    external_id: str = ""
    external_url: str = ""
    payload_updates: dict[str, Any] = field(default_factory=dict)
    attention_delivery: AttentionDelivery | None = None
    attention_lease_token: str = ""


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
        return await self._process_candidates(candidates[: max(0, limit)])

    async def process_intent(self, intent: OutboxIntent) -> OutboxProcessReport:
        """Attempt a newly queued intent using the same atomic worker claim."""
        if intent.status != "pending":
            raise ValueError("immediate processing requires a pending intent")
        return await self._process_candidates([intent])

    async def _process_candidates(self, candidates: list[OutboxIntent]) -> OutboxProcessReport:
        processed = succeeded = failed = skipped = 0
        for intent in candidates:
            handler = self.handlers.get(intent.intent_type)
            if handler is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                continue
            claimed = intent.model_copy(deep=True)
            claimed.status = "in_progress"
            claimed.attempts += 1
            claimed.claim_token = uuid4().hex
            claimed.claim_expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
            stored_claim = await self.store.update_outbox_if_status(
                claimed, expected_status=intent.status, expected_claim_token=intent.claim_token,
            )
            if stored_claim is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
                continue
            processed += 1
            claimed = stored_claim
            try:
                if claimed.intent_type == "report":
                    async with asyncio.timeout(180):
                        result = await handler(claimed)
                else:
                    result = await handler(claimed)
            except Exception as exc:
                errored = claimed.model_copy(deep=True)
                errored.status = "failed"
                errored.error = f"{type(exc).__name__}: {exc}"
                errored.next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=self.retry_backoff_s)).isoformat()
                stored_error = await self.store.update_outbox_if_status(
                    errored, expected_status="in_progress", expected_claim_token=claimed.claim_token,
                )
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
            if result is not None and result.attention_delivery is not None:
                stored_completion = await self.store.complete_attention(
                    completed, result.attention_delivery,
                    expected_sequence=result.attention_delivery.sequence - 1,
                    expected_claim_token=claimed.claim_token,
                    lease_token=result.attention_lease_token,
                )
            else:
                stored_completion = await self.store.update_outbox_if_status(
                    completed, expected_status="in_progress", expected_claim_token=claimed.claim_token,
                )
            if stored_completion is None:
                skipped += 1
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="skipped")
            else:
                succeeded += 1
                if (claimed.intent_type == "report" and result is not None
                        and result.payload_updates.get("card_update_delivered")
                        and claimed.payload.get("safe_category")):
                    record_sanitized_discord_failure(str(claimed.payload["safe_category"]))
                record_case_service_outbox_processed(intent_type=intent.intent_type, outcome="succeeded")
        return OutboxProcessReport(processed=processed, succeeded=succeeded, failed=failed, skipped=skipped)

    async def _due_intents(self) -> list[OutboxIntent]:
        # Only report delivery is safe to replay automatically. Other side
        # effects retain their existing recovery policy.
        if "report" in self.handlers:
            now = datetime.now(timezone.utc)
            for intent in await self.store.list_outbox(status="in_progress"):
                if intent.intent_type != "report" or not _claim_expired(intent, now=now):
                    continue
                retry = intent.model_copy(deep=True)
                retry.status = "pending"
                retry.next_attempt_at = utc_now()
                retry.error = "Notification claim expired; retrying"
                await self.store.update_outbox_if_status(
                    retry, expected_status="in_progress", expected_claim_token=intent.claim_token,
                )
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


def _claim_expired(intent: OutboxIntent, *, now: datetime) -> bool:
    # Legacy claims have no lease; allow a full lease interval from creation.
    try:
        value = intent.claim_expires_at or intent.created_at
        expires = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if not intent.claim_expires_at:
            expires += timedelta(minutes=10)
        return expires <= now
    except (ValueError, TypeError):
        return False
