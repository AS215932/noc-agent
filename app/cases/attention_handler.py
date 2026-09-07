"""Delivery-time attention eligibility and bounded per-case ownership.

The transport must use the request's durable identity when retrying an external
send. A database lease cannot close an external-send/commit crash window by
itself. New-incident delivery must share the initial facts card identity.
"""
import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable

from app.cases.attention import AttentionDelivery, AttentionRequest, attention_due
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.outbox import OutboxHandler, OutboxHandlerResult
from app.cases.store import CaseStore


AttentionSender = Callable[[AtomicCaseProjection, AttentionRequest, OutboxIntent], Awaitable[bool | None]]


def build_attention_handler(store: CaseStore, *, sender: AttentionSender) -> OutboxHandler:
    """Keep ownership until the processor atomically commits successful delivery."""
    async def eligible(intent: OutboxIntent, request: AttentionRequest) -> AtomicCaseProjection | None:
        if not intent.case_id:
            raise ValueError("attention requires a case")
        case = await store.get_case(intent.case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError("attention case unavailable")
        if case.identity.get("source") not in {"alertmanager", "icinga2"}:
            return None
        previous = await store.get_attention(case.case_id)
        current = attention_due(case, previous, now=datetime.now(timezone.utc))
        return case if current == request else None

    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        request = AttentionRequest.model_validate(intent.payload.get("attention_request"))
        if request.idempotency_key != intent.idempotency_key:
            raise ValueError("attention identity mismatch")
        if await eligible(intent, request) is None:
            return OutboxHandlerResult(payload_updates={"notification_suppressed": "attention_no_longer_due"})
        lease = await store.claim_attention(intent, expected_sequence=request.expected_sequence, lease_seconds=120)
        if lease is None:
            raise RuntimeError("attention delivery already claimed or superseded")
        success = False
        try:
            # This deadline includes the fresh database reads, transport locking,
            # and send. It leaves time to commit within the 120-second lease.
            async with asyncio.timeout(60):
                case = await eligible(intent, request)
                if case is None:
                    return OutboxHandlerResult(payload_updates={"notification_suppressed": "attention_no_longer_due"})
                sent = await sender(case, request, intent)
                if sent is None:
                    return OutboxHandlerResult(payload_updates={"notification_suppressed": "attention_verbosity"})
                if sent is not True:
                    raise RuntimeError("attention notification was not delivered")
                delivery = AttentionDelivery(
                    case_id=case.case_id, generation=request.generation, phase=request.phase,
                    severity=request.severity, sequence=request.expected_sequence + 1,
                    delivered_at=datetime.now(timezone.utc),
                )
            success = True
            return OutboxHandlerResult(attention_delivery=delivery, attention_lease_token=lease,
                payload_updates={"attention_delivered": request.kind})
        finally:
            if not success and intent.case_id:
                # A failed release cannot strand ownership indefinitely: the
                # durable lease expires, including after process cancellation.
                try:
                    async with asyncio.timeout(5):
                        await store.release_attention(intent.case_id, lease)
                except Exception:
                    pass

    return handle
