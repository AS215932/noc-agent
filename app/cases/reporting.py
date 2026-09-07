"""Durably retry investigation card updates through the existing case outbox."""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from app import log
from app.case_cards import CardDeliveryOutcome
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.outbox import OutboxProcessor
from app.discord import Verbosity, get_verbosity, send_case_notification
from app.model_metrics import record_sanitized_discord_failure


def reactive_reporting_owns_cards() -> bool:
    return all(os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"} for name in (
        "NOC_CASESERVICE_REACTIVE_REPORT", "NOC_CASE_OUTBOX_ENABLED",
    ))


async def send_investigation_card(*, runtime: Any, case_id: str, notifier=send_case_notification, safe_category: str | None = None, **card) -> bool:
    card.setdefault("level", Verbosity.INFO)
    card.setdefault("fields", [])
    card.setdefault("color", 0x3498DB)
    if card["level"] < get_verbosity():
        return False
    revision = time.time()
    intent = None
    if runtime is not None:
        try:
            case = await runtime.store.get_case(case_id)
            if isinstance(case, AtomicCaseProjection):
                intent = await runtime.store.enqueue_outbox(OutboxIntent(
                    case_id=case_id,
                    intent_type="report",
                    idempotency_key=f"card-update:{case_id}:{revision}",
                    payload={"card_update": card, "card_revision": revision, "safe_category": safe_category},
                ))
        except Exception as exc:
            log.warn("investigation_card_enqueue_failed", error_type=type(exc).__name__, case_id=case_id)
    if intent is not None:
        from app.cases.handlers import build_report_handler

        # Immediate delivery and scheduled retry share one atomic claim and
        # completion path, including delivery metrics. A concurrent worker wins
        # the same claim rather than sending or counting this intent twice.
        processor = OutboxProcessor(runtime.store, {
            "report": build_report_handler(runtime.service, notifier=notifier),
        })
        try:
            result = await processor.process_intent(intent)
            return result.succeeded == 1
        except Exception as exc:
            log.warn("investigation_card_processing_failed", error_type=type(exc).__name__, case_id=case_id)
            return False
    # Standalone/dev use has no outbox; give transient failures a bounded retry.
    for attempt in range(3):
        delivered = await notifier(case_id=case_id, revision=revision, **card)
        if delivered is not False:
            if safe_category and delivered is not CardDeliveryOutcome.SUPERSEDED:
                record_sanitized_discord_failure(safe_category)
            return True
        if attempt < 2:
            await asyncio.sleep(1)
    return False
