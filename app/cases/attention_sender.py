"""Same-channel attention using durable Discord message identities."""
from datetime import datetime
import time

from app.cases.attention import AttentionRequest
from app.cases.attention_handler import AttentionSender
from app.cases.lhp import sanitize_lhp_text
from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.discord import Verbosity, get_verbosity, send_case_notification


def attention_level(request: AttentionRequest) -> Verbosity:
    return {"HIGH": Verbosity.ERROR, "MEDIUM": Verbosity.WARNING}.get(request.severity, Verbosity.INFO)


def build_attention_sender(*, notifier=send_case_notification) -> AttentionSender:
    async def send(case: AtomicCaseProjection, request: AttentionRequest, intent: OutboxIntent) -> bool | None:
        level = attention_level(request)
        if level < get_verbosity():
            return None
        # Initial facts and initial attention share exactly one message. Later
        # lifecycle events get one message per durable request, so a retry after
        # a database failure edits its existing message rather than creating one.
        identity = case.case_id if request.kind == "new" else request.idempotency_key
        revision = time.time() if request.kind == "new" else datetime.fromisoformat(intent.created_at).timestamp()
        labels = {"new": "New incident", "recurrence": "Incident recurring", "escalation": "Incident escalated",
                  "recovery": "Incident recovered", "reminder": "Unacknowledged critical incident"}
        title = sanitize_lhp_text(f"{labels[request.kind]}: {case.case_number or case.case_id} — {case.title or case.detector}", limit=256)
        description = sanitize_lhp_text(case.summary or "Monitoring reported an incident state change.", limit=1600)
        fields = [
            {"name": "Case", "value": sanitize_lhp_text(case.case_number or case.case_id, limit=128), "inline": True},
            {"name": "Status", "value": sanitize_lhp_text(case.status, limit=64), "inline": True},
            {"name": "Severity", "value": request.severity, "inline": True},
            {"name": "Resource", "value": sanitize_lhp_text(case.resource_id or "unknown", limit=256), "inline": False},
        ]
        if case.recommendations:
            fields.append({"name": "Next checks", "value": sanitize_lhp_text("; ".join(case.recommendations), limit=1000), "inline": False})
        result = await notifier(case_id=identity, title=title, description=description, fields=fields,
                                color=0x2ECC71 if request.phase == "recovered" else 0xE74C3C,
                                level=level, revision=revision)
        return result is True

    return send
