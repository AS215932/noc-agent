"""Default side-effect handlers for case-service outbox intents."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.outbox import OutboxHandler, OutboxHandlerResult
from app.cases.service import CaseService
from app.discord import Verbosity, send_case_notification
from app.knowledge.outbox import build_knowledge_candidate_handler


def build_default_outbox_handlers(
    case_service: CaseService,
    *,
    knowledge_candidate_dir: str | Path | None = None,
    control_public_url: str = "",
) -> dict[str, OutboxHandler]:
    """Build handlers that are safe to enable behind the outbox worker flag.

    `report` sends an operator-facing Discord case notification and stamps the
    case as reported. `knowledge_candidate` writes a review-gated learning event
    only when an output directory is configured. Handoff remains intentionally
    separate until the case model owns enough issue-body context.
    """

    handlers: dict[str, OutboxHandler] = {
        "report": build_report_handler(case_service, control_public_url=control_public_url),
    }
    if knowledge_candidate_dir:
        handlers["knowledge_candidate"] = build_knowledge_candidate_handler(case_service.store, knowledge_candidate_dir)
    return handlers


def build_report_handler(
    case_service: CaseService,
    *,
    notifier=send_case_notification,
    control_public_url: str = "",
) -> OutboxHandler:
    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        if not intent.case_id:
            raise ValueError("report intent requires case_id")
        case = await case_service.store.get_case(intent.case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found for report intent: {intent.case_id}")
        state_signature = intent.state_signature or case_service.report_state_signature(case)
        title, description, fields = _render_case_report(case, intent)
        await notifier(
            case_id=case.case_id,
            title=title,
            description=description,
            color=_severity_color(case.severity),
            fields=fields,
            level=Verbosity.WARNING if case.severity in {"HIGH", "MEDIUM"} else Verbosity.INFO,
        )
        reasserted = bool(case.last_reported_signature and case.last_reported_signature == state_signature)
        await case_service.mark_reported(case.case_id, state_signature=state_signature, reasserted=reasserted)
        return OutboxHandlerResult(
            external_id=case.case_number or case.case_id,
            external_url=_case_url(case, control_public_url),
            payload_updates={"state_signature": state_signature, "case_number": case.case_number},
        )

    return handle


def _render_case_report(case: AtomicCaseProjection, intent: OutboxIntent) -> tuple[str, str, list[dict[str, Any]]]:
    override_title = str(intent.payload.get("title") or "").strip()
    override_description = str(intent.payload.get("description") or "").strip()
    title = override_title or f"NOC case {case.case_number or case.case_id}: {case.title or case.detector or case.rule_id}"
    description = override_description or case.summary or "Case state changed."
    fields = [
        {"name": "Status", "value": _clip(case.status), "inline": True},
        {"name": "Severity", "value": _clip(case.severity), "inline": True},
        {"name": "Resource", "value": _clip(case.resource_id or "unknown"), "inline": True},
    ]
    if case.last_diagnosis:
        fields.append({"name": "Last diagnosis", "value": _clip(_diagnosis_summary(case.last_diagnosis)), "inline": False})
    if case.recommendations:
        fields.append({"name": "Recommendations", "value": _clip("\n".join(f"- {item}" for item in case.recommendations)), "inline": False})
    if case.issue_url:
        fields.append({"name": "Handoff", "value": _clip(case.issue_url), "inline": False})
    if intent.payload.get("fields"):
        for item in intent.payload.get("fields") or []:
            if isinstance(item, dict) and item.get("name") and item.get("value"):
                fields.append({"name": _clip(str(item["name"]), limit=256), "value": _clip(str(item["value"])), "inline": bool(item.get("inline", False))})
    return _clip(title, limit=256), _clip(description, limit=4096), fields[:10]


def _diagnosis_summary(value: dict[str, Any]) -> str:
    for key in ("summary", "incident_summary", "root_cause", "status", "incident_id"):
        if value.get(key):
            return str(value[key])
    return ", ".join(f"{key}={val}" for key, val in list(value.items())[:5]) or "recorded"


def _case_url(case: AtomicCaseProjection, public_url: str) -> str:
    if not public_url:
        return ""
    identifier = case.case_number or case.case_id
    return f"{public_url.rstrip('/')}/control/cases/{identifier}"


def _severity_color(severity: str) -> int:
    return {"HIGH": 0xE74C3C, "MEDIUM": 0xF39C12, "LOW": 0x2ECC71}.get(str(severity).upper(), 0x3498DB)


def _clip(value: str, *, limit: int = 1024) -> str:
    text = str(value or "—")
    return text if len(text) <= limit else text[: limit - 1] + "…"
