"""Default side-effect handlers for case-service outbox intents."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.cases.models import AtomicCaseProjection, OutboxIntent
from app.cases.outbox import OutboxHandler, OutboxHandlerResult
from app.cases.service import CaseService
from app.discord import Verbosity, send_case_notification
from app.knowledge.outbox import build_knowledge_candidate_handler
from app.proactive.handoff import GitHubHandoff, handoff_from_env


def build_default_outbox_handlers(
    case_service: CaseService,
    *,
    knowledge_candidate_dir: str | Path | None = None,
    control_public_url: str = "",
    handoff_repo: str = "",
    handoff_client: GitHubHandoff | None = None,
) -> dict[str, OutboxHandler]:
    """Build handlers that are safe to enable behind the outbox worker flag.

    `report` sends an operator-facing Discord case notification and stamps the
    case as reported. `knowledge_candidate` writes a review-gated learning event
    only when an output directory is configured. `handoff` opens or refreshes a
    loop:candidate GitHub issue only when a handoff repo/client is configured.
    """

    handlers: dict[str, OutboxHandler] = {
        "report": build_report_handler(case_service, control_public_url=control_public_url),
    }
    if knowledge_candidate_dir:
        handlers["knowledge_candidate"] = build_knowledge_candidate_handler(case_service.store, knowledge_candidate_dir)
    if handoff_client is None and handoff_repo:
        handoff_client = handoff_from_env(handoff_repo)
    if handoff_client is not None:
        handlers["handoff"] = build_handoff_handler(
            case_service,
            handoff_client=handoff_client,
            control_public_url=control_public_url,
        )
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


def build_handoff_handler(
    case_service: CaseService,
    *,
    handoff_client: GitHubHandoff,
    control_public_url: str = "",
) -> OutboxHandler:
    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        if not intent.case_id:
            raise ValueError("handoff intent requires case_id")
        case = await case_service.store.get_case(intent.case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found for handoff intent: {intent.case_id}")
        if case.issue_url:
            return OutboxHandlerResult(external_id=case.issue_id, external_url=case.issue_url)
        marker = f"noc-case-id:{case.case_id}"
        url = await handoff_client.ensure_candidate_issue_from_body(
            marker=marker,
            title=_handoff_issue_title(case, intent),
            body=_handoff_issue_body(case, intent, marker=marker, control_public_url=control_public_url),
            refresh_comment=f"Case still requires handoff as of {_clip(case.updated_at or case.opened_at, limit=64)} ({case.case_number or case.case_id}).",
            log_prefix="case_handoff",
        )
        if not url:
            raise RuntimeError("handoff client did not return an issue URL")
        issue_id = _issue_id_from_url(url)
        await case_service.record_handoff_result(case.case_id, issue_url=url, issue_id=issue_id)
        return OutboxHandlerResult(
            external_id=issue_id,
            external_url=url,
            payload_updates={"issue_url": url, "issue_id": issue_id, "case_number": case.case_number},
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


def _handoff_issue_title(case: AtomicCaseProjection, intent: OutboxIntent) -> str:
    override = str(intent.payload.get("title") or "").strip()
    if override:
        return override[:240]
    label = case.case_number or case.case_id
    subject = case.title or case.detector or case.rule_id or case.resource_id or "case handoff"
    return f"[noc] {label}: {subject}"[:240]


def _handoff_issue_body(
    case: AtomicCaseProjection,
    intent: OutboxIntent,
    *,
    marker: str,
    control_public_url: str = "",
) -> str:
    lines = [
        f"_Filed by the AS215932 NOC case-service outbox ({case.origin or 'case'})._",
        "",
        f"## Case\n{case.summary or case.title or 'Case requires operator handoff.'}",
        "",
        "## Current state",
        f"- case: `{case.case_number or case.case_id}`",
        f"- status: `{case.status}`  ·  severity: `{case.severity}`",
        f"- resource: `{case.resource_id or 'unknown'}`",
        f"- detector: `{case.detector or case.rule_id or 'unknown'}`",
        f"- signal signature: `{case.signal_signature or 'unknown'}`",
    ]
    case_url = _case_url(case, control_public_url)
    if case_url:
        lines.append(f"- NOC case: {case_url}")
    if case.last_diagnosis:
        lines.extend(["", "## Last diagnosis", _diagnosis_summary(case.last_diagnosis)])
    if case.recommendations:
        lines.append("\n## Recommendations")
        lines.extend(f"- {item}" for item in case.recommendations)
    if case.knowledge_citations:
        lines.append("\n## Hyrule knowledge citations")
        for citation in case.knowledge_citations[:8]:
            doc = citation.get("doc_path") or citation.get("doc_id") or "unknown"
            section = citation.get("section") or ""
            revision = citation.get("repo_revision") or citation.get("export_version") or ""
            lines.append(f"- `{doc}` {section} {revision}".strip())
    if intent.payload.get("body"):
        lines.extend(["", "## Additional context", str(intent.payload["body"])])
    lines.append(
        "\n> Case-service handoff candidate. Promote to `loop:approved` to let the engineering-loop draft a PR; "
        "merge stays human-gated."
    )
    lines.append(f"\n<!-- {marker} -->")
    return "\n".join(lines)


def _issue_id_from_url(url: str) -> str:
    stripped = str(url or "").rstrip("/")
    return stripped.rsplit("/", 1)[-1] if stripped else ""


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
