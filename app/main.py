import os
import asyncio
import fcntl
import hashlib
import hmac
import json
from html import escape
from fastapi import FastAPI, BackgroundTasks, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import uvicorn
from contextlib import asynccontextmanager, suppress

from app import log
from app.agent import noc_triage_agent
from app.discord import Verbosity, send_case_notification, notify_start, notify_finish
from app.discord import install_bot_notifier, install_case_notifier
from app.discord_bot import build_bot
from app.mail import check_mailbox_connection, process_mailbox_once
from app.model_config import load_model_config
from app.model_metrics import STATE as MODEL_STATE
from app.model_metrics import metrics_response, record_failure, record_success, start_run
from app.quota import check_model_provider_credits
from app.safe_errors import classify_exception, log_exception, safe_health_error
from app.mcp_runtime import MCPRuntime
from app.config import load_proactive_settings
from app.proactive.loop import ProactiveLoop
import app.graph_runtime as graph_runtime
from app.graph_runtime import inject_case_event, intake_alert, pending_summaries, record_operator_decision, run_investigation_graph, summary_for
from app.incident_memory import CaseIntakeResult, RECOVERY_COOLDOWN_SECONDS, case_display_title, case_event_from_alert
from app.noc_state import ApprovalDecision

mcp_runtime = MCPRuntime(owner="api")
mail_poller_task = None
mail_poller_lock_fd = None
discord_bot = None
discord_bot_task = None
proactive_loop = None
proactive_lock_fd = None
MAIL_POLLER_LOCK_PATH = os.getenv("MAIL_POLLER_LOCK_PATH", "/var/lib/noc-agent/mail-poller.lock")
PROACTIVE_LOCK_PATH = os.getenv("PROACTIVE_LOCK_PATH", "/var/lib/noc-agent/proactive-instance.lock")

REQUIRED_CONFIG = [
    "DISCORD_WEBHOOK_URL",
    "HYRULE_MCP_CMD",
    "HYRULE_MCP_URL",
    "XO_MCP_CMD",
    "XO_MCP_URL",
    "XO_TOKEN",
    "ICINGA_API_USER",
    "ICINGA_API_PASSWORD",
    "MAIL_IMAP_PASSWORD",
]

def _try_acquire_mail_poller_lock() -> int | None:
    os.makedirs(os.path.dirname(MAIL_POLLER_LOCK_PATH), exist_ok=True)
    lock_fd = os.open(MAIL_POLLER_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o640)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock_fd)
        return None
    os.ftruncate(lock_fd, 0)
    os.write(lock_fd, str(os.getpid()).encode())
    return lock_fd


def _release_mail_poller_lock(lock_fd: int | None):
    if lock_fd is None:
        return
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    os.close(lock_fd)


def _try_acquire_proactive_lock() -> int | None:
    """Singleton guard so only one worker drives the proactive loop (mirrors the
    mail-poller lock). Per-cycle exclusivity is additionally enforced by the
    ledger run-lock."""
    os.makedirs(os.path.dirname(PROACTIVE_LOCK_PATH), exist_ok=True)
    lock_fd = os.open(PROACTIVE_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o640)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock_fd)
        return None
    os.ftruncate(lock_fd, 0)
    os.write(lock_fd, str(os.getpid()).encode())
    return lock_fd


def _release_proactive_lock(lock_fd: int | None):
    if lock_fd is None:
        return
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    os.close(lock_fd)


async def _mail_poll_loop(lock_fd: int):
    log.info("mail_poll_loop_starting", interval_seconds=300)
    try:
        while True:
            try:
                await process_mailbox_once()
            except Exception as e:
                safe = classify_exception(e)
                log_exception("mail_poll_loop_failed", e, category=safe.category)
            await asyncio.sleep(300)
    finally:
        _release_mail_poller_lock(lock_fd)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp_runtime
    global mail_poller_task
    global mail_poller_lock_fd
    global discord_bot
    global discord_bot_task
    global proactive_loop
    global proactive_lock_fd
    log.info("startup_begin")

    mcp_runtime = MCPRuntime(owner="api")
    await mcp_runtime.connect_tools()

    if os.getenv("MAIL_IMAP_PASSWORD"):
        mail_poller_lock_fd = _try_acquire_mail_poller_lock()
        if mail_poller_lock_fd is None:
            log.info("mail_polling_disabled", reason="lock-held-by-other-worker")
        else:
            mail_poller_task = asyncio.create_task(_mail_poll_loop(mail_poller_lock_fd))
    else:
        log.info("mail_polling_disabled", reason="MAIL_IMAP_PASSWORD-not-set")

    if _embedded_discord_bot_enabled():
        try:
            discord_bot = build_bot()
            if discord_bot is not None:
                install_bot_notifier(discord_bot.send_embed)
                install_case_notifier(discord_bot.send_case_embed)
                discord_bot_task = asyncio.create_task(discord_bot.start())
        except Exception as e:
            safe = classify_exception(e)
            log_exception("discord_bot_start_failed", e, category=safe.category)
    else:
        log.info("discord_bot_embedded_disabled")

    proactive_settings = load_proactive_settings()
    if proactive_settings.enabled:
        proactive_lock_fd = _try_acquire_proactive_lock()
        if proactive_lock_fd is None:
            log.info("proactive_loop_disabled", reason="lock-held-by-other-worker")
        else:
            try:
                from app.proactive.memory import ProactiveMemory

                proactive_memory = ProactiveMemory(proactive_settings.memory_dir)
                proactive_memory.ensure()
                proactive_loop = ProactiveLoop(
                    mcp_runtime,
                    settings=proactive_settings,
                    investigator=_proactive_investigator(proactive_settings),
                    incident_memory=graph_runtime.INCIDENT_MEMORY,
                    memory=proactive_memory,
                )
                proactive_loop.start()
            except Exception as e:
                safe = classify_exception(e)
                log_exception("proactive_loop_start_failed", e, category=safe.category)
                _release_proactive_lock(proactive_lock_fd)
                proactive_lock_fd = None
                proactive_loop = None
    else:
        log.info("proactive_loop_disabled", reason="NOC_PROACTIVE_ENABLED-not-set")

    yield

    if proactive_loop is not None:
        await proactive_loop.stop()
        proactive_loop = None
    if proactive_lock_fd is not None:
        _release_proactive_lock(proactive_lock_fd)
        proactive_lock_fd = None

    if discord_bot_task:
        discord_bot_task.cancel()
        with suppress(asyncio.CancelledError):
            await discord_bot_task
        discord_bot_task = None
    discord_bot = None

    if mail_poller_task:
        mail_poller_task.cancel()
        with suppress(asyncio.CancelledError):
            await mail_poller_task
        mail_poller_task = None
    elif mail_poller_lock_fd is not None:
        _release_mail_poller_lock(mail_poller_lock_fd)
    mail_poller_lock_fd = None

    await mcp_runtime.disconnect()

    log.info("shutdown")


def _embedded_discord_bot_enabled() -> bool:
    return os.getenv("NOC_AGENT_START_EMBEDDED_BOT", "").strip() == "1"


def _proactive_investigator(settings):
    """Investigator callable the proactive loop uses for autonomous (non-shadow)
    investigation: synthetic alert → existing investigation graph, with heavy
    read-only probes stripped unless ``auto_heavy_probes`` is set."""
    from app.proactive.investigate import build_investigator

    return build_investigator(mcp_runtime, settings)


app = FastAPI(title="AS215932 NOC Agent", lifespan=lifespan)


class AlertManagerPayload(BaseModel):
    receiver: str
    status: str
    alerts: list[dict]
    groupLabels: dict
    commonLabels: dict
    commonAnnotations: dict
    externalURL: str
    version: str
    groupKey: str
    truncatedAlerts: int = 0


class IcingaNotification(BaseModel):
    host_name: str
    host_address: str | None = None
    service_name: str | None = None
    check_command: str | None = None
    state: str
    state_type: str | None = None
    output: str | None = None
    tags: dict = Field(default_factory=dict)


class MailPollResponse(BaseModel):
    status: str
    message: str


DISCORD_FIELD_LIMIT = 1024
DISCORD_DESCRIPTION_LIMIT = 4096


def _truncate_discord(value: str, limit: int = DISCORD_FIELD_LIMIT) -> str:
    value = str(value or "").strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def _format_list(items: list[str], fallback: str, *, limit_items: int = 6) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return fallback

    visible = cleaned[:limit_items]
    lines = [f"- {item}" for item in visible]
    if len(cleaned) > limit_items:
        lines.append(f"- Plus {len(cleaned) - limit_items} more")
    return _truncate_discord("\n".join(lines))


def _first_alert(alert_payload: dict) -> dict:
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts:
        return alerts[0] if isinstance(alerts[0], dict) else {}
    return {}


def _labels(alert_payload: dict) -> dict:
    first = _first_alert(alert_payload)
    labels = {}
    for key in ("groupLabels", "commonLabels"):
        if isinstance(alert_payload.get(key), dict):
            labels.update(alert_payload[key])
    if isinstance(first.get("labels"), dict):
        labels.update(first["labels"])
    return labels


def _is_recovery_alert(alert_payload: dict) -> bool:
    status_value = str(alert_payload.get("status") or "").lower()
    if status_value in {"resolved", "recovery", "ok", "up"}:
        return True

    first_status = str(_first_alert(alert_payload).get("status") or "").lower()
    return first_status in {"resolved", "recovery", "ok", "up"}


def _instance_host(instance: str) -> str:
    if instance.startswith("[") and "]" in instance:
        return instance[1:instance.index("]")]
    if instance.count(":") == 1:
        return instance.rsplit(":", 1)[0]
    return instance


def _alert_overview(alert_payload: dict) -> str:
    labels = _labels(alert_payload)
    first = _first_alert(alert_payload)
    facts = [
        f"Status: {alert_payload.get('status') or first.get('status') or 'unknown'}",
        f"Alert: {labels.get('alertname', 'unknown')}",
    ]

    if instance := labels.get("instance"):
        facts.append(f"Instance: {instance}")
    if host := labels.get("host") or labels.get("hostname"):
        facts.append(f"Host: {host}")
    if service := labels.get("service") or labels.get("job"):
        facts.append(f"Service: {service}")
    if severity := labels.get("severity"):
        facts.append(f"Alert severity: {severity}")
    if starts_at := first.get("startsAt"):
        facts.append(f"Started: {starts_at}")

    return _truncate_discord("\n".join(facts))


def _severity_color(severity: str, requires_human: bool) -> int:
    severity = (severity or "").upper()
    if requires_human or severity in {"CRITICAL", "HIGH"}:
        return 0xe74c3c
    if severity == "MEDIUM":
        return 0xf39c12
    return 0x2ecc71


def _operator_reason(plan) -> str:
    reason = (plan.human_escalation_reason or "").strip()
    lower_reason = reason.lower()
    internal_schema_hint = (
        "internal system schema limitation" in lower_reason
        or "schema limitation" in lower_reason
        or "tool validation" in lower_reason
    )
    if internal_schema_hint:
        return (
            "Live diagnostics were not completed by the agent runtime. Treat this as an alert-only "
            "assessment until MCP, Prometheus, and SSH access are verified."
        )
    return reason or "Human review is required before remediation."


def _fallback_operator_next_steps(alert_payload: dict) -> list[str]:
    labels = _labels(alert_payload)
    steps = ["Check `/health/mcp` and the NOC agent logs to confirm diagnostic tools are connected."]

    if instance := labels.get("instance"):
        steps.append(f"Run Prometheus query `up{{instance=\"{instance}\"}}` and inspect scrape errors for the target.")
        host = _instance_host(instance)
        if host:
            steps.append(f"SSH to `{host}` and check `systemctl status node_exporter` plus recent journal entries.")
    elif host := labels.get("host") or labels.get("hostname"):
        steps.append(f"SSH to `{host}` and check the failing service plus host health.")

    steps.append("Acknowledge or silence the alert only after confirming whether customer-impacting symptoms are present.")
    return steps


def _diagnostic_next_steps_text(plan, alert_payload: dict) -> str:
    proposal = plan.remediation_proposal
    if proposal is not None:
        actions = proposal.proposed_actions or plan.recommended_next_checks or _fallback_operator_next_steps(alert_payload)
        details = [
            "**Remediation proposal requires human approval**",
            f"Reason: {_operator_reason(plan)}",
            "",
            "Proposed actions:",
            _format_list(actions, "No proposed actions were provided."),
        ]
        if proposal.risk:
            details.extend(["", f"Risk: {proposal.risk}"])
        if proposal.rollback:
            details.extend(["", f"Rollback: {proposal.rollback}"])
        return _truncate_discord("\n".join(details))

    if plan.requires_human:
        steps = plan.recommended_next_checks or _fallback_operator_next_steps(alert_payload)
        return _truncate_discord(
            "**Human review required**\n"
            f"Reason: {_operator_reason(plan)}\n\n"
            "Next steps:\n"
            f"{_format_list(steps, 'No operator steps were provided.')}"
        )

    return _format_list(plan.recommended_next_checks, "No further diagnostic checks were recommended.")


def _evidence_lines(plan) -> list[str]:
    return [
        " | ".join(
            part
            for part in [
                item.evidence_id,
                item.tool,
                item.target,
                item.interpretation or item.observed_value,
            ]
            if part
        )
        for item in plan.evidence_chain
    ]


def _delta_lines(plan) -> list[str]:
    return [
        f"{delta.subject} {delta.attribute}: expected `{delta.expected_value or 'unknown'}`, observed `{delta.observed_value or 'unknown'}`"
        for delta in plan.deltas
    ]


def _contradiction_lines(plan) -> list[str]:
    return [
        f"{item.status}: {item.summary}" + (f" Next check: {item.next_check}" if item.next_check else "")
        for item in plan.contradictions
    ]


def _assessment_text(plan) -> str:
    facts = [fact.statement for fact in plan.confirmed_facts if fact.statement]
    hypotheses = [hypothesis.statement for hypothesis in plan.hypotheses if hypothesis.statement]
    parts = []
    if facts:
        parts.append("Facts: " + "; ".join(facts[:3]))
    if hypotheses:
        parts.append("Hypotheses: " + "; ".join(hypotheses[:3]))
    if plan.confidence_basis:
        parts.append("Confidence basis: " + plan.confidence_basis)
    return _truncate_discord("\n".join(parts) or plan.incident_summary)


def _triage_fields(plan, alert_payload: dict) -> list[dict]:
    return [
        {"name": "Alert", "value": _alert_overview(alert_payload)},
        {"name": "Assessment", "value": _assessment_text(plan)},
        {
            "name": "Evidence Chain",
            "value": _format_list(
                _evidence_lines(plan),
                "No live diagnostic evidence was recorded. Review the synthesis before making changes.",
            ),
        },
        {
            "name": "Deltas",
            "value": _format_list(_delta_lines(plan), "No manifest-vs-telemetry deltas were confirmed."),
        },
        {
            "name": "Contradictions / Missing Evidence",
            "value": _format_list(
                _contradiction_lines(plan),
                "No telemetry contradictions were reported.",
            ),
        },
        {"name": "Confidence", "value": f"{plan.confidence_score * 100:.1f}%", "inline": True},
        {"name": "Severity", "value": plan.severity, "inline": True},
        {"name": "Next Checks / Proposal", "value": _diagnostic_next_steps_text(plan, alert_payload)},
    ]


def _case_update_fields(case: dict, event: dict) -> list[dict]:
    timeline = []
    latest = case.get("latest_event") or event
    if latest:
        timeline.append(
            " | ".join(
                part
                for part in [
                    latest.get("received_at", ""),
                    latest.get("state", ""),
                    latest.get("summary", ""),
                ]
                if part
            )
        )
    victims = list(case.get("downstream_victims", []))
    victim_lines = [f"- {victim}" for victim in victims[:10]]
    if len(victims) > 10:
        victim_lines.append(f"- Plus {len(victims) - 10} more")
    return [
        {"name": "Case", "value": f"{case.get('case_number', 'NOC')} · {case.get('status', 'unknown')}"},
        {"name": "Events", "value": str(case.get("event_count", 0)), "inline": True},
        {"name": "Latest Event", "value": _truncate_discord("\n".join(timeline) or "No event details.")},
        {"name": "Downstream Victims", "value": _truncate_discord("\n".join(victim_lines) or "None")},
    ]


def _case_update_title(action: str, case: dict, event: dict) -> str:
    if action == "recovered":
        return f"✅ {case.get('case_number', 'NOC')}: recovered, cooling down"
    if action == "reopened":
        return f"⚠️ {case.get('case_number', 'NOC')}: reopened during cooldown"
    if action == "escalated":
        return f"⚠️ {case.get('case_number', 'NOC')}: escalated to {event.get('state', 'UNKNOWN')}"
    if action == "linked_parent":
        return f"🔁 {case.get('case_number', 'NOC')}: downstream event linked"
    return f"🔁 {case.get('case_number', 'NOC')}: duplicate event attached"


def _case_update_description(action: str, case: dict, event: dict) -> str:
    if action == "recovered":
        cooldown = _format_duration(RECOVERY_COOLDOWN_SECONDS)
        return f"Recovery event attached. The case is in recovered_pending cooldown for {cooldown}."
    if action == "reopened":
        return "A firing event arrived during recovered_pending cooldown, so the existing case was reopened."
    if action == "linked_parent":
        parent = case.get("linked_parent_case") or "parent case"
        return f"Downstream event attached to `{parent}` instead of starting a separate investigation."
    summary = event.get("summary") or "No output summary was provided."
    return _truncate_discord(f"Event attached to the existing case. Latest state: `{event.get('state', 'UNKNOWN')}`.\n{summary}", DISCORD_DESCRIPTION_LIMIT)


def _format_duration(seconds: int) -> str:
    if seconds % 3600 == 0:
        value = seconds // 3600
        unit = "hour" if value == 1 else "hours"
    elif seconds % 60 == 0:
        value = seconds // 60
        unit = "minute" if value == 1 else "minutes"
    else:
        value = seconds
        unit = "second" if value == 1 else "seconds"
    return f"{value} {unit}"


def _case_color(action: str, event: dict) -> int:
    if action == "recovered":
        return 0x2ecc71
    if action in {"escalated", "reopened"} or str(event.get("state", "")).upper() == "CRITICAL":
        return 0xe74c3c
    return 0xf39c12


async def _handle_case_update(result: CaseIntakeResult) -> None:
    target_case = result.parent_case if result.parent_case is not None else result.case
    if target_case is None:
        return
    if result.should_inject:
        try:
            await inject_case_event(target_case, result.event)
        except Exception as e:
            safe = classify_exception(e)
            log_exception(
                "case_event_injection_failed",
                e,
                category=safe.category,
                case_number=target_case.get("case_number"),
                action=result.action,
            )
    await send_case_notification(
        case_id=target_case["incident_id"],
        title=_case_update_title(result.action, target_case, result.event),
        description=_case_update_description(result.action, result.case or target_case, result.event),
        color=_case_color(result.action, result.event),
        fields=_case_update_fields(target_case, result.event),
        level=Verbosity.INFO,
    )


async def investigate_alert(alert_payload: dict, model=None, case: dict | None = None, *, mcp_runtime=None):
    runtime = mcp_runtime if mcp_runtime is not None else globals()["mcp_runtime"]
    event = (case or {}).get("latest_event") or case_event_from_alert(alert_payload)
    display_title = case_display_title(case, event)
    if _is_recovery_alert(alert_payload):
        log.info(
            "investigation_skipped",
            reason="recovery",
            alert_status=alert_payload.get("status"),
            title_source=display_title,
        )
        return

    log.info(
        "investigation_started",
        alert_status=alert_payload.get("status"),
        title_source=display_title,
        incident_id=(case or {}).get("incident_id"),
        case_number=(case or {}).get("case_number"),
    )
    await send_case_notification(
        case_id=(case or {}).get("incident_id", display_title),
        title=f"⏳ {display_title}",
        description="Starting investigation and collecting telemetry.",
        color=0xf39c12,
        level=Verbosity.INFO,
    )

    run_started = start_run("triage")
    try:
        plan, graph_state = await run_investigation_graph(alert_payload, model=model, mcp_runtime=runtime, case=case)
        record_success("triage", run_started, _SyntheticRunResult())
    except Exception as e:
        safe = classify_exception(e)
        record_failure("triage", run_started, safe)
        log_exception(
            "noc_triage_failed",
            e,
            category=safe.category,
            provider=safe.provider,
            model=safe.model_name,
        )
        await notify_finish(
            f"NOC Triage: {display_title}",
            safe.discord_description("NOC triage"),
            is_error=True,
            safe_category=safe.category,
        )
        # Return None so callers (e.g. the proactive investigator) can tell a
        # swallowed triage failure from a successful investigation.
        return None

    log.info(
        "investigation_complete",
        summary=plan.incident_summary,
        confidence=plan.confidence_score,
        severity=plan.severity,
        requires_human=plan.requires_human,
        escalation_reason=plan.human_escalation_reason if plan.requires_human else None,
        proposed_actions=list(plan.remediation_proposal.proposed_actions) if plan.remediation_proposal else [],
        incident_id=graph_state.get("incident_id"),
        active_specialist=graph_state.get("active_specialist"),
    )

    color = _severity_color(plan.severity, plan.requires_human)
    fields = _triage_fields(plan, alert_payload)
    await send_case_notification(
        case_id=(case or {}).get("incident_id", display_title),
        title=f"Detailed Report: {display_title}",
        description=_truncate_discord(
            f"{'Escalated to human review' if plan.requires_human else 'Triage completed'} "
            f"with {plan.confidence_score * 100:.1f}% confidence.",
            DISCORD_DESCRIPTION_LIMIT,
        ),
        color=color,
        fields=fields,
    )
    return plan


def _icinga_to_alert_payload(notif: IcingaNotification) -> dict:
    """Reshape an Icinga notification into the dict shape investigate_alert expects."""
    name = notif.service_name or notif.check_command or "host-check"
    return {
        "source": "icinga2",
        "status": "firing" if (notif.state_type or "").upper() == "PROBLEM" or notif.state.upper() not in {"OK", "UP"} else "resolved",
        "groupLabels": {
            "alertname": name,
            "host": notif.host_name,
        },
        "commonLabels": {
            "host": notif.host_name,
            "service": notif.service_name or "",
            "address": notif.host_address or "",
            "check_command": notif.check_command or "",
        },
        "commonAnnotations": {
            "summary": notif.output or "",
        },
        "alerts": [{
            "labels": {
                "alertname": name,
                "host": notif.host_name,
                "service": notif.service_name or "",
                "state": notif.state,
            },
            "annotations": {
                "summary": notif.output or "",
            },
            "status": notif.state,
        }],
        "tags": notif.tags,
    }


@app.post("/webhook/alertmanager")
async def alertmanager_webhook(payload: AlertManagerPayload, background_tasks: BackgroundTasks):
    """Receives alerts from Prometheus Alertmanager and triggers the NOC agent."""
    alert_payload = payload.model_dump()
    alert_payload["source"] = "alertmanager"
    result = await intake_alert(alert_payload)
    if result.should_investigate and result.case is not None:
        background_tasks.add_task(investigate_alert, alert_payload, case=result.case)
    elif result.case is not None or result.parent_case is not None:
        background_tasks.add_task(_handle_case_update, result)
    return {
        "status": "accepted" if result.case or result.parent_case else "ignored",
        "message": f"Alert {result.action}",
        "action": result.action,
        "case_number": (result.case or result.parent_case or {}).get("case_number"),
        "incident_id": (result.case or result.parent_case or {}).get("incident_id"),
    }


@app.post("/webhook/icinga")
async def icinga_webhook(payload: IcingaNotification, background_tasks: BackgroundTasks):
    """Receives Icinga2 NotificationCommand POSTs and triggers the NOC agent."""
    alert_payload = _icinga_to_alert_payload(payload)
    result = await intake_alert(alert_payload)
    if result.should_investigate and result.case is not None:
        background_tasks.add_task(investigate_alert, alert_payload, case=result.case)
    elif result.case is not None or result.parent_case is not None:
        background_tasks.add_task(_handle_case_update, result)
    return {
        "status": "accepted" if result.case or result.parent_case else "ignored",
        "message": f"Icinga notification {result.action}",
        "action": result.action,
        "case_number": (result.case or result.parent_case or {}).get("case_number"),
        "incident_id": (result.case or result.parent_case or {}).get("incident_id"),
    }


@app.post("/mail/poll", response_model=MailPollResponse)
async def poll_mailbox(background_tasks: BackgroundTasks):
    """Poll the shared NOC mailbox and store draft replies for human approval."""
    background_tasks.add_task(process_mailbox_once)
    return {"status": "accepted", "message": "Mailbox poll queued; drafts require human approval"}


class TaskRequest(BaseModel):
    prompt: str


class ManualInvestigationRequest(BaseModel):
    prompt: str
    operator: str = "web"


class LocalDecisionRequest(BaseModel):
    decision: str = Field(pattern="^(approved|rejected|acknowledged)$")
    operator: str
    comment: str = ""


class SignedApprovalRequest(LocalDecisionRequest):
    incident_id: str


class CommentRequest(BaseModel):
    operator: str = "web"
    comment: str


class FastStatusRequest(BaseModel):
    target: str
    qualifiers: dict[str, str] = Field(default_factory=dict)

@app.post("/task", response_model=MailPollResponse)
async def run_task(request: TaskRequest, background_tasks: BackgroundTasks):
    """Run an arbitrary task on the NOC Triage agent (e.g. 'Draft email to LocIX')."""
    async def _run_task():
        await notify_start("Manual Task", f"Task: {request.prompt}")
        run_started = start_run("manual_task")
        try:
            result = await noc_triage_agent.run(request.prompt)
            plan = result.data if hasattr(result, 'data') else result.output
            record_success("manual_task", run_started, result)
            await notify_finish("Manual Task", f"Task completed: {plan.incident_summary}")
        except Exception as e:
            safe = classify_exception(e)
            record_failure("manual_task", run_started, safe)
            log_exception("manual_task_failed", e, category=safe.category, provider=safe.provider, model=safe.model_name)
            await notify_finish(
                "Manual Task",
                safe.discord_description("Manual task"),
                is_error=True,
                safe_category=safe.category,
            )

    background_tasks.add_task(_run_task)
    return {"status": "accepted", "message": "Task queued"}

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "AS215932 NOC Agent"}


@app.get("/control/incidents/pending")
async def pending_incidents(x_noc_control_token: str | None = Header(default=None)):
    _require_control_token(x_noc_control_token)
    return {"status": "ok", "incidents": await pending_summaries()}


@app.get("/control/incidents/{incident_id}")
async def incident_status(incident_id: str, x_noc_control_token: str | None = Header(default=None)):
    _require_control_token(x_noc_control_token)
    summary = await summary_for(incident_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return summary


@app.get("/control", response_class=HTMLResponse)
async def control_ui(request: Request, token: str | None = Query(default=None)):
    _require_control_request(request, token)
    return HTMLResponse(_control_html(token or ""))


@app.get("/control/cases")
async def control_cases(request: Request, token: str | None = Query(default=None), x_noc_control_token: str | None = Header(default=None)):
    _require_control_request(request, token, x_noc_control_token)
    cases = await graph_runtime.INCIDENT_MEMORY.list_cases()
    pending = {item["incident_id"] for item in await pending_summaries()}
    return {
        "status": "ok",
        "cases": [
            {
                "incident_id": case.get("incident_id"),
                "case_number": case.get("case_number"),
                "status": case.get("status"),
                "severity": (case.get("latest_event") or {}).get("severity") or (case.get("latest_event") or {}).get("state"),
                "resource_id": case.get("resource_id"),
                "title": case.get("title"),
                "event_count": case.get("event_count", 0),
                "updated_at": case.get("updated_at"),
                "pending_approval": case.get("incident_id") in pending or case.get("status") == "waiting_approval",
            }
            for case in cases
        ],
    }


@app.get("/control/cases/{case_id}")
async def control_case_detail(case_id: str, request: Request, token: str | None = Query(default=None), x_noc_control_token: str | None = Header(default=None)):
    _require_control_request(request, token, x_noc_control_token)
    detail = await graph_runtime.INCIDENT_MEMORY.case_detail(case_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Case not found")
    return {"status": "ok", **detail}


@app.get("/control/cases/{case_id}/events")
async def control_case_events(case_id: str, request: Request, token: str | None = Query(default=None), x_noc_control_token: str | None = Header(default=None)):
    _require_control_request(request, token, x_noc_control_token)
    resolved = await graph_runtime.INCIDENT_MEMORY.resolve_case_identifier(case_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Case not found")
    return {"status": "ok", "events": await graph_runtime.INCIDENT_MEMORY.case_events(resolved)}


@app.post("/control/cases/{case_id}/comment")
async def control_case_comment(
    case_id: str,
    request_body: CommentRequest,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    case = await graph_runtime.INCIDENT_MEMORY.append_operator_comment(case_id, request_body.operator, request_body.comment)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")
    return {"status": "ok", "case": case}


@app.post("/control/cases/{case_id}/decision")
async def control_case_decision(
    case_id: str,
    request_body: LocalDecisionRequest,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    resolved = await graph_runtime.INCIDENT_MEMORY.resolve_case_identifier(case_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Case not found")
    decision = ApprovalDecision(incident_id=resolved, **request_body.model_dump())
    summary = await record_operator_decision(resolved, decision.model_dump(), mcp_runtime=mcp_runtime)
    if summary is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    await graph_runtime.INCIDENT_MEMORY.append_operator_comment(
        resolved,
        request_body.operator,
        f"Decision {request_body.decision}: {request_body.comment}".strip(),
    )
    return {"status": "ok", "incident": summary}


@app.post("/control/cases/investigate")
async def control_manual_investigation(
    request_body: ManualInvestigationRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    alert_payload = _manual_investigation_payload(request_body.prompt, "web", request_body.operator)
    result = await intake_alert(alert_payload)
    if result.case is None:
        raise HTTPException(status_code=409, detail=f"Manual investigation was not queued: {result.action}")
    background_tasks.add_task(investigate_alert, alert_payload, case=result.case)
    return {"status": "accepted", "case": result.case, "case_number": result.case.get("case_number"), "incident_id": result.case.get("incident_id")}


@app.post("/control/status")
async def control_fast_status(
    request_body: FastStatusRequest,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    from app.discord_bot import run_fast_status_check

    overview = await run_fast_status_check(request_body.target, request_body.qualifiers, mcp_runtime)
    return {"status": "ok", "overview": overview.__dict__}


@app.get("/control/proactive/status")
async def control_proactive_status(
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    from pathlib import Path as _Path

    from app.proactive.ledger import load_ledger

    settings = load_proactive_settings()
    running = proactive_loop is not None and proactive_loop.running
    return {
        "status": "ok",
        "running": running,
        "paused": bool(proactive_loop.paused) if proactive_loop is not None else None,
        "settings": {
            "enabled": settings.enabled,
            "shadow": settings.shadow,
            "interval_s": settings.interval_s,
            "deep_scan_s": settings.deep_scan_s,
            "max_investigations_per_cycle": settings.max_investigations_per_cycle,
            "max_investigations_per_day": settings.max_investigations_per_day,
            "max_cost_usd_per_day": settings.max_cost_usd_per_day,
            "auto_heavy_probes": settings.auto_heavy_probes,
            "handoff_enabled": settings.handoff_enabled,
            "severity_floor": settings.severity_floor,
        },
        "ledger_today": load_ledger(_Path(settings.state_dir)),
        "hotspots": _proactive_hotspots_view(),
        "suppressions": _suppression_store().active(),
    }


def _proactive_hotspots_view() -> list[dict]:
    """The last cycle's (un-suppressed) hotspots, shaped for the dashboard."""
    if proactive_loop is None or proactive_loop.last_report is None:
        return []
    return [
        {
            "ack_id": h.fingerprint()[:12],
            "severity": h.severity,
            "category": h.category,
            "title": h.title,
            "resource": h.resource,
            "summary": h.summary,
            "warrants_change": h.warrants_change,
        }
        for h in proactive_loop.last_report.top(20)
    ]


@app.post("/control/proactive/pause")
async def control_proactive_pause(
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    if proactive_loop is None:
        raise HTTPException(status_code=409, detail="Proactive loop is not running")
    proactive_loop.pause()
    return {"status": "ok", "paused": True}


@app.post("/control/proactive/resume")
async def control_proactive_resume(
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    if proactive_loop is None:
        raise HTTPException(status_code=409, detail="Proactive loop is not running")
    proactive_loop.resume()
    return {"status": "ok", "paused": False}


@app.post("/control/proactive/run-once")
async def control_proactive_run_once(
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    if proactive_loop is None:
        raise HTTPException(status_code=409, detail="Proactive loop is not running")
    report = await proactive_loop.run_once()
    return {"status": "ok", "report": report.model_dump(mode="json")}


class ProactiveAckRequest(BaseModel):
    fingerprint: str
    reason: str = ""
    issue: str = ""
    operator: str = "web"
    ttl_hours: float | None = None


class ProactiveUnackRequest(BaseModel):
    fingerprint: str


def _suppression_store():
    from pathlib import Path as _Path

    from app.proactive.suppressions import SuppressionStore

    return SuppressionStore(_Path(load_proactive_settings().state_dir) / "suppressions.json")


@app.get("/control/proactive/suppressions")
async def control_proactive_suppressions(
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    return {"status": "ok", "suppressions": _suppression_store().active()}


@app.post("/control/proactive/ack")
async def control_proactive_ack(
    request_body: ProactiveAckRequest,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    entry = _suppression_store().add(
        fingerprint=request_body.fingerprint.strip(),
        reason=request_body.reason,
        issue=request_body.issue,
        operator=request_body.operator,
        ttl_seconds=(request_body.ttl_hours * 3600) if request_body.ttl_hours else None,
    )
    return {"status": "ok", "suppressed": entry}


@app.post("/control/proactive/unack")
async def control_proactive_unack(
    request_body: ProactiveUnackRequest,
    request: Request,
    token: str | None = Query(default=None),
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_request(request, token, x_noc_control_token)
    removed = _suppression_store().remove(request_body.fingerprint.strip())
    return {"status": "ok", "removed": removed}


@app.post("/control/incidents/{incident_id}/decision")
async def decide_incident(
    incident_id: str,
    request: LocalDecisionRequest,
    x_noc_control_token: str | None = Header(default=None),
):
    _require_control_token(x_noc_control_token)
    decision = ApprovalDecision(incident_id=incident_id, **request.model_dump())
    summary = await record_operator_decision(incident_id, decision.model_dump(), mcp_runtime=mcp_runtime)
    if summary is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return {"status": "ok", "incident": summary}


@app.post("/approval/resume")
async def signed_resume(request: SignedApprovalRequest, x_noc_signature: str | None = Header(default=None)):
    _require_signed_callback(request, x_noc_signature)
    decision = ApprovalDecision(
        incident_id=request.incident_id,
        decision=request.decision,
        operator=request.operator,
        comment=request.comment,
    )
    summary = await record_operator_decision(request.incident_id, decision.model_dump(), mcp_runtime=mcp_runtime)
    if summary is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return {"status": "ok", "incident": summary}


@app.get("/health/mcp")
async def health_mcp(response: Response):
    health = await mcp_runtime.live_health()
    if health["status"] != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health


@app.get("/health/config")
async def health_config(response: Response):
    model_config = load_model_config()
    missing = [name for name in REQUIRED_CONFIG if not os.getenv(name)]
    missing.extend(_missing_model_provider_config(model_config.active_model_chain, model_config.provider_api_key_envs))
    if os.getenv("HYRULE_MCP_URL"):
        missing = [name for name in missing if name != "HYRULE_MCP_CMD"]
    if os.getenv("HYRULE_MCP_CMD"):
        missing = [name for name in missing if name != "HYRULE_MCP_URL"]
    if os.getenv("XO_MCP_URL"):
        missing = [name for name in missing if name != "XO_MCP_CMD"]
    if os.getenv("XO_MCP_CMD"):
        missing = [name for name in missing if name != "XO_MCP_URL"]
    missing = sorted(set(missing))
    disabled = []
    if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
        disabled.append("mcp")

    health_status = "ok" if not missing and not disabled else "degraded"
    if health_status != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": health_status,
        "missing": missing,
        "disabled": disabled,
        "mail_polling": "enabled" if os.getenv("MAIL_IMAP_PASSWORD") else "disabled",
    }


def _missing_model_provider_config(active_models: list[str], provider_api_key_envs: dict[str, str]) -> list[str]:
    missing: list[str] = []
    providers = {_provider_from_model_name(model) for model in active_models}
    if "openrouter" in providers:
        env_name = provider_api_key_envs.get("openrouter", "OPENROUTER_API_KEY")
        if not (os.getenv(env_name) or os.getenv("OPENROUTER_API_KEY")):
            missing.append(env_name)
    if providers.intersection({"google", "google-gla"}):
        if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
            missing.append("GOOGLE_API_KEY or GEMINI_API_KEY")
    if "google-vertex" in providers:
        if not (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_API_KEY")):
            missing.append("GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_API_KEY")
    direct_provider_envs = {
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "openai-chat": "OPENAI_API_KEY",
        "openai-responses": "OPENAI_API_KEY",
    }
    for provider, env_name in direct_provider_envs.items():
        if provider in providers and not os.getenv(env_name):
            missing.append(env_name)
    return missing


def _provider_from_model_name(model_name: str) -> str:
    if ":" in model_name:
        return model_name.split(":", 1)[0]
    if model_name.startswith("gemini"):
        return "google-gla"
    if model_name.startswith("claude"):
        return "anthropic"
    if model_name.startswith(("gpt", "o1", "o3")):
        return "openai"
    return "unknown"


@app.get("/health/model")
async def health_model(response: Response):
    config = load_model_config()
    provider_monitoring = check_model_provider_credits(config)
    health = MODEL_STATE.health()
    if provider_monitoring.status == "degraded":
        health["status"] = "degraded"
    if health["status"] != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    provider_health = provider_monitoring.health_value()
    return {
        **health,
        "primary_model": config.primary_model,
        "fallback_models": config.fallback_models,
        "provider_monitoring": provider_health,
        "quota_monitoring": provider_monitoring.status,
        "quota": provider_health,
    }


@app.get("/metrics")
async def metrics():
    check_model_provider_credits(load_model_config())
    body, content_type = metrics_response()
    return Response(content=body, media_type=content_type)


@app.get("/health/mail")
async def health_mail(response: Response):
    try:
        return check_mailbox_connection()
    except Exception as e:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        log_exception("mail_health_failed", e, category=classify_exception(e).category)
        return {
            "status": "degraded",
            "error": safe_health_error(e),
        }


class _SyntheticRunResult:
    def new_messages(self):
        return []

    def usage(self):
        return None


def _require_control_token(header_value: str | None) -> None:
    expected = os.getenv("NOC_CONTROL_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Local control plane is not configured")
    header_value = header_value if isinstance(header_value, str) else None
    if not header_value or not hmac.compare_digest(header_value, expected):
        raise HTTPException(status_code=401, detail="Invalid control token")


def _require_control_request(request: Request, token: str | None = None, header_value: str | None = None) -> None:
    auth = request.headers.get("authorization", "")
    bearer = auth.removeprefix("Bearer ").strip() if auth.lower().startswith("bearer ") else ""
    header_value = header_value if isinstance(header_value, str) else None
    _require_control_token(header_value or request.headers.get("x-noc-control-token") or bearer or token)


def _manual_investigation_payload(prompt: str, source: str, operator: str) -> dict:
    return {
        "source": source,
        "status": "firing",
        "groupLabels": {"alertname": "Operator Investigation", "host": "manual"},
        "commonLabels": {"severity": "manual", "host": "manual", "operator": operator},
        "commonAnnotations": {"summary": prompt},
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "Operator Investigation", "host": "manual", "state": "MANUAL"},
                "annotations": {"summary": prompt},
            }
        ],
    }


# Proactive panel markup + script. Kept as plain strings (interpolated into the
# template) so their braces don't collide with the f-string's brace escaping.
_PROACTIVE_PANEL = """
    <div class="panel">
      <div class="toolbar"><strong>Proactive loop</strong><span id="proStatus" class="meta"></span></div>
      <div id="proHotspots" class="grid"></div>
      <details><summary class="meta">Muted (acked) hotspots</summary><div id="proSuppressions" class="grid"></div></details>
    </div>"""

_PROACTIVE_JS = """
function _sevEmoji(s){ return ({HIGH:'\\u{1F534}',MEDIUM:'\\u{1F7E0}',LOW:'\\u{1F7E2}'})[s] || '\\u2022'; }
async function loadProactive(){
  let d;
  try { d = await api('/control/proactive/status'); }
  catch(e){ const el=document.querySelector('#proStatus'); if(el) el.textContent='unavailable'; return; }
  const st = d.settings || {};
  document.querySelector('#proStatus').textContent =
    (d.running ? 'running' : 'stopped') + (d.paused ? ' (paused)' : '') + ' \\u00b7 ' +
    (st.shadow ? 'shadow' : 'autonomous') + ' \\u00b7 today ' +
    ((d.ledger_today||{}).investigations ?? 0) + '/' + (st.max_investigations_per_day ?? '?');
  const hs = d.hotspots || [];
  document.querySelector('#proHotspots').innerHTML = hs.length ? hs.map(h =>
    '<div class="panel"><strong>' + _sevEmoji(h.severity) + ' ' + esc(h.title) + '</strong>' +
    '<div class="meta">' + esc(h.resource) + ' \\u00b7 ' + esc(h.category) + ' \\u00b7 ack id <code>' + esc(h.ack_id) + '</code></div>' +
    '<div>' + esc(h.summary) + '</div>' +
    '<div class="toolbar"><input placeholder="reason (e.g. tracked in #268)" data-reason="' + esc(h.ack_id) + '">' +
    '<input type="number" min="0" placeholder="hours" style="width:80px" data-hours="' + esc(h.ack_id) + '">' +
    '<button data-ack="' + esc(h.ack_id) + '">Ack</button></div></div>'
  ).join('') : '<div class="meta">No active hotspots.</div>';
  document.querySelectorAll('[data-ack]').forEach(b => b.onclick = async () => {
    const id = b.dataset.ack;
    const r = document.querySelector('[data-reason="' + id + '"]');
    const hr = document.querySelector('[data-hours="' + id + '"]');
    const hours = parseInt((hr && hr.value) || '0', 10) || 0;
    try { await api('/control/proactive/ack', {method:'POST', body: JSON.stringify({fingerprint:id, reason:(r&&r.value)||'', ttl_hours: hours||null, operator:'web'})}); await loadProactive(); }
    catch(e){ alert(e.message); }
  });
  const sup = d.suppressions || {};
  const keys = Object.keys(sup);
  document.querySelector('#proSuppressions').innerHTML = keys.length ? keys.map(fp =>
    '<div class="meta"><code>' + esc(fp) + '</code> ' + esc(sup[fp].reason || 'no reason') +
    ' <button class="secondary" data-unack="' + esc(fp) + '">Unack</button></div>'
  ).join('') : '<div class="meta">None.</div>';
  document.querySelectorAll('[data-unack]').forEach(b => b.onclick = async () => {
    try { await api('/control/proactive/unack', {method:'POST', body: JSON.stringify({fingerprint:b.dataset.unack})}); await loadProactive(); }
    catch(e){ alert(e.message); }
  });
}
const _refreshBtn = document.querySelector('#refresh');
if (_refreshBtn) _refreshBtn.addEventListener('click', () => loadProactive().catch(()=>{}));
loadProactive().catch(()=>{});
"""


def _control_html(token: str) -> str:
    safe_token = escape(token, quote=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NOC Control</title>
  <style>
    :root {{ color-scheme: light dark; --line:#d8dee4; --muted:#6b7280; --bg:#f7f8fa; --panel:#fff; --text:#111827; --accent:#0f766e; --warn:#b45309; }}
    @media (prefers-color-scheme: dark) {{ :root {{ --line:#30363d; --muted:#9ca3af; --bg:#0d1117; --panel:#161b22; --text:#f3f4f6; --accent:#2dd4bf; --warn:#f59e0b; }} }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; background:var(--bg); color:var(--text); }}
    header {{ display:flex; gap:12px; align-items:center; justify-content:space-between; padding:14px 18px; border-bottom:1px solid var(--line); background:var(--panel); position:sticky; top:0; z-index:2; }}
    h1 {{ font-size:18px; margin:0; }}
    main {{ display:grid; grid-template-columns:minmax(280px,420px) 1fr; min-height:calc(100vh - 58px); }}
    aside {{ border-right:1px solid var(--line); background:var(--panel); overflow:auto; }}
    section {{ padding:16px; overflow:auto; }}
    input, textarea, button, select {{ font:inherit; border:1px solid var(--line); border-radius:6px; padding:8px 10px; background:var(--panel); color:var(--text); }}
    button {{ cursor:pointer; background:var(--accent); color:white; border-color:var(--accent); }}
    button.secondary {{ background:transparent; color:var(--text); }}
    .toolbar {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
    .case {{ display:grid; gap:4px; padding:12px 14px; border-bottom:1px solid var(--line); cursor:pointer; }}
    .case:hover {{ background:color-mix(in srgb, var(--accent) 8%, transparent); }}
    .case strong {{ font-size:13px; }}
    .meta {{ color:var(--muted); font-size:12px; }}
    .badge {{ display:inline-block; border:1px solid var(--line); border-radius:999px; padding:1px 7px; font-size:12px; color:var(--muted); }}
    .pending {{ color:var(--warn); border-color:var(--warn); }}
    .grid {{ display:grid; gap:12px; }}
    .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }}
    .timeline {{ display:grid; gap:10px; }}
    .event {{ border-left:3px solid var(--line); padding-left:10px; }}
    pre {{ white-space:pre-wrap; overflow:auto; background:var(--bg); padding:10px; border-radius:6px; }}
    textarea {{ width:100%; min-height:80px; }}
    @media (max-width: 820px) {{ main {{ grid-template-columns:1fr; }} aside {{ border-right:0; border-bottom:1px solid var(--line); max-height:45vh; }} }}
  </style>
</head>
<body>
<header>
  <h1>NOC Control</h1>
  <div class="toolbar">
    <input id="token" type="password" placeholder="Control token" value="{safe_token}" autocomplete="off">
    <button id="saveToken" class="secondary">Save</button>
    <button id="refresh">Refresh</button>
  </div>
</header>
<main>
  <aside><div id="cases"></div></aside>
  <section class="grid">
    <div class="panel">
      <form id="investigate" class="grid">
        <strong>Manual Investigation</strong>
        <textarea name="prompt" placeholder="Describe what to investigate"></textarea>
        <div class="toolbar"><input name="operator" placeholder="Operator" value="web"><button>Start</button></div>
      </form>
    </div>
    <div class="panel">
      <form id="fastStatus" class="toolbar">
        <input name="target" placeholder="Fast status target">
        <button>Status</button>
      </form>
      <pre id="statusOut"></pre>
    </div>
    {_PROACTIVE_PANEL}
    <div id="detail" class="grid"></div>
  </section>
</main>
<script>
const tokenInput = document.querySelector('#token');
const casesEl = document.querySelector('#cases');
const detailEl = document.querySelector('#detail');
const statusOut = document.querySelector('#statusOut');
tokenInput.value = tokenInput.value || sessionStorage.getItem('nocToken') || '';
function token() {{ return tokenInput.value.trim(); }}
function headers() {{ return {{'content-type':'application/json','x-noc-control-token':token()}}; }}
async function api(path, opts={{}}) {{
  const res = await fetch(path, {{...opts, headers: {{...headers(), ...(opts.headers||{{}})}}}});
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}}
function esc(v) {{ return String(v ?? '').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c])); }}
async function loadCases() {{
  const data = await api('/control/cases');
  casesEl.innerHTML = data.cases.map(c => `<div class="case" data-id="${{esc(c.case_number||c.incident_id)}}"><strong>${{esc(c.case_number)}} <span class="badge ${{c.pending_approval?'pending':''}}">${{esc(c.status)}}</span></strong><span>${{esc(c.title||c.resource_id)}}</span><span class="meta">${{esc(c.severity||'')}} events=${{esc(c.event_count)}} updated=${{esc(c.updated_at||'')}}</span></div>`).join('');
  document.querySelectorAll('.case').forEach(el => el.onclick = () => loadDetail(el.dataset.id));
}}
async function loadDetail(id) {{
  const data = await api('/control/cases/' + encodeURIComponent(id));
  const c = data.case, s = data.summary || {{}};
  detailEl.innerHTML = `<div class="panel"><h2>${{esc(c.case_number)}} <span class="badge">${{esc(c.status)}}</span></h2><p class="meta">${{esc(c.incident_id)}} · ${{esc(c.resource_id)}} · updated ${{esc(c.updated_at)}}</p><p>${{esc(s.title || c.diagnostic_summary || c.title || '')}}</p><div class="toolbar"><button data-decision="approved">Approve</button><button data-decision="rejected" class="secondary">Reject</button><button data-decision="acknowledged" class="secondary">Acknowledge</button></div><textarea id="comment" placeholder="Comment"></textarea><div class="toolbar"><input id="operator" value="web"><button id="addComment" class="secondary">Comment</button></div></div><div class="panel"><strong>Proposal</strong><pre>${{esc(JSON.stringify(s.proposals || [], null, 2))}}</pre></div><div class="panel"><strong>Execution</strong><pre>${{esc(JSON.stringify({{executed_actions:s.executed_actions||c.executed_actions||[], verification_results:s.verification_results||c.verification_results||[]}}, null, 2))}}</pre></div><div class="panel timeline"><strong>Timeline</strong>${{data.events.map(e => `<div class="event"><span class="meta">${{esc(e.received_at)}} · ${{esc(e.event_type||e.state||'event')}}</span><div>${{esc(e.summary||'')}}</div></div>`).join('')}}</div>`;
  detailEl.querySelectorAll('[data-decision]').forEach(btn => btn.onclick = async () => {{
    await api('/control/cases/' + encodeURIComponent(id) + '/decision', {{method:'POST', body:JSON.stringify({{decision:btn.dataset.decision, operator:document.querySelector('#operator').value||'web', comment:document.querySelector('#comment').value||''}})}});
    await loadDetail(id); await loadCases();
  }});
  detailEl.querySelector('#addComment').onclick = async () => {{
    await api('/control/cases/' + encodeURIComponent(id) + '/comment', {{method:'POST', body:JSON.stringify({{operator:document.querySelector('#operator').value||'web', comment:document.querySelector('#comment').value||''}})}});
    await loadDetail(id); await loadCases();
  }};
}}
document.querySelector('#saveToken').onclick = () => sessionStorage.setItem('nocToken', token());
document.querySelector('#refresh').onclick = () => loadCases().catch(e => alert(e.message));
document.querySelector('#investigate').onsubmit = async e => {{ e.preventDefault(); const fd = new FormData(e.target); await api('/control/cases/investigate', {{method:'POST', body:JSON.stringify(Object.fromEntries(fd))}}); e.target.prompt.value=''; await loadCases(); }};
document.querySelector('#fastStatus').onsubmit = async e => {{ e.preventDefault(); const fd = new FormData(e.target); const data = await api('/control/status', {{method:'POST', body:JSON.stringify({{target:fd.get('target'), qualifiers:{{}}}})}}); statusOut.textContent = JSON.stringify(data.overview, null, 2); }};
loadCases().catch(e => {{ casesEl.innerHTML = '<div class="case">Authorization required or API unavailable.</div>'; }});
{_PROACTIVE_JS}
</script>
</body>
</html>"""


def _require_signed_callback(request: SignedApprovalRequest, signature: str | None) -> None:
    secret = os.getenv("NOC_APPROVAL_SIGNING_SECRET", "").encode()
    if not secret:
        raise HTTPException(status_code=503, detail="Approval signing secret is not configured")
    if not signature:
        raise HTTPException(status_code=401, detail="Missing callback signature")
    body = json.dumps(request.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise HTTPException(status_code=401, detail="Invalid callback signature")


def main():
    uvicorn.run("app.main:app", host="::", port=8000)


if __name__ == "__main__":
    main()
