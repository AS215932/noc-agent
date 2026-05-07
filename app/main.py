import os
import shlex
import asyncio
import fcntl
from fastapi import FastAPI, BackgroundTasks, Response, status
from pydantic import BaseModel, Field
import uvicorn
from contextlib import asynccontextmanager, suppress

from app.agent import noc_triage_agent, noc_mail_agent
from app.discord import send_discord_notification, notify_start, notify_finish
from app.mail import check_mailbox_connection, process_mailbox_once, MailSettings
from app.tools.mcp_client import HyruleMCPClient

mcp_client = None
xo_mcp_client = None
mail_poller_task = None
mail_poller_lock_fd = None
MAIL_POLLER_LOCK_PATH = os.getenv("MAIL_POLLER_LOCK_PATH", "/var/lib/noc-agent/mail-poller.lock")

REQUIRED_CONFIG = [
    "GEMINI_API_KEY",
    "DISCORD_WEBHOOK_URL",
    "HYRULE_MCP_CMD",
    "XO_MCP_CMD",
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


async def _mail_poll_loop(lock_fd: int):
    print("Starting background mail polling loop (every 5 mins)...")
    try:
        while True:
            try:
                await process_mailbox_once()
            except Exception as e:
                print(f"Error in mail poll loop: {e}")
            await asyncio.sleep(300)
    finally:
        _release_mail_poller_lock(lock_fd)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp_client
    global xo_mcp_client
    global mail_poller_task
    global mail_poller_lock_fd
    print("NOC Agent starting up...")

    if os.getenv("NOC_AGENT_DISABLE_MCP") != "1":
        hyrule_cmd = shlex.split(os.environ["HYRULE_MCP_CMD"])
        mcp_client = HyruleMCPClient(hyrule_cmd)
        try:
            await mcp_client.connect()
            tools = await mcp_client.get_tools()
            for t in tools:
                noc_triage_agent._function_toolset.add_tool(t)
                print(f"Loaded MCP Tool: {t.name}")
        except Exception as e:
            print(f"Warning: Failed to connect to hyrule-mcp: {e}")

        xo_cmd = shlex.split(os.environ["XO_MCP_CMD"])
        xo_env = os.environ.copy()
        xo_env.setdefault("XO_URL", "https://xoa.as215932.net")
        xo_env.setdefault("XO_MCP_ENABLE_ACTIONS", "0")
        xo_mcp_client = HyruleMCPClient(xo_cmd, env=xo_env)
        try:
            await xo_mcp_client.connect()
            xo_tools = await xo_mcp_client.get_tools()
            for t in xo_tools:
                noc_triage_agent._function_toolset.add_tool(t)
                print(f"Loaded XO MCP Tool: {t.name}")
        except Exception as e:
            print(f"Warning: Failed to connect to Xen Orchestra MCP: {e}")

    if os.getenv("MAIL_IMAP_PASSWORD"):
        mail_poller_lock_fd = _try_acquire_mail_poller_lock()
        if mail_poller_lock_fd is None:
            print("Mail polling disabled in this worker: another worker owns the poller lock.")
        else:
            mail_poller_task = asyncio.create_task(_mail_poll_loop(mail_poller_lock_fd))
    else:
        print("Mail polling disabled: MAIL_IMAP_PASSWORD not configured.")

    yield

    if mail_poller_task:
        mail_poller_task.cancel()
        with suppress(asyncio.CancelledError):
            await mail_poller_task
        mail_poller_task = None
    elif mail_poller_lock_fd is not None:
        _release_mail_poller_lock(mail_poller_lock_fd)
    mail_poller_lock_fd = None

    if mcp_client:
        await mcp_client.disconnect()

    if xo_mcp_client:
        await xo_mcp_client.disconnect()

    print("NOC Agent shutting down...")


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


def _action_plan_text(plan, alert_payload: dict) -> str:
    if plan.requires_human:
        steps = plan.operator_next_steps or _fallback_operator_next_steps(alert_payload)
        return _truncate_discord(
            "**Human review required**\n"
            f"Reason: {_operator_reason(plan)}\n\n"
            "Next steps:\n"
            f"{_format_list(steps, 'No operator steps were provided.')}"
        )

    if plan.automated_actions_proposed:
        return (
            "**Autonomous actions proposed**\n"
            f"{_format_list(plan.automated_actions_proposed, 'No automated actions were proposed.')}"
        )

    return "No further action necessary."


def _triage_fields(plan, alert_payload: dict) -> list[dict]:
    return [
        {"name": "Alert", "value": _alert_overview(alert_payload)},
        {"name": "Assessment", "value": _truncate_discord(plan.diagnosis.root_cause_analysis)},
        {
            "name": "Evidence",
            "value": _format_list(
                plan.diagnostic_evidence,
                "No live diagnostic evidence was recorded. Review the action plan before making changes.",
            ),
        },
        {
            "name": "Tools",
            "value": _format_list(plan.tools_used, "No MCP diagnostics were recorded for this run."),
            "inline": True,
        },
        {"name": "Confidence", "value": f"{plan.diagnosis.confidence_score * 100:.1f}%", "inline": True},
        {"name": "Severity", "value": plan.diagnosis.severity, "inline": True},
        {"name": "Action Plan", "value": _action_plan_text(plan, alert_payload)},
    ]


async def investigate_alert(alert_payload: dict, model=None):
    title_source = alert_payload.get("groupLabels") or alert_payload.get("source") or alert_payload.get("host_name")
    print(f"Starting investigation for alert: {alert_payload.get('status')} - {title_source}")
    await notify_start(f"NOC Triage: Alert from {title_source}", "Initializing investigation and collecting telemetry...")

    prompt = f"We received the following Prometheus AlertManager payload. Please investigate.\n\nPayload: {alert_payload}"
    try:
        result = await noc_triage_agent.run(prompt, model=model)
        plan = result.data if hasattr(result, 'data') else result.output
    except Exception as e:
        await notify_finish(f"NOC Triage: Alert from {title_source}", f"Investigation failed with error: {e}", is_error=True)
        raise

    print("\n[INVESTIGATION COMPLETE]")
    print(f"Summary: {plan.diagnosis.issue_summary}")
    print(f"Confidence: {plan.diagnosis.confidence_score}")
    print(f"Escalate to Human: {plan.requires_human}")

    if plan.requires_human:
        print(f"ESCALATION REASON: {plan.human_escalation_reason}")
    else:
        print("Proposed Auto-actions:")
        for action in plan.automated_actions_proposed:
            print(f"- {action}")

    color = _severity_color(plan.diagnosis.severity, plan.requires_human)
    fields = _triage_fields(plan, alert_payload)
    finish_description = f"Triggered by alert group: {title_source}"
    if plan.requires_human:
        finish_description += "\nStatus: escalated to human review"

    await notify_finish(
        f"NOC Triage: {plan.diagnosis.issue_summary}",
        finish_description,
        is_error=False
    )

    # We still want to send the detailed full embed:
    await send_discord_notification(
        title=f"Detailed Report: {plan.diagnosis.issue_summary}",
        description=_truncate_discord(
            f"{'Escalated to human review' if plan.requires_human else 'Triage completed'} "
            f"with {plan.diagnosis.confidence_score * 100:.1f}% confidence.",
            DISCORD_DESCRIPTION_LIMIT,
        ),
        color=color,
        fields=fields
    )


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
    background_tasks.add_task(investigate_alert, payload.model_dump())
    return {"status": "accepted", "message": "Alert received and agent triggered"}


@app.post("/webhook/icinga")
async def icinga_webhook(payload: IcingaNotification, background_tasks: BackgroundTasks):
    """Receives Icinga2 NotificationCommand POSTs and triggers the NOC agent."""
    background_tasks.add_task(investigate_alert, _icinga_to_alert_payload(payload))
    return {"status": "accepted", "message": "Icinga notification accepted"}


@app.post("/mail/poll", response_model=MailPollResponse)
async def poll_mailbox(background_tasks: BackgroundTasks):
    """Poll the shared NOC mailbox and store draft replies for human approval."""
    background_tasks.add_task(process_mailbox_once)
    return {"status": "accepted", "message": "Mailbox poll queued; drafts require human approval"}


class TaskRequest(BaseModel):
    prompt: str

@app.post("/task", response_model=MailPollResponse)
async def run_task(request: TaskRequest, background_tasks: BackgroundTasks):
    """Run an arbitrary task on the NOC Triage agent (e.g. 'Draft email to LocIX')."""
    async def _run_task():
        await notify_start("Manual Task", f"Task: {request.prompt}")
        try:
            result = await noc_triage_agent.run(request.prompt)
            plan = result.data if hasattr(result, 'data') else result.output
            await notify_finish("Manual Task", f"Task completed: {plan.diagnosis.issue_summary}")
        except Exception as e:
            await notify_finish("Manual Task", f"Task failed: {e}", is_error=True)

    background_tasks.add_task(_run_task)
    return {"status": "accepted", "message": "Task queued"}

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "AS215932 NOC Agent"}


@app.get("/health/mcp")
async def health_mcp(response: Response):
    health = {
        "hyrule": mcp_client is not None and mcp_client.session is not None,
        "xo": xo_mcp_client is not None and xo_mcp_client.session is not None,
    }
    health["status"] = "ok" if all(health.values()) else "degraded"
    if health["status"] != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return health


@app.get("/health/config")
async def health_config(response: Response):
    missing = [name for name in REQUIRED_CONFIG if not os.getenv(name)]
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


@app.get("/health/mail")
async def health_mail(response: Response):
    try:
        return check_mailbox_connection()
    except Exception as e:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "degraded",
            "error": str(e),
        }


def main():
    uvicorn.run("app.main:app", host="::", port=8000)


if __name__ == "__main__":
    main()
