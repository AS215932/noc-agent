import os
import shlex
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
import uvicorn
from contextlib import asynccontextmanager

from app.agent import noc_triage_agent
from app.discord import send_discord_notification
from app.mail import process_mailbox_once
from app.tools.mcp_client import HyruleMCPClient

mcp_client = None
xo_mcp_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp_client
    global xo_mcp_client
    print("NOC Agent starting up...")

    if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
        yield
        print("NOC Agent shutting down...")
        return

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

    yield

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


async def investigate_alert(alert_payload: dict, model=None):
    print(f"Starting investigation for alert: {alert_payload.get('status')} - {alert_payload.get('groupLabels')}")

    prompt = f"We received the following Prometheus AlertManager payload. Please investigate.\n\nPayload: {alert_payload}"

    result = await noc_triage_agent.run(prompt, model=model)
    plan = result.data if hasattr(result, 'data') else result.output

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

    color = 0xe74c3c if plan.requires_human else 0x2ecc71
    fields = [
        {"name": "Root Cause", "value": plan.diagnosis.root_cause_analysis},
        {"name": "Confidence", "value": f"{plan.diagnosis.confidence_score * 100:.1f}%", "inline": True},
        {"name": "Severity", "value": plan.diagnosis.severity, "inline": True},
    ]

    if plan.requires_human:
        action_text = f"🚨 **HUMAN ESCALATION REQUIRED** 🚨\nReason: {plan.human_escalation_reason}"
    elif plan.automated_actions_proposed:
        action_text = "🤖 **Autonomous Actions Proposed:**\n" + "\n".join(f"- {a}" for a in plan.automated_actions_proposed)
    else:
        action_text = "No further action necessary."

    fields.append({"name": "Action Plan", "value": action_text})

    title_source = alert_payload.get("groupLabels") or alert_payload.get("source") or alert_payload.get("host_name")
    await send_discord_notification(
        title=f"NOC Triage: {plan.diagnosis.issue_summary}",
        description=f"Triggered by alert group: {title_source}",
        color=color,
        fields=fields,
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


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "AS215932 NOC Agent"}


@app.get("/health/mcp")
async def health_mcp():
    return {
        "hyrule": mcp_client is not None and mcp_client.session is not None,
        "xo": xo_mcp_client is not None and xo_mcp_client.session is not None,
    }


def main():
    uvicorn.run("app.main:app", host="::", port=8000)


if __name__ == "__main__":
    main()
