import sys
import os
from fastapi import FastAPI, BackgroundTasks, Request
from pydantic import BaseModel
import uvicorn
from contextlib import asynccontextmanager

from app.agent import noc_triage_agent
from app.discord import send_discord_notification
from app.mail import process_mailbox_once
from app.tools.mcp_client import HyruleMCPClient

# Global client
mcp_client = None
xo_mcp_client = None

# Standard context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp_client
    global xo_mcp_client
    print("NOC Agent starting up...")

    if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
        yield
        print("NOC Agent shutting down...")
        return
    
    # Path to the actual MCP server script. Depending on the environment, we run the python module.
    # To run locally we'll point to our Dev/hyrule-mcp folder.
    hyrule_mcp_cmd = [sys.executable, "-m", "mcp_server"]
    hyrule_mcp_cwd = "/home/svag/Dev/hyrule-mcp"
    
    # FastMCP uses the target script directly.
    # To use the target venv we pass the correct python binary path
    cmd = ["/home/svag/Dev/hyrule-mcp/.venv/bin/python", "/home/svag/Dev/hyrule-mcp/mcp_server.py"]
    
    mcp_client = HyruleMCPClient(cmd)
    try:
        await mcp_client.connect()
        # Fetch the tools uniquely exposed dynamically by the AS215932 Infrastructure God-Server
        tools = await mcp_client.get_tools()
        for t in tools:
            # Register the dynamic tool to the Pydantic AI agent
            noc_triage_agent._function_toolset.add_tool(t)
            print(f"Loaded MCP Tool: {t.name}")
    except Exception as e:
        print(f"Warning: Failed to connect to MCP: {e}")

    # ===== XOA MCP Integration =====
    xo_cmd = ["npx", "-y", "@xen-orchestra/mcp"]
    xo_mcp_client = HyruleMCPClient(xo_cmd)
    
    # We patch the client connect to use custom environment variables for xo_env
    # Defaults provided - in production this handles credentials for Xen Orchestra.
    original_env = os.environ.copy()
    if 'XO_URL' not in os.environ:
        os.environ['XO_URL'] = 'https://xoa.as215932.net'
    
    # Expose write operations like EmergencyShutdownPool, HardShutdownVm, etc.
    if 'XO_MCP_ENABLE_ACTIONS' not in os.environ:
        os.environ['XO_MCP_ENABLE_ACTIONS'] = '1'

    try:
        await xo_mcp_client.connect()
        xo_tools = await xo_mcp_client.get_tools()
        for t in xo_tools:
            noc_triage_agent._function_toolset.add_tool(t)
            print(f"Loaded XO MCP Tool: {t.name}")
    except Exception as e:
        print(f"Warning: Failed to connect to Xen Orchestra MCP: {e}")
    finally:
        os.environ.clear()
        os.environ.update(original_env)

    yield
    
    if mcp_client:
        await mcp_client.disconnect()
    
    if xo_mcp_client:
        await xo_mcp_client.disconnect()

    print("NOC Agent shutting down...")
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

class MailPollResponse(BaseModel):
    status: str
    message: str

async def investigate_alert(alert_payload: dict, model=None):
    print(f"Starting investigation for alert: {alert_payload.get('status')} - {alert_payload.get('groupLabels')}")
    
    # We pass the alert data as a prompt to the PydanticAI NOC Agent.
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
            
    # Group results for Discord
    color = 0xe74c3c if plan.requires_human else 0x2ecc71 # Red for escalation, Green for autonomous
    fields = [
        {"name": "Root Cause", "value": plan.diagnosis.root_cause_analysis},
        {"name": "Confidence", "value": f"{plan.diagnosis.confidence_score * 100:.1f}%", "inline": True},
        {"name": "Severity", "value": plan.diagnosis.severity, "inline": True},
    ]

    action_text = ""
    if plan.requires_human:
        action_text = f"🚨 **HUMAN ESCALATION REQUIRED** 🚨\nReason: {plan.human_escalation_reason}"
    elif plan.automated_actions_proposed:
        action_text = "🤖 **Autonomous Actions Proposed:**\n" + "\n".join(f"- {a}" for a in plan.automated_actions_proposed)
    else:
        action_text = "No further action necessary."

    fields.append({"name": "Action Plan", "value": action_text})

    await send_discord_notification(
        title=f"NOC Triage: {plan.diagnosis.issue_summary}",
        description=f"Triggered by alert group: {alert_payload.get('groupLabels')}",
        color=color,
        fields=fields
    )

@app.post("/webhook/alertmanager")
async def alertmanager_webhook(payload: AlertManagerPayload, background_tasks: BackgroundTasks):
    """
    Receives alerts from Prometheus Alertmanager and triggers the NOC agent.
    """
    # Run the investigation in the background to avoid blocking the webhook response
    background_tasks.add_task(investigate_alert, payload.model_dump())
    return {"status": "accepted", "message": "Alert received and agent triggered"}

@app.post("/mail/poll", response_model=MailPollResponse)
async def poll_mailbox(background_tasks: BackgroundTasks):
    """
    Poll the shared NOC mailbox and store draft replies for human approval.
    """
    background_tasks.add_task(process_mailbox_once)
    return {"status": "accepted", "message": "Mailbox poll queued; drafts require human approval"}

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "AS215932 NOC Agent"}

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
