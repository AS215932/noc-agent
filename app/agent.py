import os
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from dotenv import load_dotenv

load_dotenv()

class DiagnosisResult(BaseModel):
    issue_summary: str = Field(description="A brief 1-2 sentence description of the diagnosed issue")
    root_cause_analysis: str = Field(description="Detailed explanation of what is failing and why")
    confidence_score: float = Field(description="Confidence that this diagnosis is correct, from 0.0 to 1.0", ge=0.0, le=1.0)
    severity: str = Field(description="Severity of the issue: HIGH, MEDIUM, or LOW")

class ActionPlan(BaseModel):
    diagnosis: DiagnosisResult
    requires_human: bool = Field(description="True if the issue requires human intervention due to high impact/risk or low confidence")
    automated_actions_proposed: list[str] = Field(description="Steps the agent will take to resolve or mitigate the issue")
    human_escalation_reason: str | None = Field(default=None, description="Reason for escalating to a human, if applicable")

class MailDraftPlan(BaseModel):
    classification: str = Field(description="Operational category such as noc, abuse, peering, dh, billing, or unknown")
    urgency: str = Field(description="Urgency of the message: HIGH, MEDIUM, or LOW")
    summary: str = Field(description="Short operational summary of the inbound email")
    requires_human: bool = Field(default=True, description="Always true for v1 draft-and-approve handling")
    suggested_reply_subject: str = Field(description="Subject line for the draft response")
    suggested_reply_body: str = Field(description="Plain-text draft response for human review")
    internal_notes: list[str] = Field(default_factory=list, description="Notes for the operator reviewing the draft")

# NOC Triage Agent
# Evaluates alerts and metrics to diagnose issues and form a plan.
AGENT_MODEL = os.environ.get("AGENT_MODEL", "google-gla:gemini-3.1-pro")

noc_triage_agent = Agent(
    AGENT_MODEL,
    output_type=ActionPlan,
    defer_model_check=True,
    system_prompt=(
        "You are an expert Senior NOC Engineer for AS215932, a modern ISP. "
        "Your mission is to autonomously triage, diagnose, and resolve network and infrastructure alerts. "
        "You have access to MCP tools to run Prometheus queries, SSH commands, and check router/switch statuses. "
        "When an alert comes in, analyze the context, use tools to gather telemetry, form a diagnosis, and decide on an ActionPlan. "
        "If you are confident (>0.85) and the required actions are safe (e.g., restarting a stranded service, clearing caches), you can propose automated fixes. "
        "If the issue is highly destructive, complex, or you have low confidence, you MUST escalate to a human engineer."
    )
)

noc_mail_agent = Agent(
    AGENT_MODEL,
    output_type=MailDraftPlan,
    defer_model_check=True,
    system_prompt=(
        "You are the AS215932 operational email assistant. "
        "You handle mail sent to noc@as215932.net, abuse@as215932.net, "
        "peering@as215932.net, and dh@as215932.net. "
        "Classify the email, summarize what is being requested, and draft a concise professional reply. "
        "Do not claim that an action was completed unless the email context proves it. "
        "For abuse reports, preserve evidence references and avoid admitting liability. "
        "For peering requests, ask for or confirm ASN, PeeringDB, locations, sessions, and max-prefix details as needed. "
        "For v1, every response MUST require human approval and MUST NOT be sent automatically."
    )
)
