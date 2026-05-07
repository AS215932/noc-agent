import os
from typing import Any
from pydantic import BaseModel, Field, model_validator
from pydantic_ai import Agent, RunContext
from dotenv import load_dotenv

load_dotenv()

class DiagnosisResult(BaseModel):
    issue_summary: str = Field(description="A brief 1-2 sentence description of the diagnosed issue")
    root_cause_analysis: str = Field(description="Detailed explanation of what is failing and why")
    confidence_score: float = Field(description="Confidence that this diagnosis is correct, from 0.0 to 1.0", ge=0.0, le=1.0)
    severity: str = Field(description="Severity of the issue: HIGH, MEDIUM, or LOW")

class ActionPlan(BaseModel):
    issue_summary: str = Field(description="A brief 1-2 sentence description of the diagnosed issue")
    root_cause_analysis: str = Field(description="Detailed explanation of what is failing and why")
    confidence_score: float = Field(description="Confidence that this diagnosis is correct, from 0.0 to 1.0", ge=0.0, le=1.0)
    severity: str = Field(description="Severity of the issue: HIGH, MEDIUM, or LOW")
    requires_human: bool = Field(description="True if the issue requires human intervention due to high impact/risk or low confidence")
    automated_actions_proposed: list[str] = Field(default_factory=list, description="Safe remediation steps the agent recommends or can take")
    human_escalation_reason: str = Field(default="", description="Reason for escalating to a human, if applicable; leave empty when not escalating")
    diagnostic_evidence: list[str] = Field(default_factory=list, description="Specific telemetry, command output, or alert facts used in the diagnosis")
    tools_used: list[str] = Field(default_factory=list, description="Diagnostic tools or data sources used, such as Prometheus query names or SSH commands")
    operator_next_steps: list[str] = Field(default_factory=list, description="Concrete next actions for the on-call operator")

    @model_validator(mode="before")
    @classmethod
    def _accept_legacy_nested_diagnosis(cls, value: Any) -> Any:
        if isinstance(value, dict) and isinstance(value.get("diagnosis"), dict):
            diagnosis = value["diagnosis"]
            flattened = {**value}
            for key in ("issue_summary", "root_cause_analysis", "confidence_score", "severity"):
                flattened.setdefault(key, diagnosis.get(key))
            flattened.pop("diagnosis", None)
            return flattened
        return value

    @property
    def diagnosis(self) -> DiagnosisResult:
        return DiagnosisResult(
            issue_summary=self.issue_summary,
            root_cause_analysis=self.root_cause_analysis,
            confidence_score=self.confidence_score,
            severity=self.severity,
        )

class MailDraftPlan(BaseModel):
    classification: str = Field(description="Operational category such as noc, abuse, peering, dh, billing, or unknown")
    urgency: str = Field(description="Urgency of the message: HIGH, MEDIUM, or LOW")
    summary: str = Field(description="Short operational summary of the inbound email")
    requires_human: bool = Field(default=True, description="Always true for v1 draft-and-approve handling")
    suggested_reply_subject: str = Field(description="Subject line for the draft response")
    reply_summary: str = Field(description="A brief summary of what the drafted response says")
    suggested_reply_body: str = Field(description="Plain-text draft response for human review")
    internal_notes: list[str] = Field(default_factory=list, description="Notes for the operator reviewing the draft")

import imaplib
import time
from email.message import EmailMessage
from app.discord import notify_start, notify_finish
from app.model_config import build_agent_model
from app.safe_errors import classify_exception, log_exception

# NOC Triage Agent
# Evaluates alerts and metrics to diagnose issues and form a plan.
AGENT_MODEL = build_agent_model()

noc_triage_agent = Agent(
    AGENT_MODEL,
    output_type=ActionPlan,
    defer_model_check=True,
    system_prompt=(
        "You are an expert Senior NOC Engineer for AS215932, a modern ISP. "
        "Your mission is to autonomously triage, diagnose, and resolve network and infrastructure alerts. "
        "You have access to MCP tools to run Prometheus queries, SSH commands, and check router/switch statuses. "
        "When an alert comes in, analyze the context, use tools to gather telemetry, form a diagnosis, and decide on an ActionPlan. "
        "Populate diagnostic_evidence with concrete observations and tools_used with each data source or command you used. "
        "If a diagnostic tool fails or is unavailable, say that plainly as missing evidence; never describe provider, schema, or framework internals to the operator. "
        "When escalation is required, provide operator_next_steps that a human can execute immediately. "
        "If you are confident (>0.85) and the required actions are safe (e.g., restarting a stranded service, clearing caches), you can propose automated fixes. "
        "If the issue is highly destructive, complex, or you have low confidence, you MUST escalate to a human engineer.\n"
        "You can also use create_email_draft tooling to proactively write messages (e.g. peering requests to IXPs) which will be appended to the Drafts folder for a human to review."
    )
)

@noc_triage_agent.tool
async def create_email_draft(ctx: RunContext, to_address: str, subject: str, text_body: str) -> str:
    """
    Creates an outbound email draft and saves it to the IMAP Drafts folder for human review.
    Use this exactly when you need to send an email to peering partners, abuse desks, or IXPs.
    """
    from app.mail import MailSettings
    settings = MailSettings.from_env()

    await notify_start("Email Drafting", f"Drafting proactive message to '{to_address}'...")

    msg = EmailMessage()
    msg["From"] = "noc@as215932.net"
    msg["To"] = to_address
    msg["Subject"] = f"DRAFT: {subject}"
    msg.set_content(text_body)

    try:
        if settings.imap_password:
            with imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port) as client:
                client.login(settings.imap_user, settings.imap_password)
                client.append("Drafts", '\\Draft', imaplib.Time2Internaldate(time.time()), msg.as_bytes())
            await notify_finish("Email Drafting", f"Saved drafted email to {to_address} to Drafts folder")
            return f"Draft saved successfully to {to_address} with subject {subject}."
        else:
            return "Failed to save draft: MAIL_IMAP_PASSWORD not provided."
    except Exception as e:
        safe = classify_exception(e)
        log_exception("email_draft_save_failed", e, category=safe.category, provider=safe.provider)
        await notify_finish(
            "Email Drafting",
            safe.discord_description("Email draft storage"),
            is_error=True,
            safe_category=safe.category,
        )
        return "Failed to save IMAP draft: infrastructure issue. A human operator should review mail health."

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
