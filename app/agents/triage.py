from __future__ import annotations

import asyncio
import imaplib
import time
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator
from pydantic_ai import Agent, RunContext

from app.model_config import build_agent_model
from app.safe_errors import classify_exception, log_exception


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


@dataclass(slots=True)
class TriageAgentDeps:
    perimeter_context: str = ""


def build_triage_agent(model=None) -> Agent[TriageAgentDeps, ActionPlan]:
    agent = Agent(
        model or build_agent_model(),
        output_type=ActionPlan,
        deps_type=TriageAgentDeps,
        defer_model_check=True,
        output_retries=2,
        system_prompt=(
            "You are an expert Senior NOC Engineer for AS215932, a modern ISP. "
            "LangGraph owns workflow routing and approval; your job is focused diagnostic reasoning. "
            "Use available read-only MCP tools to gather bounded telemetry, then return a validated ActionPlan. "
            "Populate diagnostic_evidence with concrete observations and tools_used with each data source or command used. "
            "If a tool returns unsupported_os, choose an OS-compatible diagnostic path such as rcctl on OpenBSD or systemd on Linux. "
            "Never propose or imply that destructive remediation has run. Default posture is diagnostic-only."
        ),
    )

    @agent.tool
    async def create_email_draft(ctx: RunContext[TriageAgentDeps], to_address: str, subject: str, text_body: str) -> str:
        """Create a draft email for human review."""
        return await asyncio.to_thread(_save_email_draft_sync, to_address, subject, text_body)

    return agent


def _save_email_draft_sync(to_address: str, subject: str, text_body: str) -> str:
    from app.mail import MailSettings

    settings = MailSettings.from_env()
    msg = EmailMessage()
    msg["From"] = "noc@as215932.net"
    msg["To"] = to_address
    msg["Subject"] = f"DRAFT: {subject}"
    msg.set_content(text_body)
    try:
        if settings.imap_password:
            with imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port, timeout=settings.imap_timeout) as client:
                client.login(settings.imap_user, settings.imap_password)
                client.append("Drafts", "\\Draft", imaplib.Time2Internaldate(time.time()), msg.as_bytes())
            return f"Draft saved successfully to {to_address} with subject {subject}."
        return "Failed to save draft: MAIL_IMAP_PASSWORD not provided."
    except Exception as exc:
        safe = classify_exception(exc)
        log_exception("email_draft_save_failed", exc, category=safe.category, provider=safe.provider)
        return "Failed to save IMAP draft: infrastructure issue. A human operator should review mail health."


noc_triage_agent = build_triage_agent()

