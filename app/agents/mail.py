from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from app.model_config import build_agent_model


class MailDraftPlan(BaseModel):
    classification: str = Field(description="Operational category such as noc, abuse, peering, dh, billing, or unknown")
    urgency: str = Field(description="Urgency of the message: HIGH, MEDIUM, or LOW")
    summary: str = Field(description="Short operational summary of the inbound email")
    requires_human: bool = Field(default=True, description="Always true for v1 draft-and-approve handling")
    suggested_reply_subject: str = Field(description="Subject line for the draft response")
    reply_summary: str = Field(description="A brief summary of what the drafted response says")
    suggested_reply_body: str = Field(description="Plain-text draft response for human review")
    internal_notes: list[str] = Field(default_factory=list, description="Notes for the operator reviewing the draft")


def build_mail_agent(model=None) -> Agent[None, MailDraftPlan]:
    return Agent(
        model or build_agent_model(),
        output_type=MailDraftPlan,
        defer_model_check=True,
        output_retries=2,
        system_prompt=(
            "You are the AS215932 operational email assistant. "
            "Classify inbound operational email, summarize it, and draft a concise professional reply. "
            "For v1, every response MUST require human approval and MUST NOT be sent automatically."
        ),
    )


noc_mail_agent = build_mail_agent()

