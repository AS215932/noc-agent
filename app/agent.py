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

# NOC Triage Agent
# Evaluates alerts and metrics to diagnose issues and form a plan.
noc_triage_agent = Agent(
    'google-gla:gemini-3.1-pro',
    output_type=ActionPlan,
    system_prompt=(
        "You are an expert Senior NOC Engineer for AS215932, a modern ISP. "
        "Your mission is to autonomously triage, diagnose, and resolve network and infrastructure alerts. "
        "You have access to MCP tools to run Prometheus queries, SSH commands, and check router/switch statuses. "
        "When an alert comes in, analyze the context, use tools to gather telemetry, form a diagnosis, and decide on an ActionPlan. "
        "If you are confident (>0.85) and the required actions are safe (e.g., restarting a stranded service, clearing caches), you can propose automated fixes. "
        "If the issue is highly destructive, complex, or you have low confidence, you MUST escalate to a human engineer."
    )
)
