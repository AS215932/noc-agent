"""Compatibility facade for agent models and factories.

New code should import from ``app.agents``.
"""

from app.agents import (  # noqa: F401
    ActionPlan,
    DiagnosisResult,
    MailDraftPlan,
    TriageAgentDeps,
    build_mail_agent,
    build_triage_agent,
    noc_mail_agent,
    noc_triage_agent,
)

