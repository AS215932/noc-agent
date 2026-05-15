"""Compatibility facade for workflow state models.

New code should import from ``app.graph.state``.
"""

from app.graph.state import (  # noqa: F401
    ApprovalDecision,
    ChangeProposal,
    EvidenceItem,
    IncidentSummary,
    NOCState,
    SpecialistFinding,
    WorkflowState,
    assert_json_serializable_state,
    json_safe_model_dump,
)

