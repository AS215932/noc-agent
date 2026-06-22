"""Case-grounded state-machine foundation.

This package defines the typed contracts and store boundary used by the
CaseService-backed proactive, reactive-primary, and control-primary paths.
"""

from app.cases.correlation import CorrelationService, MetaCaseResult, event_fingerprint_from_parts
from app.cases.handlers import build_default_outbox_handlers, build_engineering_lhp_handoff_handler, build_handoff_handler, build_report_handler
from app.cases.lhp import (
    CallbackInboxRecord,
    CaseHandoff,
    EvidenceRef,
    HandoffTransportDelivery,
    HandoffUpdate,
    KnowledgeArtifact,
    OutcomeRecord,
    VerificationObjective,
    allowed_handoff_transition,
    assert_lhp_payload_size,
    build_loop_signature,
    lhp_payload_hash,
    require_handoff_transition,
    sanitize_lhp_payload,
    sanitize_lhp_text,
    verify_loop_signature,
)
from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    CaseKind,
    CaseStatus,
    MetaCaseProjection,
    ObservationRecord,
    ObservationStatus,
    OperatorFeedback,
    OutboxIntent,
    OutboxStatus,
    Severity,
    SourceHealth,
    TraceRecord,
    stable_signal_signature,
)
from app.cases.notifications import observation_from_icinga_alert_payload, observations_from_alertmanager
from app.cases.outbox import OutboxHandlerResult, OutboxProcessReport, OutboxProcessor
from app.cases.policy import CasePolicy
from app.cases.proactive import observation_from_hotspot
from app.cases.replay import ReplayResult, load_observation_fixture, replay_observations
from app.cases.runtime import CaseServiceRuntime, build_case_service_runtime_from_env, process_case_outbox_once
from app.cases.service import CaseService, ObserveResult, observation_identity_fingerprint
from app.cases.store import CallbackClaimResult, CaseStore, HandoffCreateResult, HandoffUpdateResult, InMemoryCaseStore

__all__ = [
    "AtomicCaseProjection",
    "CallbackClaimResult",
    "CallbackInboxRecord",
    "CaseEvent",
    "CaseHandoff",
    "CaseIdentityAlias",
    "CaseKind",
    "CasePolicy",
    "CaseService",
    "CaseServiceRuntime",
    "CaseStatus",
    "CaseStore",
    "CorrelationService",
    "EvidenceRef",
    "HandoffCreateResult",
    "HandoffTransportDelivery",
    "HandoffUpdate",
    "HandoffUpdateResult",
    "InMemoryCaseStore",
    "KnowledgeArtifact",
    "MetaCaseProjection",
    "MetaCaseResult",
    "ObservationRecord",
    "ObservationStatus",
    "ObserveResult",
    "OperatorFeedback",
    "OutboxHandlerResult",
    "OutboxIntent",
    "OutboxProcessReport",
    "OutboxProcessor",
    "OutboxStatus",
    "OutcomeRecord",
    "ReplayResult",
    "Severity",
    "SourceHealth",
    "TraceRecord",
    "VerificationObjective",
    "allowed_handoff_transition",
    "assert_lhp_payload_size",
    "build_case_service_runtime_from_env",
    "build_default_outbox_handlers",
    "build_engineering_lhp_handoff_handler",
    "build_handoff_handler",
    "build_loop_signature",
    "build_report_handler",
    "event_fingerprint_from_parts",
    "lhp_payload_hash",
    "load_observation_fixture",
    "observation_from_hotspot",
    "observation_from_icinga_alert_payload",
    "observation_identity_fingerprint",
    "observations_from_alertmanager",
    "process_case_outbox_once",
    "replay_observations",
    "require_handoff_transition",
    "sanitize_lhp_payload",
    "sanitize_lhp_text",
    "stable_signal_signature",
    "verify_loop_signature",
]
