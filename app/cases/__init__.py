"""Case-grounded state-machine foundation.

This package is intentionally dormant at first: it defines the typed contracts
and store boundary that later phases will wire into the proactive loop and
reactive IncidentMemory. Runtime behavior remains owned by the existing modules
until the strangler flips land.
"""

from app.cases.correlation import CorrelationService, MetaCaseResult, event_fingerprint_from_parts
from app.cases.handlers import build_default_outbox_handlers, build_report_handler
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
from app.cases.store import CaseStore, InMemoryCaseStore

__all__ = [
    "AtomicCaseProjection",
    "CaseEvent",
    "CaseIdentityAlias",
    "CaseKind",
    "CasePolicy",
    "CaseService",
    "CaseServiceRuntime",
    "CaseStatus",
    "CaseStore",
    "CorrelationService",
    "InMemoryCaseStore",
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
    "ReplayResult",
    "Severity",
    "SourceHealth",
    "TraceRecord",
    "build_case_service_runtime_from_env",
    "build_default_outbox_handlers",
    "build_report_handler",
    "event_fingerprint_from_parts",
    "load_observation_fixture",
    "observation_from_hotspot",
    "observation_from_icinga_alert_payload",
    "observation_identity_fingerprint",
    "observations_from_alertmanager",
    "process_case_outbox_once",
    "replay_observations",
    "stable_signal_signature",
]
