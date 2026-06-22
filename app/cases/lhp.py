"""Loop Handoff Protocol v1 typed contracts and safety helpers.

LHP-v1 is the cross-loop contract between the NOC Agent Loop,
Engineering Agent Loop, Knowledge Loop, and future peer loops.  This module is
intentionally transport-neutral and side-effect free: it defines bounded
Pydantic schemas, state-machine helpers, payload hashing/signing primitives, and
sanitizers.  Service/store methods and transports are layered on top in later
tranches.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

LHP_SCHEMA_VERSION = "lhp.v1"
DEFAULT_LHP_CALLBACK_MAX_BYTES = 65_536
DEFAULT_TEXT_LIMIT = 1_000
MAX_COLLECTION_ITEMS = 50
MAX_MAPPING_ITEMS = 100
MAX_PAYLOAD_DEPTH = 6

LoopName = Literal["noc", "engineering", "knowledge", "soc"]
HandoffStatus = Literal[
    "requested",
    "accepted",
    "in_progress",
    "change_planned",
    "implemented",
    "blocked",
    "failed",
    "needs_human",
    "verified",
    "resolved",
    "cancelled",
    "expired",
]
HandoffUpdateType = Literal[
    "accepted",
    "investigating",
    "blocked",
    "change_planned",
    "change_applied",
    "implemented",
    "failed",
    "needs_human",
]
VerificationStatus = Literal["pending", "pass", "fail", "unknown", "skipped"]
KnowledgeArtifactStatus = Literal["proposed", "approved", "rejected", "superseded", "deprecated", "published"]
KnowledgeReviewStatus = Literal["pending", "approved", "rejected", "superseded", "deprecated", "published"]
HandoffTransport = Literal["github_issue", "http", "queue"]
DeliveryStatus = Literal["pending", "in_progress", "succeeded", "failed", "abandoned"]
CallbackStatus = Literal["accepted", "duplicate", "rejected"]

TERMINAL_HANDOFF_STATUSES = frozenset({"resolved", "cancelled", "expired"})
VERIFIER_ONLY_HANDOFF_STATUSES = frozenset({"verified", "resolved"})
_ALLOWED_HANDOFF_TRANSITIONS: dict[str, frozenset[str]] = {
    "requested": frozenset({"accepted", "blocked", "failed", "needs_human", "cancelled", "expired"}),
    "accepted": frozenset({"in_progress", "blocked", "failed", "needs_human", "cancelled", "expired"}),
    "in_progress": frozenset(
        {"blocked", "failed", "needs_human", "change_planned", "implemented", "cancelled", "expired"}
    ),
    "change_planned": frozenset({"blocked", "failed", "needs_human", "implemented", "cancelled", "expired"}),
    "implemented": frozenset({"failed", "needs_human", "verified", "cancelled", "expired"}),
    "verified": frozenset({"resolved", "failed", "needs_human"}),
    "blocked": frozenset({"accepted", "needs_human", "failed", "cancelled", "expired"}),
    "failed": frozenset({"accepted", "needs_human", "cancelled", "expired"}),
    "needs_human": frozenset({"accepted", "in_progress", "cancelled", "expired"}),
    "resolved": frozenset(),
    "cancelled": frozenset(),
    "expired": frozenset(),
}

_SECRET_TEXT_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", re.I),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.I),
    re.compile(r"\bAuthorization\s*:\s*[^\s]+(?:\s+[^\s]+)?", re.I),
    re.compile(r"\b(password|passwd|secret|token|credential)\s*[:=]\s*[^\s,;]+", re.I),
)
_FORBIDDEN_TEXT_MARKERS = {"raw_log", "packet_capture", "transcript", "authorization", "credential", "secret"}
_BLOCKED_TEXT_CHARS = set("`<>[]{}")
_TOKEN_ALLOWED = {"_", "-", ":", ".", "/"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_lhp_json(value: Any) -> str:
    """Stable JSON for hashing/signing bounded LHP payloads."""

    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def lhp_payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_lhp_json(value).encode("utf-8")).hexdigest()


def lhp_payload_size(value: Any) -> int:
    return len(canonical_lhp_json(value).encode("utf-8"))


def assert_lhp_payload_size(value: Any, *, max_bytes: int = DEFAULT_LHP_CALLBACK_MAX_BYTES) -> None:
    size = lhp_payload_size(value)
    if size > max(0, max_bytes):
        raise ValueError(f"LHP payload exceeds max size: {size} > {max_bytes}")


def sanitize_lhp_token(value: Any, *, limit: int = 128) -> str:
    text = str(value or "")
    kept = [ch for ch in text if ch.isalnum() or ch in _TOKEN_ALLOWED]
    return ("".join(kept) or "unknown")[: max(1, limit)]


def require_lhp_token(value: Any, *, field_name: str, limit: int = 128) -> str:
    if not str(value or "").strip():
        raise ValueError(f"{field_name} is required")
    return sanitize_lhp_token(value, limit=limit)


def require_lhp_text(value: Any, *, field_name: str, limit: int = DEFAULT_TEXT_LIMIT) -> str:
    if not str(value or "").strip():
        raise ValueError(f"{field_name} is required")
    return sanitize_lhp_text(value, limit=limit)


def sanitize_lhp_text(value: Any, *, limit: int = DEFAULT_TEXT_LIMIT) -> str:
    """Return single-line, redacted, prompt-injection-resistant evidence text."""

    text = str(value or "")
    for pattern in _SECRET_TEXT_PATTERNS:
        text = pattern.sub("[redacted]", text)
    cleaned = []
    for ch in text:
        if ch in _BLOCKED_TEXT_CHARS or ord(ch) < 32:
            cleaned.append(" ")
        else:
            cleaned.append(ch)
    rendered = " ".join("".join(cleaned).split())
    return (rendered or "—")[: max(1, limit)]


def sanitize_lhp_payload(
    value: Any,
    *,
    string_limit: int = 2_000,
    max_depth: int = MAX_PAYLOAD_DEPTH,
) -> Any:
    """Recursively bound and redact untrusted LHP evidence payloads."""

    if max_depth <= 0:
        return sanitize_lhp_text(value, limit=string_limit)
    if isinstance(value, BaseModel):
        return sanitize_lhp_payload(value.model_dump(mode="json"), string_limit=string_limit, max_depth=max_depth - 1)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return sanitize_lhp_text(value, limit=string_limit)
    if isinstance(value, bool | int | float) or value is None:
        return value
    if isinstance(value, list | tuple | set):
        return [
            sanitize_lhp_payload(item, string_limit=string_limit, max_depth=max_depth - 1)
            for item in list(value)[:MAX_COLLECTION_ITEMS]
        ]
    if isinstance(value, dict):
        rendered: dict[str, Any] = {}
        for key, child in list(value.items())[:MAX_MAPPING_ITEMS]:
            safe_key = sanitize_lhp_token(key, limit=80)
            lowered = safe_key.lower()
            if any(marker in lowered for marker in _FORBIDDEN_TEXT_MARKERS):
                safe_key = "redacted_key"
            rendered[safe_key] = sanitize_lhp_payload(child, string_limit=string_limit, max_depth=max_depth - 1)
        return rendered
    return sanitize_lhp_text(value, limit=string_limit)


def build_loop_signature(*, secret: str, method: str, path: str, timestamp: str, body: Any) -> str:
    """HMAC signature used by loop-to-loop fetch/callback traffic."""

    if not secret:
        raise ValueError("loop signature secret is required")
    message = "\n".join(
        [
            sanitize_lhp_token(method.upper(), limit=16),
            str(path or ""),
            sanitize_lhp_token(timestamp, limit=80),
            canonical_lhp_json(body),
        ]
    ).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def verify_loop_signature(
    *, secret: str, method: str, path: str, timestamp: str, body: Any, signature: str | None
) -> bool:
    if not signature:
        return False
    expected = build_loop_signature(secret=secret, method=method, path=path, timestamp=timestamp, body=body)
    return hmac.compare_digest(signature, expected)


def allowed_handoff_transition(current: HandoffStatus, target: HandoffStatus, *, actor_loop: LoopName = "noc") -> bool:
    if target in VERIFIER_ONLY_HANDOFF_STATUSES and actor_loop != "noc":
        return False
    if current == target:
        return True
    return target in _ALLOWED_HANDOFF_TRANSITIONS.get(current, frozenset())


def require_handoff_transition(current: HandoffStatus, target: HandoffStatus, *, actor_loop: LoopName = "noc") -> None:
    if not allowed_handoff_transition(current, target, actor_loop=actor_loop):
        raise ValueError(f"invalid LHP handoff transition {current!r} -> {target!r} for {actor_loop!r}")


def _json_safe(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_safe(value.model_dump(mode="json"))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, list | tuple | set):
        return [_json_safe(child) for child in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


class EvidenceRef(BaseModel):
    """A bounded reference to evidence stored elsewhere or summarized safely."""

    model_config = ConfigDict(extra="forbid")

    type: str
    ref: str
    summary: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type", "ref", mode="before")
    @classmethod
    def _safe_token(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=160)

    @field_validator("summary", mode="before")
    @classmethod
    def _safe_summary(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=700)

    @field_validator("payload", mode="before")
    @classmethod
    def _safe_payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}


class CaseHandoff(BaseModel):
    """Current-state record for one structured cross-loop handoff."""

    model_config = ConfigDict(extra="forbid")

    handoff_id: str = Field(default_factory=lambda: f"handoff_{uuid4().hex[:12]}")
    case_id: str
    source_loop: LoopName = "noc"
    target_loop: LoopName
    objective: str
    objective_key: str
    knowledge_scope: str = ""
    status: HandoffStatus = "requested"
    owner: str = ""
    verifier: LoopName = "noc"
    idempotency_key: str
    fingerprint: str = ""
    resource: dict[str, Any] = Field(default_factory=dict)
    case_type: str = ""
    constraints: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(default_factory=list)
    knowledge_context_refs: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str = Field(default_factory=lambda: f"corr_{uuid4().hex[:12]}")
    trace_id: str = ""
    created_by: str = "noc_agent_loop"
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    expires_at: str = ""
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("handoff_id", "fingerprint", "case_type", "correlation_id", "trace_id", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("case_id", "objective_key", "idempotency_key", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("knowledge_scope", "owner", "created_by", mode="before")
    @classmethod
    def _texts(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=500)

    @field_validator("objective", mode="before")
    @classmethod
    def _required_texts(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_text(value, field_name=info.field_name or "field", limit=500)

    @field_validator("constraints", "acceptance_criteria", "knowledge_context_refs", mode="before")
    @classmethod
    def _safe_list(cls, value: Any) -> list[str]:
        if not isinstance(value, list | tuple | set):
            value = [value] if value else []
        return [sanitize_lhp_text(item, limit=500) for item in list(value)[:MAX_COLLECTION_ITEMS]]

    @field_validator("resource", "payload", mode="before")
    @classmethod
    def _safe_dict(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}


class HandoffUpdate(BaseModel):
    """Structured progress update from a target loop."""

    model_config = ConfigDict(extra="forbid")

    update_id: str = Field(default_factory=lambda: f"hu_{uuid4().hex[:12]}")
    handoff_id: str
    case_id: str
    source_loop: LoopName
    update_type: HandoffUpdateType
    status: HandoffStatus
    summary: str = ""
    evidence: list[EvidenceRef] = Field(default_factory=list)
    confidence: Literal["low", "medium", "high", "unknown"] = "unknown"
    external_event_id: str
    correlation_id: str
    trace_id: str = ""
    payload_hash: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("update_id", "trace_id", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("handoff_id", "case_id", "external_event_id", "correlation_id", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("summary", mode="before")
    @classmethod
    def _summary(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=1_200)

    @field_validator("payload", mode="before")
    @classmethod
    def _payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}

    @model_validator(mode="after")
    def _fill_hash_and_validate_status(self) -> "HandoffUpdate":
        if self.source_loop != "noc" and self.status in VERIFIER_ONLY_HANDOFF_STATUSES:
            raise ValueError("non-NOC loops cannot set verified/resolved handoff status")
        if not self.payload_hash:
            self.payload_hash = lhp_payload_hash(
                {
                    "case_id": self.case_id,
                    "handoff_id": self.handoff_id,
                    "source_loop": self.source_loop,
                    "update_type": self.update_type,
                    "status": self.status,
                    "summary": self.summary,
                    "evidence": [item.model_dump(mode="json") for item in self.evidence],
                    "payload": self.payload,
                }
            )
        return self


class VerificationObjective(BaseModel):
    """Machine-checkable or operator-reviewable condition for case resolution."""

    model_config = ConfigDict(extra="forbid")

    objective_id: str = Field(default_factory=lambda: f"vo_{uuid4().hex[:12]}")
    case_id: str
    handoff_id: str = ""
    objective_key: str
    objective_type: str
    name: str
    description: str = ""
    required_status: VerificationStatus = "pass"
    status: VerificationStatus = "pending"
    required: bool = True
    required_consecutive_passes: int = Field(default=3, ge=1)
    consecutive_pass_count: int = Field(default=0, ge=0)
    last_checked_at: str = ""
    next_check_at: str = ""
    evidence_ref: str = ""
    failure_reason: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("objective_id", "handoff_id", "evidence_ref", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("case_id", "objective_key", "objective_type", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("description", "failure_reason", mode="before")
    @classmethod
    def _texts(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=800)

    @field_validator("name", mode="before")
    @classmethod
    def _required_texts(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_text(value, field_name=info.field_name or "field", limit=800)

    @field_validator("payload", mode="before")
    @classmethod
    def _payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}


class KnowledgeArtifact(BaseModel):
    """Review-gated, versioned Knowledge Loop output attached to a case."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str = Field(default_factory=lambda: f"ka_{uuid4().hex[:12]}")
    case_id: str
    handoff_id: str = ""
    artifact_type: str
    scope: str = ""
    status: KnowledgeArtifactStatus = "proposed"
    review_status: KnowledgeReviewStatus = "pending"
    version: int = Field(default=1, ge=1)
    content_hash: str = ""
    summary: str = ""
    source_refs: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_by: LoopName = "knowledge"
    created_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("artifact_id", "handoff_id", "content_hash", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("case_id", "artifact_type", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("scope", "summary", mode="before")
    @classmethod
    def _texts(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=1_200)

    @field_validator("source_refs", mode="before")
    @classmethod
    def _refs(cls, value: Any) -> list[str]:
        if not isinstance(value, list | tuple | set):
            value = [value] if value else []
        return [sanitize_lhp_text(item, limit=300) for item in list(value)[:MAX_COLLECTION_ITEMS]]

    @field_validator("payload", mode="before")
    @classmethod
    def _payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}

    @model_validator(mode="after")
    def _fill_content_hash(self) -> "KnowledgeArtifact":
        if not self.content_hash:
            self.content_hash = lhp_payload_hash(
                {"summary": self.summary, "payload": self.payload, "source_refs": self.source_refs}
            )
        return self


class CallbackInboxRecord(BaseModel):
    """Deduplication record for external loop callbacks before state mutation."""

    model_config = ConfigDict(extra="forbid")

    callback_id: str = Field(default_factory=lambda: f"cb_{uuid4().hex[:12]}")
    source_loop: LoopName
    external_event_id: str
    payload_hash: str
    handoff_id: str = ""
    case_id: str = ""
    status: CallbackStatus = "accepted"
    result_payload: dict[str, Any] = Field(default_factory=dict)
    received_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("callback_id", "handoff_id", "case_id", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("external_event_id", "payload_hash", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("result_payload", mode="before")
    @classmethod
    def _payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}


class HandoffTransportDelivery(BaseModel):
    """Transport-specific delivery state for a CaseHandoff."""

    model_config = ConfigDict(extra="forbid")

    delivery_id: str = Field(default_factory=lambda: f"deliv_{uuid4().hex[:12]}")
    handoff_id: str
    case_id: str
    transport: HandoffTransport = "github_issue"
    status: DeliveryStatus = "pending"
    idempotency_key: str
    external_id: str = ""
    external_url: str = ""
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=10, ge=1, le=100)
    next_attempt_at: str = Field(default_factory=utc_now)
    last_error: str = ""
    payload_hash: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("delivery_id", "external_id", "payload_hash", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("handoff_id", "case_id", "idempotency_key", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("external_url", "last_error", mode="before")
    @classmethod
    def _texts(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=800)

    @field_validator("payload", mode="before")
    @classmethod
    def _payload(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}

    @model_validator(mode="after")
    def _fill_hash(self) -> "HandoffTransportDelivery":
        if not self.payload_hash:
            self.payload_hash = lhp_payload_hash(self.payload)
        return self


class OutcomeRecord(BaseModel):
    """Final cross-loop outcome written into the Hyrule Learning Substrate."""

    model_config = ConfigDict(extra="forbid")

    outcome_id: str = Field(default_factory=lambda: f"outcome_{uuid4().hex[:12]}")
    work_item_type: Literal["case"] = "case"
    work_item_id: str
    case_type: str = ""
    fingerprint: str = ""
    agent_roles: list[LoopName] = Field(default_factory=list)
    proposed_action: str = ""
    action_taken: str = ""
    human_review: dict[str, Any] = Field(default_factory=dict)
    validation: dict[str, Any] = Field(default_factory=dict)
    safety: dict[str, Any] = Field(default_factory=dict)
    evidence_quality: dict[str, Any] = Field(default_factory=dict)
    learning: dict[str, Any] = Field(default_factory=dict)
    final_score: dict[str, float] = Field(default_factory=dict)
    evidence_refs: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now)
    schema_version: str = LHP_SCHEMA_VERSION

    @field_validator("outcome_id", "case_type", "fingerprint", mode="before")
    @classmethod
    def _tokens(cls, value: Any) -> str:
        return sanitize_lhp_token(value, limit=180)

    @field_validator("work_item_id", mode="before")
    @classmethod
    def _required_tokens(cls, value: Any, info: ValidationInfo) -> str:
        return require_lhp_token(value, field_name=info.field_name or "field", limit=180)

    @field_validator("proposed_action", "action_taken", mode="before")
    @classmethod
    def _texts(cls, value: Any) -> str:
        return sanitize_lhp_text(value, limit=1_200)

    @field_validator("human_review", "validation", "safety", "evidence_quality", "learning", "payload", mode="before")
    @classmethod
    def _payloads(cls, value: Any) -> dict[str, Any]:
        sanitized = sanitize_lhp_payload(value or {})
        return sanitized if isinstance(sanitized, dict) else {"value": sanitized}

    @field_validator("evidence_refs", mode="before")
    @classmethod
    def _refs(cls, value: Any) -> list[str]:
        if not isinstance(value, list | tuple | set):
            value = [value] if value else []
        return [sanitize_lhp_text(item, limit=300) for item in list(value)[:MAX_COLLECTION_ITEMS]]

    @field_validator("final_score", mode="before")
    @classmethod
    def _scores(cls, value: Any) -> dict[str, float]:
        if not isinstance(value, dict):
            return {}
        scores: dict[str, float] = {}
        for key, raw_score in value.items():
            safe_key = sanitize_lhp_token(key, limit=64)
            try:
                score = float(raw_score)
            except TypeError, ValueError:
                continue
            scores[safe_key] = min(1.0, max(0.0, score))
        return scores
