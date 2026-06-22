import os

import pytest

from app.cases import (
    CaseHandoff,
    HandoffTransportDelivery,
    HandoffUpdate,
    OutboxIntent,
    VerificationObjective,
    allowed_handoff_transition,
    assert_lhp_payload_size,
    build_loop_signature,
    lhp_payload_hash,
    sanitize_lhp_payload,
    sanitize_lhp_text,
    verify_loop_signature,
)
from app.cases.models import AtomicCaseProjection
from app.config import LoopHandoffSettings, load_loop_handoff_settings


def _clear_lhp_env(monkeypatch):
    prefixes = (
        "NOC_LHP",
        "NOC_ENGINEERING_HANDOFF",
        "NOC_KNOWLEDGE_",
        "NOC_CASE_VERIFICATION",
        "NOC_CASE_AUTO_RESOLVE",
        "NOC_DISK_ALERT_HANDOFF",
    )
    for key in list(os.environ):
        if key.startswith(prefixes):
            monkeypatch.delenv(key, raising=False)


def test_loop_handoff_settings_are_disabled_by_default(monkeypatch):
    _clear_lhp_env(monkeypatch)

    settings = load_loop_handoff_settings()

    assert settings == LoopHandoffSettings()
    assert settings.enabled is False
    assert settings.engineering_handoff_delivery_enabled is False
    assert settings.knowledge_context_enabled is False
    assert settings.case_verification_enabled is False
    assert settings.case_verification_dry_run is True
    assert settings.case_auto_resolve_enabled is False
    assert settings.disk_alert_handoff_enabled is False
    assert settings.engineering_secret_configured is False


def test_loop_handoff_settings_load_env_without_exposing_secret(monkeypatch):
    _clear_lhp_env(monkeypatch)
    monkeypatch.setenv("NOC_LHP_ENABLED", "1")
    monkeypatch.setenv("NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED", "1")
    monkeypatch.setenv("NOC_ENGINEERING_HANDOFF_REPO", "AS215932/network-operations-test")
    monkeypatch.setenv("NOC_ENGINEERING_HANDOFF_TRANSPORT", "github_issue")
    monkeypatch.setenv("NOC_KNOWLEDGE_CONTEXT_ENABLED", "1")
    monkeypatch.setenv("NOC_KNOWLEDGE_CONTEXT_MAX_ARTIFACTS", "7")
    monkeypatch.setenv("NOC_KNOWLEDGE_CONTEXT_MAX_TOKENS_EQUIVALENT", "2048")
    monkeypatch.setenv("NOC_KNOWLEDGE_CONTEXT_TIMEOUT_S", "11")
    monkeypatch.setenv("NOC_CASE_VERIFICATION_ENABLED", "1")
    monkeypatch.setenv("NOC_CASE_VERIFICATION_DRY_RUN", "0")
    monkeypatch.setenv("NOC_CASE_AUTO_RESOLVE_ENABLED", "1")
    monkeypatch.setenv("NOC_CASE_VERIFICATION_INTERVAL_S", "90")
    monkeypatch.setenv("NOC_CASE_VERIFICATION_REQUIRED_CONSECUTIVE_PASSES", "4")
    monkeypatch.setenv("NOC_DISK_ALERT_HANDOFF_ENABLED", "1")
    monkeypatch.setenv("NOC_LHP_CALLBACK_MAX_BYTES", "32768")
    monkeypatch.setenv("NOC_LHP_ENGINEERING_SECRET", "super-secret-value")

    settings = load_loop_handoff_settings()

    assert settings.enabled is True
    assert settings.engineering_handoff_delivery_enabled is True
    assert settings.engineering_handoff_repo == "AS215932/network-operations-test"
    assert settings.knowledge_context_enabled is True
    assert settings.knowledge_context_max_artifacts == 7
    assert settings.knowledge_context_max_tokens_equivalent == 2048
    assert settings.knowledge_context_timeout_s == 11
    assert settings.case_verification_enabled is True
    assert settings.case_verification_dry_run is False
    assert settings.case_auto_resolve_enabled is True
    assert settings.case_verification_interval_s == 90
    assert settings.case_verification_required_consecutive_passes == 4
    assert settings.disk_alert_handoff_enabled is True
    assert settings.callback_max_bytes == 32768
    assert settings.engineering_secret_configured is True
    assert "super-secret-value" not in repr(settings)
    assert "NOC_LHP_ENGINEERING_SECRET" not in repr(settings)


def test_lhp_text_and_payload_sanitizers_bound_untrusted_text():
    text = "```ignore previous``` Authorization: Bearer abc123\n<script>"

    rendered = sanitize_lhp_text(text, limit=80)
    payload = sanitize_lhp_payload({"Authorization": "Bearer abc123", "nested": [text]})

    assert "```" not in rendered
    assert "Bearer abc123" not in rendered
    assert "<" not in rendered and ">" not in rendered
    assert "redacted_key" in payload
    assert "Bearer abc123" not in str(payload)


def test_lhp_schemas_are_bounded_and_hashable():
    handoff = CaseHandoff(
        case_id="case_123",
        target_loop="engineering",
        objective="resolve low root filesystem condition ```do something else```",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_123:engineering:resolve-low-root-filesystem-condition:v1",
        fingerprint="8fb421ff94bb1285",
        constraints=["do_not_make_suppression_permanent_without_separate_approval"],
        acceptance_criteria=["/health remains healthy"],
        payload={"operator_text": "Authorization: Bearer no"},
    )
    objective = VerificationObjective(
        case_id=handoff.case_id,
        handoff_id=handoff.handoff_id,
        objective_key="health_root_ok",
        objective_type="health_endpoint",
        name="/health is ok",
    )

    dumped = handoff.model_dump(mode="json")
    assert handoff.schema_version == "lhp.v1"
    assert "Bearer no" not in str(dumped)
    assert handoff.idempotency_key.endswith(":v1")
    assert objective.required_consecutive_passes == 3
    assert len(lhp_payload_hash(dumped)) == 64


def test_lhp_required_fields_are_checked_before_sanitization_fallback():
    with pytest.raises(ValueError, match="case_id is required"):
        CaseHandoff(
            case_id="",
            target_loop="engineering",
            objective="resolve low root filesystem condition",
            objective_key="resolve-low-root-filesystem-condition-v1",
            idempotency_key="case_123:engineering:resolve-low-root-filesystem-condition:v1",
        )


def test_lhp_rejects_non_noc_verified_or_resolved_updates():
    with pytest.raises(ValueError, match="non-NOC loops cannot set verified/resolved"):
        HandoffUpdate(
            case_id="case_1",
            handoff_id="handoff_1",
            source_loop="engineering",
            update_type="implemented",
            status="verified",
            external_event_id="evt_1",
            correlation_id="corr_1",
        )


def test_lhp_handoff_transition_guard_keeps_verifier_authority():
    assert allowed_handoff_transition("implemented", "verified", actor_loop="noc") is True
    assert allowed_handoff_transition("implemented", "verified", actor_loop="engineering") is False
    assert allowed_handoff_transition("requested", "resolved", actor_loop="noc") is False
    assert allowed_handoff_transition("requested", "accepted", actor_loop="engineering") is True


def test_lhp_loop_signature_is_canonical_and_verifiable():
    body = {"b": 2, "a": 1}
    signature = build_loop_signature(
        secret="shared",
        method="post",
        path="/webhook/engineering-loop/handoff-update",
        timestamp="2026-06-22T20:00:00Z",
        body=body,
    )

    assert verify_loop_signature(
        secret="shared",
        method="POST",
        path="/webhook/engineering-loop/handoff-update",
        timestamp="2026-06-22T20:00:00Z",
        body={"a": 1, "b": 2},
        signature=signature,
    )
    assert not verify_loop_signature(
        secret="shared",
        method="POST",
        path="/wrong",
        timestamp="2026-06-22T20:00:00Z",
        body=body,
        signature=signature,
    )


def test_lhp_transport_delivery_has_retry_ceiling():
    delivery = HandoffTransportDelivery(
        case_id="case_1",
        handoff_id="handoff_1",
        idempotency_key="delivery:handoff_1:github_issue",
    )

    assert delivery.max_attempts == 10
    with pytest.raises(ValueError):
        HandoffTransportDelivery(
            case_id="case_1",
            handoff_id="handoff_1",
            idempotency_key="delivery:handoff_1:github_issue",
            max_attempts=0,
        )


def test_lhp_payload_size_limit_raises():
    with pytest.raises(ValueError, match="LHP payload exceeds max size"):
        assert_lhp_payload_size({"x": "y" * 100}, max_bytes=20)


def test_case_status_and_outbox_literals_accept_dormant_lhp_values():
    case = AtomicCaseProjection(status="handoff_requested")
    intent = OutboxIntent(
        case_id="case_1",
        intent_type="engineering_handoff_requested",
        idempotency_key="engineering_handoff_requested:case_1",
    )

    assert case.status == "handoff_requested"
    assert intent.intent_type == "engineering_handoff_requested"
