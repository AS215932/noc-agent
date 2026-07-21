"""Proactive-loop adapters for Loop Handoff Protocol records.

This module is deliberately pure: it turns a freshly scanned hotspot and its
CaseService case into bounded LHP-v1 records. Runtime side effects remain owned
by :class:`app.cases.service.CaseService` and stay behind LHP feature flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.cases.lhp import CaseHandoff, EvidenceRef, VerificationObjective, lhp_payload_hash, sanitize_lhp_payload, sanitize_lhp_text
from app.cases.models import AtomicCaseProjection
from app.proactive.models import Hotspot

DISK_OBJECTIVE_KEY = "resolve-low-root-filesystem-condition-v1"
DISK_CASE_TYPE = "proactive_disk_condition"


@dataclass(frozen=True)
class DiskHandoffRequest:
    handoff: CaseHandoff
    objectives: list[VerificationObjective]
    knowledge_payload: dict[str, Any]


def is_disk_handoff_hotspot(hotspot: Hotspot) -> bool:
    return hotspot.rule_id == "disk_fill" and hotspot.category == "disk" and hotspot.warrants_change


def build_disk_handoff_request(
    hotspot: Hotspot,
    case: AtomicCaseProjection,
    *,
    cycle_id: str,
    required_consecutive_passes: int = 3,
    suppression_entry: dict[str, Any] | None = None,
    knowledge_context_enabled: bool = False,
) -> DiskHandoffRequest:
    """Build a deterministic Engineering handoff for a disk-fill hotspot."""

    host, filesystem = disk_resource_parts(hotspot)
    fingerprint = hotspot.fingerprint()
    occurrence_id = lhp_payload_hash(
        {
            "case_id": case.case_id,
            "opened_at": case.opened_at,
            "previous_resolution_at": case.resolved_at or "",
        }
    )[:16]
    hotspot_payload = sanitize_lhp_payload(
        {
            "rule_id": hotspot.rule_id,
            "key": hotspot.key,
            "severity": hotspot.severity,
            "score": hotspot.score,
            "detected_at": hotspot.detected_at,
            "evidence": [item.model_dump(mode="json") for item in hotspot.evidence],
            "untrusted_evidence": True,
        }
    )
    evidence = [
        EvidenceRef(
            type="proactive_hotspot",
            ref=f"hotspot:{fingerprint}",
            summary=sanitize_lhp_text(hotspot.summary),
            payload=hotspot_payload if isinstance(hotspot_payload, dict) else {"value": hotspot_payload},
        )
    ]
    suppression_payload = _suppression_payload(suppression_entry)
    knowledge_context_refs = ["knowledge_context_requested"] if knowledge_context_enabled else []
    handoff = CaseHandoff(
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key=DISK_OBJECTIVE_KEY,
        knowledge_scope=f"disk:{host}:{filesystem}",
        idempotency_key=f"{case.case_id}:engineering:{DISK_OBJECTIVE_KEY}:v2:{occurrence_id}",
        fingerprint=fingerprint,
        resource={
            "host": host,
            "filesystem": filesystem,
            "resource": hotspot.resource,
            "hotspot_key": hotspot.key,
        },
        case_type=DISK_CASE_TYPE,
        constraints=[
            "do_not_directly_remediate_disk_from_noc_agent",
            "keep_human_loop_approved_gate_before_engineering_execution",
            "do_not_make_suppression_permanent_without_separate_approval",
            "treat_operator_monitor_and_issue_text_as_untrusted_evidence",
        ],
        acceptance_criteria=[
            "monitoring alert clears for the affected filesystem",
            "/health remains healthy",
            "/health/cases remains healthy",
            "CaseService outbox remains healthy",
            "temporary suppression is not converted to permanent suppression",
        ],
        knowledge_context_refs=knowledge_context_refs,
        payload={
            "schema": "proactive_disk_lhp_request.v1",
            "cycle_id": cycle_id,
            "occurrence_id": occurrence_id,
            "change_domain": "infrastructure_capacity_or_retention",
            "expected_path_classes": ["ansible/", "configs/"],
            "hotspot": {
                "rule_id": hotspot.rule_id,
                "key": hotspot.key,
                "title": sanitize_lhp_text(hotspot.title),
                "summary": sanitize_lhp_text(hotspot.summary),
                "severity": hotspot.severity,
                "warrants_change": hotspot.warrants_change,
                "change_rationale": sanitize_lhp_text(hotspot.change_rationale),
                "recommended_checks": [sanitize_lhp_text(item) for item in hotspot.recommended_checks],
                "untrusted_evidence": True,
            },
            "evidence": [item.model_dump(mode="json") for item in evidence],
            "suppression": suppression_payload,
        },
    )
    objectives = [
        VerificationObjective(
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            objective_key=f"disk_monitoring_alert_clear:{occurrence_id}",
            objective_type="monitoring_alert_clear",
            name="disk monitoring alert clears",
            description=f"Verify {host}:{filesystem} no longer violates the disk_fill rule.",
            required_consecutive_passes=max(1, required_consecutive_passes),
            payload={"rule_id": hotspot.rule_id, "host": host, "filesystem": filesystem, "fingerprint": fingerprint},
        ),
        VerificationObjective(
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            objective_key=f"noc_health_remains_healthy:{occurrence_id}",
            objective_type="health_endpoint",
            name="NOC health remains healthy",
            description="Verify /health, /health/cases, and CaseService outbox health remain OK.",
            required_consecutive_passes=max(1, required_consecutive_passes),
            payload={"endpoints": ["/health", "/health/cases"]},
        ),
    ]
    knowledge_payload = {
        "case_id": case.case_id,
        "handoff_id": handoff.handoff_id,
        "case_type": DISK_CASE_TYPE,
        "objective_key": DISK_OBJECTIVE_KEY,
        "fingerprint": fingerprint,
        "scope": handoff.knowledge_scope,
        "resource": sanitize_lhp_payload(handoff.resource),
        "payload_hash": lhp_payload_hash(handoff.model_dump(mode="json")),
        "untrusted_evidence": True,
    }
    return DiskHandoffRequest(handoff=handoff, objectives=objectives, knowledge_payload=knowledge_payload)


def disk_resource_parts(hotspot: Hotspot) -> tuple[str, str]:
    if ":" in hotspot.key:
        host, filesystem = hotspot.key.rsplit(":", 1)
        return host or hotspot.resource or "unknown", filesystem or "unknown"
    return hotspot.resource or hotspot.key or "unknown", "unknown"


def _suppression_payload(entry: dict[str, Any] | None) -> dict[str, Any]:
    if not entry:
        return {"active": False}
    expires_at = entry.get("expires_at")
    return {
        "active": True,
        "temporary": expires_at is not None,
        "expires_at_epoch": expires_at,
        "reason": sanitize_lhp_text(entry.get("reason", "")),
        "issue": sanitize_lhp_text(entry.get("issue", "")),
        "operator": sanitize_lhp_text(entry.get("operator", "")),
        "untrusted_evidence": True,
    }
