"""Adapters from proactive-loop hotspots to case-service observations."""

from __future__ import annotations

from typing import Literal

from app.cases.models import ObservationRecord, SourceHealth
from app.proactive.models import Hotspot


ProactiveObservationStatus = Literal["firing"]


def observation_from_hotspot(
    hotspot: Hotspot,
    *,
    cycle_id: str,
    source_health: SourceHealth = "healthy",
    status: ProactiveObservationStatus = "firing",
) -> ObservationRecord:
    """Normalize a freshly scanned hotspot into a durable observation.

    Important: callers must pass only *fresh scan output*, not carried-forward
    hotspots. A cheap cycle that did not run a deep rule must not look like a
    fresh unhealthy observation for that deep rule.
    """

    fingerprint = hotspot.fingerprint()
    evidence = [item.model_dump(mode="json") for item in hotspot.evidence]
    signal_snapshot = {
        "rule_id": hotspot.rule_id,
        "key": hotspot.key,
        "category": hotspot.category,
        "severity": hotspot.severity,
        "score": hotspot.score,
        "title": hotspot.title,
        "summary": hotspot.summary,
        "resource": hotspot.resource,
        "evidence": evidence,
        "recommended_checks": list(hotspot.recommended_checks),
        "suggested_specialist": hotspot.suggested_specialist,
        "warrants_change": hotspot.warrants_change,
        "change_rationale": hotspot.change_rationale,
    }
    return ObservationRecord(
        source="proactive",
        source_event_id=f"{cycle_id}:{fingerprint}",
        source_fingerprint=fingerprint,
        dedup_key=f"proactive:{cycle_id}:{fingerprint}",
        detector=hotspot.rule_id,
        rule_id=hotspot.rule_id,
        entity=hotspot.resource,
        resource=hotspot.resource,
        service=hotspot.category,
        severity=hotspot.severity,
        status=status,
        observed_at=hotspot.detected_at,
        scan_cycle_id=cycle_id,
        labels={
            "proactive_rule": hotspot.rule_id,
            "proactive_key": hotspot.key,
            "category": hotspot.category,
            "resource": hotspot.resource,
        },
        annotations={"summary": hotspot.summary, "title": hotspot.title},
        signal_snapshot=signal_snapshot,
        source_health=source_health,
        observation_confidence="high" if source_health == "healthy" else "medium",
    )
