"""Normalize monitoring notifications into case-service observations."""

from __future__ import annotations

import hashlib
from typing import Any

from app.cases.models import NotificationRoute, ObservationRecord, ObservationStatus, Severity, stable_json


AI_ROUTE_TERMS = (
    "engineering-loop",
    "engineering_loop",
    "noc-agent-model",
    "noc_agent_model",
    "noc-agent-mcp",
    "noc_agent_mcp",
    "knowledge-mcp",
    "agentic-observatory",
    "model-fallback",
    "allmodels",
    "gemini",
    "openrouter",
    "venice",
)
CI_ROUTE_TERMS = ("ci-runner", "pipeline", "workflow", "deploy", "promotion", "drift-detection")


def notification_route_for(
    *,
    labels: dict[str, Any] | None = None,
    tags: dict[str, Any] | None = None,
    detector: str = "",
    service: str = "",
) -> NotificationRoute:
    """Resolve an explicit route, with a deterministic compatibility fallback.

    Routing never invokes a model. Unknown production alerts deliberately land
    in the network/NOC channel so a missing label cannot hide an incident.
    """

    labels = labels or {}
    tags = tags or {}
    explicit = str(labels.get("notification_route") or tags.get("notification_route") or "").strip().lower()
    if explicit in {"network", "ai", "ci"}:
        return explicit  # type: ignore[return-value]
    subject = " ".join((detector, service)).lower()
    if any(term in subject for term in AI_ROUTE_TERMS):
        return "ai"
    if any(term in subject for term in CI_ROUTE_TERMS):
        return "ci"
    return "network"


def observations_from_alertmanager(payload: dict[str, Any]) -> list[ObservationRecord]:
    source = str(payload.get("source") or "alertmanager")
    group_key = str(payload.get("groupKey") or "")
    common_labels = _dict(payload.get("commonLabels"))
    common_annotations = _dict(payload.get("commonAnnotations"))
    raw_alerts = payload.get("alerts")
    alerts: list[Any] = raw_alerts if isinstance(raw_alerts, list) else []
    observations: list[ObservationRecord] = []
    for index, raw_alert in enumerate(alerts):
        alert = raw_alert if isinstance(raw_alert, dict) else {}
        labels = {**common_labels, **_dict(alert.get("labels"))}
        annotations = {**common_annotations, **_dict(alert.get("annotations"))}
        alert_fp = str(alert.get("fingerprint") or labels.get("fingerprint") or "")
        if alert_fp:
            source_event_id = alert_fp
        elif group_key:
            source_event_id = f"{group_key}:{index}"
        else:
            source_event_id = _fingerprint({"labels": labels, "index": index})
        status = _status_from_alertmanager(str(alert.get("status") or payload.get("status") or ""))
        detector = str(labels.get("alertname") or payload.get("receiver") or "alertmanager")
        resource = _first_label(labels, "host", "instance", "device", "router", "node", "target")
        service = _first_label(labels, "service", "job", "check", "alertname")
        notification_route = notification_route_for(labels=labels, detector=detector, service=service)
        starts_at = str(alert.get("startsAt") or alert.get("starts_at") or "")
        signal_snapshot = {
            "labels": labels,
            "annotations": annotations,
            "startsAt": starts_at,
            "endsAt": alert.get("endsAt") or alert.get("ends_at") or "",
            "generatorURL": alert.get("generatorURL") or "",
            "groupKey": group_key,
        }
        observations.append(
            ObservationRecord(
                source=source,
                source_event_id=source_event_id,
                source_fingerprint=alert_fp or _fingerprint({"detector": detector, "resource": resource, "service": service}),
                dedup_key=f"{source}:{source_event_id}:{status}",
                detector=detector,
                rule_id=detector,
                entity=resource,
                resource=resource,
                service=service,
                site=_first_label(labels, "site", "pop", "region"),
                customer=_first_label(labels, "customer", "tenant"),
                severity=_severity(labels.get("severity") or labels.get("priority") or "UNKNOWN"),
                notification_route=notification_route,
                status=status,
                observed_at=starts_at,
                labels=labels,
                annotations=annotations,
                signal_snapshot=signal_snapshot,
                source_health="healthy",
                observation_confidence="high",
                payload_ref=str(alert.get("generatorURL") or payload.get("externalURL") or ""),
            )
        )
    return observations


def observation_from_icinga_alert_payload(payload: dict[str, Any]) -> ObservationRecord:
    source = str(payload.get("source") or "icinga2")
    labels = _dict(payload.get("commonLabels"))
    annotations = _dict(payload.get("commonAnnotations"))
    raw_alerts = payload.get("alerts")
    alerts: list[Any] = raw_alerts if isinstance(raw_alerts, list) else []
    first_alert = alerts[0] if alerts and isinstance(alerts[0], dict) else {}
    labels = {**labels, **_dict(first_alert.get("labels"))}
    annotations = {**annotations, **_dict(first_alert.get("annotations"))}
    tags = _dict(payload.get("tags"))
    if tags.get("notification_route") and not labels.get("notification_route"):
        labels["notification_route"] = tags["notification_route"]
    detector = str(labels.get("alertname") or labels.get("service") or labels.get("check_command") or "icinga-check")
    resource = str(labels.get("host") or labels.get("instance") or "")
    service = str(labels.get("service") or labels.get("check_command") or detector)
    notification_route = notification_route_for(labels=labels, tags=tags, detector=detector, service=service)
    state = str(labels.get("state") or first_alert.get("status") or payload.get("status") or "")
    status = _status_from_icinga(state, str(payload.get("status") or ""))
    source_event_id = _fingerprint({"source": source, "detector": detector, "resource": resource, "service": service})
    signal_snapshot = {
        "labels": labels,
        "annotations": annotations,
        "state": state,
        "tags": tags,
    }
    return ObservationRecord(
        source=source,
        source_event_id=source_event_id,
        source_fingerprint=source_event_id,
        dedup_key=f"{source}:{source_event_id}:{status}:{state}",
        detector=detector,
        rule_id=detector,
        entity=resource,
        resource=resource,
        service=service,
        severity=_severity(state),
        notification_route=notification_route,
        status=status,
        labels=labels,
        annotations=annotations,
        signal_snapshot=signal_snapshot,
        source_health="healthy",
        observation_confidence="high",
    )


def _status_from_alertmanager(status: str) -> ObservationStatus:
    return "firing" if status.lower() == "firing" else "resolved"


def _status_from_icinga(state: str, payload_status: str) -> ObservationStatus:
    if payload_status.lower() == "resolved" or state.upper() in {"OK", "UP"}:
        return "resolved"
    return "firing"


def _severity(value: object) -> Severity:
    text = str(value or "").upper()
    if text in {"CRITICAL", "HIGH", "DOWN"}:
        return "HIGH"
    if text in {"WARNING", "WARN", "MEDIUM"}:
        return "MEDIUM"
    if text in {"OK", "UP", "INFO", "LOW"}:
        return "LOW"
    return "UNKNOWN"


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_label(labels: dict[str, Any], *names: str) -> str:
    for name in names:
        value = labels.get(name)
        if value:
            return str(value)
    return ""


def _fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:16]
