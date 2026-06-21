"""Alert identity and display helpers shared by graph and case paths."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def labels_from_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels: dict[str, Any] = {}
    for key in ("groupLabels", "commonLabels"):
        if isinstance(alert_payload.get(key), dict):
            labels.update(alert_payload[key])
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alerts[0], dict):
        nested = alerts[0].get("labels")
        if isinstance(nested, dict):
            labels.update(nested)
    return labels


def fingerprint_alert(alert_payload: dict[str, Any]) -> str:
    identity = alert_identity(alert_payload)
    payload = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def alert_identity(alert_payload: dict[str, Any]) -> dict[str, str]:
    labels = labels_from_alert(alert_payload)
    source = _norm_key(alert_payload.get("source") or "alertmanager")
    alertname = _norm_key(labels.get("alertname") or alert_payload.get("source") or "unknown")
    resource_key = _resource_key(alert_payload, labels)
    service_key = _norm_key(labels.get("service") or labels.get("check_command") or labels.get("job") or alertname)
    instance = str(labels.get("instance") or "")
    instance_key = _norm_key(instance_host(instance) if (labels.get("host") or labels.get("hostname") or service_key) else instance)
    return {
        "source": source,
        "alertname": alertname,
        "resource_key": resource_key,
        "service_key": service_key,
        "instance_key": instance_key,
    }


def case_event_from_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels = labels_from_alert(alert_payload)
    identity = alert_identity(alert_payload)
    status = str(alert_payload.get("status") or _first_alert(alert_payload).get("status") or labels.get("state") or "unknown")
    state = str(labels.get("state") or labels.get("severity") or status or "unknown").upper()
    fingerprint = fingerprint_alert(alert_payload)
    return {
        "received_at": utc_now(),
        "source": identity["source"],
        "fingerprint": fingerprint,
        "identity": identity,
        "alertname": identity["alertname"],
        "display_alertname": _alert_name(alert_payload),
        "resource_key": identity["resource_key"],
        "display_resource": _display_resource(alert_payload, labels),
        "service_key": identity["service_key"],
        "status": status,
        "state": state,
        "severity": str(labels.get("severity") or "").upper(),
        "summary": _alert_summary(alert_payload),
        "labels": _jsonish(labels),
        "is_recovery": is_recovery_alert(alert_payload),
        "raw_excerpt": _json_excerpt(alert_payload),
    }


def is_recovery_alert(alert_payload: dict[str, Any]) -> bool:
    status_value = str(alert_payload.get("status") or "").lower()
    if status_value in {"resolved", "recovery", "ok", "up"}:
        return True
    first_status = str(_first_alert(alert_payload).get("status") or "").lower()
    if first_status in {"resolved", "recovery", "ok", "up"}:
        return True
    labels = labels_from_alert(alert_payload)
    return str(labels.get("state") or "").upper() in {"OK", "UP"}


def case_display_title(case: dict[str, Any] | None, event: dict[str, Any] | None = None) -> str:
    if case is None and event is None:
        return "NOC: unknown alert"
    case_number = (case or {}).get("case_number") or "NOC"
    identity = (event or {}).get("identity") or (case or {}).get("identity") or {}
    alertname = (event or {}).get("display_alertname") or identity.get("alertname") or (event or {}).get("alertname") or "unknown"
    resource = (
        (event or {}).get("display_resource")
        or identity.get("resource_key")
        or (event or {}).get("resource_key")
        or (case or {}).get("resource_id")
        or "unknown"
    )
    return f"{case_number}: {alertname} on {resource}"


def instance_host(instance: str) -> str:
    if instance.startswith("[") and "]" in instance:
        return instance[1:instance.index("]")]
    if instance.count(":") == 1:
        return instance.rsplit(":", 1)[0]
    return instance


def utc_now(ts: float | None = None) -> str:
    dt = datetime.fromtimestamp(ts, timezone.utc) if ts is not None else datetime.now(timezone.utc)
    return dt.isoformat()


def _alert_name(alert_payload: dict[str, Any]) -> str:
    return str(labels_from_alert(alert_payload).get("alertname") or alert_payload.get("source") or "unknown")


def _first_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alerts[0], dict):
        return alerts[0]
    return {}


def _alert_summary(alert_payload: dict[str, Any]) -> str:
    first = _first_alert(alert_payload)
    annotations = {}
    for key in ("commonAnnotations", "annotations"):
        if isinstance(alert_payload.get(key), dict):
            annotations.update(alert_payload[key])
    if isinstance(first.get("annotations"), dict):
        annotations.update(first["annotations"])
    return str(annotations.get("summary") or annotations.get("description") or alert_payload.get("output") or "").strip()


def _resource_key(alert_payload: dict[str, Any], labels: dict[str, Any]) -> str:
    host = labels.get("host") or labels.get("hostname")
    if host:
        return _norm_key(host)
    instance = labels.get("instance")
    if instance:
        return _norm_key(instance_host(str(instance)))
    return _norm_key(alert_payload.get("source") or "unknown")


def _display_resource(alert_payload: dict[str, Any], labels: dict[str, Any]) -> str:
    host = labels.get("host") or labels.get("hostname")
    if host:
        return str(host)
    instance = labels.get("instance")
    if instance:
        return instance_host(str(instance))
    return str(alert_payload.get("source") or "unknown")


def _norm_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _jsonish(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _json_excerpt(value: Any, limit: int = 3000) -> dict[str, Any] | str:
    safe = _jsonish(value)
    text = json.dumps(safe, sort_keys=True, separators=(",", ":"))
    if len(text) <= limit:
        return safe
    return text[: limit - 3] + "..."
