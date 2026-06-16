from __future__ import annotations

from typing import Any, Literal

from app.graph.state import WorkflowState


Specialist = Literal["bgp", "security_firewall", "infrastructure"]


def resource_id_from_alert(alert_payload: dict[str, Any]) -> str:
    labels = labels_from_alert(alert_payload)
    host = labels.get("host") or labels.get("hostname")
    if host:
        return str(host)
    instance = labels.get("instance")
    if instance:
        return instance_host(str(instance))
    return str(alert_payload.get("source") or "unknown")


def labels_from_alert(alert_payload: dict[str, Any]) -> dict[str, Any]:
    labels: dict[str, Any] = {}
    for key in ("groupLabels", "commonLabels"):
        if isinstance(alert_payload.get(key), dict):
            labels.update(alert_payload[key])
    alerts = alert_payload.get("alerts")
    if isinstance(alerts, list) and alerts and isinstance(alert_payload["alerts"][0], dict):
        nested = alert_payload["alerts"][0].get("labels")
        if isinstance(nested, dict):
            labels.update(nested)
    return labels


def instance_host(instance: str) -> str:
    if instance.startswith("[") and "]" in instance:
        return instance[1:instance.index("]")]
    if instance.count(":") == 1:
        return instance.rsplit(":", 1)[0]
    return instance


def supervisor_route(state: WorkflowState) -> dict[str, str]:
    labels = labels_from_alert(state["normalized_alert"])
    hint = str(labels.get("specialist_hint") or "").strip()
    if hint in ("bgp", "security_firewall", "infrastructure"):
        return {
            "active_specialist": hint,
            "specialist_type": hint,
            "routing_reason": "Explicit specialist hint from alert labels.",
        }
    blob = " ".join(str(value) for value in [labels, state["normalized_alert"]]).lower()
    if any(token in blob for token in ("bgp", "peer", "route", "frr", "ospf")):
        specialist = "bgp"
        reason = "Routing keywords in the alert payload indicate a control-plane investigation."
    elif any(token in blob for token in ("pf", "nft", "firewall", "icmp unreachable", "drop", "blocked")):
        specialist = "security_firewall"
        reason = "Firewall or packet-filter symptoms dominate the alert payload."
    else:
        specialist = "infrastructure"
        reason = "The alert is host/service oriented rather than routing-specific."
    return {"active_specialist": specialist, "specialist_type": specialist, "routing_reason": reason}


def route_specialist(state: WorkflowState) -> str:
    specialist = state.get("active_specialist") or state.get("specialist_type")
    if specialist == "bgp":
        return "bgp_specialist"
    if specialist == "security_firewall":
        return "firewall_specialist"
    return "infrastructure_specialist"


def is_direct_measurement(value: str) -> bool:
    lowered = str(value or "").lower()
    return any(token in lowered for token in ("tcpdump", "pflog", "nft_log", "ndp", "arp_state", "capture"))

