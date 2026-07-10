"""JSON-safe data models for the proactive loop.

These are deliberately separate from ``app.graph.state.WorkflowState``: a
``Hotspot`` is the proactive analogue of an inbound alert, and
:func:`hotspot_to_alert_payload` turns it into the exact dict shape the existing
investigation graph already accepts (mirrors ``_icinga_to_alert_payload`` in
``app/main.py``), so a proactive concern flows through CaseService case ownership →
routing → evidence validation → drift check → reporting with no graph changes.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


Severity = Literal["HIGH", "MEDIUM", "LOW"]
Specialist = Literal["bgp", "security_firewall", "infrastructure"]
HotspotCategory = Literal[
    "bgp",
    "wireguard",
    "disk",
    "tls",
    "logs",
    "scrape",
    "dns",
    "nat64",
    "service",
    "other",
]
LessonStatus = Literal[
    "candidate",
    "validated_advisory",
    "approved_policy",
    "deprecated",
    "reverted",
]
CycleOutcome = Literal[
    "disabled",
    "locked",
    "over_budget",
    "idle",
    "scanned",
    "investigated",
    "error",
]


_SEVERITY_RANK: dict[str, int] = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_label(value: object, *, limit: int = 160) -> str:
    """Defang an untrusted telemetry value before it is embedded into hotspot
    text (which becomes Discord output, a synthetic-alert payload, and ultimately
    LLM prompt context). Prometheus labels can be derived from external sources
    (DNS, service discovery, a compromised exporter), so collapse all whitespace
    (kills newline-based prompt injection), drop non-printable characters, and
    cap the length to bound prompt bloat."""
    text = " ".join(str(value).split())
    text = "".join(ch for ch in text if ch.isprintable())
    return text[:limit]


def severity_rank(severity: str) -> int:
    return _SEVERITY_RANK.get(str(severity or "").upper(), 0)


class HotspotEvidence(BaseModel):
    """One read-only observation backing a hotspot (a metric sample, a count,
    an ETA). Carried into the synthetic alert so the investigation has a
    starting point and the Discord digest can show why the hotspot fired."""

    model_config = ConfigDict(extra="forbid")

    label: str = Field(default="", description="Human label, e.g. 'disk free % on log:/var'.")
    query: str = Field(default="", description="PromQL / tool query that produced the value.")
    value: str = Field(default="", description="Observed value, stringified.")
    threshold: str = Field(default="", description="Threshold or expected band, if any.")
    detail: str = Field(default="", description="Why this matters (trend, ETA, slope).")


class Hotspot(BaseModel):
    """A ranked, read-only early-warning signal — the proactive analogue of an
    inbound alert."""

    model_config = ConfigDict(extra="forbid")

    rule_id: str = Field(description="Scan rule that produced this hotspot.")
    key: str = Field(description="Stable identity within a rule, e.g. 'log:/var'.")
    category: HotspotCategory = "other"
    severity: Severity = "LOW"
    score: float = Field(default=0.0, ge=0.0, description="Ranking score (higher = act sooner).")
    title: str = ""
    resource: str = Field(default="", description="Host / router / service the signal concerns.")
    summary: str = ""
    evidence: list[HotspotEvidence] = Field(default_factory=list)
    recommended_checks: list[str] = Field(default_factory=list)
    suggested_specialist: Specialist | None = None
    warrants_change: bool = Field(
        default=False,
        description="True if this likely needs a config/docs change (candidate for handoff).",
    )
    change_rationale: str = ""
    detected_at: str = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def _sanitize_untrusted_text(self) -> "Hotspot":
        # Hotspot text is built from raw telemetry labels; scrub it once here so
        # nothing downstream (Discord, synthetic alert, LLM prompt) sees raw,
        # attacker-influencable label content.
        self.key = sanitize_label(self.key, limit=200)
        self.resource = sanitize_label(self.resource, limit=120)
        self.title = sanitize_label(self.title, limit=200)
        self.summary = sanitize_label(self.summary, limit=600)
        self.change_rationale = sanitize_label(self.change_rationale, limit=300)
        self.recommended_checks = [sanitize_label(c, limit=200) for c in self.recommended_checks]
        for ev in self.evidence:
            ev.label = sanitize_label(ev.label, limit=160)
            ev.value = sanitize_label(ev.value, limit=200)
            ev.threshold = sanitize_label(ev.threshold, limit=120)
            ev.detail = sanitize_label(ev.detail, limit=200)
            # ev.query is loop-authored PromQL, not untrusted input.
        return self

    def fingerprint(self) -> str:
        payload = f"{self.rule_id}|{self.key}"
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


class DecisionContext(BaseModel):
    """Immutable per-cycle governance snapshot (mirrors hyperliquid's
    ``DecisionContextRecorder``): what the loop knew and was configured to do
    when it acted, so a report can be replayed/audited later."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str = Field(default_factory=lambda: f"dcx_{uuid4().hex[:12]}")
    cycle_id: str = ""
    created_at: str = Field(default_factory=utc_now)
    manifest_hash: str = ""
    perimeter_context_version: str = ""
    scanner_ruleset_version: str = ""
    model_chain: list[str] = Field(default_factory=list)
    shadow: bool = True
    auto_heavy_probes: bool = False
    budget_state: dict[str, Any] = Field(default_factory=dict)
    injected_lesson_ids: list[str] = Field(default_factory=list)


class Observation(BaseModel):
    """A recorded scan observation, fed to the learning flywheel."""

    model_config = ConfigDict(extra="forbid")

    observation_id: str = Field(default_factory=lambda: f"obs_{uuid4().hex[:12]}")
    created_at: str = Field(default_factory=utc_now)
    cycle_id: str = ""
    rule_id: str = ""
    hotspot_key: str = ""
    fingerprint: str = ""
    resource: str = ""
    category: HotspotCategory = "other"
    severity: Severity = "LOW"
    summary: str = ""
    investigated: bool = False
    incident_id: str | None = None


class CandidateLesson(BaseModel):
    """A proposed lesson (scan-threshold tweak, runbook note) awaiting human
    review. Promotion gates are enforced by
    :class:`app.proactive.memory.MemoryPolicyEngine`."""

    model_config = ConfigDict(extra="forbid")

    lesson_id: str = Field(default_factory=lambda: f"les_{uuid4().hex[:12]}")
    created_at: str = Field(default_factory=utc_now)
    lesson_type: Literal["scan_tuning", "runbook", "threshold"] = "runbook"
    scope: dict[str, Any] = Field(default_factory=dict)
    claim: str = ""
    evidence: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    occurrences: int = 1
    source: Literal["outcome_eval", "operator"] = "outcome_eval"
    status: LessonStatus = "candidate"


class ProactiveCycleReport(BaseModel):
    """Outcome of one loop cycle (the proactive analogue of
    ``engineering-loop``'s ``DaemonReport``)."""

    model_config = ConfigDict(extra="forbid")

    cycle_id: str = Field(default_factory=lambda: f"cyc_{uuid4().hex[:12]}")
    started_at: str = Field(default_factory=utc_now)
    finished_at: str = ""
    outcome: CycleOutcome = "idle"
    detail: str = ""
    hotspots: list[Hotspot] = Field(default_factory=list)
    investigated: list[str] = Field(default_factory=list)
    auto_snoozed: list[str] = Field(default_factory=list)
    handoffs: list[str] = Field(default_factory=list)
    # hotspot key -> handoff URL, so insight records can tell draft from notify.
    handoffs_by_key: dict[str, str] = Field(default_factory=dict)
    cost_usd: float = 0.0
    decision_id: str | None = None
    errors: list[str] = Field(default_factory=list)

    def top(self, limit: int = 6) -> list[Hotspot]:
        return sorted(self.hotspots, key=lambda h: h.score, reverse=True)[:limit]


def hotspot_to_alert_payload(hotspot: Hotspot) -> dict[str, Any]:
    """Render a hotspot as a synthetic Alertmanager-style payload accepted by
    ``graph_runtime.run_investigation_graph``.

    ``source="proactive"`` keeps proactive cases distinct from real reactive
    alerts (different fingerprint). A ``specialist_hint`` label makes routing
    deterministic (``supervisor_route`` honours it) instead of relying on
    keywords in free-text summaries.
    """

    alertname = f"Proactive: {hotspot.title or hotspot.rule_id}"
    summary_lines = [hotspot.summary]
    for ev in hotspot.evidence:
        bits = [b for b in (ev.label, ev.value, ev.threshold, ev.detail) if b]
        if bits:
            summary_lines.append(" · ".join(bits))
    summary = "\n".join(line for line in summary_lines if line)
    labels = {
        "alertname": alertname,
        "host": hotspot.resource or "as215932",
        "service": hotspot.category,
        "severity": hotspot.severity,
        "proactive_rule": hotspot.rule_id,
        "proactive_category": hotspot.category,
        "state": hotspot.severity,
    }
    if hotspot.suggested_specialist:
        labels["specialist_hint"] = hotspot.suggested_specialist
    return {
        "source": "proactive",
        "status": "firing",
        "groupLabels": {"alertname": alertname, "host": labels["host"]},
        "commonLabels": labels,
        "commonAnnotations": {"summary": summary},
        "alerts": [
            {
                "status": "firing",
                "labels": labels,
                "annotations": {"summary": summary},
            }
        ],
        "proactive": {
            "rule_id": hotspot.rule_id,
            "key": hotspot.key,
            "fingerprint": hotspot.fingerprint(),
            "category": hotspot.category,
            "score": hotspot.score,
            "warrants_change": hotspot.warrants_change,
            "recommended_checks": list(hotspot.recommended_checks),
        },
    }
