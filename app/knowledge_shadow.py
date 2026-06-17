"""Read-only fixture evals for AS215932 knowledge context packs.

This module deliberately does not integrate the live NOC runtime with the
knowledge service. It validates committed shadow fixtures only, so NOC Agent can
prove the shape and safety boundaries of future knowledge consumption without
calling MCP, Prometheus, Icinga, LLMs, or production tools.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FORBIDDEN_TOKENS = ["packet_capture", "raw_log", "credential", "secret_value", "authorization:", "bearer "]
LIVE_MARKERS = ["live_mcp_call", "prometheus_query_live", "icinga_live", "packet_capture"]


@dataclass(frozen=True)
class KnowledgeShadowEvalResult:
    fixture: str
    passed: bool
    failures: list[str]
    metrics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "fixture": self.fixture,
            "passed": self.passed,
            "failures": self.failures,
            "metrics": self.metrics,
        }


def load_context_pack_fixture(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"context-pack fixture must be a JSON object: {path}")
    return loaded


def evaluate_context_pack_fixture(path: Path) -> KnowledgeShadowEvalResult:
    pack = load_context_pack_fixture(path)
    failures: list[str] = []
    if pack.get("role") != "noc_shadow":
        failures.append("context pack role must be noc_shadow")
    raw_decision = pack.get("policy_decision")
    decision: dict[str, Any] = raw_decision if isinstance(raw_decision, dict) else {}
    if decision.get("result") != "allow":
        failures.append("policy decision must allow read-only shadow context")
    raw_sections = pack.get("sections")
    sections: list[Any] = raw_sections if isinstance(raw_sections, list) else []
    section_names = {str(section.get("name")) for section in sections if isinstance(section, dict)}
    required_sections = {"intended_state", "observed_state_fixture", "safe_diagnostic_boundaries", "forbidden_actions"}
    missing = sorted(required_sections - section_names)
    if missing:
        failures.append(f"missing required sections: {', '.join(missing)}")
    raw_included_refs = pack.get("included_refs")
    included_refs: list[Any] = raw_included_refs if isinstance(raw_included_refs, list) else []
    if not included_refs:
        failures.append("fixture must include cited references")
    uncited = [ref.get("concept_id") for ref in included_refs if isinstance(ref, dict) and not ref.get("source_refs")]
    if uncited:
        failures.append(f"uncited refs: {', '.join(str(ref) for ref in uncited)}")
    vector_scores = [
        (ref.get("retrieval_scores") or {}).get("vector")
        for ref in included_refs
        if isinstance(ref, dict) and isinstance(ref.get("retrieval_scores"), dict)
    ]
    if any(score is not None for score in vector_scores):
        failures.append("vector scores must remain null in shadow fixtures")
    failures.extend(_forbidden_payload_failures(pack, label="fixture"))
    metrics = {
        "section_count": len(sections),
        "included_ref_count": len(included_refs),
        "vector_scores_null": all(score is None for score in vector_scores),
        "policy_result": decision.get("result"),
    }
    return KnowledgeShadowEvalResult(path.name, not failures, failures, metrics)


def evaluate_fixture_dir(path: Path) -> list[KnowledgeShadowEvalResult]:
    return [evaluate_context_pack_fixture(fixture) for fixture in sorted(path.glob("*.json"))]


def evaluate_learning_event_fixture(path: Path) -> KnowledgeShadowEvalResult:
    event = load_context_pack_fixture(path)
    failures: list[str] = []
    if event.get("ledger_version") != "learning_ledger_v1":
        failures.append("learning event ledger_version must be learning_ledger_v1")
    if event.get("event_type") != "noc_shadow_eval_summary":
        failures.append("learning event must be a noc_shadow_eval_summary")
    if event.get("producer") != "noc_shadow":
        failures.append("learning event producer must be noc_shadow")
    if event.get("authority_tier") != "A4":
        failures.append("shadow learning fixture must remain A4 until reviewed")
    raw_citations = event.get("citations")
    citations: list[Any] = raw_citations if isinstance(raw_citations, list) else []
    if not citations:
        failures.append("learning event requires citations")
    raw_data_classes = event.get("data_classes")
    data_classes: list[Any] = raw_data_classes if isinstance(raw_data_classes, list) else []
    denied = sorted({str(item) for item in data_classes} & {"secret", "credential", "raw_log", "packet_capture", "live_trace"})
    if denied:
        failures.append(f"learning event contains forbidden data classes: {', '.join(denied)}")
    raw_metrics = event.get("metrics")
    metrics: dict[str, Any] = raw_metrics if isinstance(raw_metrics, dict) else {}
    if metrics.get("shadow_only") is not True:
        failures.append("learning event must declare shadow_only=true")
    if metrics.get("live_calls") not in {0, None}:
        failures.append("learning event must not record live calls")
    if metrics.get("vector_scores_null") is not True:
        failures.append("learning event must preserve null vector-score placeholder semantics")
    failures.extend(_forbidden_payload_failures(event, label="learning event"))
    return KnowledgeShadowEvalResult(path.name, not failures, failures, {"citation_count": len(citations), **metrics})


def evaluate_learning_event_dir(path: Path) -> list[KnowledgeShadowEvalResult]:
    return [evaluate_learning_event_fixture(fixture) for fixture in sorted(path.glob("*.json"))]


def _forbidden_payload_failures(payload: dict[str, Any], *, label: str) -> list[str]:
    rendered = json.dumps(payload, sort_keys=True).lower()
    failures: list[str] = []
    leaked = [token for token in FORBIDDEN_TOKENS if token in rendered]
    if leaked:
        failures.append(f"{label} contains forbidden data class marker: {', '.join(leaked)}")
    live_seen = [token for token in LIVE_MARKERS if token in rendered]
    if live_seen:
        failures.append(f"{label} must not include live tool traces: {', '.join(live_seen)}")
    return failures
