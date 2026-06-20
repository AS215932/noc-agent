"""Review-gated learning-ledger candidate generation.

This module creates sanitized `learning_ledger_v1` events for the
hyrule-knowledge repo. It never writes approved OKF documents and never promotes
knowledge directly; candidates remain A4/proposed until human review.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from app.cases.models import AtomicCaseProjection, CaseEvent, MetaCaseProjection, utc_now
from app.knowledge.retrieval import KnowledgeCitation

FORBIDDEN_DATA_CLASSES = {"secret", "credential", "raw_log", "packet_capture", "live_trace"}
FORBIDDEN_TEXT_MARKERS = ["secret_value", "authorization:", "bearer ", "packet_capture", "raw_log", "credential"]


def build_lesson_candidate_event(
    case: AtomicCaseProjection | MetaCaseProjection,
    *,
    case_events: Iterable[CaseEvent] = (),
    citations: Iterable[KnowledgeCitation | dict[str, Any]] = (),
    lessons: Iterable[str] = (),
    producer: str = "noc_shadow",
    data_classes: Iterable[str] = ("sanitized_case_summary",),
    target: str = "okf/observed/noc-agent",
    event_id: str | None = None,
) -> dict[str, Any]:
    """Build a sanitized, review-required `lesson_candidate` event."""

    checked_data_classes = [str(item) for item in data_classes]
    denied = sorted(set(checked_data_classes) & FORBIDDEN_DATA_CLASSES)
    if denied:
        raise ValueError(f"forbidden data classes for knowledge candidate: {', '.join(denied)}")

    source_case = _case_source(case)
    event_items = list(case_events)
    citation_items = list(citations)
    event = {
        "id": event_id or f"learn_{uuid4().hex}",
        "ledger_version": "learning_ledger_v1",
        "event_type": "lesson_candidate",
        "event_time": utc_now(),
        "producer": producer,
        "subject": _case_subject(case),
        "summary": _redact(_case_summary(case)),
        "status": "proposed",
        "authority_tier": "A4",
        "source": source_case,
        "data_classes": checked_data_classes,
        "citations": _candidate_citations(case, citation_items),
        "context_pack_ids": [],
        "policy_decision_ids": [],
        "eval_case_ids": list(getattr(case, "feedback_ids", []) or []),
        "metrics": {
            "case_event_count": len(event_items),
            "trace_count": len(getattr(case, "trace_ids", []) or []),
            "knowledge_citation_count": len(citation_items),
            "review_required": True,
        },
        "lessons": [_redact(str(lesson)) for lesson in lessons] or [_default_lesson(case)],
        "promotion": {"review_required": True, "target": target},
        "metadata": {
            "case_kind": case.kind,
            "case_status": case.status,
            "timeline": [_event_timeline_item(event) for event in event_items[-20:]],
            "schema_note": "candidate only; human review required before OKF promotion",
        },
    }
    _assert_no_forbidden_text(event)
    return event


def write_lesson_candidate_event(path: str | Path, event: dict[str, Any]) -> Path:
    """Write a proposed event JSON file without mutating approved knowledge."""

    destination = Path(path)
    if destination.is_dir():
        destination = destination / f"{event['id']}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(event, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def _candidate_citations(
    case: AtomicCaseProjection | MetaCaseProjection,
    citations: Iterable[KnowledgeCitation | dict[str, Any]],
) -> list[dict[str, str]]:
    rendered = [{"source_uri": f"noc-case:{case.case_id}"}]
    for citation in citations:
        if isinstance(citation, KnowledgeCitation):
            rendered.append({"concept_id": citation.doc_id, "source_uri": citation.doc_path})
        else:
            concept_id = str(citation.get("concept_id") or citation.get("doc_id") or "")
            source_uri = str(citation.get("source_uri") or citation.get("doc_path") or "")
            item: dict[str, str] = {}
            if concept_id:
                item["concept_id"] = concept_id
            if source_uri:
                item["source_uri"] = source_uri
            if item:
                rendered.append(item)
    return rendered


def _case_source(case: AtomicCaseProjection | MetaCaseProjection) -> dict[str, Any]:
    source: dict[str, Any] = {"case_id": case.case_id, "case_kind": case.kind, "status": case.status}
    if isinstance(case, AtomicCaseProjection):
        source.update(
            {
                "case_number": case.case_number,
                "fingerprint": case.fingerprint,
                "rule_id": case.rule_id,
                "detector": case.detector,
                "resource_id": case.resource_id,
                "resolution_reason": case.resolution_reason,
            }
        )
    else:
        source.update(
            {
                "event_fingerprint": case.event_fingerprint,
                "event_type": case.event_type,
                "child_case_ids": list(case.child_case_ids),
                "resolution_reason": case.resolution_reason,
                "correlation_quality": case.final_correlation_quality_label,
            }
        )
    return source


def _case_subject(case: AtomicCaseProjection | MetaCaseProjection) -> str:
    return f"noc_case:{case.case_id}"


def _case_summary(case: AtomicCaseProjection | MetaCaseProjection) -> str:
    title = getattr(case, "title", "") or case.case_id
    summary = getattr(case, "summary", "") or getattr(case, "resolution_reason", "") or "case outcome"
    return f"{title}: {summary}"


def _default_lesson(case: AtomicCaseProjection | MetaCaseProjection) -> str:
    if isinstance(case, MetaCaseProjection):
        return _redact(
            f"Review meta-case {case.case_id} for reusable storm-correlation guidance: {case.blast_radius_summary or case.summary}"
        )
    return _redact(f"Review case {case.case_id} for reusable runbook or suppression guidance: {case.summary or case.title}")


def _event_timeline_item(event: CaseEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "occurred_at": event.occurred_at,
        "observed_at": event.observed_at,
    }


def _redact(value: str) -> str:
    redacted = str(value or "")
    lowered = redacted.lower()
    for marker in FORBIDDEN_TEXT_MARKERS:
        while marker in lowered:
            idx = lowered.index(marker)
            redacted = redacted[:idx] + "[redacted]" + redacted[idx + len(marker) :]
            lowered = redacted.lower()
    return redacted.strip() or "sanitized NOC learning candidate"


def _assert_no_forbidden_text(event: dict[str, Any]) -> None:
    rendered = json.dumps(event, sort_keys=True).lower()
    leaked = [marker for marker in FORBIDDEN_TEXT_MARKERS if marker in rendered]
    if leaked:
        raise ValueError(f"knowledge candidate contains forbidden text markers: {', '.join(sorted(leaked))}")
