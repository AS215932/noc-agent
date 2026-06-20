"""Case-aware hyrule-knowledge retrieval helpers."""

from __future__ import annotations

from typing import Any

from app.cases.models import AtomicCaseProjection, MetaCaseProjection
from app.knowledge.retrieval import KnowledgeExportRetriever, KnowledgeSearchResult


def case_to_knowledge_context(case: AtomicCaseProjection | MetaCaseProjection) -> dict[str, Any]:
    if isinstance(case, AtomicCaseProjection):
        return {
            "fingerprint": case.fingerprint,
            "rule_id": case.rule_id,
            "detector": case.detector,
            "resource": case.resource_id,
            "site": case.site,
            "customer": case.customer,
            "service": case.service,
            "severity": case.severity,
            "summary": case.summary,
            "title": case.title,
            "symptoms": [case.signal_snapshot.get("summary"), case.signal_snapshot.get("title")],
        }
    return {
        "fingerprint": case.event_fingerprint,
        "event_type": case.event_type,
        "resource": case.suspected_primary_entity,
        "site": case.suspected_site,
        "customer": case.suspected_customer,
        "service": case.suspected_service,
        "summary": case.summary,
        "title": case.title,
        "root_cause_hypothesis": case.root_cause_hypothesis,
        "symptoms": [case.blast_radius_summary, *case.affected_entities, *case.affected_services],
    }


def retrieve_case_knowledge(
    case: AtomicCaseProjection | MetaCaseProjection,
    retriever: KnowledgeExportRetriever,
    *,
    limit: int = 5,
    include_non_authoritative: bool = False,
) -> list[KnowledgeSearchResult]:
    return retriever.search_case_context(
        case_to_knowledge_context(case),
        limit=limit,
        include_non_authoritative=include_non_authoritative,
    )


def trace_citations(results: list[KnowledgeSearchResult]) -> list[dict[str, Any]]:
    return [result.citation.as_trace_dict() for result in results]
