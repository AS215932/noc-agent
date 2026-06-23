"""Replayable hyrule-knowledge integration helpers."""

from app.knowledge.case_context import case_to_knowledge_context, retrieve_case_knowledge, trace_citations
from app.knowledge.lhp import (
    KnowledgeArtifactProposalRequest,
    KnowledgeContextRequest,
    KnowledgeContextResponse,
    KnowledgeTransport,
    LocalKnowledgeTransport,
    build_knowledge_artifact_proposals,
    build_lhp_knowledge_artifact_handler,
    build_lhp_knowledge_context_handler,
)
from app.knowledge.outbox import build_knowledge_candidate_handler
from app.knowledge.promotion import build_lesson_candidate_event, write_lesson_candidate_event
from app.knowledge.retrieval import KnowledgeCitation, KnowledgeExport, KnowledgeExportRetriever, KnowledgeSearchResult

__all__ = [
    "KnowledgeArtifactProposalRequest",
    "KnowledgeCitation",
    "KnowledgeContextRequest",
    "KnowledgeContextResponse",
    "KnowledgeExport",
    "KnowledgeExportRetriever",
    "KnowledgeSearchResult",
    "KnowledgeTransport",
    "LocalKnowledgeTransport",
    "build_knowledge_artifact_proposals",
    "build_knowledge_candidate_handler",
    "build_lesson_candidate_event",
    "build_lhp_knowledge_artifact_handler",
    "build_lhp_knowledge_context_handler",
    "case_to_knowledge_context",
    "retrieve_case_knowledge",
    "trace_citations",
    "write_lesson_candidate_event",
]
