"""Loop Handoff Protocol integration for Hyrule Knowledge.

The handlers in this module keep Knowledge Loop side effects behind LHP feature
flags. They consume CaseService outbox intents, read from a pinned local
knowledge export, and write only review-gated learning candidates plus
``KnowledgeArtifact`` rows. They do not promote durable knowledge directly.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from app import log
from app.cases.lhp import KnowledgeArtifact, OutcomeRecord, lhp_payload_hash, sanitize_lhp_payload, sanitize_lhp_text, sanitize_lhp_token
from app.cases.models import AtomicCaseProjection, CaseEvent, OutboxIntent, TraceRecord
from app.cases.outbox import OutboxHandlerResult
from app.cases.service import CaseService
from app.config import LoopHandoffSettings
from app.knowledge.case_context import retrieve_case_knowledge, trace_citations
from app.knowledge.promotion import build_lesson_candidate_event, write_lesson_candidate_event
from app.knowledge.retrieval import KnowledgeExportRetriever, KnowledgeSearchResult
from app.safe_errors import classify_exception


@dataclass(frozen=True, slots=True)
class KnowledgeContextRequest:
    case_id: str
    handoff_id: str = ""
    objective_key: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    max_artifacts: int = 10
    max_tokens_equivalent: int = 3000


@dataclass(frozen=True, slots=True)
class KnowledgeContextResponse:
    context_id: str
    citations: list[dict[str, Any]]
    context_refs: list[str]
    bounded_summary: str
    export_version: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeArtifactProposalRequest:
    case_id: str
    handoff_id: str = ""
    outcome_id: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


class KnowledgeTransport(Protocol):
    async def request_context(
        self,
        request: KnowledgeContextRequest,
        case: AtomicCaseProjection,
    ) -> KnowledgeContextResponse: ...

    async def propose_artifacts(
        self,
        request: KnowledgeArtifactProposalRequest,
        case: AtomicCaseProjection,
        outcome: OutcomeRecord | None,
    ) -> list[KnowledgeArtifact]: ...


class LocalKnowledgeTransport:
    """Local read-only Knowledge export transport.

    This is the first LHP-v1 backend. It reads ``knowledge.sqlite`` and the
    paired manifest from a pinned export path; future CLI/MCP transports can be
    added behind the same protocol.
    """

    def __init__(self, *, sqlite_path: str | Path, manifest_path: str | Path | None = None) -> None:
        self.retriever = KnowledgeExportRetriever(sqlite_path, manifest_path=manifest_path)

    async def request_context(
        self,
        request: KnowledgeContextRequest,
        case: AtomicCaseProjection,
    ) -> KnowledgeContextResponse:
        results = await asyncio.to_thread(
            retrieve_case_knowledge,
            case,
            self.retriever,
            limit=max(0, request.max_artifacts),
            include_non_authoritative=False,
        )
        safe_citations = _safe_citations(trace_citations(results))
        context_refs = _context_refs(safe_citations)
        bounded_summary = _bounded_context_summary(results, max_tokens_equivalent=request.max_tokens_equivalent)
        payload = sanitize_lhp_payload(
            {
                "schema": "lhp_knowledge_context.v1",
                "case_id": request.case_id,
                "handoff_id": request.handoff_id,
                "objective_key": request.objective_key,
                "context_refs": context_refs,
                "citation_count": len(safe_citations),
                "untrusted_request_payload": request.payload,
            }
        )
        safe_payload = payload if isinstance(payload, dict) else {"value": payload}
        context_id = f"trace_lhp_knowledge_{lhp_payload_hash({'case_id': request.case_id, 'refs': context_refs})[:12]}"
        return KnowledgeContextResponse(
            context_id=sanitize_lhp_token(context_id, limit=180),
            citations=safe_citations,
            context_refs=context_refs,
            bounded_summary=bounded_summary,
            export_version=self.retriever.export.export_version,
            payload=safe_payload,
        )

    async def propose_artifacts(
        self,
        request: KnowledgeArtifactProposalRequest,
        case: AtomicCaseProjection,
        outcome: OutcomeRecord | None,
    ) -> list[KnowledgeArtifact]:
        return build_knowledge_artifact_proposals(case, outcome=outcome, handoff_id=request.handoff_id, payload=request.payload)


def build_lhp_knowledge_context_handler(
    case_service: CaseService,
    *,
    settings: LoopHandoffSettings,
    transport: KnowledgeTransport | None = None,
):
    local_transport = transport

    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        if not intent.case_id:
            raise ValueError("knowledge context intent requires case_id")
        case = await case_service.store.get_case(intent.case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found for LHP knowledge context: {intent.case_id}")
        request = KnowledgeContextRequest(
            case_id=case.case_id,
            handoff_id=sanitize_lhp_token(intent.payload.get("handoff_id", ""), limit=180),
            objective_key=sanitize_lhp_token(intent.payload.get("objective_key", ""), limit=180),
            payload=_safe_dict(intent.payload),
            max_artifacts=max(0, min(settings.knowledge_context_max_artifacts, 50)),
            max_tokens_equivalent=max(200, min(settings.knowledge_context_max_tokens_equivalent, 12_000)),
        )
        try:
            active_transport = local_transport or LocalKnowledgeTransport(
                sqlite_path=settings.knowledge_export_sqlite,
                manifest_path=settings.knowledge_export_manifest,
            )
            response = await asyncio.wait_for(
                active_transport.request_context(request, case),
                timeout=max(1, settings.knowledge_context_timeout_s),
            )
        except Exception as exc:
            await _record_context_unavailable(case_service, case, intent, exc)
            raise
        await case_service.record_trace(
            TraceRecord(
                trace_id=response.context_id,
                case_id=case.case_id,
                trace_type="knowledge_retrieval",
                policy_version=case_service.policy.policy_version,
                knowledge_export_version=response.export_version,
                payload={
                    "handoff_id": request.handoff_id,
                    "objective_key": request.objective_key,
                    "context_refs": response.context_refs,
                    "bounded_summary": response.bounded_summary,
                    "citation_count": len(response.citations),
                    "untrusted_evidence": True,
                },
            )
        )
        await case_service.record_knowledge_citations(case.case_id, response.citations, trace_id=response.context_id)
        return OutboxHandlerResult(
            external_id=response.context_id,
            payload_updates={
                "trace_id": response.context_id,
                "context_refs": response.context_refs,
                "citation_count": len(response.citations),
                "export_version": response.export_version,
                "bounded_summary": response.bounded_summary,
            },
        )

    return handle


def build_lhp_knowledge_artifact_handler(
    case_service: CaseService,
    *,
    settings: LoopHandoffSettings,
    transport: KnowledgeTransport | None = None,
):
    local_transport = transport
    destination = Path(settings.knowledge_candidate_dir)

    async def handle(intent: OutboxIntent) -> OutboxHandlerResult:
        if not intent.case_id:
            raise ValueError("knowledge artifact proposal intent requires case_id")
        case = await case_service.store.get_case(intent.case_id)
        if not isinstance(case, AtomicCaseProjection):
            raise KeyError(f"atomic case not found for LHP knowledge artifact proposal: {intent.case_id}")
        request = KnowledgeArtifactProposalRequest(
            case_id=case.case_id,
            handoff_id=sanitize_lhp_token(intent.payload.get("handoff_id", ""), limit=180),
            outcome_id=sanitize_lhp_token(intent.payload.get("outcome_id", ""), limit=180),
            payload=_safe_dict(intent.payload),
        )
        outcome = await _select_outcome(case_service, case.case_id, request.outcome_id)
        active_transport = local_transport or _ArtifactOnlyKnowledgeTransport()
        artifacts = await active_transport.propose_artifacts(request, case, outcome)
        if not artifacts:
            outcome = None
            artifacts = build_knowledge_artifact_proposals(
                case,
                outcome=None,
                handoff_id=request.handoff_id,
                payload={"reason": "no_artifacts_proposed"},
            )
        events = await case_service.store.case_events(case.case_id)
        learning_event = build_lesson_candidate_event(
            case,
            case_events=events,
            citations=case.knowledge_citations,
            lessons=[artifact.summary for artifact in artifacts],
            producer="noc_lhp_knowledge_loop",
            target=str(request.payload.get("target") or "okf/observed/noc-agent"),
            event_id=_learning_event_id(case.case_id, outcome, request.handoff_id),
        )
        path = write_lesson_candidate_event(destination, learning_event)
        stored: list[KnowledgeArtifact] = []
        for artifact in artifacts:
            enriched = artifact.model_copy(deep=True)
            enriched.payload.update(
                {
                    "learning_event_id": learning_event["id"],
                    "candidate_path": str(path),
                    "review_required": True,
                    "untrusted_evidence": True,
                }
            )
            stored.append(await case_service.record_lhp_knowledge_artifact(enriched))
        log.info(
            "lhp_knowledge_artifacts_proposed",
            case_id=case.case_id,
            outcome_id=outcome.outcome_id if outcome else "",
            artifact_count=len(stored),
            learning_event_id=learning_event["id"],
        )
        return OutboxHandlerResult(
            external_id=str(learning_event["id"]),
            external_url=str(path),
            payload_updates={
                "learning_event_id": learning_event["id"],
                "artifact_ids": [artifact.artifact_id for artifact in stored],
                "artifact_count": len(stored),
                "review_status": "pending",
            },
        )

    return handle


def build_knowledge_artifact_proposals(
    case: AtomicCaseProjection,
    *,
    outcome: OutcomeRecord | None,
    handoff_id: str = "",
    payload: dict[str, Any] | None = None,
) -> list[KnowledgeArtifact]:
    safe_payload = _safe_dict(payload or {})
    if outcome is None:
        return [
            _artifact(
                case,
                handoff_id=handoff_id,
                artifact_type="learning_gap_recorded",
                summary="No verified outcome record was available for post-resolution learning; human review should decide whether a durable lesson is warranted.",
                source_refs=_source_refs(case, handoff_id=handoff_id, outcome=None),
                payload={"reason": safe_payload.get("reason", "missing_outcome"), "review_required": True},
            )
        ]
    artifacts = [
        _artifact(
            case,
            handoff_id=handoff_id,
            artifact_type="root_cause_summary",
            summary=_root_cause_summary(case, outcome),
            source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
            payload={"outcome_id": outcome.outcome_id, "case_type": outcome.case_type, "fingerprint": outcome.fingerprint},
        ),
        _artifact(
            case,
            handoff_id=handoff_id,
            artifact_type="remediation_summary",
            summary=_remediation_summary(case, outcome),
            source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
            payload={"outcome_id": outcome.outcome_id, "validation": outcome.validation},
        ),
        _artifact(
            case,
            handoff_id=handoff_id,
            artifact_type="runbook_update_proposal",
            summary=_runbook_update_summary(case, outcome),
            source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
            payload={"outcome_id": outcome.outcome_id, "proposal_target": "runbook", "review_required": True},
        ),
        _artifact(
            case,
            handoff_id=handoff_id,
            artifact_type="memory_proposal",
            summary=_memory_summary(case, outcome),
            source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
            payload={"outcome_id": outcome.outcome_id, "proposal_target": "case_memory", "review_required": True},
        ),
        _artifact(
            case,
            handoff_id=handoff_id,
            artifact_type="private_eval_proposal",
            summary=_eval_summary(case, outcome),
            source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
            payload={"outcome_id": outcome.outcome_id, "proposal_target": "private_eval", "review_required": True},
        ),
    ]
    if _guardrail_applicable(outcome, safe_payload):
        artifacts.append(
            _artifact(
                case,
                handoff_id=handoff_id,
                artifact_type="schema_tool_guardrail_improvement_proposal",
                summary=_guardrail_summary(case, outcome),
                source_refs=_source_refs(case, handoff_id=handoff_id, outcome=outcome),
                payload={"outcome_id": outcome.outcome_id, "proposal_target": "schema_tool_guardrail", "review_required": True},
            )
        )
    return artifacts


class _ArtifactOnlyKnowledgeTransport:
    async def request_context(
        self,
        request: KnowledgeContextRequest,
        case: AtomicCaseProjection,
    ) -> KnowledgeContextResponse:
        raise NotImplementedError("artifact-only transport does not provide context")

    async def propose_artifacts(
        self,
        request: KnowledgeArtifactProposalRequest,
        case: AtomicCaseProjection,
        outcome: OutcomeRecord | None,
    ) -> list[KnowledgeArtifact]:
        return build_knowledge_artifact_proposals(case, outcome=outcome, handoff_id=request.handoff_id, payload=request.payload)


async def _select_outcome(case_service: CaseService, case_id: str, outcome_id: str) -> OutcomeRecord | None:
    outcomes = await case_service.list_lhp_outcomes(case_id=case_id)
    if outcome_id:
        for outcome in outcomes:
            if outcome.outcome_id == outcome_id:
                return outcome
    return outcomes[-1] if outcomes else None


async def _record_context_unavailable(case_service: CaseService, case: AtomicCaseProjection, intent: OutboxIntent, exc: Exception) -> None:
    safe = classify_exception(exc)
    await case_service.store.append_event(
        CaseEvent(
            case_id=case.case_id,
            event_type="knowledge_context_unavailable",
            actor_type="system",
            source="knowledge",
            policy_version=case_service.policy.policy_version,
            payload={
                "outbox_id": intent.outbox_id,
                "category": safe.category,
                "error_type": sanitize_lhp_token(type(exc).__name__, limit=80),
                "retry_later": True,
            },
        )
    )


def _safe_dict(value: Any) -> dict[str, Any]:
    sanitized = sanitize_lhp_payload(value or {})
    return sanitized if isinstance(sanitized, dict) else {"value": sanitized}


def _safe_citations(citations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe = sanitize_lhp_payload(citations)
    if not isinstance(safe, list):
        return []
    return [item for item in safe if isinstance(item, dict)]


def _context_refs(citations: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for citation in citations[:50]:
        doc = citation.get("doc_path") or citation.get("doc_id")
        if doc:
            refs.append(f"knowledge:{sanitize_lhp_token(doc, limit=220)}")
    return refs


def _bounded_context_summary(results: list[KnowledgeSearchResult], *, max_tokens_equivalent: int) -> str:
    char_budget = max(400, min(max_tokens_equivalent * 4, 12_000))
    lines = ["Hyrule Knowledge context (bounded, reviewable citations):"]
    for result in results:
        citation = result.citation
        doc = sanitize_lhp_text(citation.doc_path or citation.doc_id, limit=240)
        title = sanitize_lhp_text(result.title or citation.doc_id, limit=200)
        snippet = sanitize_lhp_text(result.snippet, limit=500)
        status = sanitize_lhp_text(f"{citation.review_status}/{citation.authority}", limit=80)
        lines.append(f"- {title} [{doc}] {status}: {snippet}")
        rendered = "\n".join(lines)
        if len(rendered) >= char_budget:
            return rendered[:char_budget]
    return sanitize_lhp_text("\n".join(lines), limit=char_budget)


def _artifact(
    case: AtomicCaseProjection,
    *,
    handoff_id: str,
    artifact_type: str,
    summary: str,
    source_refs: list[str],
    payload: dict[str, Any],
) -> KnowledgeArtifact:
    safe_payload = _safe_dict({**payload, "case_status": case.status, "review_required": True, "untrusted_evidence": True})
    return KnowledgeArtifact(
        case_id=case.case_id,
        handoff_id=handoff_id,
        artifact_type=artifact_type,
        scope=_artifact_scope(case),
        status="proposed",
        review_status="pending",
        summary=sanitize_lhp_text(summary, limit=1_000),
        source_refs=source_refs,
        payload=safe_payload,
    )


def _source_refs(case: AtomicCaseProjection, *, handoff_id: str, outcome: OutcomeRecord | None) -> list[str]:
    refs = [f"noc-case:{sanitize_lhp_token(case.case_id, limit=180)}"]
    if handoff_id:
        refs.append(f"lhp-handoff:{sanitize_lhp_token(handoff_id, limit=180)}")
    if outcome is not None:
        refs.append(f"lhp-outcome:{sanitize_lhp_token(outcome.outcome_id, limit=180)}")
    for citation in case.knowledge_citations[:10]:
        doc = citation.get("doc_path") or citation.get("doc_id")
        if doc:
            refs.append(f"knowledge:{sanitize_lhp_token(doc, limit=220)}")
    return refs[:50]


def _artifact_scope(case: AtomicCaseProjection) -> str:
    parts = [case.rule_id or case.detector or "case", case.resource_id or case.fingerprint or case.case_id]
    return sanitize_lhp_text(":".join(parts), limit=300)


def _root_cause_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    action = outcome.action_taken or outcome.proposed_action or case.resolution_reason or "verified remediation cleared the monitored condition"
    return f"Root cause candidate for {case.case_id}: {case.summary or case.title or 'case condition'}; outcome indicates {action}."


def _remediation_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    validation = ", ".join(sorted(str(key) for key in outcome.validation)[:8]) or "verification objectives passed"
    return f"Remediation candidate for {case.case_id}: NOC verifier accepted the outcome after {validation}."


def _runbook_update_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    return f"Review runbook guidance for {case.rule_id or outcome.case_type or 'this case type'} using case {case.case_id} as evidence; keep any resulting content human-reviewed."


def _memory_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    return f"Review whether fingerprint {outcome.fingerprint or case.fingerprint or 'unknown'} should become a reusable case-memory pattern after human approval."


def _eval_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    return f"Create a private eval candidate that replays case {case.case_id}, expected verification evidence, and safety constraints."


def _guardrail_summary(case: AtomicCaseProjection, outcome: OutcomeRecord) -> str:
    return f"Review schema/tool/guardrail improvements from case {case.case_id}; proposed only because outcome metadata indicated applicability."


def _guardrail_applicable(outcome: OutcomeRecord, payload: dict[str, Any]) -> bool:
    safety = outcome.safety or {}
    learning = outcome.learning or {}
    return bool(
        payload.get("guardrail_applicable")
        or safety.get("policy_violations")
        or safety.get("unauthorized_tool_calls")
        or learning.get("schema_or_guardrail_improved")
    )


def _learning_event_id(case_id: str, outcome: OutcomeRecord | None, handoff_id: str) -> str:
    parts = ["learn_lhp", sanitize_lhp_token(case_id, limit=80)]
    if outcome is not None:
        parts.append(sanitize_lhp_token(outcome.outcome_id, limit=80))
    elif handoff_id:
        parts.append(sanitize_lhp_token(handoff_id, limit=80))
    else:
        parts.append("gap")
    return "_".join(parts)[:180]
