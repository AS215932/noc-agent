import json
import sqlite3
from pathlib import Path

import pytest

from app.knowledge import KnowledgeExportRetriever


def _write_export(tmp_path: Path) -> Path:
    db = tmp_path / "knowledge.sqlite"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "run_id": "export-1",
                "retrieval_version": "retrieval_v1",
                "policy_version": "knowledge_policy_v1",
                "source_shas": {"knowledge": "knowledge-sha"},
            }
        ),
        encoding="utf-8",
    )
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE concepts (
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            type TEXT NOT NULL,
            title TEXT,
            description TEXT,
            resource TEXT,
            tags_json TEXT NOT NULL DEFAULT '[]',
            truth_owner TEXT,
            authority TEXT,
            confidence TEXT,
            dispute_policy TEXT,
            last_verified_at TEXT,
            review_status TEXT,
            quality_score REAL,
            observed_at TEXT,
            expires_at TEXT,
            enrichment_json TEXT,
            body TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE source_refs (
            id INTEGER PRIMARY KEY,
            concept_id TEXT NOT NULL,
            repo TEXT,
            path TEXT,
            commit_sha TEXT,
            lines TEXT,
            url TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO concepts (id, path, type, title, description, tags_json, authority, review_status, quality_score, body)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "curated/runbooks/router-down",
                "okf/curated/runbooks/router-down.md",
                "Runbook",
                "Router down cascade runbook",
                "Router down events can cause BGP reconvergence and customer reachability storms.",
                json.dumps(["router", "bgp", "storm"]),
                "advisory",
                "approved",
                0.95,
                "# Triage\nRouter down can trigger downstream reachability alerts during reconvergence.",
            ),
            (
                "observed/candidates/noisy-router",
                "okf/observed/noisy-router.md",
                "Lesson",
                "Noisy router hypothesis",
                "Unreviewed router guess.",
                json.dumps(["router"]),
                "A4",
                "proposed",
                0.3,
                "# Candidate\nThis is not approved.",
            ),
            (
                "curated/deprecated/old-router",
                "okf/curated/deprecated/old-router.md",
                "Runbook",
                "Old router advice",
                "Deprecated router procedure.",
                json.dumps(["router"]),
                "advisory",
                "deprecated",
                0.1,
                "# Deprecated\nDo not use.",
            ),
        ],
    )
    conn.execute(
        "INSERT INTO source_refs (concept_id, repo, path, commit_sha, url) VALUES (?, ?, ?, ?, ?)",
        ("curated/runbooks/router-down", "AS215932/knowledge", "okf/curated/runbooks/router-down.md", "abc123", "https://example.invalid"),
    )
    conn.commit()
    conn.close()
    return db


def test_knowledge_retriever_returns_only_approved_authoritative_by_default(tmp_path):
    db = _write_export(tmp_path)
    retriever = KnowledgeExportRetriever(db)

    results = retriever.search("router down BGP storm", limit=5)

    assert [result.citation.doc_id for result in results] == ["curated/runbooks/router-down"]
    citation = results[0].citation
    assert citation.authoritative is True
    assert citation.doc_path == "okf/curated/runbooks/router-down.md"
    assert citation.review_status == "approved"
    assert citation.authority == "advisory"
    assert citation.repo_revision == "abc123"
    assert citation.export_version == "export-1:retrieval_v1:knowledge_policy_v1"
    assert results[0].snippet
    trace = results[0].as_trace_dict()
    assert trace["doc_id"] == citation.doc_id
    assert trace["authoritative"] is True


def test_knowledge_retriever_can_include_non_authoritative_as_labeled_context(tmp_path):
    db = _write_export(tmp_path)
    retriever = KnowledgeExportRetriever(db)

    results = retriever.search("router", limit=10, include_non_authoritative=True)
    by_id = {result.citation.doc_id: result.citation for result in results}

    assert by_id["curated/runbooks/router-down"].authoritative is True
    assert by_id["observed/candidates/noisy-router"].authoritative is False
    assert by_id["curated/deprecated/old-router"].authoritative is False


def test_knowledge_retriever_builds_query_from_case_context(tmp_path):
    db = _write_export(tmp_path)
    retriever = KnowledgeExportRetriever(db)

    results = retriever.search_case_context(
        {
            "rule_id": "bgp_session",
            "resource": "router-r1",
            "event_type": "router_down",
            "symptoms": ["customer reachability storm", "reconvergence"],
        }
    )

    assert results
    assert results[0].citation.doc_id == "curated/runbooks/router-down"


def test_knowledge_retriever_requires_existing_sqlite(tmp_path):
    with pytest.raises(FileNotFoundError):
        KnowledgeExportRetriever(tmp_path / "missing.sqlite")


@pytest.mark.asyncio
async def test_case_knowledge_context_retrieves_and_records_trace_citations(tmp_path):
    from app.cases import CaseService, InMemoryCaseStore, ObservationRecord
    from app.knowledge import retrieve_case_knowledge, trace_citations

    db = _write_export(tmp_path)
    retriever = KnowledgeExportRetriever(db)
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="alertmanager",
            detector="RouterDown",
            rule_id="RouterDown",
            resource="r1",
            service="bgp",
            status="firing",
            annotations={"summary": "router down BGP storm"},
            signal_snapshot={"summary": "router down BGP storm"},
        )
    )
    assert created.case is not None

    results = retrieve_case_knowledge(created.case, retriever)
    citations = trace_citations(results)
    updated = await service.record_knowledge_citations(created.case.case_id, citations, trace_id="trace_1")

    assert results[0].citation.doc_id == "curated/runbooks/router-down"
    assert updated.knowledge_citations[0]["doc_path"] == "okf/curated/runbooks/router-down.md"
    assert updated.knowledge_citations[0]["export_version"] == "export-1:retrieval_v1:knowledge_policy_v1"
    assert updated.trace_ids == ["trace_1"]
    assert [event.event_type for event in await store.case_events(created.case.case_id)][-1] == "hyrule_knowledge_retrieved"
