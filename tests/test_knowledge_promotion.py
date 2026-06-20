import json

import pytest

from app.cases import AtomicCaseProjection, CaseEvent, MetaCaseProjection
from app.knowledge import KnowledgeCitation, build_lesson_candidate_event, write_lesson_candidate_event


def test_build_lesson_candidate_event_is_review_gated_and_cited():
    case = AtomicCaseProjection(
        case_id="case_1",
        case_number="NOC-1",
        fingerprint="fp",
        rule_id="disk_fill",
        detector="disk_fill",
        resource_id="log:/var",
        title="Disk fill",
        summary="Disk recovered after cleanup",
        status="resolved",
        resolution_reason="positive_clean_observation",
    )
    event = build_lesson_candidate_event(
        case,
        case_events=[CaseEvent(case_id="case_1", event_type="case_resolved_positive_clean")],
        citations=[
            KnowledgeCitation(
                doc_id="curated/runbooks/disk-fill",
                doc_path="okf/curated/runbooks/disk-fill.md",
                review_status="approved",
                authority="advisory",
                section="Cleanup",
                repo_revision="abc123",
                export_version="export-1",
                score=3.0,
            )
        ],
        lessons=["Disk fill cases should cite the cleanup runbook and wait for a clean observation."],
        event_id="learn_" + "a" * 32,
    )

    assert event["id"] == "learn_" + "a" * 32
    assert event["ledger_version"] == "learning_ledger_v1"
    assert event["event_type"] == "lesson_candidate"
    assert event["producer"] == "noc_shadow"
    assert event["status"] == "proposed"
    assert event["authority_tier"] == "A4"
    assert event["promotion"]["review_required"] is True
    assert event["source"]["case_id"] == "case_1"
    assert event["source"]["resolution_reason"] == "positive_clean_observation"
    assert {tuple(sorted(citation.items())) for citation in event["citations"]} >= {
        tuple(sorted({"source_uri": "noc-case:case_1"}.items())),
        tuple(sorted({"concept_id": "curated/runbooks/disk-fill", "source_uri": "okf/curated/runbooks/disk-fill.md"}.items())),
    }
    assert event["metadata"]["timeline"][0]["event_type"] == "case_resolved_positive_clean"


def test_build_lesson_candidate_event_rejects_forbidden_data_classes():
    case = AtomicCaseProjection(case_id="case_1", title="Safe case", summary="Safe summary")

    with pytest.raises(ValueError):
        build_lesson_candidate_event(case, data_classes=["sanitized_case_summary", "raw_log"])


def test_build_lesson_candidate_event_redacts_forbidden_text_markers():
    case = AtomicCaseProjection(
        case_id="case_1",
        title="Credential exposure avoided",
        summary="secret_value should not enter the learning ledger",
    )

    event = build_lesson_candidate_event(case, event_id="learn_" + "b" * 32)
    rendered = json.dumps(event).lower()

    assert "secret_value" not in rendered
    assert "[redacted]" in rendered


def test_build_meta_case_lesson_candidate_captures_child_scope():
    meta = MetaCaseProjection(
        case_id="meta_1",
        title="Router cascade",
        summary="Router down caused downstream alerts",
        status="resolved",
        event_type="router_down",
        child_case_ids=["case_a", "case_b"],
        blast_radius_summary="two customer services affected",
        final_correlation_quality_label="confirmed",
    )

    event = build_lesson_candidate_event(meta, event_id="learn_" + "c" * 32)

    assert event["source"]["case_kind"] == "meta"
    assert event["source"]["event_type"] == "router_down"
    assert event["source"]["child_case_ids"] == ["case_a", "case_b"]
    assert "storm-correlation" in event["lessons"][0]


def test_write_lesson_candidate_event_writes_json(tmp_path):
    case = AtomicCaseProjection(case_id="case_1", title="Case", summary="Summary")
    event = build_lesson_candidate_event(case, event_id="learn_" + "d" * 32)

    path = write_lesson_candidate_event(tmp_path, event)

    assert path.name == event["id"] + ".json"
    assert json.loads(path.read_text(encoding="utf-8"))["id"] == event["id"]
