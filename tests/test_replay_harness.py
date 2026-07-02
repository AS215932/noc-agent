import json

import pytest

from app.cases import (
    InsightDecisionRecord,
    InsightLabel,
    ObservationRecord,
    load_insight_fixture,
    load_observation_fixture,
    replay_insights,
    replay_observations,
)


@pytest.mark.asyncio
async def test_replay_observations_keeps_case_active_when_no_clean_observation_occurs():
    result = await replay_observations(
        [
            ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing"),
            # No second observation: skipped scan / absence is intentionally not clean evidence.
        ]
    )

    metrics = await result.metrics()

    assert metrics["atomic_case_count"] == 1
    assert metrics["resolved_case_count"] == 0
    assert metrics["active_case_count"] == 1


@pytest.mark.asyncio
async def test_replay_observations_resolves_only_on_positive_clean():
    result = await replay_observations(
        [
            ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing"),
            ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="resolved", source_health="healthy"),
        ]
    )

    metrics = await result.metrics()

    assert metrics["atomic_case_count"] == 1
    assert metrics["resolved_case_count"] == 1
    assert metrics["active_case_count"] == 0


@pytest.mark.asyncio
async def test_replay_observations_detects_router_down_meta_case():
    result = await replay_observations(
        [
            ObservationRecord(source="alertmanager", detector="RouterDown", resource="r1", site="ams", service="network", status="firing"),
            ObservationRecord(
                source="alertmanager",
                detector="CustomerReachability",
                resource="cust-a",
                site="ams",
                service="network",
                status="firing",
            ),
        ]
    )

    metrics = await result.metrics()
    meta_cases = await result.meta_cases()

    assert metrics["atomic_case_count"] == 2
    assert metrics["meta_case_count"] == 1
    assert meta_cases[0].status == "active_event"
    assert len(meta_cases[0].child_case_ids) == 2


def test_load_observation_fixture_accepts_list_or_object(tmp_path):
    rows = [ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing").model_dump(mode="json")]
    list_path = tmp_path / "list.json"
    object_path = tmp_path / "object.json"
    list_path.write_text(json.dumps(rows), encoding="utf-8")
    object_path.write_text(json.dumps({"observations": rows}), encoding="utf-8")

    assert load_observation_fixture(list_path)[0].rule_id == "bgp"
    assert load_observation_fixture(object_path)[0].rule_id == "bgp"


@pytest.mark.asyncio
async def test_replay_insights_scores_idq_and_cgs():
    decision = InsightDecisionRecord(
        insight_id="ins1",
        fingerprint="fp1",
        sampling_class="surfaced",
        candidate_type="hotspot",
        candidate_source="scanner",
        action_selected="notify",
        support_facts=["disk free below threshold", "eta 2h"],
    )
    label = InsightLabel(
        insight_id="ins1",
        reference_action="notify",
        support_facts=["disk free below threshold", "eta 2h"],
        faithfulness_verdict="faithful",
    )

    result = await replay_insights([decision], [label])
    metrics = await result.metrics()

    assert metrics["idq"] == 1.0
    assert metrics["cgs"] == 1.0
    assert metrics["silence_rate"] == 0.0


def test_load_insight_fixture(tmp_path):
    decision = InsightDecisionRecord(
        insight_id="ins1",
        fingerprint="fp1",
        sampling_class="withheld_logged",
        candidate_type="hotspot",
        candidate_source="scanner",
        action_selected="stay_silent",
    )
    label = InsightLabel(insight_id="ins1", reference_action="stay_silent")
    fixture = tmp_path / "insights.json"
    fixture.write_text(
        json.dumps(
            {
                "insights": [decision.model_dump(mode="json")],
                "labels": [label.model_dump(mode="json")],
            }
        ),
        encoding="utf-8",
    )

    decisions, labels = load_insight_fixture(fixture)

    assert decisions[0].insight_id == "ins1"
    assert labels[0].reference_action == "stay_silent"
