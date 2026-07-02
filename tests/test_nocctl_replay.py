import json

import pytest

from app.cases import InsightDecisionRecord, InsightLabel, ObservationRecord
from app.nocctl import _run_replay


@pytest.mark.asyncio
async def test_nocctl_run_replay_returns_metrics(tmp_path):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "observations": [
                    ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing").model_dump(mode="json")
                ]
            }
        ),
        encoding="utf-8",
    )

    result = await _run_replay(str(fixture))

    assert result["fixture"] == str(fixture)
    assert result["metrics"]["atomic_case_count"] == 1
    assert result["metrics"]["active_case_count"] == 1


@pytest.mark.asyncio
async def test_nocctl_run_insight_replay_returns_policy_metrics(tmp_path):
    fixture = tmp_path / "insights.json"
    decision = InsightDecisionRecord(
        insight_id="ins1",
        fingerprint="fp1",
        sampling_class="surfaced",
        candidate_type="hotspot",
        candidate_source="scanner",
        action_selected="notify",
        support_facts=["fact"],
    )
    label = InsightLabel(insight_id="ins1", reference_action="notify", support_facts=["fact"])
    fixture.write_text(
        json.dumps({"insights": [decision.model_dump(mode="json")], "labels": [label.model_dump(mode="json")]}),
        encoding="utf-8",
    )

    result = await _run_replay(str(fixture), insights=True)

    assert result["fixture"] == str(fixture)
    assert result["metrics"]["idq"] == 1.0
    assert result["metrics"]["cgs"] == 1.0
