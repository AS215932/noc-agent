import json

import pytest

from app.cases import ObservationRecord
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
