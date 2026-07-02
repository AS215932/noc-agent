from __future__ import annotations

import json
from pathlib import Path

from agent_core.contracts import InsightDecisionRecord as CoreInsightDecisionRecord
from agent_core.contracts import InsightLabel as CoreInsightLabel

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "insights" / "decisions.json"


def test_noc_insight_fixtures_validate_against_agent_core_contract() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    actions: set[str] = set()
    for row in payload["insights"]:
        actions.add(str(row["action_selected"]))
        CoreInsightDecisionRecord.model_validate(row)

    assert actions == {"notify", "question", "draft", "stay_silent"}

    for row in payload["labels"]:
        CoreInsightLabel.model_validate(row)
