from __future__ import annotations

import json
from pathlib import Path

from app.knowledge_shadow import (
    evaluate_context_pack_fixture,
    evaluate_fixture_dir,
    evaluate_learning_event_dir,
    evaluate_learning_event_fixture,
)


FIXTURE_DIR = Path("evals/knowledge_shadow/fixtures")
LEARNING_EVENT_DIR = Path("evals/knowledge_shadow/learning-events")


def test_committed_knowledge_shadow_fixtures_pass() -> None:
    results = evaluate_fixture_dir(FIXTURE_DIR)
    assert len(results) == 4
    assert all(result.passed for result in results), [result.as_dict() for result in results]
    assert all(result.metrics["vector_scores_null"] for result in results)


def test_knowledge_shadow_eval_rejects_live_trace_markers(tmp_path: Path) -> None:
    fixture = json.loads((FIXTURE_DIR / "noc-agent.json").read_text(encoding="utf-8"))
    fixture["sections"][1]["body"] = "live_mcp_call returned data"
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")

    result = evaluate_context_pack_fixture(path)

    assert not result.passed
    assert any("live tool traces" in failure for failure in result.failures)


def test_knowledge_shadow_eval_requires_citations(tmp_path: Path) -> None:
    fixture = json.loads((FIXTURE_DIR / "noc-agent.json").read_text(encoding="utf-8"))
    fixture["included_refs"][0]["source_refs"] = []
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")

    result = evaluate_context_pack_fixture(path)

    assert not result.passed
    assert any("uncited refs" in failure for failure in result.failures)


def test_committed_learning_event_fixtures_pass() -> None:
    results = evaluate_learning_event_dir(LEARNING_EVENT_DIR)
    assert len(results) == 1
    assert all(result.passed for result in results), [result.as_dict() for result in results]
    assert results[0].metrics["shadow_only"] is True


def test_learning_event_rejects_live_calls(tmp_path: Path) -> None:
    fixture = json.loads((LEARNING_EVENT_DIR / "noc-shadow-summary.json").read_text(encoding="utf-8"))
    fixture["metrics"]["live_calls"] = 1
    path = tmp_path / "bad-learning.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")

    result = evaluate_learning_event_fixture(path)

    assert not result.passed
    assert any("live calls" in failure for failure in result.failures)
