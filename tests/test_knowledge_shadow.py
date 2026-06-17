from __future__ import annotations

import json
from pathlib import Path

from app.knowledge_shadow import evaluate_context_pack_fixture, evaluate_fixture_dir


FIXTURE_DIR = Path("evals/knowledge_shadow/fixtures")


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
