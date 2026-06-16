from dataclasses import replace

from app.config import ProactiveLoopSettings
from app.proactive import governance
from app.proactive.models import Hotspot


def _hs(key: str, severity: str, score: float) -> Hotspot:
    return Hotspot(rule_id="r", key=key, severity=severity, score=score)


BASE = ProactiveLoopSettings(enabled=True, shadow=False, severity_floor="MEDIUM")


def test_eligible_hotspots_respects_floor_and_orders():
    hotspots = [_hs("a", "LOW", 10), _hs("b", "HIGH", 50), _hs("c", "MEDIUM", 30)]
    eligible = governance.eligible_hotspots(BASE, hotspots)
    assert [h.key for h in eligible] == ["b", "c"]  # LOW dropped, score order


def test_shadow_mode_blocks_investigation():
    settings = replace(BASE, shadow=True)
    decision = governance.evaluate_gate(settings, {"investigations": 0, "cost_usd": 0.0}, [_hs("b", "HIGH", 50)])
    assert decision.max_investigations == 0
    assert "shadow" in decision.reason
    assert decision.eligible  # still surfaced for reporting


def test_gate_allows_within_budget():
    settings = replace(BASE, max_investigations_per_cycle=2, max_investigations_per_day=12)
    hotspots = [_hs("b", "HIGH", 50), _hs("c", "MEDIUM", 30), _hs("d", "LOW", 5)]
    decision = governance.evaluate_gate(settings, {"investigations": 0, "cost_usd": 0.0}, hotspots)
    assert decision.max_investigations == 2  # cycle cap, 2 eligible
    assert decision.allowed


def test_gate_blocks_when_daily_investigations_exhausted():
    settings = replace(BASE, max_investigations_per_day=3)
    decision = governance.evaluate_gate(settings, {"investigations": 3, "cost_usd": 0.0}, [_hs("b", "HIGH", 50)])
    assert decision.max_investigations == 0
    assert decision.over_budget


def test_gate_blocks_when_daily_cost_exhausted():
    settings = replace(BASE, max_cost_usd_per_day=10.0)
    decision = governance.evaluate_gate(settings, {"investigations": 0, "cost_usd": 10.5}, [_hs("b", "HIGH", 50)])
    assert decision.max_investigations == 0
    assert decision.over_budget


def test_gate_blocks_when_no_eligible_hotspot():
    decision = governance.evaluate_gate(BASE, {"investigations": 0, "cost_usd": 0.0}, [_hs("a", "LOW", 10)])
    assert decision.max_investigations == 0
    assert not decision.over_budget


def test_decision_context_snapshot():
    ctx = governance.build_decision_context(
        BASE, cycle_id="cyc_1", model_chain=["openrouter:deepseek/deepseek-v4-pro"], budget_state={"investigations": 0}
    )
    assert ctx.manifest_hash.startswith("sha256:")
    assert ctx.model_chain == ["openrouter:deepseek/deepseek-v4-pro"]
    assert ctx.shadow is False
