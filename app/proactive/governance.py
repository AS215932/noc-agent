"""Governance for the proactive loop: per-cycle decision-context snapshots and
the budget / severity / blast-radius gate.

This is the proactive analogue of hyperliquid's governance layer. It does not
itself run anything — it decides *whether* and *how much* the loop may invest in
expensive LLM investigation this cycle, and records an immutable snapshot of the
context the decision was made in (mirrors ``DecisionContextRecorder``).

The blast-radius half (heavy-probe stripping) is enforced where tools are
selected: see ``MCPRuntime.tools_for("proactive", ...)`` and
``PROACTIVE_CHEAP_TOOLS`` in ``app/mcp_runtime.py``. ``auto_heavy_probes`` here
is the policy flag that selection consults.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.config import ProactiveLoopSettings
from app.golden_state import load_golden_manifest
from app.proactive.models import DecisionContext, Hotspot, severity_rank


def _manifest_hash() -> str:
    import hashlib
    import json

    manifest = load_golden_manifest()
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode()).hexdigest()[:32]


def build_decision_context(
    settings: ProactiveLoopSettings,
    *,
    cycle_id: str,
    model_chain: list[str],
    budget_state: dict[str, Any],
    injected_lesson_ids: list[str] | None = None,
) -> DecisionContext:
    return DecisionContext(
        cycle_id=cycle_id,
        manifest_hash=_manifest_hash(),
        perimeter_context_version=settings.ruleset_version,
        scanner_ruleset_version=settings.ruleset_version,
        model_chain=list(model_chain),
        shadow=settings.shadow,
        auto_heavy_probes=settings.auto_heavy_probes,
        budget_state=dict(budget_state),
        injected_lesson_ids=list(injected_lesson_ids or []),
    )


@dataclass(frozen=True)
class GateDecision:
    """How many hotspots the loop may autonomously investigate this cycle."""

    max_investigations: int
    reason: str
    over_budget: bool = False
    eligible: list[Hotspot] = field(default_factory=list)

    @property
    def allowed(self) -> bool:
        return self.max_investigations > 0


def eligible_hotspots(settings: ProactiveLoopSettings, hotspots: list[Hotspot]) -> list[Hotspot]:
    """Hotspots at or above the severity floor, highest score first."""
    floor = severity_rank(settings.severity_floor)
    eligible = [h for h in hotspots if severity_rank(h.severity) >= floor]
    return sorted(eligible, key=lambda h: h.score, reverse=True)


def evaluate_gate(
    settings: ProactiveLoopSettings,
    ledger: dict[str, Any],
    hotspots: list[Hotspot],
) -> GateDecision:
    """Decide the per-cycle investigation budget given daily ledger + flags."""
    eligible = eligible_hotspots(settings, hotspots)

    if settings.shadow:
        return GateDecision(0, "shadow mode: report only", eligible=eligible)
    if not eligible:
        return GateDecision(0, "no hotspot at or above severity floor", eligible=eligible)

    spent_cost = float(ledger.get("cost_usd", 0.0))
    if spent_cost >= settings.max_cost_usd_per_day:
        return GateDecision(
            0,
            f"daily cost budget reached (${settings.max_cost_usd_per_day:.2f})",
            over_budget=True,
            eligible=eligible,
        )

    done_today = int(ledger.get("investigations", 0))
    remaining_day = settings.max_investigations_per_day - done_today
    if remaining_day <= 0:
        return GateDecision(
            0,
            f"daily investigation budget reached ({settings.max_investigations_per_day})",
            over_budget=True,
            eligible=eligible,
        )

    allowed = min(settings.max_investigations_per_cycle, remaining_day, len(eligible))
    return GateDecision(
        allowed,
        f"{allowed} investigation(s) within budget ({done_today}/{settings.max_investigations_per_day} today)",
        eligible=eligible,
    )
