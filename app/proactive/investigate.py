"""Phase 2 bridge: turn a ranked :class:`~app.proactive.models.Hotspot` into a
synthetic alert and drive the *existing* investigation graph on it.

A proactive concern is rendered as an Alertmanager-style payload
(:func:`hotspot_to_alert_payload`), run through the same dedupe → routing →
evidence-validation → drift-check → Discord-report pipeline the reactive webhook
uses (``app.main.investigate_alert``), and the only proactive-specific twist is
the **tool view**: heavy read-only probes are stripped unless
``auto_heavy_probes`` is set (the user's "cheap reads auto, escalate heavy"
posture). Stripping is per-run (a wrapper around the shared runtime) so it never
affects concurrent reactive investigations.
"""

from __future__ import annotations

from typing import Any

from app import log
from app.config import ProactiveLoopSettings
from app.mcp_runtime import PROACTIVE_HEAVY_TOOLS
from app.proactive.loop import InvestigationOutcome, Investigator
from app.proactive.models import DecisionContext, Hotspot, hotspot_to_alert_payload
from app.safe_errors import classify_exception, log_exception


class HeavyProbeFilteredRuntime:
    """A per-run view of the shared ``MCPRuntime`` that hides heavy read-only
    probe tools from the agent's toolset. Everything else (``call_tool``,
    health, clients) delegates unchanged."""

    def __init__(self, inner: Any, *, allow_heavy: bool):
        self._inner = inner
        self._allow_heavy = allow_heavy

    def tools_for(self, specialist: str | None = None) -> list[Any]:
        tools = self._inner.tools_for(specialist)
        if self._allow_heavy:
            return tools
        return [t for t in tools if getattr(t, "name", "") not in PROACTIVE_HEAVY_TOOLS]

    def toolsets_for(self, specialist: str | None = None) -> list[Any]:
        if self._allow_heavy:
            return self._inner.toolsets_for(specialist)
        from pydantic_ai.toolsets import FunctionToolset

        tools = self.tools_for(specialist)
        return [FunctionToolset(tools)] if tools else []

    def __getattr__(self, name: str) -> Any:
        # Delegate call_tool, clients, health, live_health, disconnect, etc.
        return getattr(self._inner, name)


def build_investigator(mcp_runtime: Any, settings: ProactiveLoopSettings) -> Investigator:
    """Build the coroutine the proactive loop calls per eligible hotspot.

    Returns ``None`` for hotspots that intake de-dupes/suppresses (e.g. the same
    hotspot was just investigated), so the loop neither double-spends nor spams.
    """

    async def investigate(hotspot: Hotspot, decision: DecisionContext) -> InvestigationOutcome | None:
        # Lazy import avoids an import cycle: app.main imports this module's
        # builder at startup, after app.main is fully initialised.
        from app import graph_runtime
        from app.main import investigate_alert

        payload = hotspot_to_alert_payload(hotspot)
        intake = await graph_runtime.intake_alert(payload)
        if not intake.should_investigate or intake.case is None:
            log.info(
                "proactive_investigation_skipped",
                hotspot=hotspot.key,
                action=intake.action,
                reason="deduped_or_suppressed",
            )
            return None

        runtime = HeavyProbeFilteredRuntime(mcp_runtime, allow_heavy=settings.auto_heavy_probes)
        log.info(
            "proactive_investigation_started",
            hotspot=hotspot.key,
            severity=hotspot.severity,
            decision_id=decision.decision_id,
            allow_heavy=settings.auto_heavy_probes,
        )
        try:
            synthesis = await investigate_alert(payload, case=intake.case, mcp_runtime=runtime)
        except Exception as exc:  # surfaced to the loop's error list
            safe = classify_exception(exc)
            log_exception("proactive_investigation_graph_failed", exc, category=safe.category, hotspot=hotspot.key)
            raise

        if synthesis is None:
            # investigate_alert swallows graph/model errors and returns None on
            # failure. Do NOT count a failed run as an investigation, and do not
            # hand off on scanner evidence alone — only successful synthesis
            # counts and is eligible for a loop:candidate issue.
            log.info("proactive_investigation_unsuccessful", hotspot=hotspot.key, reason="no_synthesis")
            return None

        incident_id = intake.case.get("incident_id")
        handoff_url = await _maybe_handoff(hotspot, settings, incident_id=incident_id, decision=decision)
        # Per-run token→USD metering is not yet plumbed, so charge a conservative
        # flat estimate per investigation. This keeps the daily *dollar* budget
        # (max_cost_usd_per_day) meaningful rather than a no-op; the per-day
        # investigation *count* remains the primary hard cap.
        return InvestigationOutcome(
            incident_id=incident_id,
            cost_usd=settings.cost_usd_per_investigation,
            handoff_url=handoff_url,
        )

    return investigate


async def _maybe_handoff(
    hotspot: Hotspot, settings: ProactiveLoopSettings, *, incident_id: str | None, decision: DecisionContext
) -> str | None:
    """Open/refresh a loop:candidate issue when the finding warrants a change and
    handoff is enabled. Best-effort: never raises into the cycle."""
    if not settings.handoff_enabled or not hotspot.warrants_change:
        return None
    from app.proactive.handoff import handoff_from_env

    client = handoff_from_env(settings.handoff_repo)
    if client is None:
        log.info("proactive_handoff_skipped", reason="NOC_GITHUB_TOKEN-not-set", hotspot=hotspot.key)
        return None
    return await client.ensure_candidate_issue(
        hotspot, incident_id=incident_id, manifest_hash=decision.manifest_hash
    )
