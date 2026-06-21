"""CaseService-backed proactive investigation bridge.

A proactive concern is rendered as an Alertmanager-style payload
(:func:`hotspot_to_alert_payload`), attached to a CaseService case, and then run
through the same investigation/reporting pipeline the reactive webhook uses
(``app.main.investigate_alert``). Heavy read-only probes are stripped unless
``auto_heavy_probes`` is set (the user's "cheap reads auto, escalate heavy"
posture). Stripping is per-run (a wrapper around the shared runtime) so it never
affects concurrent reactive investigations.
"""

from __future__ import annotations

from typing import Any

from app import log
from app.cases.graph_memory import CaseServiceGraphMemory
from app.cases.proactive import observation_from_hotspot
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


def build_investigator(
    mcp_runtime: Any,
    settings: ProactiveLoopSettings,
    *,
    case_service_runtime: Any | None = None,
) -> Investigator:
    """Build the coroutine the proactive loop calls per eligible hotspot.

    Returns ``None`` for hotspots that CaseService de-dupes/suppresses (e.g. the
    same hotspot was just investigated), so the loop neither double-spends nor
    spams. Proactive graph runs are case-grounded: they require a CaseService
    runtime and use :class:`CaseServiceGraphMemory` rather than legacy graph memory.
    """

    async def investigate(hotspot: Hotspot, decision: DecisionContext) -> InvestigationOutcome | None:
        # Lazy import avoids an import cycle: app.main imports this module's
        # builder at startup, after app.main is fully initialised.
        import app.main as main

        payload = hotspot_to_alert_payload(hotspot)
        runtime_owner = case_service_runtime if case_service_runtime is not None else getattr(main, "case_service_runtime", None)
        if runtime_owner is None or not hasattr(runtime_owner, "service") or not hasattr(runtime_owner, "store"):
            log.info(
                "proactive_investigation_skipped",
                hotspot=hotspot.key,
                reason="case_service_runtime_unavailable",
            )
            return None

        claimed = await _claim_case_service_hotspot(runtime_owner, hotspot, decision)
        if claimed is None:
            log.info(
                "proactive_investigation_skipped",
                hotspot=hotspot.key,
                reason="case_service_gate",
            )
            return None
        case, graph_memory, graph_case = claimed

        runtime = HeavyProbeFilteredRuntime(mcp_runtime, allow_heavy=settings.auto_heavy_probes)
        log.info(
            "proactive_investigation_started",
            hotspot=hotspot.key,
            severity=hotspot.severity,
            decision_id=decision.decision_id,
            case_id=getattr(case, "case_id", ""),
            allow_heavy=settings.auto_heavy_probes,
        )
        try:
            synthesis = await main.investigate_alert(
                payload,
                case=graph_case,
                mcp_runtime=runtime,
                graph_memory=graph_memory,
            )
        except Exception as exc:  # surfaced to the loop's error list
            safe = classify_exception(exc)
            await _record_failed_case_service_investigation(runtime_owner, case, error=safe.category)
            log_exception("proactive_investigation_graph_failed", exc, category=safe.category, hotspot=hotspot.key)
            raise

        if synthesis is None:
            # investigate_alert swallows graph/model errors and returns None on
            # failure. Do NOT count a failed run as an investigation, and do not
            # hand off on scanner evidence alone — only successful synthesis
            # counts and is eligible for a loop:candidate issue.
            await _record_failed_case_service_investigation(runtime_owner, case, error="no_synthesis")
            log.info("proactive_investigation_unsuccessful", hotspot=hotspot.key, reason="no_synthesis")
            return None

        incident_id = str(graph_case.get("incident_id") or getattr(case, "case_id", ""))
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


async def _claim_case_service_hotspot(
    case_service_runtime: Any,
    hotspot: Hotspot,
    decision: DecisionContext,
) -> tuple[Any, CaseServiceGraphMemory, dict[str, Any]] | None:
    service = case_service_runtime.service
    graph_memory = CaseServiceGraphMemory(case_service_runtime.store)
    cycle_id = decision.cycle_id or decision.decision_id or "proactive"
    fingerprint = hotspot.fingerprint()
    case = await service.case_for_alias("source_fp", fingerprint)
    if case is None:
        result = await service.observe(
            observation_from_hotspot(
                hotspot,
                cycle_id=cycle_id,
                source_health="healthy",
            )
        )
        case = getattr(result, "case", None)
    if case is None:
        return None
    claimed = await service.claim_investigation(case)
    if claimed is None:
        return None
    graph_case = await graph_memory.get_case(claimed.case_id)
    if graph_case is None:
        raise RuntimeError(f"CaseService graph case not found after claim: {claimed.case_id}")
    return claimed, graph_memory, graph_case


async def _record_failed_case_service_investigation(case_service_runtime: Any, case: Any, *, error: str) -> None:
    try:
        await case_service_runtime.service.record_investigation_result(
            getattr(case, "case_id", ""),
            diagnosis={"source": "proactive_loop", "error": error},
            recommendations=[],
            status="failed",
            error=error,
        )
    except Exception as exc:
        safe = classify_exception(exc)
        log_exception("proactive_investigation_failure_record_failed", exc, category=safe.category)


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
