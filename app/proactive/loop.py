"""The proactive control loop.

A self-paced asyncio task (mounted in ``app/main.py``'s ``lifespan``, mirroring
the existing mail poller) that each cycle: scans for hotspots, snapshots a
decision context, gates expensive investigation against the daily ledger,
reports a digest to Discord + an Icinga heartbeat, and (Phase 2+) drives the
existing investigation graph on the top hotspots.

Cheap and read-only by construction; ships disabled (``NOC_PROACTIVE_ENABLED=0``)
and starts in shadow mode (report-only) so a canary validates the scanners
before any autonomous LLM spend.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from app import log
from app.config import ProactiveLoopSettings, load_proactive_settings
from app.discord import Verbosity, send_discord_notification
from app.proactive.governance import GateDecision, build_decision_context, evaluate_gate
from app.proactive.ledger import acquire_lock, load_ledger, release_lock, update_ledger
from app.proactive.models import (
    CycleOutcome,
    DecisionContext,
    Hotspot,
    Observation,
    ProactiveCycleReport,
    utc_now,
)
from app.proactive.scanner import ScanContext, scan
from app.safe_errors import classify_exception, log_exception


@dataclass(slots=True)
class InvestigationOutcome:
    """Result of investigating one hotspot (returned by the Phase 2 investigator)."""

    incident_id: str | None = None
    cost_usd: float = 0.0
    handoff_url: str | None = None


# async (hotspot, decision_context) -> InvestigationOutcome | None
Investigator = Callable[[Hotspot, DecisionContext], Awaitable[InvestigationOutcome | None]]
# async (report, gate_decision) -> None
Reporter = Callable[[ProactiveCycleReport, GateDecision], Awaitable[None]]

_SEVERITY_EMOJI = {"HIGH": "🔴", "MEDIUM": "🟠", "LOW": "🟢"}
_OUTCOME_EXIT = {
    "scanned": 0,
    "idle": 0,
    "investigated": 0,
    "disabled": 0,
    "locked": 0,
    "over_budget": 1,
    "error": 2,
}


class ProactiveLoop:
    def __init__(
        self,
        mcp_runtime: Any,
        *,
        settings: ProactiveLoopSettings | None = None,
        reporter: Reporter | None = None,
        investigator: Investigator | None = None,
        model_chain: Callable[[], list[str]] | None = None,
        active_lessons: Callable[[], list[str]] | None = None,
        incident_memory: Any | None = None,
        memory: Any | None = None,
    ):
        self.mcp_runtime = mcp_runtime
        self.settings = settings or load_proactive_settings()
        self._reporter = reporter or self._default_report
        self._investigator = investigator
        self._model_chain = model_chain or _default_model_chain
        self.incident_memory = incident_memory
        self.memory = memory
        if active_lessons is not None:
            self._active_lessons = active_lessons
        elif memory is not None:
            self._active_lessons = memory.active_lessons
        else:
            self._active_lessons = lambda: []
        self.running = False
        self.paused = False
        self._task: asyncio.Task[None] | None = None
        self._last_deep_scan = 0.0
        self._state_dir = Path(self.settings.state_dir)
        # Digest de-dup: remember the last reported hotspot set + when, so an
        # unchanged set doesn't re-post every scan cycle (it re-asserts at most
        # every report_reassert_s).
        self._last_report_signature: frozenset[tuple[str, str]] | None = None
        self._last_report_ts = 0.0

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self.running = True
        self._task = asyncio.create_task(self._run())
        log.info(
            "proactive_loop_started",
            shadow=self.settings.shadow,
            interval_s=self.settings.interval_s,
            handoff=self.settings.handoff_enabled,
        )

    async def stop(self) -> None:
        self.running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

    async def _run(self) -> None:
        while self.running:
            await asyncio.sleep(max(1, self.settings.interval_s))
            if self.paused:
                continue
            try:
                await self.run_once()
            except Exception as exc:  # the loop must survive a bad cycle
                safe = classify_exception(exc)
                log_exception("proactive_cycle_uncaught", exc, category=safe.category)

    # --- one cycle --------------------------------------------------------

    async def run_once(self, *, deep: bool | None = None) -> ProactiveCycleReport:
        report = ProactiveCycleReport()
        if not self.settings.enabled:
            report.outcome = "disabled"
            report.finished_at = utc_now()
            return report

        lock = acquire_lock(self._state_dir)
        if lock is None:
            report.outcome = "locked"
            report.detail = "another cycle holds the lock"
            report.finished_at = utc_now()
            return report

        try:
            ledger = load_ledger(self._state_dir)
            do_deep = self._should_deep_scan() if deep is None else deep
            ctx = ScanContext(self.mcp_runtime, self.settings, lessons=self._active_lessons())
            report.hotspots = await scan(ctx, deep=do_deep)

            decision = build_decision_context(
                self.settings,
                cycle_id=report.cycle_id,
                model_chain=self._model_chain(),
                budget_state=ledger,
                injected_lesson_ids=self._active_lessons(),
            )
            report.decision_id = decision.decision_id

            gate = evaluate_gate(self.settings, ledger, report.hotspots)
            investigated = await self._investigate(gate, decision, report)

            update_ledger(
                self._state_dir,
                cycles=1,
                investigations=investigated,
                cost_usd=report.cost_usd,
                handoffs=len(report.handoffs),
            )
            if do_deep:
                self._last_deep_scan = time.time()
            report.outcome = self._classify_outcome(report, gate, investigated)
            await self._persist_memory(report, do_deep)
            await self._safe_report(report, gate)
        except Exception as exc:
            safe = classify_exception(exc)
            report.outcome = "error"
            report.errors.append(safe.category)
            log_exception("proactive_cycle_failed", exc, category=safe.category)
        finally:
            release_lock(lock)

        report.finished_at = utc_now()
        return report

    async def _investigate(
        self, gate: GateDecision, decision: DecisionContext, report: ProactiveCycleReport
    ) -> int:
        if self._investigator is None or gate.max_investigations <= 0:
            return 0
        investigated = 0
        for hotspot in gate.eligible[: gate.max_investigations]:
            try:
                outcome = await self._investigator(hotspot, decision)
            except Exception as exc:  # one bad investigation isn't fatal
                safe = classify_exception(exc)
                report.errors.append(safe.category)
                log_exception("proactive_investigation_failed", exc, category=safe.category, hotspot=hotspot.key)
                continue
            if outcome is None:
                continue
            investigated += 1
            report.investigated.append(hotspot.key)
            report.cost_usd = round(report.cost_usd + outcome.cost_usd, 6)
            if outcome.handoff_url:
                report.handoffs.append(outcome.handoff_url)
        return investigated

    def _should_deep_scan(self) -> bool:
        return (time.time() - self._last_deep_scan) >= max(self.settings.interval_s, self.settings.deep_scan_s)

    @staticmethod
    def _classify_outcome(report: ProactiveCycleReport, gate: GateDecision, investigated: int) -> CycleOutcome:
        if investigated:
            return "investigated"
        if gate.over_budget:
            return "over_budget"
        return "scanned" if report.hotspots else "idle"

    async def _persist_memory(self, report: ProactiveCycleReport, do_deep: bool) -> None:
        """Best-effort flywheel: record this cycle's predictions, and on deep
        cycles evaluate prior predictions against reality + journal. A memory
        failure must never break a cycle."""
        if self.memory is None:
            return
        try:
            investigated = set(report.investigated)
            for hotspot in report.hotspots:
                self.memory.record_observation(
                    Observation(
                        cycle_id=report.cycle_id,
                        rule_id=hotspot.rule_id,
                        hotspot_key=hotspot.key,
                        fingerprint=hotspot.fingerprint(),
                        resource=hotspot.resource,
                        category=hotspot.category,
                        severity=hotspot.severity,
                        summary=hotspot.summary,
                        investigated=hotspot.key in investigated,
                    )
                )
            if do_deep:
                if self.incident_memory is not None:
                    proposed = await self.memory.evaluate_outcomes(self.incident_memory)
                    if proposed:
                        log.info("proactive_lessons_proposed", count=len(proposed))
                self.memory.write_journal(
                    report.cycle_id,
                    {
                        "outcome": report.outcome,
                        "hotspots": len(report.hotspots),
                        "investigated": ",".join(report.investigated) or "none",
                        "handoffs": len(report.handoffs),
                        "decision_id": report.decision_id,
                    },
                )
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_persist_memory_failed", exc, category=safe.category)

    # --- reporting --------------------------------------------------------

    def _report_decision(
        self, report: ProactiveCycleReport
    ) -> tuple[bool, frozenset[tuple[str, str]], float]:
        """Decide whether to post a digest WITHOUT mutating de-dup state (state is
        committed only after a successful send). Posts when the hotspot set
        changes (new/resolved/severity), something was investigated/handed off,
        the set goes all-clear, or the persistent set is due a re-assert.
        Returns ``(should_post, signature, now)``."""
        signature = frozenset((h.fingerprint(), h.severity) for h in report.hotspots)
        now = time.time()
        if report.investigated or report.handoffs:
            return True, signature, now
        if not report.hotspots:
            # All-clear: post once iff we previously reported an active set, so
            # operators get confirmation the issue resolved.
            return bool(self._last_report_signature), signature, now
        changed = signature != self._last_report_signature
        stale = (now - self._last_report_ts) >= max(1, self.settings.report_reassert_s)
        return (changed or stale), signature, now

    async def _safe_report(self, report: ProactiveCycleReport, gate: GateDecision) -> None:
        should, signature, now = self._report_decision(report)
        if should:
            try:
                await self._reporter(report, gate)
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("proactive_report_failed", exc, category=safe.category)
            else:
                # Commit de-dup state only after a successful send, so a transient
                # webhook failure doesn't suppress the next retry.
                self._last_report_signature, self._last_report_ts = signature, now
        try:
            await _heartbeat(report)
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_heartbeat_failed", exc, category=safe.category)

    async def _default_report(self, report: ProactiveCycleReport, gate: GateDecision) -> None:
        if not report.hotspots and not report.investigated:
            # Reached only on an all-clear transition (the dedup gate suppresses
            # steady-state empty cycles), so confirm the resolution.
            await send_discord_notification(
                title="✅ Proactive sweep: all clear",
                description="All previously flagged hotspots have resolved.",
                color=0x2ECC71,
                level=Verbosity.INFO,
            )
            return
        top = report.top(6)
        prefix = "🛰️ Proactive sweep"
        if self.settings.shadow:
            prefix += " (shadow)"
        worst = max((_sev_rank(h.severity) for h in top), default=0)
        color = 0xE74C3C if worst >= 3 else (0xF39C12 if worst == 2 else 0x2ECC71)
        lines = [gate.reason]
        if report.investigated:
            lines.append(f"Investigated: {', '.join(report.investigated)}")
        if report.handoffs:
            lines.append(f"Handoffs: {', '.join(report.handoffs)}")
        await send_discord_notification(
            title=f"{prefix}: {len(report.hotspots)} hotspot(s)",
            description="\n".join(lines),
            color=color,
            fields=[_hotspot_field(h) for h in top],
            level=Verbosity.INFO,
        )


def _sev_rank(severity: str) -> int:
    return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(str(severity).upper(), 0)


def _hotspot_field(hotspot: Hotspot) -> dict[str, Any]:
    emoji = _SEVERITY_EMOJI.get(hotspot.severity, "•")
    checks = "; ".join(hotspot.recommended_checks[:2])
    value = hotspot.summary
    if checks:
        value += f"\nNext: {checks}"
    if hotspot.warrants_change:
        value += "\n⚙️ candidate for config change (handoff)"
    return {"name": f"{emoji} {hotspot.title}"[:256], "value": value[:1024] or "—"}


def _default_model_chain() -> list[str]:
    try:
        from app.model_config import load_model_config

        config = load_model_config()
        chain = [config.primary_model, *config.fallback_models]
        return [m for m in chain if m]
    except Exception:  # model config is advisory metadata here
        return []


async def _heartbeat(report: ProactiveCycleReport) -> None:
    """Submit a passive check result to Icinga so a freshness rule alerts if the
    loop stops running (mirrors engineering-loop's ``notify_icinga``). No-op
    unless ``NOC_PROACTIVE_ICINGA_URL`` is configured."""
    import os
    from base64 import b64encode

    import httpx

    url = os.getenv("NOC_PROACTIVE_ICINGA_URL", "").strip()
    user = os.getenv("ICINGA_API_USER", "").strip()
    password = os.getenv("ICINGA_API_PASSWORD", "").strip()
    check = os.getenv("NOC_PROACTIVE_ICINGA_CHECK", "noc!proactive-loop").strip()
    if not (url and user and password):
        return
    payload = {
        "type": "Service",
        "filter": f'service.__name=="{check}"',
        "exit_status": _OUTCOME_EXIT.get(report.outcome, 2),
        "plugin_output": (
            f"proactive {report.outcome}: {len(report.hotspots)} hotspot(s), "
            f"{len(report.investigated)} investigated, {len(report.handoffs)} handoff(s)"
        ),
    }
    auth = b64encode(f"{user}:{password}".encode()).decode()
    verify = os.getenv("ICINGA_VERIFY_TLS", "true").strip().lower() not in {"0", "false", "no"}
    async with httpx.AsyncClient(timeout=10.0, verify=verify) as client:
        await client.post(
            f"{url.rstrip('/')}/v1/actions/process-check-result",
            json=payload,
            headers={"Authorization": f"Basic {auth}", "Accept": "application/json"},
        )
