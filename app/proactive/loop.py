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
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from app import log
from app.cases.models import AtomicCaseProjection, InsightDecisionRecord, InsightScore, SourceHealth
from app.config import LoopHandoffSettings, ProactiveLoopSettings, load_loop_handoff_settings, load_proactive_settings
from app.discord import Verbosity, send_discord_notification
from app.icinga_ack import acknowledge_icinga
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
from app.model_metrics import record_case_service_shadow_failure, record_case_service_shadow_observation
from app.proactive.scanner import DEEP_RULE_IDS, ScanContext, scan
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
        memory: Any | None = None,
        suppressions: Any | None = None,
        case_service: Any | None = None,
        case_service_primary: bool | None = None,
        loop_handoff_settings: LoopHandoffSettings | None = None,
    ):
        self.mcp_runtime = mcp_runtime
        self.settings = settings or load_proactive_settings()
        self._reporter = reporter or self._default_report
        self._investigator = investigator
        self._model_chain = model_chain or _default_model_chain
        self.memory = memory
        self.case_service = case_service
        self.loop_handoff_settings = loop_handoff_settings or load_loop_handoff_settings()
        env_case_service_control = os.getenv("NOC_CASESERVICE_CONTROL", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        requested_case_service_primary = env_case_service_control if case_service_primary is None else case_service_primary
        self.case_service_control = bool(self.case_service is not None and requested_case_service_primary)
        if active_lessons is not None:
            self._active_lessons = active_lessons
        elif memory is not None:
            self._active_lessons = memory.active_lessons
        else:
            self._active_lessons = lambda: []
        self._state_dir = Path(self.settings.state_dir)
        if suppressions is not None:
            self.suppressions = suppressions
        else:
            from app.proactive.suppressions import SuppressionStore

            self.suppressions = SuppressionStore(self._state_dir / "suppressions.json")
        # Per-fingerprint last-investigated timestamps, so a persistent hotspot
        # isn't re-diagnosed (and re-posted) every cycle (investigation_cooldown_s).
        self._investigations_path = self._state_dir / "investigations.json"
        self.running = False
        self.paused = False
        self.last_report: ProactiveCycleReport | None = None
        self._task: asyncio.Task[None] | None = None
        self._last_deep_scan = 0.0
        # Digest de-dup: remember the last reported hotspot set + when, so an
        # unchanged set doesn't re-post every scan cycle (it re-asserts at most
        # every report_reassert_s).
        self._last_report_signature: frozenset[tuple[str, str]] | None = None
        self._last_report_ts = 0.0
        # Deep rules (disk, TLS) only run on deep cycles. Remember their last
        # *clean* result so cheap-only / degraded cycles don't read "not scanned"
        # as "resolved" and post a false all-clear. _last_effective is the last
        # trustworthy reported set, carried through a degraded (query-failed) scan.
        self._last_deep_hotspots: list[Hotspot] = []
        self._last_effective: list[Hotspot] = []

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
            raw_hotspots = await scan(ctx, deep=do_deep)
            await self._shadow_observe_hotspots(
                raw_hotspots,
                cycle_id=report.cycle_id,
                source_health="degraded" if ctx.degraded else "healthy",
            )
            effective = self._merge_with_carried(raw_hotspots, do_deep=do_deep, degraded=ctx.degraded)
            report.auto_snoozed = await self._auto_snooze(effective)
            report.hotspots = self._apply_suppressions(effective)

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
            await self._record_insight_decisions(
                effective=effective,
                report=report,
                decision=decision,
                gate=gate,
                ledger=ledger,
            )

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
        self.last_report = report  # surfaced read-only to the control dashboard
        return report

    async def _investigate(
        self, gate: GateDecision, decision: DecisionContext, report: ProactiveCycleReport
    ) -> int:
        if self._investigator is None or gate.max_investigations <= 0:
            return 0
        # Skip hotspots diagnosed within the cooldown — a persistent condition is
        # investigated once, then surfaced via the digest de-dup gate, not re-run
        # (and re-posted) every cycle until the daily budget is gone. In guarded
        # case-service control mode, the case owns this decision instead of
        # investigations.json.
        use_case_service = self.case_service_control and self.case_service is not None
        if use_case_service:
            fresh = [h for h in gate.eligible if await self._case_service_should_investigate(h)]
        else:
            recent = self._recent_investigation_fps()
            fresh = [h for h in gate.eligible if h.fingerprint() not in recent]
        investigated = 0
        done: list[str] = []
        for hotspot in fresh[: gate.max_investigations]:
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
            done.append(hotspot.fingerprint())
            report.investigated.append(hotspot.key)
            report.cost_usd = round(report.cost_usd + outcome.cost_usd, 6)
            if outcome.handoff_url:
                report.handoffs.append(outcome.handoff_url)
            if use_case_service:
                await self._case_service_record_investigation(hotspot, outcome)
        if not use_case_service:
            self._record_investigations(done)
        return investigated

    async def _record_insight_decisions(
        self,
        *,
        effective: list[Hotspot],
        report: ProactiveCycleReport,
        decision: DecisionContext,
        gate: GateDecision,
        ledger: dict[str, Any],
    ) -> None:
        """Best-effort Level 3 insight ledger: includes explicit silence.

        The operational loop already performs the right conservative actions.
        This records those actions in a replayable form before the reporting
        de-dup gate and suppressions hide the denominator.
        """

        case_service = self.case_service
        if case_service is None:
            return
        try:
            should_notify, _, _ = self._report_decision(report)
            reported_fps = {h.fingerprint() for h in report.hotspots}
            investigated_keys = set(report.investigated)
            eligible_fps = {h.fingerprint() for h in gate.eligible}
            snoozed_keys = set(report.auto_snoozed)
            if not effective:
                await case_service.record_insight_decision(
                    InsightDecisionRecord(
                        loop="noc",
                        fingerprint=f"quiet:{report.cycle_id}",
                        sampling_class="sampled_quiet_interval",
                        candidate_type="quiet_interval",
                        candidate_source="proactive_scanner",
                        action_selected="stay_silent",
                        why_now="No scanner hotspot was firing in this cycle.",
                        why_not_other_actions={
                            "notify": "No operator-relevant change was observed.",
                            "question": "No ambiguity requires operator input.",
                            "draft": "No case or handoff candidate exists.",
                        },
                        expected_utility=InsightScore(total=0.0, rationale=["No active hotspot."]),
                        interruption_cost=InsightScore(
                            total=0.2,
                            components={"baseline_operator_interruption": 0.2},
                            rationale=["Avoids an empty-cycle notification."],
                        ),
                        confidence=1.0,
                        policy_version=self.settings.ruleset_version,
                        budget_context=_insight_budget_context(ledger, gate),
                    )
                )
                return
            for hotspot in effective:
                fp = hotspot.fingerprint()
                case_id = await self._case_id_for_fingerprint(fp)
                if fp not in reported_fps:
                    reason = self._silence_reason_for_hotspot(hotspot, gate=gate, snoozed_keys=snoozed_keys)
                    await case_service.record_insight_decision(
                        self._insight_from_hotspot(
                            hotspot,
                            report=report,
                            decision=decision,
                            gate=gate,
                            ledger=ledger,
                            case_id=case_id,
                            action_selected="stay_silent",
                            sampling_class="withheld_logged",
                            why_now=reason,
                            interruption_reason=reason,
                        )
                    )
                    continue
                if hotspot.key in investigated_keys:
                    action = "draft" if hotspot.warrants_change else "notify"
                    why = "Investigated within budget and surfaced with diagnostic context."
                elif should_notify:
                    action = "notify"
                    why = "Hotspot set changed or was due for reassertion."
                else:
                    action = "stay_silent"
                    why = "Digest de-duplication suppressed an unchanged hotspot set."
                sampling_class = "surfaced" if action != "stay_silent" else "withheld_logged"
                await case_service.record_insight_decision(
                    self._insight_from_hotspot(
                        hotspot,
                        report=report,
                        decision=decision,
                        gate=gate,
                        ledger=ledger,
                        case_id=case_id,
                        action_selected=action,
                        sampling_class=sampling_class,
                        why_now=why,
                        interruption_reason=why,
                        eligible=fp in eligible_fps,
                    )
                )
        except Exception as exc:
            safe = classify_exception(exc)
            record_case_service_shadow_failure(path="proactive_insight_ledger", category=safe.category)
            log_exception("proactive_insight_record_failed", exc, category=safe.category)

    def _insight_from_hotspot(
        self,
        hotspot: Hotspot,
        *,
        report: ProactiveCycleReport,
        decision: DecisionContext,
        gate: GateDecision,
        ledger: dict[str, Any],
        case_id: str | None,
        action_selected: str,
        sampling_class: str,
        why_now: str,
        interruption_reason: str,
        eligible: bool = False,
    ) -> InsightDecisionRecord:
        support_facts = _support_facts(hotspot)
        return InsightDecisionRecord(
            loop="noc",
            fingerprint=hotspot.fingerprint(),
            sampling_class=sampling_class,  # type: ignore[arg-type]
            candidate_type="hotspot",
            candidate_source=f"proactive_scanner:{hotspot.rule_id}",
            case_id=case_id,
            observation_ids=[],
            state_snapshot_refs=[report.cycle_id, decision.decision_id],
            support_facts=support_facts,
            evidence_refs=[
                {
                    "kind": "hotspot_evidence",
                    "ref": f"{hotspot.rule_id}:{idx}",
                    "label": evidence.label,
                    "query": evidence.query,
                }
                for idx, evidence in enumerate(hotspot.evidence)
            ],
            action_selected=action_selected,  # type: ignore[arg-type]
            why_now=why_now,
            why_not_other_actions=_why_not_other_actions(
                action_selected=action_selected,
                eligible=eligible,
                gate_reason=gate.reason,
            ),
            expected_utility=_expected_utility(hotspot),
            interruption_cost=_interruption_cost(hotspot, reason=interruption_reason),
            confidence=_bounded_confidence(hotspot.score),
            risk_class=_risk_class(hotspot.severity),
            policy_version=self.settings.ruleset_version,
            tool_versions={"scanner_ruleset": self.settings.ruleset_version},
            budget_context=_insight_budget_context(ledger, gate),
        )

    async def _case_id_for_fingerprint(self, fingerprint: str) -> str | None:
        if self.case_service is None:
            return None
        try:
            case = await self.case_service.case_for_alias("source_fp", fingerprint)
            return getattr(case, "case_id", None) if case is not None else None
        except Exception:
            return None

    def _silence_reason_for_hotspot(
        self, hotspot: Hotspot, *, gate: GateDecision, snoozed_keys: set[str]
    ) -> str:
        if hotspot.key in snoozed_keys:
            return "Agent auto-snoozed this non-urgent hotspot."
        entry = self._suppression_entry_for_hotspot(hotspot)
        if entry is not None:
            return f"Operator suppression active: {entry.get('reason', 'suppressed')}"
        if gate.over_budget:
            return gate.reason
        return "Hotspot was withheld by reporting or investigation policy."

    async def _case_service_should_investigate(self, hotspot: Hotspot) -> bool:
        case_service = self.case_service
        if case_service is None:
            return True
        try:
            case = await case_service.case_for_alias("source_fp", hotspot.fingerprint())
            return True if case is None else bool(case_service.should_investigate(case))
        except Exception as exc:
            safe = classify_exception(exc)
            record_case_service_shadow_failure(path="proactive_control", category=safe.category)
            log_exception("proactive_case_service_investigation_gate_failed", exc, category=safe.category)
            return True

    async def _case_service_record_investigation(self, hotspot: Hotspot, outcome: InvestigationOutcome) -> None:
        case_service = self.case_service
        if case_service is None:
            return
        try:
            case = await case_service.case_for_alias("source_fp", hotspot.fingerprint())
            if case is None:
                return
            existing_diagnosis = dict(getattr(case, "last_diagnosis", {}) or {})
            graph_summary = existing_diagnosis.get("graph_summary")
            diagnosis: dict[str, Any] = {
                "incident_id": outcome.incident_id,
                "source": "proactive_loop",
            }
            if isinstance(graph_summary, dict):
                diagnosis["graph_summary"] = graph_summary
            await case_service.record_investigation_result(
                case.case_id,
                diagnosis=diagnosis,
                recommendations=[],
                status="complete",
            )
        except Exception as exc:
            safe = classify_exception(exc)
            record_case_service_shadow_failure(path="proactive_control", category=safe.category)
            log_exception("proactive_case_service_investigation_record_failed", exc, category=safe.category)

    def _recent_investigation_fps(self, now: float | None = None) -> set[str]:
        """Fingerprints investigated within ``investigation_cooldown_s``."""
        cooldown = self.settings.investigation_cooldown_s
        if cooldown <= 0:
            return set()
        now = now if now is not None else time.time()
        try:
            data = json.loads(self._investigations_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        if not isinstance(data, dict):
            return set()
        return {
            fp for fp, ts in data.items() if isinstance(ts, (int, float)) and (now - ts) < cooldown
        }

    def _record_investigations(self, fingerprints: list[str], now: float | None = None) -> None:
        """Stamp fingerprints as just-investigated; prune entries past the
        cooldown so the file stays small. Best-effort — never breaks a cycle."""
        if self.settings.investigation_cooldown_s <= 0 or not fingerprints:
            return
        now = now if now is not None else time.time()
        cooldown = self.settings.investigation_cooldown_s
        try:
            try:
                data = json.loads(self._investigations_path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    data = {}
            except (OSError, json.JSONDecodeError):
                data = {}
            for fp in fingerprints:
                data[fp] = now
            data = {
                fp: ts
                for fp, ts in data.items()
                if isinstance(ts, (int, float)) and (now - ts) < cooldown
            }
            self._investigations_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._investigations_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            tmp.replace(self._investigations_path)
        except OSError as exc:
            safe = classify_exception(exc)
            log_exception("proactive_investigation_log_failed", exc, category=safe.category)

    def _apply_suppressions(self, raw: list[Hotspot]) -> list[Hotspot]:
        """Drop operator-acked hotspots from the digest + investigation, and
        prune *resolved* suppressions so a recurrence re-alerts.

        A timed snooze (``expires_at`` set) is kept until it expires even while
        its hotspot flaps clear — otherwise a signal hovering at its threshold
        (e.g. a disk oscillating around its fill mark) sheds its ack on every
        brief all-clear and re-alerts. An untimed ack ("until resolved") is
        still pruned the moment its hotspot stops firing."""
        if self.suppressions is None:
            return raw
        try:
            active = self.suppressions.active()
            if not active:
                return raw
            # An ack id is a *prefix* of the full hotspot fingerprint (the digest
            # shows a short id), matched like a git short-SHA.
            firing = [h.fingerprint() for h in raw]
            for ack_id, entry in list(active.items()):
                if entry.get("expires_at") is not None:
                    continue  # timed snooze: survives flapping until it expires
                if not any(fp.startswith(ack_id) for fp in firing):
                    self.suppressions.remove(ack_id)
            active_ids = list(self.suppressions.active())
            kept = [h for h in raw if not any(h.fingerprint().startswith(a) for a in active_ids)]
            if len(kept) != len(raw):
                log.info("proactive_hotspots_suppressed", suppressed=len(raw) - len(kept))
            return kept
        except Exception as exc:  # suppression failure must not break the cycle
            safe = classify_exception(exc)
            log_exception("proactive_suppressions_failed", exc, category=safe.category)
            return raw

    @staticmethod
    def _is_auto_snoozable(h: Hotspot) -> bool:
        """Non-urgent = LOW severity and not a config-change candidate. MEDIUM/HIGH
        and anything warranting a change always stay surfaced for an operator."""
        return h.severity == "LOW" and not h.warrants_change

    async def _auto_snooze(self, raw: list[Hotspot]) -> list[str]:
        """Autonomously mute non-urgent hotspots: add a TTL suppression
        (operator='agent', audited) so they drop from the digest + investigation
        for auto_snooze_ttl_s, and best-effort ack the matching Icinga WARNING
        problem with an expiry. Bounded per cycle; already-snoozed ones are
        skipped so we don't re-ack every cycle. Returns the keys snoozed now."""
        if not self.settings.auto_snooze_enabled or self.suppressions is None:
            return []
        try:
            active_ids = list(self.suppressions.active())
        except Exception:  # never break a cycle on suppression I/O
            active_ids = []
        ttl = self.settings.auto_snooze_ttl_s
        snoozed: list[str] = []
        for h in raw:
            if len(snoozed) >= max(1, self.settings.auto_snooze_max_per_cycle):
                break
            if not self._is_auto_snoozable(h):
                continue
            fp = h.fingerprint()
            if any(fp.startswith(a) or a.startswith(fp) for a in active_ids):
                continue  # already acked/snoozed — don't re-act every cycle
            try:
                self.suppressions.add(
                    fingerprint=fp,
                    key=h.key,
                    reason=f"auto-snoozed (non-urgent {h.severity}): {h.title}",
                    operator="agent",
                    ttl_seconds=ttl,
                )
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("proactive_auto_snooze_failed", exc, category=safe.category, hotspot=h.key)
                continue
            snoozed.append(h.key)
            active_ids.append(fp)
            if self.settings.auto_snooze_icinga_ack:
                await self._auto_snooze_icinga_ack(h, ttl)
        if snoozed:
            log.info("proactive_auto_snoozed", count=len(snoozed), keys=snoozed)
            try:
                await self._report_auto_snooze(snoozed, raw)
            except Exception as exc:  # audit post is advisory
                safe = classify_exception(exc)
                log_exception("proactive_auto_snooze_report_failed", exc, category=safe.category)
        return snoozed

    async def _auto_snooze_icinga_ack(self, h: Hotspot, ttl: int) -> None:
        target = await self._icinga_target_for(h)
        if target is None:
            return
        host, service = target
        await acknowledge_icinga(
            self.mcp_runtime,
            host_name=host,
            service_name=service,
            comment=f"NOC agent auto-snoozed non-urgent {h.severity}: {h.title}; ack auto-expires.",
            ack_ttl_seconds=ttl,
            notify=False,
        )

    async def _icinga_target_for(self, h: Hotspot) -> tuple[str, str | None] | None:
        """Best-effort: the single Icinga WARNING problem matching this hotspot,
        so the ack hits a real object (never a fabricated name). None if there's
        no confident unique match — the internal snooze still applies."""
        host = (h.resource or "").strip()
        if host == "" or self.mcp_runtime is None:
            return None
        try:
            res = await self.mcp_runtime.call_tool(
                "hyrule", "icinga_list_problems", {"object_type": "service", "limit": 100}
            )
        except Exception:
            return None
        if not isinstance(res, dict):
            return None
        cat = (h.category or "").lower()
        matches: list[tuple[str, str | None]] = []
        for p in res.get("problems") or []:
            if not isinstance(p, dict):
                continue
            try:
                state = int(float(p.get("state")))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if state != 1:  # WARNING only — never auto-ack a CRITICAL (2)
                continue
            if str(p.get("host") or "").strip() != host:
                continue
            name = str(p.get("name") or "")
            svc = name.split("!", 1)[1] if "!" in name else name
            if cat and cat not in svc.lower() and cat not in name.lower():
                continue
            matches.append((host, svc or None))
        return matches[0] if len(matches) == 1 else None

    async def _report_auto_snooze(self, snoozed_keys: list[str], raw: list[Hotspot]) -> None:
        by_key = {h.key: h for h in raw}
        ttl_label = _format_ttl(self.settings.auto_snooze_ttl_s)
        lines = [
            f"• {by_key[k].title} ({by_key[k].severity})"
            for k in snoozed_keys
            if k in by_key
        ]
        await send_discord_notification(
            title=f"🔕 Auto-snoozed {len(snoozed_keys)} non-urgent finding(s) for {ttl_label}",
            description="\n".join(lines) or "non-urgent findings muted",
            color=0x95A5A6,
            level=Verbosity.INFO,
        )

    async def _shadow_observe_hotspots(
        self, raw: list[Hotspot], *, cycle_id: str, source_health: SourceHealth
    ) -> None:
        """Best-effort shadow write of freshly scanned hotspots to CaseService.

        This intentionally observes only `raw` scanner output, not the effective
        carried-forward set. A deep-rule hotspot carried through a cheap or
        degraded cycle is operationally still active, but it was not freshly
        evaluated and therefore must not become a fresh observation.
        """
        if self.case_service is None or not raw:
            return
        try:
            from app.cases.proactive import observation_from_hotspot

            for hotspot in raw:
                observation = observation_from_hotspot(
                    hotspot,
                    cycle_id=cycle_id,
                    source_health=source_health,
                )
                result = await self.case_service.observe(observation)
                record_case_service_shadow_observation(
                    path="proactive",
                    source=observation.source,
                    status=observation.status,
                    action=str(getattr(result, "action", "unknown")),
                )
                case = getattr(result, "case", None)
                if isinstance(case, AtomicCaseProjection):
                    await self._maybe_request_disk_lhp_handoff(
                        hotspot,
                        case=case,
                        cycle_id=cycle_id,
                    )
            log.info("proactive_case_shadow_observed", count=len(raw), source_health=source_health)
        except Exception as exc:
            safe = classify_exception(exc)
            record_case_service_shadow_failure(path="proactive", category=safe.category)
            log_exception("proactive_case_shadow_observe_failed", exc, category=safe.category)
            if self.case_service_control:
                raise

    async def _maybe_request_disk_lhp_handoff(self, hotspot: Hotspot, *, case: Any, cycle_id: str) -> None:
        settings = self.loop_handoff_settings
        if not (settings.enabled and settings.disk_alert_handoff_enabled):
            return
        service = self.case_service
        if service is None:
            return
        from app.proactive.lhp import build_disk_handoff_request, is_disk_handoff_hotspot

        if not is_disk_handoff_hotspot(hotspot):
            return
        request = build_disk_handoff_request(
            hotspot,
            case,
            cycle_id=cycle_id,
            required_consecutive_passes=settings.case_verification_required_consecutive_passes,
            suppression_entry=self._suppression_entry_for_hotspot(hotspot),
            knowledge_context_enabled=settings.knowledge_context_enabled,
        )
        result = await service.request_lhp_handoff(
            request.handoff,
            objectives=request.objectives,
            enqueue_delivery=settings.engineering_handoff_delivery_enabled,
        )
        if settings.knowledge_context_enabled:
            await service.request_lhp_knowledge_context(
                case.case_id,
                handoff_id=result.handoff.handoff_id,
                objective_key=request.handoff.objective_key,
                payload=request.knowledge_payload,
            )
        log.info(
            "proactive_disk_lhp_handoff_requested",
            case_id=case.case_id,
            handoff_id=result.handoff.handoff_id,
            created=result.created,
            delivery_enqueued=bool(result.outbox_intent),
            knowledge_context_requested=settings.knowledge_context_enabled,
        )

    def _suppression_entry_for_hotspot(self, hotspot: Hotspot) -> dict[str, Any] | None:
        if self.suppressions is None:
            return None
        try:
            active = self.suppressions.active()
        except Exception:
            return None
        fingerprint = hotspot.fingerprint()
        for ack_id, entry in active.items():
            if fingerprint.startswith(str(ack_id)):
                return entry
        return None

    def _merge_with_carried(
        self, raw: list[Hotspot], *, do_deep: bool, degraded: bool
    ) -> list[Hotspot]:
        """Don't read "didn't scan" / "scan failed" as "resolved".

        - **Degraded** (a query failed): the scan is untrustworthy → keep fresh
          findings but re-add anything we last reported, so nothing resolves on a
          partial scan (the error-aware hardening).
        - **Clean cheap cycle**: deep rules (disk, TLS) didn't run → carry their
          last clean result forward; cheap-rule hotspots are fresh and resolve
          normally.
        - **Clean deep cycle**: authoritative — refresh the carried sets.
        """
        if degraded:
            seen = {h.fingerprint() for h in raw}
            merged = raw + [h for h in self._last_effective if h.fingerprint() not in seen]
            # Persist the merged view so a *following* degraded cycle still has
            # everything to carry forward (don't let a streak of failed scans
            # erode the set into a false all-clear).
            self._last_effective = list(merged)
            return merged
        if do_deep:
            self._last_deep_hotspots = [h for h in raw if h.rule_id in DEEP_RULE_IDS]
            self._last_effective = list(raw)
            return raw
        effective = [h for h in raw if h.rule_id not in DEEP_RULE_IDS] + list(self._last_deep_hotspots)
        self._last_effective = list(effective)
        return effective

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
                if self.case_service_control and self.case_service is not None:
                    proposed = await self.memory.evaluate_outcomes(self.case_service)
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
                await self._case_service_mark_reported_hotspots(report)
        try:
            await _heartbeat(report)
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_heartbeat_failed", exc, category=safe.category)

    async def _case_service_mark_reported_hotspots(self, report: ProactiveCycleReport) -> None:
        if not (self.case_service_control and self.case_service is not None):
            return
        case_service = self.case_service
        for hotspot in report.hotspots:
            try:
                case = await case_service.case_for_alias("source_fp", hotspot.fingerprint())
                if case is None:
                    continue
                state_signature = case_service.report_state_signature(case)
                await case_service.mark_reported(case.case_id, state_signature=state_signature)
            except Exception as exc:
                safe = classify_exception(exc)
                record_case_service_shadow_failure(path="proactive_control", category=safe.category)
                log_exception("proactive_case_service_mark_reported_failed", exc, category=safe.category)

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
        lines.append("_Mute a known one:_ `POST /control/proactive/ack {\"fingerprint\": \"<ack id>\"}`")
        fields = []
        for hotspot in top:
            case = await self._case_for_hotspot(hotspot)
            fields.append(_hotspot_field(hotspot, case=case, public_url=self.settings.control_public_url))
        await send_discord_notification(
            title=f"{prefix}: {len(report.hotspots)} hotspot(s)",
            description="\n".join(lines),
            color=color,
            fields=fields,
            level=Verbosity.INFO,
        )

    async def _case_for_hotspot(self, hotspot: Hotspot) -> dict[str, Any] | None:
        """The NOC case this hotspot's investigation is attached to (for linking
        the digest line to the diagnosis). Best-effort."""
        if self.case_service is None:
            return None
        try:
            case = await self.case_service.case_for_alias("source_fp", hotspot.fingerprint())
            if case is None:
                return None
            return {
                "incident_id": getattr(case, "case_id", ""),
                "case_number": getattr(case, "case_number", "") or getattr(case, "case_id", ""),
            }
        except Exception:  # linking is advisory; never break the digest
            return None


def _sev_rank(severity: str) -> int:
    return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(str(severity).upper(), 0)


def _format_ttl(seconds: int) -> str:
    if seconds >= 86400:
        days = seconds / 86400
        return f"{int(days)}d" if days == int(days) else f"{days:.1f}d"
    if seconds >= 3600:
        hours = seconds / 3600
        return f"{int(hours)}h" if hours == int(hours) else f"{hours:.1f}h"
    return f"{max(1, seconds // 60)}m"


def _hotspot_field(
    hotspot: Hotspot, *, case: dict[str, Any] | None = None, public_url: str = ""
) -> dict[str, Any]:
    emoji = _SEVERITY_EMOJI.get(hotspot.severity, "•")
    checks = "; ".join(hotspot.recommended_checks[:2])
    value = hotspot.summary
    if checks:
        value += f"\nNext: {checks}"
    if hotspot.warrants_change:
        value += "\n⚙️ candidate for config change (handoff)"
    meta = [f"ack id: `{hotspot.fingerprint()[:12]}`"]
    if case:
        number = case.get("case_number") or case.get("incident_id") or ""
        if number:
            if public_url:
                meta.append(f"case: [{number}]({public_url.rstrip('/')}/control/cases/{number})")
            else:
                meta.append(f"case: {number}")
    value += "\n" + " · ".join(meta)
    return {"name": f"{emoji} {hotspot.title}"[:256], "value": value[:1024] or "—"}


def _support_facts(hotspot: Hotspot) -> list[str]:
    facts = [hotspot.summary] if hotspot.summary else []
    for evidence in hotspot.evidence:
        bits = [bit for bit in (evidence.label, evidence.value, evidence.threshold, evidence.detail) if bit]
        if bits:
            facts.append(" | ".join(bits))
    return facts[:8]


def _expected_utility(hotspot: Hotspot) -> InsightScore:
    severity_component = {"LOW": 0.2, "MEDIUM": 0.55, "HIGH": 0.85}.get(hotspot.severity, 0.1)
    score_component = min(1.0, max(0.0, float(hotspot.score) / 100.0))
    change_component = 0.15 if hotspot.warrants_change else 0.0
    total = min(1.0, round((severity_component * 0.55) + (score_component * 0.3) + change_component, 3))
    rationale = [f"{hotspot.severity} severity", f"scanner score {hotspot.score:g}"]
    if hotspot.warrants_change:
        rationale.append("candidate for config or handoff work")
    return InsightScore(
        total=total,
        components={
            "severity": severity_component,
            "scanner_score": score_component,
            "change_candidate": change_component,
        },
        rationale=rationale,
    )


def _interruption_cost(hotspot: Hotspot, *, reason: str) -> InsightScore:
    severity_cost = {"LOW": 0.45, "MEDIUM": 0.25, "HIGH": 0.1}.get(hotspot.severity, 0.3)
    change_cost = -0.05 if hotspot.warrants_change else 0.0
    suppression_cost = 0.35 if "suppression" in reason.lower() or "snoozed" in reason.lower() else 0.0
    total = min(1.0, max(0.0, round(0.2 + severity_cost + change_cost + suppression_cost, 3)))
    return InsightScore(
        total=total,
        components={
            "baseline_operator_interruption": 0.2,
            "severity_noise": severity_cost,
            "change_candidate_offset": change_cost,
            "suppression_context": suppression_cost,
        },
        rationale=[reason],
    )


def _why_not_other_actions(*, action_selected: str, eligible: bool, gate_reason: str) -> dict[str, str]:
    reasons: dict[str, str] = {}
    if action_selected != "notify":
        reasons["notify"] = "Notification utility did not exceed interruption cost for this cycle."
    if action_selected != "question":
        reasons["question"] = "No operator clarification was required before the safe next step."
    if action_selected != "draft":
        reasons["draft"] = gate_reason if not eligible else "No new draftable handoff was selected for this hotspot."
    if action_selected != "stay_silent":
        reasons["stay_silent"] = "The hotspot met the surfacing policy for this cycle."
    return reasons


def _insight_budget_context(ledger: dict[str, Any], gate: GateDecision) -> dict[str, Any]:
    return {
        "daily_cost_usd": ledger.get("cost_usd", 0.0),
        "daily_investigations": ledger.get("investigations", 0),
        "daily_handoffs": ledger.get("handoffs", 0),
        "gate_max_investigations": gate.max_investigations,
        "gate_reason": gate.reason,
        "over_budget": gate.over_budget,
    }


def _bounded_confidence(score: float) -> float:
    return min(0.99, max(0.2, round(0.35 + (float(score) / 100.0) * 0.6, 3)))


def _risk_class(severity: str) -> str:
    return {"LOW": "low", "MEDIUM": "medium", "HIGH": "high"}.get(str(severity).upper(), "low")


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
