"""Insight-decision records for the proactive loop.

Every cycle's surface/withhold decisions — including deliberate silence — become
agent-core ``InsightDecisionRecord``-shaped dicts, emitted as loop decision
envelopes (see ``app.agent_core_trace.emit_loop_decision_envelopes``). The
knowledge repo's insight-policy evaluation (IDQ/CGS) consumes them via the
collector, so records must stay sanitized: hotspot text is already scrubbed by
the ``Hotspot`` model validator, and nothing here may embed raw tool output,
prompts, or telemetry payloads.

Volume control: the loop runs every ``interval_s`` (120s deployed), so an
unchanged per-fingerprint decision re-emits at most every
``insight_reassert_s`` via :class:`InsightEmissionState`.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from app import log
from app.config import ProactiveLoopSettings
from app.proactive.governance import GateDecision
from app.proactive.models import Hotspot, ProactiveCycleReport, sanitize_label, severity_rank

POLICY_VERSION = "noc-insight.v1"

_RISK_BY_SEVERITY = {"LOW": "low", "MEDIUM": "medium", "HIGH": "high"}
# Interruption-cost baselines: a draft (handoff already filed) asks less of the
# operator right now than a bare notification; anything the digest dedup or a
# suppression withheld carries the cost of re-raising unchanged information.
_COST_NOTIFY = 0.25
_COST_DRAFT = 0.15
_COST_UNCHANGED_PENALTY = 0.25
_STATE_MAX_AGE_S = 30 * 86400

# (records, telemetry export version) for a hotspot's OKF citations.
KnowledgeRefsFn = Callable[[Hotspot], tuple[list[dict[str, Any]], str | None]]
SuppressionEntryFn = Callable[[Hotspot], Mapping[str, Any] | None]


def _sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _score(total: float, components: dict[str, float], rationale: list[str]) -> dict[str, Any]:
    return {
        "total": round(min(1.0, max(0.0, total)), 4),
        "components": {k: round(v, 4) for k, v in components.items()},
        "rationale": rationale,
    }


def _governance() -> dict[str, Any]:
    return {
        "sensitivity_class": "internal",
        "approval_tier": "none",
        "adversarial_review_required": True,
        "policy_ids": [POLICY_VERSION],
        "rationale": "Sanitized proactive surface/withhold decisions for replayable IDQ/CGS evaluation.",
    }


def _support_facts(hotspot: Hotspot) -> list[str]:
    facts = [f for f in (hotspot.title, hotspot.summary) if f]
    for ev in hotspot.evidence:
        bits = [b for b in (ev.label, ev.value, ev.threshold) if b]
        if bits:
            facts.append(" · ".join(bits))
    return facts[:8]


def _telemetry_refs(hotspot: Hotspot) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for ev in hotspot.evidence[:4]:
        # ev.query is loop-authored PromQL; ev.label is sanitized telemetry text.
        ref = ev.query or ev.label
        if ref:
            refs.append({"kind": "telemetry_probe", "ref": ref})
    return refs


def _record(
    *,
    cycle_id: str,
    fingerprint: str,
    candidate_type: str,
    candidate_source: str,
    action: str,
    sampling_class: str,
    why_now: str,
    support_facts: list[str],
    evidence_refs: list[dict[str, Any]],
    expected_utility: dict[str, Any],
    interruption_cost: dict[str, Any],
    risk_class: str | None,
    budget_context: dict[str, Any],
    case_id: str | None = None,
    tool_versions: dict[str, str] | None = None,
    downstream_outcome: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "insight_id": f"ins_noc_{_sha16(f'{cycle_id}:{fingerprint}:{action}:{why_now}')}",
        "loop": "noc",
        "fingerprint": fingerprint,
        "sampling_class": sampling_class,
        "candidate_type": candidate_type,
        "candidate_source": candidate_source,
        "action_selected": action,
        "why_now": why_now,
        "support_facts": support_facts,
        "evidence_refs": evidence_refs,
        "expected_utility": expected_utility,
        "interruption_cost": interruption_cost,
        "policy_version": POLICY_VERSION,
        "budget_context": budget_context,
        "governance": _governance(),
    }
    if risk_class:
        record["risk_class"] = risk_class
    if case_id:
        record["case_id"] = case_id
    if tool_versions:
        record["tool_versions"] = tool_versions
    if downstream_outcome:
        record["downstream_outcome"] = downstream_outcome
    return record


def build_cycle_insights(
    report: ProactiveCycleReport,
    gate: GateDecision,
    settings: ProactiveLoopSettings,
    *,
    effective: list[Hotspot],
    digest_due: bool,
    posted: bool,
    deep: bool,
    suppression_entry: SuppressionEntryFn,
    case_ids: Mapping[str, str],
    knowledge_refs: KnowledgeRefsFn | None = None,
) -> list[dict[str, Any]]:
    """One InsightDecisionRecord dict per hotspot decision this cycle.

    ``effective`` is the pre-suppression hotspot set (post carry-forward merge).
    ``digest_due`` is the dedup gate's *decision* (should the digest go out);
    ``posted`` is whether delivery actually succeeded. Only ``digest_due``
    drives the action — a failed send is still a notify decision (recorded
    with the delivery failure), never mislabelled as deliberate silence.
    """
    budget_context = {
        "gate_reason": gate.reason,
        "max_investigations": gate.max_investigations,
        "over_budget": gate.over_budget,
        "shadow": settings.shadow,
        "digest_due": digest_due,
        "digest_posted": posted,
    }
    norm = max(1.0, settings.insight_score_norm)
    records: list[dict[str, Any]] = []
    surfaced_fps = {h.fingerprint() for h in report.hotspots}
    investigated = set(report.investigated)

    for hotspot in report.hotspots:
        fingerprint = hotspot.fingerprint()
        utility = _score(
            hotspot.score / norm,
            {
                "hotspot_score": hotspot.score,
                "severity_rank": float(severity_rank(hotspot.severity)),
                "warrants_change": 1.0 if hotspot.warrants_change else 0.0,
            },
            [f"scanner score {hotspot.score} / norm {norm:g}"],
        )
        handoff_url = report.handoffs_by_key.get(hotspot.key)
        downstream: dict[str, Any] | None = None
        if hotspot.key in investigated:
            action = "draft" if handoff_url else "notify"
            sampling = "surfaced"
            why_now = f"{gate.reason} · investigated"
            if handoff_url:
                why_now += " · warrants-change handoff filed"
                downstream = {"handoff_url": handoff_url}
            cost_total, cost_note = (
                (_COST_DRAFT, "handoff drafted; operator reviews asynchronously")
                if handoff_url
                else (_COST_NOTIFY, "fresh diagnosis surfaced in digest")
            )
        elif digest_due:
            action = "notify"
            sampling = "surfaced"
            why_now = f"digest: {gate.reason}"
            cost_total, cost_note = _COST_NOTIFY, "hotspot surfaced in posted digest"
            if not posted:
                # The loop CHOSE to notify; delivery failed. Record the notify
                # decision with the failure — the dedup gate will retry the
                # send next cycle.
                why_now = f"digest: {gate.reason} · delivery failed, will retry"
                cost_note = "digest send failed; retrying next cycle"
        else:
            # The dedup gate withheld the digest — deliberate silence on an
            # unchanged set, not a missed opportunity.
            action = "stay_silent"
            sampling = "withheld_logged"
            why_now = "digest deduplicated: unchanged hotspot set"
            cost_total = _COST_NOTIFY + _COST_UNCHANGED_PENALTY
            cost_note = "re-raising unchanged information"
        refs = _telemetry_refs(hotspot)
        tool_versions: dict[str, str] | None = None
        if knowledge_refs is not None:
            okf_refs, export_version = knowledge_refs(hotspot)
            refs.extend(okf_refs)
            if export_version:
                tool_versions = {"knowledge_export": export_version}
        records.append(
            _record(
                cycle_id=report.cycle_id,
                fingerprint=fingerprint,
                candidate_type="hotspot",
                candidate_source=f"proactive_scanner:{hotspot.rule_id}",
                action=action,
                sampling_class=sampling,
                why_now=why_now,
                support_facts=_support_facts(hotspot),
                evidence_refs=refs,
                expected_utility=utility,
                interruption_cost=_score(cost_total, {"base": cost_total}, [cost_note]),
                risk_class=_RISK_BY_SEVERITY.get(hotspot.severity),
                budget_context=budget_context,
                case_id=case_ids.get(fingerprint),
                tool_versions=tool_versions,
                downstream_outcome=downstream,
            )
        )

    for hotspot in effective:
        fingerprint = hotspot.fingerprint()
        if fingerprint in surfaced_fps:
            continue
        entry = suppression_entry(hotspot) or {}
        # Operator/reason arrive via /control/proactive/ack request fields —
        # untrusted free text, so scrub like every other record input.
        operator = sanitize_label(entry.get("operator") or "operator", limit=40)
        reason = sanitize_label(entry.get("reason") or "active suppression", limit=200)
        records.append(
            _record(
                cycle_id=report.cycle_id,
                fingerprint=fingerprint,
                candidate_type="hotspot",
                candidate_source=f"proactive_scanner:{hotspot.rule_id}",
                action="stay_silent",
                sampling_class="withheld_logged",
                why_now=f"suppressed ({operator}): {reason}"[:300],
                support_facts=_support_facts(hotspot),
                evidence_refs=_telemetry_refs(hotspot),
                expected_utility=_score(
                    hotspot.score / norm, {"hotspot_score": hotspot.score}, []
                ),
                interruption_cost=_score(
                    _COST_NOTIFY + _COST_UNCHANGED_PENALTY,
                    {"base": _COST_NOTIFY, "suppressed": _COST_UNCHANGED_PENALTY},
                    ["operator or agent asked for silence on this fingerprint"],
                ),
                risk_class=_RISK_BY_SEVERITY.get(hotspot.severity),
                budget_context=budget_context,
                case_id=case_ids.get(fingerprint),
            )
        )

    if deep and not effective and not report.hotspots:
        # Quiet-interval sample: silence on a clean deep scan is a correct
        # decision that the IDQ denominator must see.
        records.append(
            _record(
                cycle_id=report.cycle_id,
                fingerprint=f"quiet:ruleset:{settings.ruleset_version}",
                candidate_type="quiet_interval",
                candidate_source="proactive_scanner",
                action="stay_silent",
                sampling_class="sampled_quiet_interval",
                why_now="deep scan all clear",
                support_facts=["deep scan found no hotspots"],
                evidence_refs=[],
                expected_utility=_score(0.0, {}, []),
                interruption_cost=_score(_COST_NOTIFY, {"base": _COST_NOTIFY}, []),
                risk_class=None,
                budget_context=budget_context,
            )
        )

    def _priority(record: dict[str, Any]) -> tuple[int, float]:
        order = {"surfaced": 0, "withheld_logged": 1, "sampled_quiet_interval": 2}
        utility = record.get("expected_utility") or {}
        return (order.get(str(record.get("sampling_class")), 3), -float(utility.get("total") or 0.0))

    records.sort(key=_priority)
    return records[: max(1, settings.insight_max_per_cycle)]


class InsightEmissionState:
    """Per-fingerprint emission dedup, persisted next to the other loop state.

    A record is due when its fingerprint is new, its decision signature
    (action, why_now, risk, coarse utility) changed, or the last emission is
    older than ``reassert_s``. Marking is separate from filtering so callers
    stamp fingerprints only after successful delivery — a collector outage
    must not silently swallow decisions until the next reassert. Best-effort:
    state I/O failures fall back to emitting (dupes over drops)."""

    def __init__(self, path: Path, *, reassert_s: int) -> None:
        self.path = path
        self.reassert_s = max(1, reassert_s)

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _signature(record: dict[str, Any]) -> str:
        # Severity/score changes matter to IDQ/CGS even when action + gate
        # reason stay the same (the digest reposts them too), so the decision
        # signature includes risk class and coarse utility.
        utility = record.get("expected_utility") or {}
        try:
            utility_bucket = f"{float(utility.get('total') or 0.0):.1f}"
        except (TypeError, ValueError):
            utility_bucket = "0.0"
        return _sha16(
            "|".join(
                [
                    str(record.get("action_selected")),
                    str(record.get("why_now")),
                    str(record.get("risk_class") or ""),
                    utility_bucket,
                ]
            )
        )[:8]

    def pending(
        self, records: list[dict[str, Any]], *, now: float | None = None
    ) -> list[dict[str, Any]]:
        """Records due for emission; does NOT stamp state."""
        now = time.time() if now is None else now
        state = self._load()
        due: list[dict[str, Any]] = []
        seen_this_call: set[str] = set()
        for record in records:
            fingerprint = str(record.get("fingerprint") or "")
            signature = self._signature(record)
            entry = state.get(fingerprint)
            fresh = (
                entry is None
                or entry.get("why") != signature
                or (now - float(entry.get("ts") or 0.0)) >= self.reassert_s
            )
            if fresh and fingerprint not in seen_this_call:
                due.append(record)
                seen_this_call.add(fingerprint)
        return due

    def mark(self, records: list[dict[str, Any]], *, now: float | None = None) -> None:
        """Stamp fingerprints as emitted (call only after successful delivery)."""
        now = time.time() if now is None else now
        state = self._load()
        for record in records:
            fingerprint = str(record.get("fingerprint") or "")
            state[fingerprint] = {"why": self._signature(record), "ts": now}
        state = {
            fp: entry
            for fp, entry in state.items()
            if isinstance(entry, dict) and (now - float(entry.get("ts") or 0.0)) < _STATE_MAX_AGE_S
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            tmp.replace(self.path)
        except OSError:
            log.info("proactive_insight_state_write_failed", path=str(self.path))

    def filter_and_mark(
        self, records: list[dict[str, Any]], *, now: float | None = None
    ) -> list[dict[str, Any]]:
        """pending() + mark() in one step (tests / callers without delivery
        feedback)."""
        due = self.pending(records, now=now)
        if due:
            self.mark(due, now=now)
        return due


def knowledge_refs_fn(
    retriever: Any, *, limit: int = 3
) -> KnowledgeRefsFn:
    """Build the per-hotspot OKF citation lookup around a KnowledgeExportRetriever.

    Uses the agent-core adapter so refs stay joinable on the bare concept id;
    if the installed agent-core predates the adapter, citations are skipped
    (the dep pin must land before the flag flips)."""

    def _lookup(hotspot: Hotspot) -> tuple[list[dict[str, Any]], str | None]:
        try:
            from agent_core.adapters.knowledge import source_ref_from_knowledge_citation
        except ImportError:
            return [], None
        try:
            results = retriever.search_case_context(
                {
                    "fingerprint": hotspot.fingerprint(),
                    "rule_id": hotspot.rule_id,
                    "resource": hotspot.resource,
                    "severity": hotspot.severity,
                    "summary": hotspot.summary,
                    "title": hotspot.title,
                },
                limit=limit,
            )
        except Exception:  # citations are advisory; never break the cycle
            return [], None
        refs: list[dict[str, Any]] = []
        export_version: str | None = None
        for result in results:
            citation = result.citation
            refs.append(
                source_ref_from_knowledge_citation(citation.as_trace_dict()).model_dump(
                    mode="json", exclude_none=True
                )
            )
            export_version = export_version or citation.export_version
        return refs, export_version

    return _lookup
