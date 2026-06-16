"""The learning flywheel for the proactive loop.

Mirrors the structure of both reference loops:

- ``engineering-loop`` file layout — ``lessons/`` (human-curated, *injected*),
  ``proposals/`` (agent-proposed candidates, *never auto-injected* — a human
  merges them into ``lessons/`` as a normal git change), and ``journal/``
  (per-cycle lab notes).
- ``hyperliquid`` governance — a :class:`MemoryPolicyEngine` gates which lesson
  statuses may be injected, and **outcome evaluation** turns "did this hotspot
  become a real alert?" into evidence that proposes candidate lessons.

Every operation is best-effort: a memory failure must never break a cycle.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app import log
from app.proactive.models import CandidateLesson, LessonStatus, Observation
from app.safe_errors import classify_exception, log_exception


_INJECTABLE_STATUSES: set[str] = {"validated_advisory", "approved_policy"}
_MAX_LESSON_CHARS = 8000
_MAX_OBSERVATIONS = 2000


class MemoryPolicyEngine:
    """Decides which lessons may be injected into scanner/investigation context.

    Read-only NOC reasoning is advisory, so both ``validated_advisory`` and
    ``approved_policy`` may be injected; raw ``candidate``/``deprecated``/
    ``reverted`` lessons may not (they await human review)."""

    def can_inject(self, status: str | LessonStatus, *, context: str = "investigation") -> bool:
        return str(status) in _INJECTABLE_STATUSES


def _utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _parse_ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value).timestamp()
        except ValueError:
            return None
    return None


def _scope_key(lesson_type: str, scope: dict[str, Any]) -> str:
    sig = json.dumps(scope, sort_keys=True, separators=(",", ":"))
    return f"{lesson_type}-" + hashlib.sha256(sig.encode()).hexdigest()[:16]


class ProactiveMemory:
    def __init__(self, base_dir: str | Path):
        self.base = Path(base_dir)
        self.lessons_dir = self.base / "lessons"
        self.proposals_dir = self.base / "proposals"
        self.journal_dir = self.base / "journal"
        self.observations_path = self.base / "observations.jsonl"
        self.policy = MemoryPolicyEngine()

    def ensure(self) -> None:
        for directory in (self.lessons_dir, self.proposals_dir, self.journal_dir):
            directory.mkdir(parents=True, exist_ok=True)

    # --- lessons (injectable) --------------------------------------------

    def active_lessons(self) -> list[str]:
        """Human-merged lessons under ``lessons/`` — treated as approved policy
        and injectable. Candidate proposals are never returned here."""
        try:
            if not self.lessons_dir.exists():
                return []
            lessons: list[str] = []
            budget = _MAX_LESSON_CHARS
            for path in sorted(self.lessons_dir.glob("*.md")):
                if not self.policy.can_inject("approved_policy"):
                    break
                text = path.read_text(encoding="utf-8").strip()
                if not text:
                    continue
                text = text[:budget]
                lessons.append(text)
                budget -= len(text)
                if budget <= 0:
                    break
            return lessons
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_active_lessons_failed", exc, category=safe.category)
            return []

    # --- candidate proposals (human-merged into lessons later) -----------

    def proposals(self) -> list[CandidateLesson]:
        out: list[CandidateLesson] = []
        if not self.proposals_dir.exists():
            return out
        for path in sorted(self.proposals_dir.glob("*.json")):
            try:
                out.append(CandidateLesson.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception:  # skip malformed proposal
                continue
        return out

    def propose_lesson(self, candidate: CandidateLesson) -> CandidateLesson:
        """Write/merge a candidate proposal. Re-proposing the same scope bumps
        ``occurrences`` and merges evidence instead of spawning duplicates."""
        self.ensure()
        path = self.proposals_dir / f"{_scope_key(candidate.lesson_type, candidate.scope)}.json"
        if path.exists():
            try:
                existing = CandidateLesson.model_validate_json(path.read_text(encoding="utf-8"))
                candidate = existing.model_copy(
                    update={
                        "occurrences": existing.occurrences + 1,
                        "evidence": list(dict.fromkeys([*existing.evidence, *candidate.evidence])),
                        "confidence": max(existing.confidence, candidate.confidence),
                    }
                )
            except Exception:  # overwrite a corrupt proposal
                pass
        path.write_text(candidate.model_dump_json(indent=2), encoding="utf-8")
        log.info("proactive_lesson_proposed", lesson_id=candidate.lesson_id, occurrences=candidate.occurrences)
        return candidate

    # --- observations (prediction tracking) ------------------------------

    def record_observation(self, observation: Observation) -> None:
        try:
            self.base.mkdir(parents=True, exist_ok=True)
            with self.observations_path.open("a", encoding="utf-8") as handle:
                handle.write(observation.model_dump_json() + "\n")
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_record_observation_failed", exc, category=safe.category)

    def load_observations(self) -> list[dict[str, Any]]:
        if not self.observations_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in self.observations_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows[-_MAX_OBSERVATIONS:]

    def _rewrite_observations(self, rows: list[dict[str, Any]]) -> None:
        self.base.mkdir(parents=True, exist_ok=True)
        self.observations_path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows[-_MAX_OBSERVATIONS:]), encoding="utf-8"
        )

    # --- journal ---------------------------------------------------------

    def write_journal(self, cycle_id: str, summary: dict[str, Any]) -> Path | None:
        try:
            self.ensure()
            path = self.journal_dir / f"{cycle_id}.md"
            lines = [f"# Proactive cycle {cycle_id}", f"- generated_at: {datetime.now(timezone.utc).isoformat()}"]
            for key, value in summary.items():
                lines.append(f"- {key}: {value}")
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return path
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_journal_failed", exc, category=safe.category)
            return None

    # --- outcome evaluation ----------------------------------------------

    async def evaluate_outcomes(
        self,
        incident_memory: Any,
        *,
        now: float | None = None,
        window_h: float = 6.0,
        min_age_h: float = 2.0,
        confirm_threshold: int = 2,
    ) -> list[CandidateLesson]:
        """Classify pending predictions against subsequent real alerts and
        propose candidate lessons for rules that reliably predicted incidents."""
        now = now or _utc_ts()
        rows = self.load_observations()
        if not rows:
            return []
        try:
            real_alerts = await _collect_real_alerts(incident_memory)
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_collect_real_alerts_failed", exc, category=safe.category)
            return []

        confirmed_by_rule: dict[str, list[dict[str, Any]]] = {}
        changed = False
        for row in rows:
            if row.get("outcome"):
                if row["outcome"] == "confirmed":
                    confirmed_by_rule.setdefault(row.get("rule_id", "?"), []).append(row)
                continue
            outcome = classify_observation(row, real_alerts, now, window_h=window_h, min_age_h=min_age_h)
            if outcome == "pending":
                continue
            row["outcome"] = outcome
            changed = True
            if outcome == "confirmed":
                confirmed_by_rule.setdefault(row.get("rule_id", "?"), []).append(row)
        if changed:
            self._rewrite_observations(rows)

        proposed: list[CandidateLesson] = []
        for rule_id, confirmations in confirmed_by_rule.items():
            if len(confirmations) < confirm_threshold:
                continue
            evidence = sorted({c.get("incident_id") or c.get("fingerprint", "") for c in confirmations if c})
            proposed.append(
                self.propose_lesson(
                    CandidateLesson(
                        lesson_type="scan_tuning",
                        scope={"rule_id": rule_id},
                        claim=(
                            f"Proactive rule '{rule_id}' has confirmed {len(confirmations)} predictions "
                            "(hotspot preceded a real alert). Consider promoting it to an earlier/automatic action."
                        ),
                        evidence=[e for e in evidence if e],
                        confidence=min(0.5 + 0.1 * len(confirmations), 0.95),
                        occurrences=len(confirmations),
                        source="outcome_eval",
                        status="candidate",
                    )
                )
            )
        return proposed


def classify_observation(
    obs: dict[str, Any],
    real_alerts: list["RealAlert"],
    now: float,
    *,
    window_h: float,
    min_age_h: float,
) -> str:
    """``confirmed`` if a real (non-proactive) alert for the same resource fired
    within ``window_h`` after the prediction; ``unconfirmed`` if the window has
    closed with nothing; ``pending`` if it is too soon to tell."""
    detected = _parse_ts(obs.get("created_at"))
    if detected is None:
        return "pending"
    age_h = (now - detected) / 3600.0
    if age_h < min_age_h:
        return "pending"
    resource = obs.get("resource", "")
    window_end = detected + window_h * 3600.0
    for alert in real_alerts:
        if alert.source == "proactive":
            continue
        if alert.resource == resource and detected <= alert.ts <= window_end:
            return "confirmed"
    if now >= window_end:
        return "unconfirmed"
    return "pending"


class RealAlert:
    __slots__ = ("alertname", "resource", "source", "ts")

    def __init__(self, resource: str, ts: float, source: str, alertname: str = ""):
        self.resource = resource
        self.ts = ts
        self.source = source
        self.alertname = alertname


async def _collect_real_alerts(incident_memory: Any) -> list[RealAlert]:
    """Best-effort: derive recent reactive alerts from the case store."""
    cases = await incident_memory.list_cases()
    alerts: list[RealAlert] = []
    for case in cases:
        event = case.get("latest_event") or {}
        source = str(event.get("source") or case.get("source") or "")
        ts = _parse_ts(event.get("received_at")) or _parse_ts(case.get("updated_at"))
        resource = str(case.get("resource_id") or event.get("resource_key") or "")
        if ts is None or not resource:
            continue
        alerts.append(RealAlert(resource=resource, ts=ts, source=source, alertname=str(event.get("alertname") or "")))
    return alerts
