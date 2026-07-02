"""Deterministic replay helpers for case-grounded behavior."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from app.cases.correlation import MetaCaseResult
from app.cases.models import AtomicCaseProjection, InsightDecisionRecord, InsightLabel, MetaCaseProjection, ObservationRecord
from app.cases.policy import CasePolicy
from app.cases.service import ObserveResult, CaseService
from app.cases.store import CaseStore, InMemoryCaseStore
from app.cases.correlation import CorrelationService


@dataclass(slots=True)
class ReplayResult:
    store: CaseStore
    observations: list[ObservationRecord]
    observe_results: list[ObserveResult]
    meta_results: list[MetaCaseResult]

    @property
    def observation_count(self) -> int:
        return len(self.observations)

    async def atomic_cases(self) -> list[AtomicCaseProjection]:
        return [case for case in await self.store.list_cases(kind="atomic", limit=1000) if isinstance(case, AtomicCaseProjection)]

    async def meta_cases(self) -> list[MetaCaseProjection]:
        return [case for case in await self.store.list_cases(kind="meta", limit=1000) if isinstance(case, MetaCaseProjection)]

    async def metrics(self) -> dict[str, Any]:
        atomic = await self.atomic_cases()
        meta = await self.meta_cases()
        return {
            "observation_count": self.observation_count,
            "atomic_case_count": len(atomic),
            "meta_case_count": len(meta),
            "resolved_case_count": sum(1 for case in atomic if case.status == "resolved"),
            "active_case_count": sum(1 for case in atomic if case.status not in {"resolved", "expired", "closed"}),
        }


@dataclass(slots=True)
class InsightReplayResult:
    store: CaseStore
    decisions: list[InsightDecisionRecord]
    labels: list[InsightLabel]

    async def metrics(self) -> dict[str, Any]:
        return insight_metrics(self.decisions, self.labels)


async def replay_observations(
    observations: Iterable[ObservationRecord],
    *,
    policy: CasePolicy | None = None,
    store: CaseStore | None = None,
    correlate: bool = True,
) -> ReplayResult:
    policy = policy or CasePolicy()
    store = store or InMemoryCaseStore()
    case_service = CaseService(store, policy=policy)
    correlation_service = CorrelationService(store, policy=policy)
    observation_list = list(observations)
    observe_results: list[ObserveResult] = []
    for observation in observation_list:
        observe_results.append(await case_service.observe(observation))
    meta_results: list[MetaCaseResult] = []
    if correlate:
        meta = await correlation_service.correlate_observations(observation_list)
        if meta is not None:
            meta_results.append(meta)
    return ReplayResult(store=store, observations=observation_list, observe_results=observe_results, meta_results=meta_results)


async def replay_insights(
    decisions: Iterable[InsightDecisionRecord],
    labels: Iterable[InsightLabel],
    *,
    store: CaseStore | None = None,
) -> InsightReplayResult:
    store = store or InMemoryCaseStore()
    decision_list = list(decisions)
    label_list = list(labels)
    for decision in decision_list:
        await store.record_insight_decision(decision)
    for label in label_list:
        await store.record_insight_label(label)
    return InsightReplayResult(store=store, decisions=decision_list, labels=label_list)


def insight_metrics(decisions: Iterable[InsightDecisionRecord], labels: Iterable[InsightLabel]) -> dict[str, Any]:
    decision_by_id = {row.insight_id: row for row in decisions}
    label_list = [label for label in labels if label.insight_id in decision_by_id]
    action_correct = 0
    cgs_scores: list[float] = []
    for label in label_list:
        decision = decision_by_id[label.insight_id]
        acceptable = {label.reference_action, *label.acceptable_alternatives}
        if decision.action_selected in acceptable:
            action_correct += 1
        cgs = _cgs_for(decision, label)
        if cgs is not None:
            cgs_scores.append(cgs)
    denominator = len(label_list)
    return {
        "decision_count": len(decision_by_id),
        "label_count": len(label_list),
        "idq": round(action_correct / denominator, 4) if denominator else None,
        "idq_action_correct": action_correct,
        "idq_action_total": denominator,
        "cgs": round(sum(cgs_scores) / len(cgs_scores), 4) if cgs_scores else None,
        "cgs_total": len(cgs_scores),
        "silence_rate": round(
            sum(1 for row in decision_by_id.values() if row.action_selected == "stay_silent") / len(decision_by_id),
            4,
        )
        if decision_by_id
        else 0.0,
    }


def load_observation_fixture(path: str | Path) -> list[ObservationRecord]:
    """Load a sanitized replay fixture.

    Shape: either `[{observation...}]` or `{ "observations": [{...}] }`.
    """

    loaded = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = loaded.get("observations") if isinstance(loaded, dict) else loaded
    if not isinstance(rows, list):
        raise ValueError("replay fixture must be a list or contain an observations list")
    return [ObservationRecord.model_validate(row) for row in rows]


def load_insight_fixture(path: str | Path) -> tuple[list[InsightDecisionRecord], list[InsightLabel]]:
    """Load a sanitized insight-policy replay fixture.

    Shape: `{ "insights": [{...}], "labels": [{...}] }`.
    """

    loaded = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("insight replay fixture must be an object")
    insight_rows = loaded.get("insights", [])
    label_rows = loaded.get("labels", [])
    if not isinstance(insight_rows, list) or not isinstance(label_rows, list):
        raise ValueError("insight replay fixture must contain insights and labels lists")
    return (
        [InsightDecisionRecord.model_validate(row) for row in insight_rows],
        [InsightLabel.model_validate(row) for row in label_rows],
    )


def _cgs_for(decision: InsightDecisionRecord, label: InsightLabel) -> float | None:
    if decision.action_selected == "stay_silent":
        return None
    if label.faithfulness_verdict == "unsupported":
        return 0.0
    reference = {_normalize_fact(item) for item in label.support_facts if _normalize_fact(item)}
    predicted = {_normalize_fact(item) for item in decision.support_facts if _normalize_fact(item)}
    if not reference:
        return None
    if not predicted:
        return 0.0
    overlap = len(reference & predicted)
    precision = overlap / len(predicted)
    recall = overlap / len(reference)
    if precision + recall == 0:
        return 0.0
    return round(2 * precision * recall / (precision + recall), 4)


def _normalize_fact(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())
