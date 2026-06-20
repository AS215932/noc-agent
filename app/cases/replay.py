"""Deterministic replay helpers for case-grounded behavior."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from app.cases.correlation import MetaCaseResult
from app.cases.models import AtomicCaseProjection, MetaCaseProjection, ObservationRecord
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


def load_observation_fixture(path: str | Path) -> list[ObservationRecord]:
    """Load a sanitized replay fixture.

    Shape: either `[{observation...}]` or `{ "observations": [{...}] }`.
    """

    loaded = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = loaded.get("observations") if isinstance(loaded, dict) else loaded
    if not isinstance(rows, list):
        raise ValueError("replay fixture must be a list or contain an observations list")
    return [ObservationRecord.model_validate(row) for row in rows]
