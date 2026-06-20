"""Runtime builder for optional case-service shadow mode."""

from __future__ import annotations

import os
from dataclasses import dataclass

from app.cases.outbox import OutboxProcessReport, OutboxProcessor
from app.cases.policy import CasePolicy
from app.cases.service import CaseService
from app.cases.store import CaseStore, InMemoryCaseStore
from app.db.config import load_database_settings


@dataclass(slots=True)
class CaseServiceRuntime:
    service: CaseService
    store: CaseStore

    async def close(self) -> None:
        close = getattr(self.store, "close", None)
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result


async def process_case_outbox_once(runtime: CaseServiceRuntime, *, limit: int | None = None) -> OutboxProcessReport:
    from app.cases.handlers import build_default_outbox_handlers

    handlers = build_default_outbox_handlers(
        runtime.service,
        knowledge_candidate_dir=_env_str("NOC_KNOWLEDGE_CANDIDATE_DIR", ""),
        control_public_url=_env_str("NOC_CONTROL_PUBLIC_URL", ""),
        handoff_repo=_env_str("NOC_CASE_HANDOFF_REPO", _env_str("NOC_PROACTIVE_HANDOFF_REPO", "")),
    )
    processor = OutboxProcessor(
        runtime.store,
        handlers,
        retry_backoff_s=_env_int("NOC_CASE_OUTBOX_RETRY_BACKOFF_S", 60),
    )
    return await processor.process_pending(limit=limit or _env_int("NOC_CASE_OUTBOX_LIMIT", 10))


async def build_case_service_runtime_from_env() -> CaseServiceRuntime | None:
    """Build optional shadow CaseService runtime.

    `NOC_CASESERVICE_SHADOW=1` enables shadow writes. If a DB URL is configured,
    Postgres is used. If Postgres is explicitly required, missing DB/supporting
    packages fail loud. Otherwise shadow mode can use in-memory storage for
    local/dev canaries.
    """

    if not _env_bool("NOC_CASESERVICE_SHADOW", False):
        return None
    db = load_database_settings()
    if db.require_postgres:
        db.assert_ready_for_production()
    policy = CasePolicy(policy_version=os.getenv("NOC_CASE_POLICY_VERSION", "case_policy_v1"))
    if db.enabled:
        try:
            from app.cases.postgres import PostgresCaseStore

            store = await PostgresCaseStore.connect(db)
            await store.setup()
            return CaseServiceRuntime(service=CaseService(store, policy=policy), store=store)
        except Exception:
            if db.require_postgres:
                raise
    memory_store: CaseStore = InMemoryCaseStore()
    return CaseServiceRuntime(service=CaseService(memory_store, policy=policy), store=memory_store)


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default
