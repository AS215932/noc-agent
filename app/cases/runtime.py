"""Runtime builder for optional case-service shadow mode."""

from __future__ import annotations

import os
from dataclasses import dataclass

from app.cases.outbox import OutboxProcessReport, OutboxProcessor
from app.cases.policy import CasePolicy
from app.cases.service import CaseService
from app.cases.store import CaseStore, InMemoryCaseStore
from app.config import load_loop_handoff_settings
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

    lhp = load_loop_handoff_settings()
    engineering_repo = lhp.engineering_handoff_repo if lhp.enabled and lhp.engineering_handoff_delivery_enabled else ""
    handlers = build_default_outbox_handlers(
        runtime.service,
        knowledge_candidate_dir=_env_str("NOC_KNOWLEDGE_CANDIDATE_DIR", ""),
        control_public_url=_env_str("NOC_CONTROL_PUBLIC_URL", ""),
        handoff_repo=_env_str("NOC_CASE_HANDOFF_REPO", _env_str("NOC_PROACTIVE_HANDOFF_REPO", "")),
        engineering_handoff_repo=engineering_repo,
        loop_handoff_settings=lhp,
    )
    processor = OutboxProcessor(
        runtime.store,
        handlers,
        retry_backoff_s=_env_int("NOC_CASE_OUTBOX_RETRY_BACKOFF_S", 60),
    )
    await runtime.service.request_due_critical_reminders(
        interval_s=_env_int("NOC_DISCORD_CRITICAL_REMINDER_S", 21600)
    )
    return await processor.process_pending(limit=limit or _env_int("NOC_CASE_OUTBOX_LIMIT", 10))


async def build_case_service_runtime_from_env(*, force: bool = False) -> CaseServiceRuntime | None:
    """Build optional CaseService runtime.

    `NOC_CASESERVICE_SHADOW=1` enables shadow writes. Primary cutover flags
    (`NOC_CASESERVICE_CONTROL_PRIMARY=1` or `NOC_CASESERVICE_REACTIVE_PRIMARY=1`)
    and the proactive loop (`NOC_PROACTIVE_ENABLED=1`) also require a runtime.
    If a DB URL is configured, Postgres is used. If
    Postgres is explicitly required, missing DB/supporting packages fail loud.
    Otherwise the runtime can use in-memory storage for local/dev canaries.
    """

    if not (
        force
        or _env_bool("NOC_CASESERVICE_SHADOW", False)
        or _env_bool("NOC_CASESERVICE_CONTROL_PRIMARY", False)
        or _env_bool("NOC_CASESERVICE_REACTIVE_PRIMARY", False)
        or _env_bool("NOC_PROACTIVE_ENABLED", False)
    ):
        return None
    db = load_database_settings()
    if db.require_postgres:
        db.assert_ready_for_production()
    policy = _case_policy_from_env()
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


def _case_policy_from_env() -> CasePolicy:
    defaults = CasePolicy(policy_version=os.getenv("NOC_CASE_POLICY_VERSION", "case_policy_v1"))
    proactive_cooldown = _env_int("NOC_PROACTIVE_INVESTIGATION_COOLDOWN_S", defaults.investigation_cooldown_s)
    proactive_failure_retry = _env_int(
        "NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S",
        proactive_cooldown,
    )
    return CasePolicy(
        policy_version=defaults.policy_version,
        report_reassert_s=_env_int("NOC_CASE_REPORT_REASSERT_S", defaults.report_reassert_s),
        investigation_cooldown_s=_env_int("NOC_CASE_INVESTIGATION_COOLDOWN_S", proactive_cooldown),
        investigation_failure_retry_s=_env_int("NOC_CASE_INVESTIGATION_FAILURE_RETRY_S", proactive_failure_retry),
        reinvestigate_stale_s=_env_int("NOC_CASE_REINVESTIGATE_STALE_S", defaults.reinvestigate_stale_s),
        recovery_cooldown_s=_env_int("NOC_CASE_RECOVERY_COOLDOWN_S", defaults.recovery_cooldown_s),
        suppression_default_ttl_s=_env_int("NOC_CASE_SUPPRESSION_DEFAULT_TTL_S", defaults.suppression_default_ttl_s),
        auto_snooze_ttl_s=_env_int("NOC_CASE_AUTO_SNOOZE_TTL_S", defaults.auto_snooze_ttl_s),
    )
