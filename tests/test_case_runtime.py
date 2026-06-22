import pytest

from app.cases.runtime import build_case_service_runtime_from_env
from app.cases.policy import CasePolicy
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_case_service_runtime_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_CONTROL_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert await build_case_service_runtime_from_env() is None


@pytest.mark.asyncio
async def test_case_service_runtime_starts_for_control_primary_without_shadow(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert isinstance(runtime.store, InMemoryCaseStore)
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_starts_for_reactive_primary_without_shadow(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_CONTROL_PRIMARY", raising=False)
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert isinstance(runtime.store, InMemoryCaseStore)
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_starts_for_proactive_loop(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_CONTROL_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.setenv("NOC_PROACTIVE_ENABLED", "1")
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert isinstance(runtime.store, InMemoryCaseStore)
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_uses_long_failed_investigation_retry_by_default(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)
    monkeypatch.delenv("NOC_CASE_INVESTIGATION_FAILURE_RETRY_S", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert runtime.service.policy.investigation_failure_retry_s == CasePolicy().investigation_cooldown_s
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_aligns_failed_retry_with_proactive_cooldown(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)
    monkeypatch.setenv("NOC_PROACTIVE_INVESTIGATION_COOLDOWN_S", "7200")
    monkeypatch.delenv("NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S", raising=False)
    monkeypatch.delenv("NOC_CASE_INVESTIGATION_FAILURE_RETRY_S", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert runtime.service.policy.investigation_failure_retry_s == 7200
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_allows_failed_investigation_retry_override(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)
    monkeypatch.setenv("NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S", "900")
    monkeypatch.setenv("NOC_CASE_INVESTIGATION_FAILURE_RETRY_S", "1800")

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert runtime.service.policy.investigation_failure_retry_s == 1800
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_uses_in_memory_for_shadow_without_database(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_SHADOW", "1")
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)

    runtime = await build_case_service_runtime_from_env()

    assert runtime is not None
    assert isinstance(runtime.store, InMemoryCaseStore)
    await runtime.close()


@pytest.mark.asyncio
async def test_case_service_runtime_fails_loud_when_postgres_required(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_SHADOW", "1")
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)
    monkeypatch.setenv("NOC_REQUIRE_POSTGRES", "1")
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        await build_case_service_runtime_from_env()
