import pytest

from app.cases.runtime import build_case_service_runtime_from_env
from app.cases.store import InMemoryCaseStore


@pytest.mark.asyncio
async def test_case_service_runtime_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NOC_CASESERVICE_SHADOW", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert await build_case_service_runtime_from_env() is None


@pytest.mark.asyncio
async def test_case_service_runtime_uses_in_memory_for_shadow_without_database(monkeypatch):
    monkeypatch.setenv("NOC_CASESERVICE_SHADOW", "1")
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
    monkeypatch.setenv("NOC_REQUIRE_POSTGRES", "1")
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        await build_case_service_runtime_from_env()
