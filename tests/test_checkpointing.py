import pytest

from app.graph import checkpointing


@pytest.mark.asyncio
async def test_build_checkpointer_uses_postgres_when_available(monkeypatch):
    class _Saver:
        setup_called = False

        @classmethod
        def from_conn_string(cls, url):
            inst = cls()
            inst.url = url
            return inst

        async def setup(self):
            self.setup_called = True

    monkeypatch.setenv("NOC_DATABASE_URL", "postgresql://noc/example")
    monkeypatch.delenv("NOC_REDIS_URL", raising=False)
    monkeypatch.setattr(checkpointing, "AsyncPostgresSaver", _Saver)

    saver = await checkpointing.build_checkpointer()

    assert isinstance(saver, _Saver)
    assert saver.url == "postgresql://noc/example"
    assert saver.setup_called is True


@pytest.mark.asyncio
async def test_build_checkpointer_fails_loud_when_postgres_required_but_unavailable(monkeypatch):
    monkeypatch.setenv("NOC_REQUIRE_POSTGRES", "true")
    monkeypatch.setenv("NOC_DATABASE_URL", "postgresql://noc/example")
    monkeypatch.delenv("NOC_REDIS_URL", raising=False)
    monkeypatch.setattr(checkpointing, "AsyncPostgresSaver", None)

    with pytest.raises(RuntimeError, match="Postgres checkpointing requested"):
        await checkpointing.build_checkpointer()


@pytest.mark.asyncio
async def test_build_checkpointer_refuses_in_memory_when_postgres_required_without_dsn(monkeypatch):
    monkeypatch.setenv("NOC_REQUIRE_POSTGRES", "true")
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REDIS_URL", raising=False)
    monkeypatch.setattr(checkpointing, "AsyncPostgresSaver", None)
    monkeypatch.setattr(checkpointing, "AsyncRedisSaver", None)

    with pytest.raises(RuntimeError, match="refusing to fall back"):
        await checkpointing.build_checkpointer()


@pytest.mark.asyncio
async def test_build_checkpointer_still_falls_back_to_memory_by_default(monkeypatch):
    monkeypatch.delenv("NOC_REQUIRE_POSTGRES", raising=False)
    monkeypatch.delenv("NOC_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("NOC_REDIS_URL", raising=False)
    monkeypatch.setattr(checkpointing, "AsyncPostgresSaver", None)
    monkeypatch.setattr(checkpointing, "AsyncRedisSaver", None)

    saver = await checkpointing.build_checkpointer()

    assert saver.__class__.__name__ == "InMemorySaver"
