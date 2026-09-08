import asyncio

import pytest
import pytest_asyncio

from app.graph import checkpointing


@pytest.mark.asyncio
async def test_build_checkpointer_uses_postgres_when_available(monkeypatch):
    class _Saver:
        setup_called = False

        def __init__(self, conn):
            self.conn = conn

        async def setup(self):
            self.setup_called = True

    monkeypatch.setenv("NOC_DATABASE_URL", "postgresql://noc/example")
    monkeypatch.delenv("NOC_REDIS_URL", raising=False)
    monkeypatch.setattr(checkpointing, "AsyncPostgresSaver", _Saver)
    monkeypatch.setattr(checkpointing, "AsyncConnectionPool", FakePool)

    saver = await checkpointing.build_checkpointer()

    assert isinstance(saver, _Saver)
    assert saver.conn.url == "postgresql://noc/example"
    assert saver.conn.opened
    assert await checkpointing.build_checkpointer() is saver
    await checkpointing.close_checkpointers()
    assert saver.conn.closed
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


class FakePool:
    check_connection = None

    def __init__(self, url, **kwargs):
        self.url = url
        self.opened = False
        self.closed = False

    async def open(self, **kwargs):
        self.opened = True

    async def close(self):
        self.closed = True


@pytest_asyncio.fixture(autouse=True)
async def close_pool_after_test():
    yield
    await checkpointing.close_checkpointers()


@pytest.mark.asyncio
async def test_concurrent_builds_share_one_owned_pool(monkeypatch):
    pools = []

    class Pool(FakePool):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            pools.append(self)

    class Saver:
        def __init__(self, conn):
            self.conn = conn

        async def setup(self):
            await asyncio.sleep(0)

    monkeypatch.setattr(checkpointing, "AsyncConnectionPool", Pool)
    savers = await asyncio.gather(*[
        checkpointing._build_postgres_saver(Saver, "fixture") for _ in range(8)
    ])
    assert len(pools) == 1
    assert all(s is savers[0] for s in savers)
    assert not pools[0].closed
    await checkpointing.close_checkpointers()
    assert pools[0].closed


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("setup failed"), asyncio.CancelledError()])
async def test_setup_failure_or_cancellation_closes_pool_and_allows_retry(monkeypatch, error):
    pools = []

    class Pool(FakePool):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            pools.append(self)

    class Saver:
        def __init__(self, conn):
            self.conn = conn

        async def setup(self):
            if len(pools) == 1:
                raise error

    monkeypatch.setattr(checkpointing, "AsyncConnectionPool", Pool)
    with pytest.raises(type(error)):
        await checkpointing._build_postgres_saver(Saver, "fixture")
    assert pools[0].closed
    saver = await checkpointing._build_postgres_saver(Saver, "fixture")
    assert saver.conn is pools[1]
    assert not pools[1].closed


@pytest.mark.asyncio
@pytest.mark.parametrize('required', [False, True])
async def test_startup_only_requires_checkpoint_connection_when_postgres_required(monkeypatch, required):
    from unittest.mock import AsyncMock
    monkeypatch.setenv('NOC_DATABASE_URL', 'postgresql://noc/example')
    monkeypatch.setenv('NOC_REQUIRE_POSTGRES', str(required).lower())
    build = AsyncMock(side_effect=RuntimeError('database unavailable'))
    monkeypatch.setattr(checkpointing, 'build_checkpointer', build)
    if required:
        with pytest.raises(RuntimeError, match='database unavailable'):
            await checkpointing.initialize_checkpointer()
        build.assert_awaited_once()
    else:
        await checkpointing.initialize_checkpointer()
        build.assert_not_awaited()


def test_module_import_and_optional_memory_work_without_postgres_driver():
    import subprocess
    import sys
    script = '''
import asyncio
import importlib.abc
import os
import sys
class NoPostgres(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'psycopg' or fullname.startswith('psycopg.'):
            raise ImportError('simulated missing libpq')
sys.meta_path.insert(0, NoPostgres())
for key in ('NOC_DATABASE_URL', 'DATABASE_URL', 'NOC_REDIS_URL', 'NOC_REQUIRE_POSTGRES'):
    os.environ.pop(key, None)
from app.graph.checkpointing import build_checkpointer, AsyncPostgresSaver
assert AsyncPostgresSaver is None
assert asyncio.run(build_checkpointer()).__class__.__name__ == 'InMemorySaver'
os.environ['NOC_REQUIRE_POSTGRES'] = 'true'
os.environ['NOC_DATABASE_URL'] = 'postgresql://noc/example'
try:
    asyncio.run(build_checkpointer())
except RuntimeError as exc:
    assert 'unavailable' in str(exc)
else:
    raise AssertionError('required PostgreSQL must not fall back')
'''
    subprocess.run([sys.executable, '-c', script], check=True, timeout=30)
