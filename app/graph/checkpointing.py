from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver

from app.db.config import load_database_settings

try:  # pragma: no cover - production Redis availability is environment-dependent
    from langgraph.checkpoint.redis import AsyncRedisSaver
except Exception:  # pragma: no cover
    AsyncRedisSaver = None

try:  # pragma: no cover - optional production dependency
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from psycopg.rows import dict_row
    from psycopg_pool import AsyncConnectionPool
except Exception:  # pragma: no cover
    AsyncPostgresSaver = None
    AsyncConnectionPool = None
    dict_row = None


async def build_checkpointer():
    db = load_database_settings()
    if db.enabled:
        if AsyncPostgresSaver is not None:
            return await _build_postgres_saver(AsyncPostgresSaver, db.url)
        if db.require_postgres:
            raise RuntimeError("Postgres checkpointing requested but langgraph-checkpoint-postgres is unavailable")

    redis_url = os.getenv("NOC_REDIS_URL", "").strip()
    if redis_url and AsyncRedisSaver is not None:
        saver = AsyncRedisSaver(redis_url=redis_url)
        await saver.setup()
        return saver
    if db.require_postgres:
        raise RuntimeError("Postgres is required; refusing to fall back to in-memory LangGraph checkpoints")
    return InMemorySaver()


async def initialize_checkpointer() -> None:
    # Optional database deployments retain lazy graph initialization: a
    # checkpoint outage must not prevent their API or bot from starting.
    # Required PostgreSQL deployments must prove readiness before serving.
    if load_database_settings().require_postgres:
        await build_checkpointer()


@dataclass
class _Resources:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    url: str | None = None
    pool: Any = None
    saver: Any = None


# API and standalone bot own separate event loops and separate bounded pools.
_RESOURCES: WeakKeyDictionary = WeakKeyDictionary()


async def _build_postgres_saver(saver_cls: Any, db_url: str):
    resources = _RESOURCES.setdefault(asyncio.get_running_loop(), _Resources())
    async with resources.lock:
        if resources.saver is not None:
            if resources.url != db_url:
                raise RuntimeError("Checkpoint database configuration changed; restart required")
            return resources.saver
        pool = AsyncConnectionPool(
            db_url, open=False, min_size=1, max_size=4, timeout=5, max_waiting=16,
            check=AsyncConnectionPool.check_connection,
            kwargs={"autocommit": True, "prepare_threshold": 0,
                    "row_factory": dict_row, "connect_timeout": 5},
        )
        try:
            await pool.open(wait=True, timeout=10)
            saver = saver_cls(conn=pool)
            await saver.setup()
        except BaseException:
            await pool.close()
            raise
        resources.url, resources.pool, resources.saver = db_url, pool, saver
        return saver


async def close_checkpointers() -> None:
    resources = _RESOURCES.pop(asyncio.get_running_loop(), None)
    if resources is not None:
        async with resources.lock:
            if resources.pool is not None:
                await resources.pool.close()


async def checkpoint_health() -> dict[str, Any]:
    if not load_database_settings().enabled:
        return {"status": "disabled", "backend": "non_postgres"}
    resources = _RESOURCES.get(asyncio.get_running_loop())
    if resources is None or resources.pool is None:
        return {"status": "degraded", "reason": "checkpoint_not_initialized"}
    try:
        async with asyncio.timeout(3):
            async with resources.pool.connection() as conn:
                await conn.execute("SELECT 1 FROM checkpoints LIMIT 0")
    except Exception:
        return {"status": "degraded", "reason": "checkpoint_database_unavailable"}
    return {"status": "ok", "backend": "PostgresCheckpointer"}
