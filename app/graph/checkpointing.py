from __future__ import annotations

import os
from typing import Any

from langgraph.checkpoint.memory import InMemorySaver

from app.db.config import load_database_settings

try:  # pragma: no cover - production Redis availability is environment-dependent
    from langgraph.checkpoint.redis import AsyncRedisSaver
except Exception:  # pragma: no cover
    AsyncRedisSaver = None

try:  # pragma: no cover - optional production dependency
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
except Exception:  # pragma: no cover
    AsyncPostgresSaver = None


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


async def _build_postgres_saver(saver_cls: Any, db_url: str):
    """Construct an AsyncPostgresSaver across package-version APIs."""

    if hasattr(saver_cls, "from_conn_string"):
        maybe_saver = saver_cls.from_conn_string(db_url)
        if hasattr(maybe_saver, "__aenter__"):
            saver = await maybe_saver.__aenter__()
        else:
            saver = maybe_saver
    else:
        saver = saver_cls(conn_string=db_url)
    setup = getattr(saver, "setup", None)
    if setup is not None:
        result = setup()
        if hasattr(result, "__await__"):
            await result
    return saver
