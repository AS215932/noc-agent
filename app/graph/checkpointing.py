from __future__ import annotations

import os

from langgraph.checkpoint.memory import InMemorySaver

try:  # pragma: no cover - production Redis availability is environment-dependent
    from langgraph.checkpoint.redis import AsyncRedisSaver
except Exception:  # pragma: no cover
    AsyncRedisSaver = None


async def build_checkpointer():
    redis_url = os.getenv("NOC_REDIS_URL", "").strip()
    if redis_url and AsyncRedisSaver is not None:
        saver = AsyncRedisSaver(redis_url=redis_url)
        await saver.setup()
        return saver
    return InMemorySaver()

