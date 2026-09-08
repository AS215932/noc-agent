"""Opt-in connection lifetime and recovery proof on a disposable PostgreSQL DB."""
import asyncio
import gc
import os
from urllib.parse import urlparse

import pytest
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from app.graph.checkpointing import build_checkpointer, checkpoint_health, close_checkpointers


@pytest.mark.asyncio
async def test_checkpoint_pool_survives_gc_disconnect_and_reopen(monkeypatch):
    url = os.getenv('NOC_TEST_CHECKPOINT_PG_URL')
    if not url:
        pytest.skip('requires disposable noc_checkpoint_test PostgreSQL')
    parsed = urlparse(url)
    assert parsed.path == '/noc_checkpoint_test'
    assert parsed.hostname in (None, 'localhost', '127.0.0.1', '::1')
    monkeypatch.setenv('NOC_DATABASE_URL', url)
    monkeypatch.setenv('NOC_REQUIRE_POSTGRES', 'true')

    class State(TypedDict):
        count: int

    workflow = StateGraph(State)
    workflow.add_node('increment', lambda state: {'count': state['count'] + 1})
    workflow.add_edge(START, 'increment')
    workflow.add_edge('increment', END)
    try:
        saver = await build_checkpointer()
        graph = workflow.compile(checkpointer=saver)
        gc.collect()
        await asyncio.sleep(0)
        results = await asyncio.gather(*[
            graph.ainvoke({'count': i}, {'configurable': {'thread_id': f'fixture-{i}'}})
            for i in range(8)
        ])
        assert [r['count'] for r in results] == list(range(1, 9))
        assert (await checkpoint_health())['status'] == 'ok'
        # A checked-out dead connection must be discarded; subsequent work
        # obtains a replacement without losing persisted state.
        async with saver.conn.connection() as conn:
            await conn.close()
        result = await graph.ainvoke({'count': 20}, {'configurable': {'thread_id': 'after-disconnect'}})
        assert result['count'] == 21
        await close_checkpointers()
        assert saver.conn.closed
        replacement = await build_checkpointer()
        assert replacement is not saver
        saved = await replacement.aget_tuple({'configurable': {'thread_id': 'after-disconnect'}})
        assert saved.checkpoint['channel_values']['count'] == 21
        assert (await checkpoint_health())['status'] == 'ok'
    finally:
        await close_checkpointers()
