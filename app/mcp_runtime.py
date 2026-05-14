from __future__ import annotations

import os
import shlex
from dataclasses import dataclass
from typing import Any

from app import log
from app.safe_errors import classify_exception, log_exception
from app.tools.mcp_client import HyruleMCPClient


@dataclass
class MCPSourceState:
    source: str
    ready: bool = False
    tool_count: int = 0
    error: str | None = None


class MCPRuntime:
    def __init__(self, *, owner: str):
        self.owner = owner
        self.clients: dict[str, HyruleMCPClient] = {}
        self.states: dict[str, MCPSourceState] = {
            "hyrule": MCPSourceState(source="hyrule"),
            "xo": MCPSourceState(source="xo"),
        }

    async def connect_tools(self, agent: Any) -> None:
        if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
            log.info("mcp_runtime_disabled", owner=self.owner)
            return
        await self._connect_source(agent, self._hyrule_config())
        await self._connect_source(agent, self._xo_config())

    async def disconnect(self) -> None:
        while self.clients:
            source, client = self.clients.popitem()
            try:
                await client.disconnect()
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("mcp_disconnect_failed", exc, category=safe.category, owner=self.owner, source=source)
            finally:
                self.states[source].ready = False
                self.states[source].tool_count = 0

    def health(self) -> dict[str, Any]:
        hyrule = self.states["hyrule"]
        xo = self.states["xo"]
        hyrule_ready = hyrule.ready and hyrule.tool_count > 0
        xo_ready = xo.ready and xo.tool_count > 0
        return {
            "hyrule": hyrule_ready,
            "xo": xo_ready,
            "hyrule_tool_count": hyrule.tool_count,
            "xo_tool_count": xo.tool_count,
            "sources": {
                "hyrule": self._source_health(hyrule),
                "xo": self._source_health(xo),
            },
            "status": "ok" if hyrule_ready and xo_ready else "degraded",
        }

    async def _connect_source(self, agent: Any, config: dict[str, Any]) -> None:
        source = config["source"]
        state = self.states[source]
        client = HyruleMCPClient(config.get("command"), env=config.get("env"), url=config.get("url"))
        try:
            await client.connect()
            tools = await client.get_tools()
            for tool in tools:
                agent._function_toolset.add_tool(tool)
                log.info("mcp_tool_loaded", owner=self.owner, source=source, name=tool.name)
            self.clients[source] = client
            state.ready = True
            state.tool_count = len(tools)
            state.error = None
            log.info("mcp_tools_loaded", owner=self.owner, source=source, count=len(tools))
        except Exception as exc:
            state.ready = False
            state.tool_count = 0
            safe = classify_exception(exc)
            state.error = safe.category
            log_exception("mcp_connect_failed", exc, category=safe.category, owner=self.owner, source=source)
            try:
                await client.disconnect()
            except Exception as disconnect_exc:
                disconnect_safe = classify_exception(disconnect_exc)
                log_exception(
                    "mcp_disconnect_failed",
                    disconnect_exc,
                    category=disconnect_safe.category,
                    owner=self.owner,
                    source=source,
                )

    @staticmethod
    def _source_health(state: MCPSourceState) -> dict[str, Any]:
        return {
            "ready": state.ready and state.tool_count > 0,
            "tool_count": state.tool_count,
            "error": state.error,
        }

    @staticmethod
    def _hyrule_config() -> dict[str, Any]:
        url = os.getenv("HYRULE_MCP_URL", "").strip()
        command = None if url else shlex.split(os.environ["HYRULE_MCP_CMD"])
        return {"source": "hyrule", "command": command, "url": url or None}

    @staticmethod
    def _xo_config() -> dict[str, Any]:
        url = os.getenv("XO_MCP_URL", "").strip()
        command = None if url else shlex.split(os.environ["XO_MCP_CMD"])
        env = os.environ.copy()
        env.setdefault("XO_URL", "https://xo.servify.network")
        env.setdefault("XO_MCP_ENABLE_ACTIONS", "0")
        return {"source": "xo", "command": command, "url": url or None, "env": env}
