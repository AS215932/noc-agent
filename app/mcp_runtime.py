from __future__ import annotations

import os
import shlex
import asyncio
from dataclasses import dataclass
from typing import Any

from pydantic_ai.toolsets import FunctionToolset

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
        self.connect_timeout_s = float(os.getenv("MCP_CONNECT_TIMEOUT_SECONDS", "15"))
        self.health_timeout_s = float(os.getenv("MCP_HEALTH_TIMEOUT_SECONDS", "10"))
        self.clients: dict[str, HyruleMCPClient] = {}
        self.tools_by_source: dict[str, list[Any]] = {"hyrule": [], "xo": []}
        self.states: dict[str, MCPSourceState] = {
            "hyrule": MCPSourceState(source="hyrule"),
            "xo": MCPSourceState(source="xo"),
        }
        self._source_locks: dict[str, asyncio.Lock] = {
            "hyrule": asyncio.Lock(),
            "xo": asyncio.Lock(),
        }
        self.source_configs = {
            "hyrule": self._hyrule_config,
            "xo": self._xo_config,
        }

    async def connect_tools(self, agent: Any | None = None) -> None:
        if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
            log.info("mcp_runtime_disabled", owner=self.owner)
            return
        await self._connect_source(self._hyrule_config())
        await self._connect_source(self._xo_config())

    async def disconnect(self) -> None:
        for source in list(self.states):
            async with self._source_locks[source]:
                client = self.clients.pop(source, None)
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception as exc:
                        safe = classify_exception(exc)
                        log_exception("mcp_disconnect_failed", exc, category=safe.category, owner=self.owner, source=source)
                self.states[source].ready = False
                self.states[source].tool_count = 0
                self.states[source].error = None
                self.tools_by_source[source] = []

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

    async def live_health(self) -> dict[str, Any]:
        for source, config_factory in self.source_configs.items():
            try:
                await asyncio.wait_for(
                    self._refresh_source_health(source, config_factory),
                    timeout=self.health_timeout_s,
                )
            except TimeoutError as exc:
                await self._reset_timed_out_source(source)
                log_exception(
                    "mcp_health_timeout",
                    exc,
                    category="mcp_timeout",
                    owner=self.owner,
                    source=source,
                    timeout_seconds=self.health_timeout_s,
                )
            except Exception as exc:
                state = self.states[source]
                safe = classify_exception(exc)
                state.ready = False
                state.tool_count = 0
                state.error = safe.category
                self.tools_by_source[source] = []
                log_exception("mcp_health_failed", exc, category=safe.category, owner=self.owner, source=source)
        return self.health()

    async def _connect_source(self, config: dict[str, Any]) -> None:
        source = config["source"]
        async with self._source_locks[source]:
            await self._connect_source_unlocked(config)

    async def _connect_source_unlocked(self, config: dict[str, Any]) -> None:
        source = config["source"]
        state = self.states[source]
        client = HyruleMCPClient(config.get("command"), env=config.get("env"), url=config.get("url"))
        try:
            tool_count = await asyncio.wait_for(
                self._connect_and_register(client, source),
                timeout=self.connect_timeout_s,
            )
            state.ready = True
            state.tool_count = tool_count
            state.error = None
            log.info("mcp_tools_loaded", owner=self.owner, source=source, count=tool_count)
        except TimeoutError as exc:
            state.ready = False
            state.tool_count = 0
            state.error = "mcp_timeout"
            log_exception(
                "mcp_connect_timeout",
                exc,
                category="mcp_timeout",
                owner=self.owner,
                source=source,
                timeout_seconds=self.connect_timeout_s,
            )
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

    async def _refresh_source_health(self, source: str, config_factory: Any) -> None:
        async with self._source_locks[source]:
            state = self.states[source]
            client = self.clients.get(source)
            if client is None:
                if state.ready:
                    return
                try:
                    config = config_factory()
                except Exception as exc:
                    safe = classify_exception(exc)
                    state.ready = False
                    state.tool_count = 0
                    state.error = safe.category
                    self.tools_by_source[source] = []
                    log_exception("mcp_health_config_failed", exc, category=safe.category, owner=self.owner, source=source)
                    return
                await self._connect_source_unlocked(config)
                return

            try:
                tool_count = await client.check_health()
            except Exception as exc:
                snapshot = client.state_snapshot() if hasattr(client, "state_snapshot") else None
                safe = classify_exception(exc)
                state.ready = False
                state.tool_count = 0
                state.error = getattr(getattr(snapshot, "state", None), "value", None) or safe.category
                self.tools_by_source[source] = []
                log_exception("mcp_health_probe_failed", exc, category=safe.category, owner=self.owner, source=source)
                return

            state.tool_count = tool_count
            state.ready = tool_count > 0
            state.error = None

    async def _reset_timed_out_source(self, source: str) -> None:
        async with self._source_locks[source]:
            client = self.clients.pop(source, None)
            state = self.states[source]
            state.ready = False
            state.tool_count = 0
            state.error = "mcp_timeout"
            self.tools_by_source[source] = []

        if client is None:
            return

        try:
            force_disconnect = getattr(client, "force_disconnect", None)
            if force_disconnect is not None:
                await asyncio.wait_for(force_disconnect(), timeout=self.connect_timeout_s)
            else:
                await asyncio.wait_for(client.disconnect(), timeout=self.connect_timeout_s)
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("mcp_timeout_disconnect_failed", exc, category=safe.category, owner=self.owner, source=source)

    async def _connect_and_register(self, client: HyruleMCPClient, source: str) -> int:
        await client.connect()
        tools = await client.get_tools()
        self.tools_by_source[source] = tools
        for tool in tools:
            log.info("mcp_tool_loaded", owner=self.owner, source=source, name=tool.name)
        self.clients[source] = client
        return len(tools)

    def tools_for(self, specialist: str | None = None) -> list[Any]:
        hyrule = list(self.tools_by_source.get("hyrule", []))
        xo = list(self.tools_by_source.get("xo", []))
        names = _allowed_tool_names(specialist)
        return [tool for tool in [*hyrule, *xo] if getattr(tool, "name", "") in names]

    def toolsets_for(self, specialist: str | None = None) -> list[FunctionToolset]:
        tools = self.tools_for(specialist)
        if not tools:
            return []
        return [FunctionToolset(tools)]

    async def call_tool(self, source: str, name: str, arguments: dict[str, Any]) -> Any:
        client = self.clients.get(source)
        if client is None:
            raise RuntimeError(f"MCP source {source!r} is not connected")
        return await client._call_tool_with_reconnect(name, arguments)

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


TRIAGE_TOOLS = {
    "prometheus_list_targets",
    "icinga_get_host_state",
    "icinga_list_problems",
}
BGP_TOOLS = {
    "frr_vtysh_cmd",
    "path_explain",
    "ecmp_path_select",
    "multi_source_probe",
    "prometheus_query",
    "prometheus_list_targets",
    "os_service_status",
    "os_service_logs",
    "os_systemd_status",
    "os_rcctl_check",
    "socket_listeners",
}
FIREWALL_TOOLS = {
    "firewall_state",
    "pf_log_tail",
    "nft_log_tail",
    "tcpdump_capture",
    "ndp_state",
    "arp_state",
    "path_explain",
    "prometheus_query",
    "socket_listeners",
}
INFRASTRUCTURE_TOOLS = {
    "icinga_get_host_state",
    "icinga_list_problems",
    "prometheus_query",
    "prometheus_list_targets",
    "os_service_status",
    "os_service_logs",
    "os_systemd_status",
    "os_rcctl_check",
    "os_journalctl",
    "dmesg_tail",
    "service_restart_history",
    "socket_listeners",
    "vault_agent_status",
    "dns_dig",
    "dns_probe_burst",
    "knot_zone_status",
    "wg_show",
}


def _allowed_tool_names(specialist: str | None) -> set[str]:
    if specialist == "bgp":
        return BGP_TOOLS
    if specialist == "security_firewall":
        return FIREWALL_TOOLS
    if specialist == "infrastructure":
        return INFRASTRUCTURE_TOOLS
    return TRIAGE_TOOLS
