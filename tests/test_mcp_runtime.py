from types import SimpleNamespace
import asyncio

import pytest

from app.mcp_runtime import MCPRuntime


class FakeToolset:
    def __init__(self):
        self.tools = []

    def add_tool(self, tool):
        self.tools.append(tool)


class FakeAgent:
    def __init__(self):
        self._function_toolset = FakeToolset()


class FakeMCPClient:
    instances = []
    fail_url = None
    hang_url = None
    health_delay_s = 0.0
    active_health_by_url = {}
    max_active_health_by_url = {}

    def __init__(self, command=None, env=None, url=None):
        self.command = command or []
        self.env = env
        self.url = url
        self.session = None
        self.disconnected = False
        FakeMCPClient.instances.append(self)

    async def connect(self):
        if self.url == FakeMCPClient.fail_url:
            raise RuntimeError("connect failed")
        if self.url == FakeMCPClient.hang_url:
            await asyncio.sleep(10)
        self.session = object()

    async def get_tools(self):
        prefix = "xo" if self.url and "8766" in self.url else "hyrule"
        return [SimpleNamespace(name=f"{prefix}_tool", description="tool")]

    async def check_health(self):
        if self.url == FakeMCPClient.fail_url:
            raise RuntimeError("health failed")
        if FakeMCPClient.health_delay_s:
            key = self.url or "stdio"
            FakeMCPClient.active_health_by_url[key] = FakeMCPClient.active_health_by_url.get(key, 0) + 1
            FakeMCPClient.max_active_health_by_url[key] = max(
                FakeMCPClient.max_active_health_by_url.get(key, 0),
                FakeMCPClient.active_health_by_url[key],
            )
            try:
                await asyncio.sleep(FakeMCPClient.health_delay_s)
            finally:
                FakeMCPClient.active_health_by_url[key] -= 1
        prefix = "xo" if self.url and "8766" in self.url else "hyrule"
        return len([SimpleNamespace(name=f"{prefix}_tool", description="tool")])

    async def disconnect(self):
        self.disconnected = True
        self.session = None

    async def force_disconnect(self):
        await self.disconnect()


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    FakeMCPClient.instances = []
    FakeMCPClient.fail_url = None
    FakeMCPClient.hang_url = None
    FakeMCPClient.health_delay_s = 0.0
    FakeMCPClient.active_health_by_url = {}
    FakeMCPClient.max_active_health_by_url = {}
    monkeypatch.setattr("app.mcp_runtime.HyruleMCPClient", FakeMCPClient)


@pytest.mark.asyncio
async def test_runtime_loads_hyrule_and_xo_from_urls(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    agent = FakeAgent()
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(agent)

    assert [client.url for client in FakeMCPClient.instances] == [
        "http://127.0.0.1:8765/mcp",
        "http://127.0.0.1:8766/mcp",
    ]
    assert [tool.name for tool in runtime.tools_by_source["hyrule"]] == ["hyrule_tool"]
    assert [tool.name for tool in runtime.tools_by_source["xo"]] == ["xo_tool"]
    assert runtime.health()["status"] == "ok"


@pytest.mark.asyncio
async def test_runtime_falls_back_to_stdio_when_url_absent(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.delenv("HYRULE_MCP_URL", raising=False)
    monkeypatch.delenv("XO_MCP_URL", raising=False)
    monkeypatch.setenv("HYRULE_MCP_CMD", "python mcp_server.py")
    monkeypatch.setenv("XO_MCP_CMD", "npx -y @xen-orchestra/mcp")
    agent = FakeAgent()
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(agent)

    assert FakeMCPClient.instances[0].command == ["python", "mcp_server.py"]
    assert FakeMCPClient.instances[1].command == ["npx", "-y", "@xen-orchestra/mcp"]
    assert FakeMCPClient.instances[1].env["XO_MCP_ENABLE_ACTIONS"] == "0"


@pytest.mark.asyncio
async def test_runtime_marks_failed_source_degraded(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    FakeMCPClient.fail_url = "http://127.0.0.1:8766/mcp"
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    health = runtime.health()

    assert health["hyrule"] is True
    assert health["xo"] is False
    assert health["xo_tool_count"] == 0
    assert health["status"] == "degraded"


@pytest.mark.asyncio
async def test_runtime_live_health_marks_all_sources_ok(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    FakeMCPClient.fail_url = None
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    health = await runtime.live_health()

    assert health["hyrule"] is True
    assert health["xo"] is True
    assert health["sources"]["hyrule"]["ready"] is True
    assert health["sources"]["xo"]["ready"] is True
    assert health["status"] == "ok"


@pytest.mark.asyncio
async def test_runtime_live_health_marks_stale_source_degraded(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    FakeMCPClient.fail_url = "http://127.0.0.1:8765/mcp"
    health = await runtime.live_health()

    assert health["hyrule"] is False
    assert health["sources"]["hyrule"]["error"] == "unknown_infrastructure"
    assert health["sources"]["hyrule"]["tool_count"] == 0
    assert health["status"] == "degraded"


@pytest.mark.asyncio
async def test_runtime_live_health_updates_source_state_across_calls(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    hyrule_url = "http://127.0.0.1:8765/mcp"
    xo_url = "http://127.0.0.1:8766/mcp"
    monkeypatch.setenv("HYRULE_MCP_URL", hyrule_url)
    monkeypatch.setenv("XO_MCP_URL", xo_url)
    FakeMCPClient.fail_url = None
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    health = await runtime.live_health()

    assert health["status"] == "ok"
    assert health["sources"]["hyrule"]["ready"] is True
    assert health["sources"]["hyrule"]["tool_count"] == 1
    assert health["sources"]["xo"]["ready"] is True
    assert health["sources"]["xo"]["tool_count"] == 1

    FakeMCPClient.fail_url = xo_url
    health = await runtime.live_health()

    assert health["status"] == "degraded"
    assert health["sources"]["hyrule"]["ready"] is True
    assert health["sources"]["hyrule"]["tool_count"] == 1
    assert health["sources"]["xo"]["ready"] is False
    assert health["sources"]["xo"]["tool_count"] == 0


@pytest.mark.asyncio
async def test_runtime_live_health_reconnects_missing_source(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    xo_url = "http://127.0.0.1:8766/mcp"
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", xo_url)
    previous_fail_url = FakeMCPClient.fail_url
    runtime = MCPRuntime(owner="test")
    try:
        FakeMCPClient.fail_url = xo_url

        await runtime.connect_tools(FakeAgent())
        health = runtime.health()
        assert health["xo"] is False
        assert "xo" not in runtime.clients

        FakeMCPClient.fail_url = None
        health = await runtime.live_health()

        assert health["xo"] is True
        assert health["sources"]["xo"]["ready"] is True
        assert health["sources"]["xo"]["tool_count"] == 1
        assert "xo" in runtime.clients
    finally:
        FakeMCPClient.fail_url = previous_fail_url


@pytest.mark.asyncio
async def test_runtime_concurrent_live_health_serializes_each_source(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    FakeMCPClient.health_delay_s = 0.02
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    results = await asyncio.gather(runtime.live_health(), runtime.live_health(), runtime.live_health())

    assert all(result["status"] == "ok" for result in results)
    assert FakeMCPClient.max_active_health_by_url["http://127.0.0.1:8765/mcp"] == 1
    assert FakeMCPClient.max_active_health_by_url["http://127.0.0.1:8766/mcp"] == 1


@pytest.mark.asyncio
async def test_runtime_live_health_times_out_stale_probe(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    monkeypatch.setenv("MCP_HEALTH_TIMEOUT_SECONDS", "0.05")
    FakeMCPClient.health_delay_s = 1
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    health = await runtime.live_health()

    assert health["status"] == "degraded"
    assert health["sources"]["hyrule"]["error"] == "mcp_timeout"
    assert health["sources"]["xo"]["error"] == "mcp_timeout"
    assert runtime.clients == {}
    assert all(client.disconnected for client in FakeMCPClient.instances)

    FakeMCPClient.health_delay_s = 0
    await runtime.connect_tools(FakeAgent())
    health = await runtime.live_health()

    assert health["status"] == "ok"
    assert health["sources"]["hyrule"]["tool_count"] == 1
    assert health["sources"]["xo"]["tool_count"] == 1


@pytest.mark.asyncio
async def test_runtime_health_preserves_ready_state_without_client():
    runtime = MCPRuntime(owner="test")
    runtime.states["hyrule"].ready = True
    runtime.states["hyrule"].tool_count = 2
    runtime.states["xo"].ready = True
    runtime.states["xo"].tool_count = 3

    health = runtime.health()

    assert health["hyrule"] is True
    assert health["xo"] is True
    assert health["sources"]["hyrule"]["tool_count"] == 2
    assert health["sources"]["xo"]["tool_count"] == 3
    assert runtime.clients == {}


def test_infrastructure_toolset_includes_read_only_freebsd_tools_but_not_restart():
    runtime = MCPRuntime(owner="test")
    runtime.tools_by_source["hyrule"] = [
        SimpleNamespace(name="os_service_status"),
        SimpleNamespace(name="os_service_logs"),
        SimpleNamespace(name="socket_listeners"),
        SimpleNamespace(name="os_service_restart"),
    ]

    names = {tool.name for tool in runtime.tools_for("infrastructure")}

    assert {"os_service_status", "os_service_logs", "socket_listeners"} <= names
    assert "os_service_restart" not in names


@pytest.mark.asyncio
async def test_runtime_times_out_stalled_source(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    monkeypatch.setenv("MCP_CONNECT_TIMEOUT_SECONDS", "0.01")
    FakeMCPClient.hang_url = "http://127.0.0.1:8766/mcp"
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    health = runtime.health()

    assert health["hyrule"] is True
    assert health["xo"] is False
    assert health["sources"]["xo"]["error"] == "mcp_timeout"


@pytest.mark.asyncio
async def test_runtime_disconnect_closes_clients(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    await runtime.disconnect()

    assert all(client.disconnected for client in FakeMCPClient.instances)
    assert runtime.health()["status"] == "degraded"
