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
        prefix = "xo" if self.url and "8766" in self.url else "hyrule"
        return len([SimpleNamespace(name=f"{prefix}_tool", description="tool")])

    async def disconnect(self):
        self.disconnected = True
        self.session = None


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    FakeMCPClient.instances = []
    FakeMCPClient.fail_url = None
    FakeMCPClient.hang_url = None
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
    FakeMCPClient.fail_url = xo_url
    runtime = MCPRuntime(owner="test")

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
