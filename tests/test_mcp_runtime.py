from types import SimpleNamespace

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
        self.session = object()

    async def get_tools(self):
        prefix = "xo" if self.url and "8766" in self.url else "hyrule"
        return [SimpleNamespace(name=f"{prefix}_tool", description="tool")]

    async def disconnect(self):
        self.disconnected = True
        self.session = None


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    FakeMCPClient.instances = []
    FakeMCPClient.fail_url = None
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
    assert [tool.name for tool in agent._function_toolset.tools] == ["hyrule_tool", "xo_tool"]
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
async def test_runtime_disconnect_closes_clients(monkeypatch):
    monkeypatch.delenv("NOC_AGENT_DISABLE_MCP", raising=False)
    monkeypatch.setenv("HYRULE_MCP_URL", "http://127.0.0.1:8765/mcp")
    monkeypatch.setenv("XO_MCP_URL", "http://127.0.0.1:8766/mcp")
    runtime = MCPRuntime(owner="test")

    await runtime.connect_tools(FakeAgent())
    await runtime.disconnect()

    assert all(client.disconnected for client in FakeMCPClient.instances)
    assert runtime.health()["status"] == "degraded"
