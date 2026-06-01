"""Regression tests for app/tools/mcp_client.py.

These tests exist because of a real incident: MCP tool inputSchemas were not
being forwarded to pydantic-ai, so the LLM received tools with no parameter
description and produced calls with invented argument names. pydantic-ai then
either rejected those calls as validation errors or the MCP server itself did,
which the agent surfaced to humans as "Unable to run ssh checks due to tool
validation errors".

The key invariants we lock down:

1. Each MCP tool's `inputSchema` is forwarded into the pydantic-ai
   `Tool.tool_def.parameters_json_schema` (so the model sees real field names).
2. The wrapped tool actually calls `session.call_tool` with the right name and
   the kwargs the model produced.
3. Tools with missing or non-object schemas register cleanly instead of
   crashing the whole MCP toolset load.
4. Errors inside the runner come back as text (so one broken tool can't crash
   the agent run).
"""

from types import SimpleNamespace
import asyncio

from anyio import BrokenResourceError, ClosedResourceError, EndOfStream
import pytest

from app.tools.mcp_client import (
    EMPTY_OBJECT_SCHEMA,
    HyruleMCPClient,
    _normalize_input_schema,
)


SSH_RUN_SCHEMA = {
    "type": "object",
    "properties": {
        "host": {"type": "string", "description": "Target host"},
        "command": {"type": "string", "description": "Command to run"},
    },
    "required": ["host", "command"],
    "additionalProperties": False,
}


def _make_mcp_tool(name="ssh_run_command", description="Run a command via SSH",
                   input_schema=None):
    if input_schema is None:
        input_schema = SSH_RUN_SCHEMA
    return SimpleNamespace(
        name=name,
        description=description,
        inputSchema=input_schema,
    )


def _make_text_block(text: str):
    return SimpleNamespace(text=text)


class _FakeSession:
    """Minimal stand-in for mcp.ClientSession.

    Records call_tool invocations and returns whatever was queued.
    """

    def __init__(
        self,
        response_text: str = "ok",
        raise_exc: Exception | None = None,
        raise_list_exc: Exception | None = None,
    ):
        self.calls: list[tuple[str, dict]] = []
        self._response_text = response_text
        self._raise_exc = raise_exc
        self._raise_list_exc = raise_list_exc

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self._raise_exc is not None:
            raise self._raise_exc
        return SimpleNamespace(content=[_make_text_block(self._response_text)])

    async def list_tools(self):
        if self._raise_list_exc is not None:
            raise self._raise_list_exc
        return SimpleNamespace(tools=[_make_mcp_tool()])


class _TaskOwnedTransport:
    instances = []

    def __init__(self):
        self.enter_task = None
        self.exit_task = None
        _TaskOwnedTransport.instances.append(self)

    async def __aenter__(self):
        self.enter_task = asyncio.current_task()
        return (object(), object())

    async def __aexit__(self, exc_type, exc, tb):
        self.exit_task = asyncio.current_task()
        if self.exit_task is not self.enter_task:
            raise RuntimeError("Attempted to exit cancel scope in a different task")


class _OwnerFakeSession:
    instances = []
    initialize_failures: list[BaseException] = []
    list_failures: list[BaseException] = []
    call_failures: list[BaseException] = []
    list_delay_s = 0.0
    call_delay_s = 0.0

    def __init__(self, read, write):
        self.read = read
        self.write = write
        self.calls: list[tuple[str, dict]] = []
        self.enter_task = None
        self.exit_task = None
        _OwnerFakeSession.instances.append(self)

    async def __aenter__(self):
        self.enter_task = asyncio.current_task()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exit_task = asyncio.current_task()
        if self.exit_task is not self.enter_task:
            raise RuntimeError("Attempted to exit cancel scope in a different task")

    async def initialize(self):
        if len(_OwnerFakeSession.instances) > 1 and _OwnerFakeSession.initialize_failures:
            raise _OwnerFakeSession.initialize_failures.pop(0)
        return None

    async def list_tools(self):
        if self.list_delay_s:
            await asyncio.sleep(self.list_delay_s)
        if _OwnerFakeSession.list_failures:
            raise _OwnerFakeSession.list_failures.pop(0)
        return SimpleNamespace(tools=[_make_mcp_tool()])

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.call_delay_s:
            await asyncio.sleep(self.call_delay_s)
        if _OwnerFakeSession.call_failures:
            raise _OwnerFakeSession.call_failures.pop(0)
        return SimpleNamespace(content=[_make_text_block(f"ok:{name}")])


@pytest.fixture
def owner_client_fakes(monkeypatch):
    _TaskOwnedTransport.instances = []
    _OwnerFakeSession.instances = []
    _OwnerFakeSession.initialize_failures = []
    _OwnerFakeSession.list_failures = []
    _OwnerFakeSession.call_failures = []
    _OwnerFakeSession.list_delay_s = 0.0
    _OwnerFakeSession.call_delay_s = 0.0
    monkeypatch.setattr("app.tools.mcp_client.streamablehttp_client", lambda url: _TaskOwnedTransport())
    monkeypatch.setattr("app.tools.mcp_client.ClientSession", _OwnerFakeSession)
    return _OwnerFakeSession


# --- _normalize_input_schema ---------------------------------------------------

def test_normalize_passes_through_object_schema():
    assert _normalize_input_schema(SSH_RUN_SCHEMA) == SSH_RUN_SCHEMA


def test_normalize_returns_empty_object_for_missing_schema():
    assert _normalize_input_schema(None) == EMPTY_OBJECT_SCHEMA
    assert _normalize_input_schema({}) == EMPTY_OBJECT_SCHEMA


def test_normalize_coerces_non_object_schema():
    """Some servers send a properties-only dict without `type: object`."""
    coerced = _normalize_input_schema({"properties": {"x": {"type": "string"}}})
    assert coerced["type"] == "object"
    assert coerced["properties"] == {"x": {"type": "string"}}


def test_normalize_does_not_mutate_input():
    """Library code shouldn't modify caller-owned dicts in place."""
    original = {"properties": {"x": {"type": "string"}}}
    snapshot = dict(original)
    _normalize_input_schema(original)
    assert original == snapshot


# --- _create_pydantic_tool: schema forwarding ---------------------------------

def test_tool_definition_carries_input_schema():
    """The MCP inputSchema must reach pydantic-ai's tool_def — this is the
    actual fix for the incident. If this regresses, the model goes back to
    inventing arg names and we get tool validation errors at runtime."""
    client = HyruleMCPClient(["dummy"])
    tool = client._create_pydantic_tool(_make_mcp_tool())

    schema = tool.tool_def.parameters_json_schema
    assert schema["type"] == "object"
    assert set(schema["properties"]) == {"host", "command"}
    assert schema["required"] == ["host", "command"]


def test_tool_definition_uses_mcp_name_and_description():
    client = HyruleMCPClient(["dummy"])
    tool = client._create_pydantic_tool(
        _make_mcp_tool(name="prom_query", description="Query Prometheus")
    )

    assert tool.name == "prom_query"
    assert tool.tool_def.name == "prom_query"
    assert tool.tool_def.description == "Query Prometheus"


def test_tool_does_not_take_run_context():
    """We pass takes_ctx=False because the MCP runner doesn't need RunContext.
    If this flips back to True, pydantic-ai will try to inject ctx and the
    function call will break with TypeError at runtime."""
    client = HyruleMCPClient(["dummy"])
    tool = client._create_pydantic_tool(_make_mcp_tool())
    assert tool.takes_ctx is False


def test_tool_with_missing_schema_still_registers():
    """One MCP server returning a malformed tool must not block all the others.
    Without normalization, Tool.from_schema raises on a non-object schema."""
    client = HyruleMCPClient(["dummy"])
    bad_tool = SimpleNamespace(name="busted", description="no schema", inputSchema=None)

    tool = client._create_pydantic_tool(bad_tool)
    assert tool.tool_def.parameters_json_schema["type"] == "object"


# --- _create_pydantic_tool: runner behavior -----------------------------------

@pytest.mark.asyncio
async def test_tool_runner_forwards_kwargs_to_session():
    """Whatever kwargs the model produces must be passed verbatim to
    session.call_tool under the original MCP tool name."""
    client = HyruleMCPClient(["dummy"])
    client.session = _FakeSession(response_text="exit 0\nhello")

    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="mail", command="systemctl status node_exporter")

    assert client.session.calls == [
        ("ssh_run_command", {"host": "mail", "command": "systemctl status node_exporter"})
    ]
    assert "hello" in result


@pytest.mark.asyncio
async def test_tool_runner_concatenates_all_text_blocks():
    client = HyruleMCPClient(["dummy"])

    class MultiBlockSession(_FakeSession):
        async def call_tool(self, name, arguments):
            self.calls.append((name, arguments))
            return SimpleNamespace(content=[
                _make_text_block("line1"),
                _make_text_block("line2"),
            ])

    client.session = MultiBlockSession()
    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")
    assert result == "line1\nline2"


@pytest.mark.asyncio
async def test_tool_runner_prefers_structured_mcp_content():
    client = HyruleMCPClient(["dummy"])

    class StructuredSession(_FakeSession):
        async def call_tool(self, name, arguments):
            self.calls.append((name, arguments))
            return SimpleNamespace(
                structuredContent={
                    "schema_version": "2026-05-15.v1",
                    "ok": False,
                    "tool": name,
                    "error_type": "unsupported_os",
                    "sanitized_error": "Use os_rcctl_check instead.",
                },
                content=[],
            )

    client.session = StructuredSession()
    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="mail", command="systemctl status smtpd")

    assert result["schema_version"] == "2026-05-15.v1"
    assert result["error_type"] == "unsupported_os"


@pytest.mark.asyncio
async def test_tool_runner_returns_default_when_no_text_blocks():
    client = HyruleMCPClient(["dummy"])

    class EmptySession(_FakeSession):
        async def call_tool(self, name, arguments):
            self.calls.append((name, arguments))
            return SimpleNamespace(content=[])

    client.session = EmptySession()
    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")
    assert result == "Executed successfully."


@pytest.mark.asyncio
async def test_tool_runner_swallows_exceptions_into_text():
    """An MCP server crashing mid-call must not surface as an unhandled
    exception inside the agent run — that would abort triage entirely."""
    client = HyruleMCPClient(["dummy"])
    client.session = _FakeSession(raise_exc=RuntimeError("connection reset"))

    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")

    assert "MCP tool execution failed" in result
    assert "connection reset" not in result


@pytest.mark.asyncio
async def test_tool_runner_reconnects_once_for_stale_session():
    client = HyruleMCPClient(["dummy"])
    stale = _FakeSession(raise_exc=RuntimeError("Session terminated"))
    fresh = _FakeSession(response_text="recovered")
    client.session = stale
    reconnects = 0

    async def reconnect():
        nonlocal reconnects
        reconnects += 1
        client.session = fresh

    client.reconnect = reconnect

    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")

    assert reconnects == 1
    assert stale.calls == [("ssh_run_command", {"host": "h", "command": "c"})]
    assert fresh.calls == [("ssh_run_command", {"host": "h", "command": "c"})]
    assert result == "recovered"


@pytest.mark.parametrize("stale_exc_type", [BrokenResourceError, ClosedResourceError, EndOfStream])
@pytest.mark.asyncio
async def test_tool_runner_reconnects_once_for_stale_session_errors(stale_exc_type):
    client = HyruleMCPClient(["dummy"])
    stale = _FakeSession(raise_exc=stale_exc_type())
    fresh = _FakeSession(response_text="recovered")
    client.session = stale
    reconnects = 0

    async def reconnect():
        nonlocal reconnects
        reconnects += 1
        client.session = fresh

    client.reconnect = reconnect

    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")

    assert reconnects == 1
    assert stale.calls == [("ssh_run_command", {"host": "h", "command": "c"})]
    assert fresh.calls == [("ssh_run_command", {"host": "h", "command": "c"})]
    assert result == "recovered"


@pytest.mark.asyncio
async def test_tool_runner_does_not_reconnect_for_non_stale_error():
    client = HyruleMCPClient(["dummy"])
    failing = _FakeSession(raise_exc=RuntimeError("some other error"))
    client.session = failing
    reconnects = 0

    async def reconnect():
        nonlocal reconnects
        reconnects += 1

    client.reconnect = reconnect

    tool = client._create_pydantic_tool(_make_mcp_tool())
    result = await tool.function(host="h", command="c")

    assert reconnects == 0
    assert failing.calls == [("ssh_run_command", {"host": "h", "command": "c"})]
    assert "MCP tool execution failed" in result


@pytest.mark.asyncio
async def test_check_health_uses_live_list_tools():
    client = HyruleMCPClient(["dummy"])
    client.session = _FakeSession()

    assert await client.check_health() == 1


@pytest.mark.parametrize("stale_exc_type", [BrokenResourceError, ClosedResourceError, EndOfStream])
@pytest.mark.asyncio
async def test_check_health_reconnects_once_for_stale_session(stale_exc_type):
    client = HyruleMCPClient(["dummy"])
    stale = _FakeSession(raise_list_exc=stale_exc_type())
    fresh = _FakeSession()
    client.session = stale
    reconnects = 0

    async def reconnect():
        nonlocal reconnects
        reconnects += 1
        client.session = fresh

    client.reconnect = reconnect

    assert await client.check_health() == 1
    assert reconnects == 1


# --- owner-task MCP session lifecycle -----------------------------------------

async def _in_new_task(awaitable_factory):
    task = asyncio.create_task(awaitable_factory())
    return await task


@pytest.mark.asyncio
async def test_owner_task_owns_connect_health_and_disconnect(owner_client_fakes):
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await _in_new_task(client.connect)
    assert await _in_new_task(client.check_health) == 1
    await _in_new_task(client.disconnect)

    assert _TaskOwnedTransport.instances
    assert all(transport.exit_task is transport.enter_task for transport in _TaskOwnedTransport.instances)
    assert all(session.exit_task is session.enter_task for session in owner_client_fakes.instances)


@pytest.mark.parametrize("stale_exc_type", [BrokenResourceError, ClosedResourceError, EndOfStream])
@pytest.mark.asyncio
async def test_owner_reconnects_and_retries_stale_tool_call(owner_client_fakes, stale_exc_type):
    owner_client_fakes.call_failures = [stale_exc_type()]
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await client.connect()
    result = await client._call_tool_with_reconnect("ssh_run_command", {"host": "h"})
    await client.disconnect()

    assert result == "ok:ssh_run_command"
    assert len(owner_client_fakes.instances) == 2
    assert owner_client_fakes.instances[0].calls == [("ssh_run_command", {"host": "h"})]
    assert owner_client_fakes.instances[1].calls == [("ssh_run_command", {"host": "h"})]


@pytest.mark.asyncio
async def test_owner_backoff_throttles_repeated_health_checks(monkeypatch, owner_client_fakes):
    owner_client_fakes.list_failures = [ClosedResourceError()]
    owner_client_fakes.initialize_failures = [RuntimeError("still down")]
    monkeypatch.setattr("app.tools.mcp_client.random.uniform", lambda low, high: 0)
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await client.connect()
    with pytest.raises(RuntimeError, match="still down"):
        await client.check_health()

    before = len(owner_client_fakes.instances)
    with pytest.raises(RuntimeError, match="reconnect backoff active"):
        await client.check_health()
    await client.disconnect()

    assert len(owner_client_fakes.instances) == before


@pytest.mark.asyncio
async def test_caller_timeout_abandons_hung_owner_operation(monkeypatch, owner_client_fakes):
    owner_client_fakes.list_delay_s = 0.05
    monkeypatch.setenv("MCP_HEALTH_TIMEOUT_SECONDS", "0.01")
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await client.connect()
    try:
        with pytest.raises(TimeoutError):
            await client.check_health()
        client.health_timeout_s = 1
        owner_client_fakes.list_delay_s = 0.0
        assert await client.check_health() == 1
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_concurrent_owner_operations_receive_own_results(owner_client_fakes):
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await client.connect()
    results = await asyncio.gather(
        client._call_tool_with_reconnect("first", {"n": 1}),
        client._call_tool_with_reconnect("second", {"n": 2}),
        client.check_health(),
    )
    await client.disconnect()

    assert results == ["ok:first", "ok:second", 1]


@pytest.mark.asyncio
async def test_shutdown_fails_pending_owner_operations(monkeypatch, owner_client_fakes):
    owner_client_fakes.call_delay_s = 0.05
    monkeypatch.setenv("MCP_OPERATION_TIMEOUT_SECONDS", "1")
    client = HyruleMCPClient(url="http://mcp.test/mcp")

    await client.connect()
    first = asyncio.create_task(client._call_tool_with_reconnect("slow", {}))
    await asyncio.sleep(0)
    shutdown = asyncio.create_task(client.disconnect())
    await asyncio.sleep(0)
    pending = [asyncio.create_task(client.check_health()) for _ in range(3)]
    await shutdown

    assert await first == "ok:slow"
    for task in pending:
        with pytest.raises(ClosedResourceError):
            await task


# --- get_tools wiring ---------------------------------------------------------

@pytest.mark.asyncio
async def test_get_tools_wraps_each_mcp_tool():
    client = HyruleMCPClient(["dummy"])

    class FakeListSession:
        def __init__(self):
            self._tools = [
                _make_mcp_tool(name="prom_query", input_schema={
                    "type": "object",
                    "properties": {"q": {"type": "string"}},
                    "required": ["q"],
                }),
                _make_mcp_tool(name="ssh_run_command"),
            ]

        async def list_tools(self):
            return SimpleNamespace(tools=self._tools)

    client.session = FakeListSession()
    tools = await client.get_tools()

    assert [t.name for t in tools] == ["prom_query", "ssh_run_command"]
    # Schema for the first tool was distinct — make sure it didn't get
    # cross-contaminated with the second tool's schema.
    assert tools[0].tool_def.parameters_json_schema["properties"] == {"q": {"type": "string"}}
    assert "host" in tools[1].tool_def.parameters_json_schema["properties"]


@pytest.mark.asyncio
async def test_get_tools_raises_when_not_connected():
    client = HyruleMCPClient(["dummy"])
    with pytest.raises(RuntimeError, match="Not connected"):
        await client.get_tools()


# --- End-to-end agent integration --------------------------------------------

@pytest.mark.asyncio
async def test_agent_calls_wrapped_tool_with_schema_field_names():
    """End-to-end check: an MCP tool wrapped through our client and registered
    on a pydantic-ai agent must reach the model with its real schema, so the
    model produces tool call args using the right field names.

    pydantic-ai's TestModel synthesizes tool call args from the registered
    JSON schema. If the schema is forwarded correctly, the call to the MCP
    session will carry kwargs named after the schema's properties. If the
    schema is dropped (the pre-fix behavior), TestModel sends `{}` and the
    real MCP server would reject the call as a validation error.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    client = HyruleMCPClient(["dummy"])
    client.session = _FakeSession(response_text="{}")
    tool = client._create_pydantic_tool(_make_mcp_tool())

    agent = Agent("test", tools=[tool], output_type=str)
    await agent.run("investigate", model=TestModel())

    assert client.session.calls, "Agent never invoked the wrapped MCP tool"
    name, kwargs = client.session.calls[0]
    assert name == "ssh_run_command"
    assert set(kwargs) == {"host", "command"}, (
        f"Expected schema-driven kwargs, got {kwargs!r}. "
        "If this is empty, the inputSchema is no longer being forwarded "
        "to pydantic-ai and the model is calling tools blind."
    )
