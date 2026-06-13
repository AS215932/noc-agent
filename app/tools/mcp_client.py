import os
import json
import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable
from contextlib import AsyncExitStack
import anyio
from anyio import BrokenResourceError, ClosedResourceError, EndOfStream, WouldBlock
from mcp.client.stdio import stdio_client
from mcp.client.stdio import StdioServerParameters
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.session import ClientSession
from pydantic_ai.tools import Tool

from app.safe_errors import classify_exception, log_exception

EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


class MCPCommandType(str, Enum):
    CONNECT = "connect"
    LIST_TOOLS = "list_tools"
    CALL_TOOL = "call_tool"
    HEALTH = "health"
    RECONNECT = "reconnect"
    SHUTDOWN = "shutdown"


class MCPClientState(str, Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DEGRADED = "degraded"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True)
class MCPClientSnapshot:
    state: MCPClientState
    tool_count: int = 0
    error: str | None = None
    next_reconnect_at: float | None = None
    reconnect_backoff_s: float | None = None


@dataclass
class CommandResult:
    value: Any = None
    error: BaseException | None = None


@dataclass
class MCPCommand:
    type: MCPCommandType
    reply_send: Any
    name: str | None = None
    arguments: dict[str, Any] | None = None


class MCPClientDegradedError(RuntimeError):
    pass


def _normalize_input_schema(raw: Any) -> dict[str, Any]:
    """Coerce an MCP tool's inputSchema into something pydantic-ai accepts.

    pydantic-ai's `Tool.from_schema` requires an object-typed JSON schema.
    Some MCP servers omit the schema entirely or send one without an explicit
    `type: object`; falling back to an empty object schema keeps the tool
    callable instead of crashing tool registration.
    """
    if not isinstance(raw, dict) or not raw:
        return dict(EMPTY_OBJECT_SCHEMA)
    schema = dict(raw)
    if schema.get("type") != "object" and "$ref" not in schema:
        schema["type"] = "object"
        schema.setdefault("properties", {})
    return schema


class HyruleMCPClient:
    def __init__(self, command: list[str] | None = None, env: dict[str, str] | None = None, url: str | None = None):
        self.command = command or []
        self.env = env
        self.url = url
        self.session: ClientSession | None = None
        self._exit_stack = AsyncExitStack()
        self._reconnect_lock = asyncio.Lock()
        self.connect_timeout_s = float(os.getenv("MCP_CONNECT_TIMEOUT_SECONDS", "15"))
        self.health_timeout_s = float(os.getenv("MCP_HEALTH_TIMEOUT_SECONDS", "10"))
        self.operation_timeout_s = float(os.getenv("MCP_OPERATION_TIMEOUT_SECONDS", "30"))
        self.shutdown_timeout_s = float(os.getenv("MCP_SHUTDOWN_TIMEOUT_SECONDS", "5"))
        self._command_send: Any | None = None
        self._owner_task: asyncio.Task | None = None
        self._snapshot = MCPClientSnapshot(state=MCPClientState.SHUTDOWN)
        self._reconnect_failures = 0

    async def connect(self):
        """Starts the MCP server process and initializes the ClientSession."""
        if self._owner_running():
            return
        send, receive = anyio.create_memory_object_stream(100)
        self._command_send = send
        self._owner_task = asyncio.create_task(self._owner_loop(receive))
        try:
            await self._send_command(MCPCommandType.CONNECT, timeout_s=self.connect_timeout_s)
        except Exception:
            await self.disconnect()
            raise

    async def disconnect(self):
        """Closes the MCP server connection."""
        if self._owner_running():
            send = self._command_send
            task = self._owner_task
            try:
                await self._send_command(MCPCommandType.SHUTDOWN, timeout_s=self.shutdown_timeout_s)
            except TimeoutError:
                if task is not None:
                    task.cancel()
            finally:
                if send is not None:
                    await send.aclose()
                if task is not None:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                self._command_send = None
                self._owner_task = None
                self.session = None
                self._exit_stack = AsyncExitStack()
            return

        await self._exit_stack.aclose()
        self.session = None
        self._exit_stack = AsyncExitStack()

    async def force_disconnect(self) -> None:
        """Best-effort teardown for a stuck owner task.

        This path is used after a health-probe timeout. It must not raise:
        the runtime has already marked the source degraded and removed the
        client, and any cleanup exception would only obscure recovery. Some
        anyio stream contexts can raise RuntimeError during cancellation if
        their cancel scope is unwound from a different task; treat that the
        same as a cancelled owner task and allow the next health check to
        create a fresh client/session.
        """
        send = self._command_send
        task = self._owner_task
        self._command_send = None
        if send is not None:
            try:
                await send.aclose()
            except Exception:
                pass
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            try:
                await task
            except (asyncio.CancelledError, RuntimeError):
                pass
            except Exception:
                pass
        self._owner_task = None
        self.session = None
        self._exit_stack = AsyncExitStack()
        self._set_snapshot(MCPClientState.SHUTDOWN, error=None)

    async def reconnect(self) -> None:
        if self._owner_running():
            await self._send_command(MCPCommandType.RECONNECT, timeout_s=self.operation_timeout_s)
            return
        async with self._reconnect_lock:
            await self.disconnect()
            await self.connect()

    async def check_health(self) -> int:
        """Verify the current session can make a live MCP request."""
        if self._owner_running():
            return await self._send_command(MCPCommandType.HEALTH, timeout_s=self.health_timeout_s)

        async def list_tool_count() -> int:
            if not self.session:
                raise RuntimeError("Not connected to MCP server")
            response = await self.session.list_tools()
            return len(response.tools)

        return await self._run_with_reconnect_once(list_tool_count, stale_event="mcp_health_session_stale")

    async def get_tools(self) -> list[Tool]:
        """
        Retrieves tools from the MCP server and wraps them as PydanticAI Tools.
        """
        if self._owner_running():
            mcp_tools = await self._send_command(MCPCommandType.LIST_TOOLS, timeout_s=self.operation_timeout_s)
            return [self._create_pydantic_tool(t) for t in mcp_tools]

        if not self.session:
            raise RuntimeError("Not connected to MCP server")

        mcp_tools_response = await self.session.list_tools()
        return [self._create_pydantic_tool(t) for t in mcp_tools_response.tools]

    def _create_pydantic_tool(self, mcp_tool: Any) -> Tool:
        async def tool_runner(**kwargs: Any) -> Any:
            try:
                return await self._call_tool_with_reconnect(mcp_tool.name, kwargs)
            except Exception as e:
                safe = classify_exception(e)
                log_exception("mcp_tool_execution_failed", e, category=safe.category, tool=mcp_tool.name)
                return "MCP tool execution failed because a diagnostic backend is unavailable. Ask the operator to verify `/health/mcp`."

        tool_runner.__name__ = mcp_tool.name
        tool_runner.__doc__ = mcp_tool.description

        json_schema = _normalize_input_schema(getattr(mcp_tool, "inputSchema", None))

        return Tool.from_schema(
            function=tool_runner,
            name=mcp_tool.name,
            description=mcp_tool.description,
            json_schema=json_schema,
            takes_ctx=False,
        )

    async def _call_tool_with_reconnect(self, name: str, arguments: dict[str, Any]) -> Any:
        if self._owner_running():
            return await self._send_command(
                MCPCommandType.CALL_TOOL,
                name=name,
                arguments=arguments,
                timeout_s=self.operation_timeout_s,
            )
        return await self._run_with_reconnect_once(
            lambda: self._call_tool_once(name, arguments),
            stale_event="mcp_tool_session_stale",
            tool=name,
        )

    async def _run_with_reconnect_once(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        stale_event: str,
        **log_context: Any,
    ) -> Any:
        try:
            return await operation()
        except Exception as exc:
            if not _looks_like_stale_session(exc):
                raise
            log_exception(stale_event, exc, category="mcp_session_stale", **log_context)
            await self.reconnect()
            return await operation()

    async def _call_tool_once(self, name: str, arguments: dict[str, Any]) -> Any:
        if not self.session:
            raise RuntimeError("Not connected to MCP server")
        result = await self.session.call_tool(name, arguments=arguments)
        structured = getattr(result, "structuredContent", None) or getattr(result, "structured_content", None)
        if structured is not None:
            return _json_safe(structured)
        out = ""
        for block in result.content:
            block_structured = getattr(block, "structuredContent", None) or getattr(block, "structured_content", None)
            if block_structured is not None:
                return _json_safe(block_structured)
            if hasattr(block, "text"):
                out += block.text + "\n"
        return out.strip() if out else "Executed successfully."

    def state_snapshot(self) -> MCPClientSnapshot:
        return self._snapshot

    def _owner_running(self) -> bool:
        return self._owner_task is not None and not self._owner_task.done() and self._command_send is not None

    async def _send_command(
        self,
        command_type: MCPCommandType,
        *,
        name: str | None = None,
        arguments: dict[str, Any] | None = None,
        timeout_s: float,
    ) -> Any:
        if self._command_send is None:
            raise RuntimeError("Not connected to MCP server")
        reply_send, reply_receive = anyio.create_memory_object_stream(1)
        command = MCPCommand(command_type, reply_send=reply_send, name=name, arguments=arguments)
        async with reply_receive:
            with anyio.fail_after(timeout_s):
                await self._command_send.send(command)
                result: CommandResult = await reply_receive.receive()
        if result.error is not None:
            raise result.error
        return result.value

    async def _owner_loop(self, receive: Any) -> None:
        try:
            async with receive:
                while True:
                    try:
                        command = await receive.receive()
                    except EndOfStream:
                        break

                    if command.type is MCPCommandType.SHUTDOWN:
                        self._command_send = None
                        self._set_snapshot(MCPClientState.SHUTDOWN, error=None)
                        await self._reply(command, value=None)
                        await self._drain_pending(receive)
                        break

                    await self._handle_owner_command(command)
        finally:
            self._set_snapshot(MCPClientState.SHUTDOWN, error=None)
            with anyio.CancelScope(shield=True):
                await self._close_owner_session()

    async def _handle_owner_command(self, command: MCPCommand) -> None:
        try:
            if command.type is MCPCommandType.CONNECT:
                value = await self._owner_connect(initial=True)
            elif command.type is MCPCommandType.LIST_TOOLS:
                value = await self._owner_list_tools()
            elif command.type is MCPCommandType.CALL_TOOL:
                value = await self._owner_call_tool(command.name or "", command.arguments or {})
            elif command.type is MCPCommandType.HEALTH:
                value = await self._owner_check_health()
            elif command.type is MCPCommandType.RECONNECT:
                value = await self._owner_connect(initial=False)
            else:
                raise ClosedResourceError()
        except Exception as exc:
            await self._reply(command, error=exc)
        else:
            await self._reply(command, value=value)

    async def _reply(self, command: MCPCommand, *, value: Any = None, error: BaseException | None = None) -> None:
        try:
            async with command.reply_send:
                await command.reply_send.send(CommandResult(value=value, error=error))
        except (BrokenResourceError, ClosedResourceError):
            return

    async def _drain_pending(self, receive: Any) -> None:
        while True:
            try:
                pending = receive.receive_nowait()
            except WouldBlock:
                return
            except EndOfStream:
                return
            await self._reply(pending, error=ClosedResourceError())

    async def _owner_connect(self, *, initial: bool) -> None:
        self._set_snapshot(MCPClientState.CONNECTING if initial else MCPClientState.RECONNECTING, error=None)
        await self._close_owner_session()
        stack = AsyncExitStack()
        try:
            if self.url:
                http_transport = await stack.enter_async_context(streamablehttp_client(self.url))
                read, write = http_transport[0], http_transport[1]
            else:
                server_params = StdioServerParameters(
                    command=self.command[0],
                    args=self.command[1:],
                    env=self.env if self.env is not None else os.environ.copy(),
                )
                stdio_transport = await stack.enter_async_context(stdio_client(server_params))
                read, write = stdio_transport
            self.session = await stack.enter_async_context(ClientSession(read, write))
            await self.session.initialize()
        except Exception as exc:
            with anyio.CancelScope(shield=True):
                await stack.aclose()
            self.session = None
            self._schedule_reconnect_backoff(exc)
            raise

        self._exit_stack = stack
        self._reconnect_failures = 0
        self._set_snapshot(MCPClientState.CONNECTED, tool_count=self._snapshot.tool_count, error=None)

    async def _close_owner_session(self) -> None:
        stack = self._exit_stack
        self._exit_stack = AsyncExitStack()
        self.session = None
        await stack.aclose()

    async def _owner_list_tools(self) -> list[Any]:
        response = await self._owner_run_with_reconnect_once(
            self._owner_list_tools_once,
            stale_event="mcp_list_tools_session_stale",
        )
        tools = list(response.tools)
        self._set_snapshot(MCPClientState.CONNECTED, tool_count=len(tools), error=None)
        return tools

    async def _owner_check_health(self) -> int:
        response = await self._owner_run_with_reconnect_once(
            self._owner_list_tools_once,
            stale_event="mcp_health_session_stale",
        )
        tool_count = len(response.tools)
        self._set_snapshot(MCPClientState.CONNECTED, tool_count=tool_count, error=None)
        return tool_count

    async def _owner_call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        return await self._owner_run_with_reconnect_once(
            lambda: self._owner_call_tool_once(name, arguments),
            stale_event="mcp_tool_session_stale",
            tool=name,
        )

    async def _owner_list_tools_once(self) -> Any:
        if not self.session:
            await self._owner_reconnect_or_raise()
        if not self.session:
            raise RuntimeError("Not connected to MCP server")
        return await self.session.list_tools()

    async def _owner_call_tool_once(self, name: str, arguments: dict[str, Any]) -> Any:
        if not self.session:
            await self._owner_reconnect_or_raise()
        return await self._call_tool_once(name, arguments)

    async def _owner_run_with_reconnect_once(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        stale_event: str,
        **log_context: Any,
    ) -> Any:
        self._raise_if_backoff_active()
        try:
            return await operation()
        except Exception as exc:
            if not _looks_like_stale_session(exc):
                raise
            log_exception(stale_event, exc, category="mcp_session_stale", **log_context)
            await self._owner_reconnect_or_raise()
            return await operation()

    async def _owner_reconnect_or_raise(self) -> None:
        self._raise_if_backoff_active()
        await self._owner_connect(initial=False)

    def _raise_if_backoff_active(self) -> None:
        next_at = self._snapshot.next_reconnect_at
        if self._snapshot.state is MCPClientState.DEGRADED and next_at is not None:
            remaining = next_at - time.monotonic()
            if remaining > 0:
                raise MCPClientDegradedError(f"MCP client degraded; reconnect backoff active for {remaining:.2f}s")

    def _schedule_reconnect_backoff(self, exc: BaseException) -> None:
        self._reconnect_failures += 1
        base = min(0.5 * (2 ** (self._reconnect_failures - 1)), 10.0)
        delay = min(base + random.uniform(0, min(base, 1.0)), 10.0)
        self._set_snapshot(
            MCPClientState.DEGRADED,
            tool_count=0,
            error=type(exc).__name__,
            next_reconnect_at=time.monotonic() + delay,
            reconnect_backoff_s=delay,
        )

    def _set_snapshot(
        self,
        state: MCPClientState,
        *,
        tool_count: int | None = None,
        error: str | None = None,
        next_reconnect_at: float | None = None,
        reconnect_backoff_s: float | None = None,
    ) -> None:
        self._snapshot = MCPClientSnapshot(
            state=state,
            tool_count=self._snapshot.tool_count if tool_count is None else tool_count,
            error=error,
            next_reconnect_at=next_reconnect_at,
            reconnect_backoff_s=reconnect_backoff_s,
        )


def _looks_like_stale_session(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenResourceError, ClosedResourceError, EndOfStream)):
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "session terminated",
            "not connected to mcp server",
        )
    )


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return json.loads(json.dumps(value, default=str))
