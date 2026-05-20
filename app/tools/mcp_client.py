import os
import json
import asyncio
from typing import Any
from contextlib import AsyncExitStack
from mcp.client.stdio import stdio_client
from mcp.client.stdio import StdioServerParameters
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.session import ClientSession
from pydantic_ai.tools import Tool

from app.safe_errors import classify_exception, log_exception

EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


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

    async def connect(self):
        """Starts the MCP server process and initializes the ClientSession."""
        if self.url:
            http_transport = await self._exit_stack.enter_async_context(streamablehttp_client(self.url))
            read, write = http_transport[0], http_transport[1]
        else:
            server_params = StdioServerParameters(
                command=self.command[0],
                args=self.command[1:],
                env=self.env if self.env is not None else os.environ.copy(),
            )
            stdio_transport = await self._exit_stack.enter_async_context(stdio_client(server_params))
            read, write = stdio_transport

        self.session = await self._exit_stack.enter_async_context(ClientSession(read, write))
        await self.session.initialize()

    async def disconnect(self):
        """Closes the MCP server connection."""
        await self._exit_stack.aclose()
        self.session = None
        self._exit_stack = AsyncExitStack()

    async def reconnect(self) -> None:
        async with self._reconnect_lock:
            await self.disconnect()
            await self.connect()

    async def check_health(self) -> int:
        """Verify the current session can make a live MCP request."""
        if not self.session:
            raise RuntimeError("Not connected to MCP server")
        response = await self.session.list_tools()
        return len(response.tools)

    async def get_tools(self) -> list[Tool]:
        """
        Retrieves tools from the MCP server and wraps them as PydanticAI Tools.
        """
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
        try:
            return await self._call_tool_once(name, arguments)
        except Exception as exc:
            if not _looks_like_stale_session(exc):
                raise
            log_exception("mcp_tool_session_stale", exc, category="mcp_session_stale", tool=name)
            await self.reconnect()
            return await self._call_tool_once(name, arguments)

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


def _looks_like_stale_session(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "session terminated",
            "client session",
            "connection reset",
            "connection closed",
            "closed resource",
            "not connected",
        )
    )


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return json.loads(json.dumps(value, default=str))
