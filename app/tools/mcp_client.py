import os
from typing import Any
from contextlib import AsyncExitStack
from mcp.client.stdio import stdio_client
from mcp.client.stdio import StdioServerParameters
from mcp.client.session import ClientSession
from pydantic_ai.tools import Tool

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
    def __init__(self, command: list[str], env: dict[str, str] | None = None):
        self.command = command
        self.env = env
        self.session: ClientSession | None = None
        self._exit_stack = AsyncExitStack()

    async def connect(self):
        """Starts the MCP server process and initializes the ClientSession."""
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
                result = await self.session.call_tool(mcp_tool.name, arguments=kwargs)
                out = ""
                for block in result.content:
                    if hasattr(block, "text"):
                        out += block.text + "\n"
                return out.strip() if out else "Executed successfully."
            except Exception as e:
                return f"MCP tool execution failed: {str(e)}"

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
