import os
from typing import Any
from contextlib import AsyncExitStack
from mcp.client.stdio import stdio_client
from mcp.client.stdio import StdioServerParameters
from mcp.client.session import ClientSession
from pydantic_ai.tools import Tool, RunContext

class HyruleMCPClient:
    def __init__(self, command: list[str]):
        self.command = command
        self.session: ClientSession | None = None
        self._exit_stack = AsyncExitStack()

    async def connect(self):
        """Starts the MCP server process and initializes the ClientSession."""
        server_params = StdioServerParameters(
            command=self.command[0],
            args=self.command[1:],
            env=os.environ.copy()
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
        tools = []
        
        for mcp_tool in mcp_tools_response.tools:
            tools.append(self._create_pydantic_tool(mcp_tool))
            
        return tools

    def _create_pydantic_tool(self, mcp_tool: Any) -> Tool:
        # Create an async wrapper function that forwards arguments to the MCP Server
        async def tool_runner(ctx: RunContext, **kwargs) -> Any:
            try:
                result = await self.session.call_tool(mcp_tool.name, arguments=kwargs)
                out = ""
                for block in result.content:
                    if hasattr(block, 'text'):
                        out += block.text + "\n"
                return out.strip() if out else "Executed successfully."
            except Exception as e:
                return f"MCP tool execution failed: {str(e)}"

        tool_runner.__name__ = mcp_tool.name
        tool_runner.__doc__ = mcp_tool.description
        return Tool(tool_runner, name=mcp_tool.name, description=mcp_tool.description)
