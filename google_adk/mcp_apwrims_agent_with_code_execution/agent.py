"""
AP WRIMS MCP Agent with Code Execution
======================================
Multi-agent architecture using AgentTool pattern to work around Gemini API limitations.

Architecture:
- DataFetcher: Specialist agent with MCP tools for fetching water resource data
- CodeExecutor: Specialist agent with BuiltInCodeExecutor for Python code execution
- Root: Coordinator with NO built-in tools, delegates to specialists via AgentTool

Key Pattern (from Google ADK Issue #53):
- Each specialist has ONE type of tool (either MCP tools OR code executor, never both)
- Specialists are wrapped in AgentTool
- Root agent has NO code_executor, only AgentTools
- This avoids "Tool use with function calling is unsupported" error

Dynamic Tool Discovery:
- MCPToolset.get_tools() fetches tool descriptions from MCP server at runtime
- Agent description is built dynamically from available MCP tools
"""

import asyncio
import threading
import os
from typing import List

from google.adk.agents import Agent, LlmAgent
from google.adk.code_executors import BuiltInCodeExecutor
from google.adk.tools.agent_tool import AgentTool
from google.adk.tools.mcp_tool.mcp_session_manager import (
    StreamableHTTPConnectionParams,
)
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset

# Configure MCP server URL from environment variable or use default
MCP_SERVER_URL = os.environ.get("APWRIMS_MCP_URL", "http://localhost:9081/mcp")

# Optional: Configure authentication headers if needed
MCP_AUTH_HEADERS = {}
if os.environ.get("APWRIMS_AUTH_TOKEN"):
    MCP_AUTH_HEADERS["Authorization"] = (
        f"Bearer {os.environ['APWRIMS_AUTH_TOKEN']}"
    )


async def get_mcp_tools_description(mcp_toolset: MCPToolset) -> str:
    """
    Dynamically fetch MCP tool descriptions from the server.

    Args:
        mcp_toolset: MCPToolset instance to fetch tools from

    Returns:
        A formatted string describing available tools
    """
    try:
        tools = await mcp_toolset.get_tools()
        if not tools:
            return "Fetches AP WRIMS water resource data."

        # Build tool list from server response
        tool_lines = []
        for tool in tools:
            if not (hasattr(tool, "name") and hasattr(tool, "description")):
                continue

            # Get description from server
            description = (
                tool.description.strip()
                if isinstance(getattr(tool, "description", None), str)
                else ""
            )

            tool_lines.append(f"- {tool.name}: {description}")

        # Build final description
        header = f"Fetches AP WRIMS water resource data using {len(tool_lines)} MCP tools."
        details = "\n".join(tool_lines) if tool_lines else None

        if details:
            return f"{header}\n\nAvailable tools:\n{details}"
        return header
    except Exception:
        # Fallback if dynamic fetch fails
        return "Fetches AP WRIMS water resource data."


def _get_mcp_tools_description_sync(mcp_toolset: MCPToolset) -> str:
    """Safely resolve the async tool description regardless of event loop state.

    - Uses asyncio.run when no loop is running
    - Falls back to running in a background thread when a loop is already running
    """
    try:
        # Will raise RuntimeError if no running loop
        asyncio.get_running_loop()
    except RuntimeError:
        # Safe to use asyncio.run in non-async context
        return asyncio.run(get_mcp_tools_description(mcp_toolset))
    # Running inside an event loop — run in a separate thread
    result: dict = {}

    def runner():
        try:
            result["value"] = asyncio.run(get_mcp_tools_description(mcp_toolset))
        except Exception:
            result["value"] = "Fetches AP WRIMS water resource data using available MCP tools."

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    # Wait without a strict timeout to avoid flakiness; rely on MCP timeout
    t.join()
    return result.get("value", "Fetches AP WRIMS water resource data using available MCP tools.")


def create_data_agent() -> Agent:
    """Create the data fetching agent with dynamic MCP tool discovery."""
    mcp_toolset = MCPToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=MCP_SERVER_URL,
            headers=MCP_AUTH_HEADERS,
        ),
    )

    # Try to get dynamic description, fallback to static
    try:
        description = _get_mcp_tools_description_sync(mcp_toolset)
    except Exception:
        description = "Fetches AP WRIMS water resource data using available MCP tools."

    return Agent(
        name="DataFetcher",
        model="gemini-2.5-flash",
        description=description,
        instruction=(
            "You fetch water resource data from AP WRIMS using the available MCP tools. "
            "Each tool has its own description explaining what data it provides. "
            "Use the appropriate tool based on what the user requests. "
            "Return data in a clear, structured format."
        ),
        tools=[mcp_toolset],
    )


# Specialist agent for fetching AP WRIMS data
data_agent = create_data_agent()

# Specialist agent for executing Python code
code_agent = LlmAgent(
    name="CodeExecutor",
    model="gemini-2.5-flash",
    code_executor=BuiltInCodeExecutor(),
    description="Executes Python code for calculations, data analysis, and transformations.",
    instruction=(
        "You execute Python code to perform calculations and data analysis. "
        "Variables persist across executions. "
        "Use pandas, numpy, matplotlib as needed. Keep output concise."
    ),
)

# Root coordinator with AgentTool wrappers (NO built-in tools at root level)
root_agent = Agent(
    name="APWRIMSCoordinator",
    model="gemini-2.5-flash",
    description="AP WRIMS water resource analyst with data access and code execution.",
    instruction=(
        "You coordinate AP WRIMS water resource data analysis using two specialists:\n\n"
        "- DataFetcher: Fetches water resource data (has access to MCP tools)\n"
        "- CodeExecutor: Executes Python code for calculations and analysis\n\n"
        "When users ask for data, call DataFetcher. "
        "When users need calculations or analysis, call CodeExecutor. "
        "For complex tasks, coordinate between both: fetch data first, then analyze with code. "
        "For greetings, respond naturally."
    ),
    tools=[
        AgentTool(agent=data_agent),
        AgentTool(agent=code_agent),
    ],
)
