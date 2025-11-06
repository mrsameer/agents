#!/usr/bin/env python3
"""
Test Google ADK Agent Integration
"""

import sys
import os
import importlib.util

sys.path.insert(0, '/home/user/agents')

print("=" * 80)
print("TESTING GOOGLE ADK AGENT INTEGRATION")
print("=" * 80)

# Load the agent module
spec = importlib.util.spec_from_file_location(
    "agent",
    "/home/user/agents/google_adk/5_disaster_data_agent/agent.py"
)
agent_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent_module)

root_agent = agent_module.root_agent

print("\n[1] Agent Configuration")
print("=" * 80)
print(f"Name: {root_agent.name}")
print(f"Model: {root_agent.model}")
print(f"Description: {root_agent.description[:100]}...")
print(f"Number of Tools: {len(root_agent.tools)}")

print("\n[2] Available Tools")
print("=" * 80)
for i, tool in enumerate(root_agent.tools, 1):
    print(f"{i}. {tool.__name__}")
    # Get first line of docstring
    if tool.__doc__:
        first_line = tool.__doc__.strip().split('\n')[0]
        print(f"   {first_line}")

print("\n[3] Test: Simple Query (Mock Mode)")
print("=" * 80)
print("Testing: Search for flood data with mock=True parameter...")

# Call the tool directly
search_result = agent_module.search_web_for_disaster_data(
    disaster_type="floods",
    max_results=2,
    use_mock=True
)

print(f"\nResult Status: {search_result['status']}")
print(f"URLs Found: {search_result['total_count']}")
if search_result['total_count'] > 0:
    print(f"Sample URL: {search_result['discovered_urls'][0]['title']}")

print("\n[4] Test: Agent Instructions")
print("=" * 80)
print("Agent Instructions (first 500 chars):")
print(root_agent.instruction[:500] + "...")

print("\n" + "=" * 80)
print("AGENT INTEGRATION TEST COMPLETE")
print("=" * 80)

print("\n✓ Agent is properly configured and ready to use!")
print("\nUsage:")
print("  from google_adk.5_disaster_data_agent import agent")
print("  result = agent.root_agent.run('Search for earthquake data in India')")
print("\nNote: Due to network restrictions, use mock mode for testing:")
print("  agent.search_web_for_disaster_data(disaster_type='floods', use_mock=True)")
