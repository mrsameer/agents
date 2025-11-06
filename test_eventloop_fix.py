#!/usr/bin/env python3
"""
Test asyncio event loop handling in crawl function
Tests both standalone and within event loop scenarios
"""

import sys
import asyncio
sys.path.insert(0, '/home/user/agents')

print("=" * 80)
print("TESTING ASYNCIO EVENT LOOP HANDLING")
print("=" * 80)

# Load the agent module
import importlib.util
spec = importlib.util.spec_from_file_location(
    "agent",
    "/home/user/agents/google_adk/5_disaster_data_agent/agent.py"
)
agent_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent_module)

crawl_urls_with_ai = agent_module.crawl_urls_with_ai

# Test 1: Standalone (no event loop)
print("\n[Test 1] Calling crawl_urls_with_ai WITHOUT an event loop (standalone)")
print("=" * 80)
try:
    test_urls = ["https://example.com"]
    result = crawl_urls_with_ai(test_urls)
    print(f"✓ Standalone call successful")
    print(f"  Status: {result['status']}")
    print(f"  Success count: {result.get('success_count', 0)}")
    print(f"  Error count: {result.get('error_count', 0)}")
except Exception as e:
    print(f"✗ Standalone call failed: {e}")

# Test 2: Within event loop (simulating Google ADK)
print("\n[Test 2] Calling crawl_urls_with_ai WITHIN an event loop (like Google ADK)")
print("=" * 80)

async def test_within_loop():
    """Simulates calling the function from within Google ADK's event loop"""
    print("Inside async context (event loop is running)...")
    try:
        test_urls = ["https://example.com"]
        result = crawl_urls_with_ai(test_urls)
        print(f"✓ Within-loop call successful")
        print(f"  Status: {result['status']}")
        print(f"  Success count: {result.get('success_count', 0)}")
        print(f"  Error count: {result.get('error_count', 0)}")
        return True
    except Exception as e:
        print(f"✗ Within-loop call failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Run the async test
try:
    success = asyncio.run(test_within_loop())
    if success:
        print("\n✓ Both scenarios handled correctly!")
    else:
        print("\n✗ Within-loop scenario failed")
except Exception as e:
    print(f"\n✗ Async test failed: {e}")

print("\n" + "=" * 80)
print("EVENT LOOP HANDLING TEST COMPLETE")
print("=" * 80)
