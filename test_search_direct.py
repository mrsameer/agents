#!/usr/bin/env python3
"""Direct test of DDGS search"""

from ddgs import DDGS

print("Testing DDGS search...")

try:
    ddgs = DDGS()
    results = list(ddgs.text("India floods latest news", max_results=3))

    print(f"Found {len(results)} results")

    for i, result in enumerate(results, 1):
        print(f"\n{i}. {result.get('title', 'No title')}")
        print(f"   URL: {result.get('href', 'No URL')}")
        print(f"   Body: {result.get('body', 'No body')[:100]}...")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
