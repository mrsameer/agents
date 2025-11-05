"""
Example usage of Advanced MCP Agent
====================================

This demonstrates how to use the agent with progressive disclosure.
"""

import asyncio
from agent import root_agent


async def main():
    """Run example queries against the advanced MCP agent."""

    print("=" * 80)
    print("Advanced MCP Agent - Progressive Disclosure Demo")
    print("=" * 80)

    # Example 1: Progressive Disclosure
    print("\n📋 Example 1: Progressive Disclosure")
    print("-" * 80)
    response = await root_agent.say_async(
        "Discover what MCP servers are available by exploring the ./servers/ directory, "
        "then show me what tools are available in the google_drive server."
    )
    print(response.text)

    # Example 2: Context-Efficient Data Processing
    print("\n📊 Example 2: Context-Efficient Data Processing")
    print("-" * 80)
    response = await root_agent.say_async(
        "Get the spreadsheet 'sheet001' and show me how many pending leads there are "
        "with a value over $70,000. Only show me the top 5 by value, not all of them."
    )
    print(response.text)

    # Example 3: Privacy-Preserving Data Transfer
    print("\n🔒 Example 3: Privacy-Preserving Data Transfer")
    print("-" * 80)
    response = await root_agent.say_async(
        "Get the document 'abc123' from Google Drive and attach it to the Salesforce lead "
        "with ID '00Q5f000001abc001'. Don't show me the full document content, just confirm "
        "it was attached successfully."
    )
    print(response.text)

    # Example 4: Complex Workflow with State Persistence
    print("\n💾 Example 4: Complex Workflow with State Persistence")
    print("-" * 80)
    response = await root_agent.say_async(
        "Get the first 10 pending leads from sheet 'sheet001', create Salesforce Lead "
        "records for them, and save the results to ./workspace/import_results.json. "
        "Show me a summary of how many were imported successfully."
    )
    print(response.text)

    # Example 5: Building a Reusable Skill
    print("\n🎯 Example 5: Building a Reusable Skill")
    print("-" * 80)
    response = await root_agent.say_async(
        "Create a reusable skill in ./skills/ that filters a Google Sheet by a field value "
        "and exports the results to JSON. Save it as 'filter_and_export.py', then use it to "
        "export all pending leads from sheet001."
    )
    print(response.text)

    print("\n" + "=" * 80)
    print("✅ Demo Complete!")
    print("=" * 80)
    print("\nKey Benefits Demonstrated:")
    print("  1. Progressive Disclosure - Only loaded needed tools")
    print("  2. Context Efficiency - Filtered data before showing results")
    print("  3. Privacy Preserving - Document content didn't enter context")
    print("  4. State Persistence - Saved results to workspace")
    print("  5. Skills - Created reusable function for future use")
    print("\nCompare this to traditional MCP approach where:")
    print("  - All 1000+ tool definitions loaded upfront (150K tokens)")
    print("  - All 10,000 spreadsheet rows would enter context (500K tokens)")
    print("  - Full document content visible in every step")
    print("  - No state persistence between operations")
    print("  - No ability to build reusable skills")


if __name__ == "__main__":
    asyncio.run(main())
