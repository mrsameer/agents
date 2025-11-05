"""
MCP Servers - Code API Interface
================================
This module presents MCP servers as a code API using progressive disclosure.

Instead of loading all tool definitions upfront, the agent:
1. Explores the filesystem to discover available servers
2. Reads only the tool definitions it needs
3. Writes code to interact with tools
4. Processes data in the execution environment

Available servers:
- google_drive: Access Google Drive documents and spreadsheets
- salesforce: Query and manage Salesforce records

Usage:
    # Progressive disclosure - import only what you need
    from servers.google_drive import get_document, get_sheet
    from servers.salesforce import update_record

    # Or import entire server modules
    from servers import google_drive, salesforce
"""

# Note: We intentionally don't import everything here.
# The agent should explore and import only what it needs!
