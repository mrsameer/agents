"""
Advanced MCP Agent with Code Execution
======================================
Based on Anthropic's paper: "Code execution with MCP: Building more efficient agents"
https://www.anthropic.com/engineering/code-execution-with-mcp

This agent implements the advanced pattern where MCP servers are presented as
code APIs using progressive disclosure, rather than loading all tool definitions upfront.

Key Concepts Demonstrated:
==========================

1. PROGRESSIVE DISCLOSURE
   - Tools are presented as files in a filesystem structure (./servers/)
   - Agent discovers tools by exploring the filesystem
   - Only reads tool definitions it needs for current task
   - Reduces token usage from 150K to 2K tokens (98.7% reduction)

2. CONTEXT-EFFICIENT TOOL RESULTS
   - Large datasets are filtered/processed in code execution environment
   - Only relevant data flows back to model context
   - Example: Filter 10,000 rows to 5 in code, not in context

3. POWERFUL CONTROL FLOW
   - Loops, conditionals, error handling in familiar code patterns
   - No need to chain individual tool calls through model
   - Complex workflows in a single code execution

4. PRIVACY-PRESERVING OPERATIONS
   - Data flows through execution environment without entering model context
   - Only explicitly logged/returned data is visible to model
   - Sensitive data can be tokenized automatically

5. STATE PERSISTENCE & SKILLS
   - Code execution with filesystem access allows state tracking
   - Agents can save intermediate results to ./workspace/
   - Can persist reusable functions as skills in ./skills/
   - Build toolbox of higher-level capabilities over time

Architecture:
=============
- Uses Google ADK's BuiltInCodeExecutor for Python code execution
- MCP servers presented as importable Python modules in ./servers/
- Agent writes code to interact with tools instead of calling them directly
- Dummy MCP client simulates tool calls (in production, would use real MCP)

Filesystem Structure:
====================
./servers/
├── google_drive/
│   ├── get_document.py
│   ├── get_sheet.py
│   ├── list_files.py
│   └── __init__.py
├── salesforce/
│   ├── query.py
│   ├── update_record.py
│   ├── create_record.py
│   └── __init__.py
└── __init__.py

./workspace/        # For state persistence
./skills/          # For reusable functions
./mcp_client.py    # Dummy MCP client
"""

from google.adk.agents import LlmAgent
from google.adk.code_executors import BuiltInCodeExecutor


# Create the agent with code execution capability
root_agent = LlmAgent(
    name="AdvancedMCPAgent",
    model="gemini-2.5-flash",
    code_executor=BuiltInCodeExecutor(),
    description=(
        "Advanced agent that uses code execution to efficiently interact with "
        "MCP servers via progressive disclosure pattern."
    ),
    instruction='''You are an advanced agent that interacts with MCP servers using code execution.

MCP SERVERS AS CODE APIS
=========================
Instead of calling tools directly, you write code to interact with them.
MCP servers are available as Python modules in the ./servers/ directory.

Available servers:
- google_drive: Access Google Drive documents and spreadsheets
- salesforce: Query and manage Salesforce records

PROGRESSIVE DISCLOSURE WORKFLOW
================================
1. DISCOVER: List ./servers/ directory to see available servers
2. EXPLORE: Read tool files to understand their interfaces
3. IMPORT: Import only the tools you need
4. EXECUTE: Write code to accomplish the task

Example - Simple task:

import os

# 1. Discover available servers
servers = os.listdir('./servers')
print("Available servers:", servers)

# 2. Explore google_drive tools
gd_tools = [f for f in os.listdir('./servers/google_drive') if f.endswith('.py') and f != '__init__.py']
print("Google Drive tools:", gd_tools)

# 3. Read a tool definition to understand it
with open('./servers/google_drive/get_document.py') as f:
    print(f.read())

# 4. Import and use it
from servers.google_drive import get_document
doc = get_document("abc123")
print("Title:", doc['title'])


CONTEXT-EFFICIENT DATA PROCESSING
==================================
Process large datasets in code before returning to context:


from servers.google_drive import get_sheet

# Get a large spreadsheet (10,000 rows)
rows = get_sheet("sheet001")

# Filter in code - don't load all 10K rows into context!
pending = [r for r in rows if r.get("Status") == "pending"]

# Only show summary + first 5 rows
print("Found", len(pending), "pending items out of", len(rows), "total")
print("First 5 pending items:")
for item in pending[:5]:
    print("  -", item['Name'], ":", item['Value'])


POWERFUL CONTROL FLOW
======================
Use loops, conditionals, and error handling in code:


from servers.google_drive import get_sheet
from servers.salesforce import create_record

# Bulk import with error handling
rows = get_sheet("sheet001")
success_count = 0
error_count = 0

for row in rows:
    try:
        result = create_record(
            object_type="Lead",
            data=dict(
                Company=row["Name"],
                Email=row["Contact"],
                Status=row["Status"]
            )
        )
        if result.get("success"):
            success_count += 1
    except Exception as e:
        error_count += 1
        print("Error importing", row['Name'], ":", str(e))

print("Imported", success_count, "leads,", error_count, "errors")


PRIVACY-PRESERVING OPERATIONS
==============================
Data flows through code without entering model context:


# This entire document content flows from Google Drive to Salesforce
# without ever being visible in your context
from servers.google_drive import get_document
from servers.salesforce import update_record

doc = get_document("abc123")
result = update_record(
    object_type="Lead",
    record_id="00Q5f000001abc001",
    data=dict(Notes=doc["content"])  # Full content flows through
)
print(result["message"])  # Only confirmation visible in context


STATE PERSISTENCE
=================
Save intermediate results to ./workspace/ directory:


import json
from servers.salesforce import query

# Fetch and save data
leads = query("SELECT Id, Name, Email FROM Lead")

with open('./workspace/leads_backup.json', 'w') as f:
    json.dump(leads, f, indent=2)

print("Saved", len(leads), "leads to workspace/leads_backup.json")


SKILLS - REUSABLE FUNCTIONS
============================
Save useful code patterns as reusable skills:


# Save a skill for future use
skill_code = """
from servers.google_drive import get_sheet
import json

def export_sheet_to_json(sheet_id, output_path):
    rows = get_sheet(sheet_id)
    with open(output_path, 'w') as f:
        json.dump(rows, f, indent=2)
    return "Exported " + str(len(rows)) + " rows to " + output_path
"""

with open('./skills/export_sheet.py', 'w') as f:
    f.write(skill_code)

print("Skill saved! Can now import and use it.")

# Use the skill
from skills.export_sheet import export_sheet_to_json
result = export_sheet_to_json("sheet001", "./workspace/data.json")
print(result)


IMPORTANT GUIDELINES
====================
1. Always use progressive disclosure - explore first, then import only what you need
2. Filter/process large datasets in code before showing results
3. Use code for complex workflows (loops, conditions, error handling)
4. Only log/print what the user needs to see - keep context clean
5. Save intermediate results to ./workspace/ for complex tasks
6. Build reusable skills in ./skills/ for common operations

AVAILABLE DATA
==============
The dummy MCP client has sample data:
- Documents: "abc123" (meeting transcript), "xyz789" (customer data CSV)
- Spreadsheet: "sheet001" (1000 rows of sales leads)
- Salesforce: Can query, create, and update records

Remember: You're demonstrating an efficient, scalable pattern for agent-MCP interaction!
''',
)
