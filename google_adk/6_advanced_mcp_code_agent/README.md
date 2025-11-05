# Advanced MCP Agent with Code Execution

**Agent #6** - Based on Anthropic's paper: ["Code execution with MCP: Building more efficient agents"](https://www.anthropic.com/engineering/code-execution-with-mcp)

## Overview

This agent demonstrates an advanced pattern for agent-MCP interaction that dramatically improves efficiency, scalability, and context usage. Instead of loading all tool definitions upfront and passing intermediate results through the model context, the agent writes code to interact with MCP servers using a **progressive disclosure** pattern.

## The Problem with Traditional MCP Integration

As MCP adoption scales, traditional approaches face two key challenges:

### 1. Tool Definitions Overload the Context Window

When agents load thousands of tools upfront:
```
gdrive.getDocument (500 tokens)
gdrive.getSheet (500 tokens)
gdrive.listFiles (500 tokens)
... (997 more tools)
= 500,000 tokens before reading user request!
```

### 2. Intermediate Tool Results Consume Additional Tokens

Every result passes through the model:
```
User: "Copy meeting transcript from Google Drive to Salesforce"

TOOL CALL: gdrive.getDocument("abc123")
→ Returns 50,000 token transcript (loaded into context)

TOOL CALL: salesforce.updateRecord(...)
→ Model must write entire 50,000 token transcript again
```

For a 2-hour meeting transcript, this wastes 100,000+ tokens and adds latency.

## The Solution: Code Execution with MCP

Present MCP servers as **code APIs** using a filesystem structure. The agent:
1. **Discovers** tools by exploring the filesystem
2. **Loads** only the tool definitions it needs
3. **Processes** data in the execution environment
4. **Returns** only filtered results to context

### Example from the Paper

Traditional approach:
```python
# All 150,000 tokens of tool definitions loaded upfront
TOOL CALL: gdrive.getDocument(documentId: "abc123")
        → returns full transcript in context (50K tokens)

TOOL CALL: salesforce.updateRecord(
    objectType: "Lead",
    recordId: "00Q5f000001abc001",
    data: { "Notes": "<entire 50K token transcript repeated>" }
)
```
**Total: 150,000 + 50,000 + 50,000 = 250,000 tokens**

Code execution approach:
```python
from servers.google_drive import get_document
from servers.salesforce import update_record

doc = get_document("abc123")
update_record(
    object_type="Lead",
    record_id="00Q5f000001abc001",
    data={"Notes": doc["content"]}
)
print("Updated successfully")
```
**Total: ~2,000 tokens (98.7% reduction!)**

## Five Key Benefits

### 1. Progressive Disclosure

**Concept**: Load tool definitions on-demand rather than all upfront.

**Implementation**: Tools presented as files in a filesystem structure:
```
servers/
├── google_drive/
│   ├── get_document.py      # Agent reads this only when needed
│   ├── get_sheet.py
│   └── list_files.py
├── salesforce/
│   ├── query.py
│   ├── update_record.py
│   └── create_record.py
```

**Agent workflow**:
```python
# 1. Discover available servers
import os
servers = os.listdir('./servers')  # ['google_drive', 'salesforce']

# 2. Explore specific server
tools = os.listdir('./servers/google_drive')

# 3. Read only needed tool definition
with open('./servers/google_drive/get_document.py') as f:
    print(f.read())  # ~200 tokens vs 150K tokens for all tools

# 4. Import and use
from servers.google_drive import get_document
doc = get_document("abc123")
```

**Token savings**: 150,000 → 2,000 tokens (98.7% reduction)

### 2. Context-Efficient Tool Results

**Concept**: Filter and transform data in code before returning to context.

**Example**: Processing a 10,000-row spreadsheet:
```python
from servers.google_drive import get_sheet

# Get all rows (happens in execution environment)
all_rows = get_sheet("sheet001")  # 10,000 rows

# Filter in code - don't load all rows into context!
pending_orders = [row for row in all_rows if row["Status"] == "pending"]

# Only log summary + first 5 rows
print(f"Found {len(pending_orders)} pending orders out of {len(all_rows)} total")
print("Sample pending orders:")
for order in pending_orders[:5]:
    print(f"  {order['Name']}: ${order['Value']}")
```

**What the model sees**:
```
Found 234 pending orders out of 10000 total
Sample pending orders:
  Acme Corp: $50000
  TechStart Inc: $75000
  ...
```

**Token usage**: 5 rows (500 tokens) instead of 10,000 rows (500,000 tokens)

### 3. More Powerful Control Flow

**Concept**: Use code for loops, conditionals, error handling instead of chaining tool calls.

**Example**: Bulk import with error handling:
```python
from servers.google_drive import get_sheet
from servers.salesforce import create_record

rows = get_sheet("sheet001")
success_count = 0
error_count = 0

for row in rows:
    try:
        result = create_record(
            object_type="Lead",
            data={
                "Company": row["Name"],
                "Email": row["Contact"],
                "Status": row["Status"]
            }
        )
        if result.get("success"):
            success_count += 1
    except Exception as e:
        error_count += 1
        print(f"Error importing {row['Name']}: {e}")

print(f"Imported {success_count} leads, {error_count} errors")
```

**Traditional approach** would require:
- 1,000 individual tool calls through the model
- Model must wait for each result before proceeding
- Model must manually handle each error case
- 1,000+ round trips = slow!

**Code execution approach**:
- Single code execution handles entire workflow
- Execution environment processes loop
- Model only sees final summary
- Much faster!

### 4. Privacy-Preserving Operations

**Concept**: Intermediate data stays in execution environment; model only sees what you explicitly log.

**Example**: Sensitive data flowing between systems:
```python
from servers.google_drive import get_document
from servers.salesforce import update_record

# Full document content flows from Drive to Salesforce
# But never enters model context!
doc = get_document("abc123")  # Contains PII, sensitive data
result = update_record(
    object_type="Lead",
    record_id="00Q5f000001abc001",
    data={"Notes": doc["content"]}  # Full content in data flow
)

# Model only sees confirmation
print(result["message"])  # "Updated Lead record 00Q5f000001abc001"
```

**Advanced**: Can tokenize PII automatically before it reaches model:
```python
# What agent sees if it logs the data:
# Email: [EMAIL_1], Phone: [PHONE_1], Name: [NAME_1]

# What actually flows between tools:
# Email: john@example.com, Phone: 555-0101, Name: John Doe
```

### 5. State Persistence & Skills

**Concept**: Save intermediate results and reusable functions.

**State persistence** example:
```python
from servers.salesforce import query
import json

# Fetch and save backup
leads = query("SELECT Id, Name, Email FROM Lead")

with open('./workspace/leads_backup.json', 'w') as f:
    json.dump(leads, f, indent=2)

print(f"Saved {len(leads)} leads to workspace/leads_backup.json")

# Later, resume from saved state
with open('./workspace/leads_backup.json') as f:
    leads = json.load(f)
```

**Skills** example - Save reusable functions:
```python
# Create a reusable skill
skill_code = '''
from servers.google_drive import get_sheet
import json

def export_sheet_to_json(sheet_id: str, output_path: str) -> str:
    """Export Google Sheet to JSON file."""
    rows = get_sheet(sheet_id)
    with open(output_path, 'w') as f:
        json.dump(rows, f, indent=2)
    return f"Exported {len(rows)} rows to {output_path}"
'''

with open('./skills/export_sheet.py', 'w') as f:
    f.write(skill_code)

# Now available for future use!
from skills.export_sheet import export_sheet_to_json
export_sheet_to_json("sheet001", "./workspace/data.json")
```

Over time, the agent builds a library of higher-level capabilities.

## Project Structure

```
6_advanced_mcp_code_agent/
├── agent.py                    # Main agent with detailed instructions
├── mcp_client.py              # Dummy MCP client (simulates real MCP)
├── servers/                   # MCP servers as code APIs
│   ├── google_drive/
│   │   ├── get_document.py    # Tool: Get document from Drive
│   │   ├── get_sheet.py       # Tool: Get spreadsheet from Drive
│   │   ├── list_files.py      # Tool: List Drive files
│   │   └── __init__.py
│   ├── salesforce/
│   │   ├── query.py           # Tool: Query Salesforce
│   │   ├── update_record.py   # Tool: Update Salesforce record
│   │   ├── create_record.py   # Tool: Create Salesforce record
│   │   └── __init__.py
│   └── __init__.py
├── workspace/                 # For state persistence
├── skills/                    # For reusable functions
└── README.md                  # This file
```

## Available Sample Data

The dummy MCP client includes sample data for testing:

### Google Drive Documents
- `abc123` - Q4 Sales Meeting Transcript (demonstrates large content transfer)
- `xyz789` - Customer Data Export (CSV format)

### Google Drive Spreadsheets
- `sheet001` - Sales Leads Q4 (1,000 rows - demonstrates filtering)

### Salesforce
- Can query, create, and update any record type
- Persists updates within execution session

## Usage Examples

### Example 1: Progressive Disclosure Workflow

```python
import os

# 1. Discover available servers
print("Available MCP servers:")
servers = os.listdir('./servers')
for s in servers:
    if not s.startswith('__'):
        print(f"  - {s}")

# 2. Explore google_drive tools
print("\nGoogle Drive tools:")
tools = [f for f in os.listdir('./servers/google_drive')
         if f.endswith('.py') and f != '__init__.py']
for tool in tools:
    print(f"  - {tool}")

# 3. Read a tool definition
print("\nReading get_document.py:")
with open('./servers/google_drive/get_document.py') as f:
    lines = f.readlines()
    # Show just the docstring
    in_docstring = False
    for line in lines:
        if '"""' in line:
            in_docstring = not in_docstring
        if in_docstring or '"""' in line:
            print(line.rstrip())

# 4. Import and use it
from servers.google_drive import get_document
doc = get_document("abc123")
print(f"\nDocument title: {doc['title']}")
print(f"Content preview: {doc['content'][:100]}...")
```

### Example 2: Context-Efficient Data Processing

```python
from servers.google_drive import get_sheet

# Get large spreadsheet
all_rows = get_sheet("sheet001")

# Filter for pending items with high value
high_value_pending = [
    row for row in all_rows
    if row.get("Status") == "pending" and row.get("Value", 0) > 70000
]

# Calculate statistics
total_value = sum(row["Value"] for row in high_value_pending)
avg_value = total_value / len(high_value_pending) if high_value_pending else 0

# Only show summary + top 5
print(f"Found {len(high_value_pending)} high-value pending leads")
print(f"Total value: ${total_value:,}")
print(f"Average value: ${avg_value:,.2f}")
print("\nTop 5 by value:")
sorted_leads = sorted(high_value_pending, key=lambda x: x["Value"], reverse=True)
for lead in sorted_leads[:5]:
    print(f"  {lead['Name']}: ${lead['Value']:,}")
```

### Example 3: Privacy-Preserving Data Transfer

```python
from servers.google_drive import get_document
from servers.salesforce import update_record

# Transfer meeting transcript to Salesforce
# The full transcript never appears in model context!
doc = get_document("abc123")

result = update_record(
    object_type="Lead",
    record_id="00Q5f000001abc001",
    data={"Notes": doc["content"]}
)

print(f"✓ {result['message']}")
print(f"  Document '{doc['title']}' attached to lead")
print(f"  Content size: {len(doc['content'])} characters")
```

### Example 4: Complex Workflow with State Persistence

```python
from servers.google_drive import get_sheet
from servers.salesforce import create_record
import json

# Step 1: Fetch data
print("Fetching leads from Google Sheet...")
rows = get_sheet("sheet001")

# Step 2: Filter for import
pending = [r for r in rows if r.get("Status") == "pending"]
print(f"Found {len(pending)} pending leads to import")

# Step 3: Save checkpoint
with open('./workspace/import_checkpoint.json', 'w') as f:
    json.dump({
        "total_rows": len(rows),
        "pending_count": len(pending),
        "timestamp": "2025-11-05"
    }, f, indent=2)

# Step 4: Import with progress tracking
imported = []
errors = []

for i, lead in enumerate(pending[:10]):  # Import first 10
    try:
        result = create_record(
            object_type="Lead",
            data={
                "Company": lead["Name"],
                "Email": lead["Contact"],
                "Status": lead["Status"]
            }
        )
        if result.get("success"):
            imported.append(result["id"])
            if (i + 1) % 5 == 0:
                print(f"  Progress: {i + 1}/{len(pending[:10])} imported")
    except Exception as e:
        errors.append({"lead": lead["Name"], "error": str(e)})

# Step 5: Save results
with open('./workspace/import_results.json', 'w') as f:
    json.dump({
        "imported_ids": imported,
        "errors": errors,
        "success_count": len(imported),
        "error_count": len(errors)
    }, f, indent=2)

print(f"\n✓ Import complete!")
print(f"  Imported: {len(imported)} leads")
print(f"  Errors: {len(errors)}")
print(f"  Results saved to workspace/import_results.json")
```

### Example 5: Building and Using Skills

```python
# Create a reusable skill
skill_code = '''
"""Skill: Filter and export sheet data"""

from servers.google_drive import get_sheet
import json

def filter_and_export(
    sheet_id: str,
    filter_field: str,
    filter_value: str,
    output_path: str
) -> dict:
    """
    Filter a Google Sheet and export matching rows to JSON.

    Returns:
        Dictionary with count and output path
    """
    rows = get_sheet(sheet_id)
    filtered = [r for r in rows if r.get(filter_field) == filter_value]

    with open(output_path, 'w') as f:
        json.dump(filtered, f, indent=2)

    return {
        "total_rows": len(rows),
        "filtered_rows": len(filtered),
        "output_path": output_path
    }
'''

# Save the skill
with open('./skills/filter_and_export.py', 'w') as f:
    f.write(skill_code)

print("✓ Skill saved to skills/filter_and_export.py")

# Use the skill
from skills.filter_and_export import filter_and_export

result = filter_and_export(
    sheet_id="sheet001",
    filter_field="Status",
    filter_value="pending",
    output_path="./workspace/pending_leads.json"
)

print(f"\n✓ Skill executed successfully!")
print(f"  Total rows: {result['total_rows']}")
print(f"  Filtered: {result['filtered_rows']}")
print(f"  Saved to: {result['output_path']}")
```

## Key Takeaways

1. **Token Efficiency**: Reduce token usage by 98%+ through progressive disclosure
2. **Scalability**: Handle thousands of tools without context overload
3. **Performance**: Faster execution with fewer round trips through model
4. **Privacy**: Keep sensitive data in execution environment
5. **Composability**: Build complex workflows with familiar code patterns
6. **Persistence**: Save state and build reusable skills over time

## Comparison to Previous Agent

**Agent #5** (mcp_apwrims_agent_with_code_execution):
- Multi-agent architecture with specialists
- MCP tools loaded directly into DataFetcher agent
- CodeExecutor agent separate from data access
- All tool definitions loaded upfront

**Agent #6** (this agent):
- Single agent with integrated code execution
- MCP servers presented as code APIs
- Progressive disclosure via filesystem exploration
- Only loads tool definitions as needed
- Demonstrates all 5 key benefits from Anthropic paper

## Production Considerations

This implementation uses a **dummy MCP client** for demonstration. In production:

1. Replace `mcp_client.py` with real MCP client using the protocol
2. Connect to actual MCP servers (Google Drive, Salesforce, etc.)
3. Add authentication and authorization
4. Implement security sandboxing for code execution
5. Add resource limits and monitoring
6. Consider PII tokenization for sensitive data

## References

- Anthropic Engineering Blog: [Code execution with MCP: Building more efficient agents](https://www.anthropic.com/engineering/code-execution-with-mcp)
- Model Context Protocol: https://modelcontextprotocol.io/
- Cloudflare "Code Mode": Similar approach with MCP servers

## License

This is an educational implementation demonstrating concepts from the Anthropic paper.
