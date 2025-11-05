"""
Dummy MCP Client for Code Execution Environment
================================================
Simulates an MCP client that would normally connect to real MCP servers.
In a production environment, this would use the actual MCP protocol.

This implementation demonstrates the core concept: instead of the model
calling tools directly, it writes code that calls these functions.
"""

from typing import Any, Dict, List
import json


class MCPClient:
    """
    Simulates MCP tool calls in the code execution environment.
    In production, this would communicate with real MCP servers.
    """

    # Simulated data store
    _documents = {
        "abc123": {
            "title": "Q4 Sales Meeting Transcript",
            "content": """Sales Meeting - Q4 2025 Planning
Date: November 1, 2025

Attendees: Sarah Chen, Mike Rodriguez, Lisa Park

Key Discussion Points:
1. Q4 revenue target: $2.5M
2. New product launch scheduled for December 15
3. Expansion into Southeast Asian markets
4. Customer retention rate at 94%
5. Need to hire 3 more sales reps

Action Items:
- Sarah: Finalize pricing strategy by Nov 10
- Mike: Complete market analysis by Nov 15
- Lisa: Draft hiring plan by Nov 8

Next meeting: November 15, 2025
""",
            "metadata": {
                "created": "2025-11-01",
                "modified": "2025-11-01",
                "owner": "sarah.chen@company.com"
            }
        },
        "xyz789": {
            "title": "Customer Data Export",
            "content": "CustomerID,Name,Email,Phone,Status\n1,John Doe,john@example.com,555-0101,Active\n2,Jane Smith,jane@example.com,555-0102,Active\n3,Bob Johnson,bob@example.com,555-0103,Pending\n",
            "metadata": {
                "created": "2025-11-04",
                "modified": "2025-11-05",
                "owner": "data@company.com"
            }
        }
    }

    _sheets = {
        "sheet001": {
            "title": "Sales Leads Q4",
            "rows": [
                {"ID": "001", "Name": "Acme Corp", "Contact": "john@acme.com", "Status": "pending", "Value": 50000},
                {"ID": "002", "Name": "TechStart Inc", "Contact": "sarah@techstart.com", "Status": "active", "Value": 75000},
                {"ID": "003", "Name": "Global Solutions", "Contact": "mike@global.com", "Status": "pending", "Value": 100000},
                {"ID": "004", "Name": "Innovation Labs", "Contact": "lisa@innovation.com", "Status": "closed", "Value": 120000},
                {"ID": "005", "Name": "Future Systems", "Contact": "bob@future.com", "Status": "pending", "Value": 60000},
            ] + [
                {"ID": f"{i:03d}", "Name": f"Company {i}", "Contact": f"contact{i}@example.com",
                 "Status": "pending" if i % 3 == 0 else "active", "Value": (i * 1000) % 100000}
                for i in range(6, 1001)  # Simulate 1000 rows
            ]
        }
    }

    _salesforce_records = {}
    _next_record_id = 1

    @classmethod
    def call_tool(cls, server: str, tool: str, params: Dict[str, Any]) -> Any:
        """
        Route tool calls to the appropriate server handler.

        Args:
            server: MCP server name (e.g., 'google_drive', 'salesforce')
            tool: Tool name (e.g., 'get_document', 'update_record')
            params: Tool parameters

        Returns:
            Tool execution result
        """
        handler_name = f"_handle_{server}_{tool}"
        handler = getattr(cls, handler_name, None)

        if not handler:
            raise ValueError(f"Unknown tool: {server}.{tool}")

        return handler(params)

    # Google Drive tool handlers

    @classmethod
    def _handle_google_drive_get_document(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve a document from Google Drive."""
        doc_id = params.get("document_id")
        if not doc_id:
            return {"error": "document_id is required"}

        doc = cls._documents.get(doc_id)
        if not doc:
            return {"error": f"Document {doc_id} not found"}

        return {
            "title": doc["title"],
            "content": doc["content"],
            "metadata": doc["metadata"]
        }

    @classmethod
    def _handle_google_drive_get_sheet(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve a spreadsheet from Google Drive."""
        sheet_id = params.get("sheet_id")
        if not sheet_id:
            return {"error": "sheet_id is required"}

        sheet = cls._sheets.get(sheet_id)
        if not sheet:
            return {"error": f"Sheet {sheet_id} not found"}

        return {
            "title": sheet["title"],
            "rows": sheet["rows"]
        }

    @classmethod
    def _handle_google_drive_list_files(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """List files in Google Drive."""
        query = params.get("query", "")
        max_results = params.get("max_results", 100)

        files = []
        for doc_id, doc in cls._documents.items():
            files.append({
                "id": doc_id,
                "name": doc["title"],
                "type": "document",
                "modified": doc["metadata"]["modified"]
            })

        for sheet_id, sheet in cls._sheets.items():
            files.append({
                "id": sheet_id,
                "name": sheet["title"],
                "type": "spreadsheet",
                "modified": "2025-11-05"
            })

        return {"files": files[:max_results]}

    # Salesforce tool handlers

    @classmethod
    def _handle_salesforce_query(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """Query Salesforce records."""
        query = params.get("query", "")

        # Simple simulation - return some mock leads
        records = [
            {
                "Id": "00Q5f000001abc001",
                "Name": "John Doe",
                "Email": "john@example.com",
                "Company": "Acme Corp",
                "Status": "Open"
            },
            {
                "Id": "00Q5f000001abc002",
                "Name": "Jane Smith",
                "Email": "jane@example.com",
                "Company": "TechStart Inc",
                "Status": "Qualified"
            }
        ]

        return {
            "totalSize": len(records),
            "records": records
        }

    @classmethod
    def _handle_salesforce_update_record(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """Update a Salesforce record."""
        object_type = params.get("object_type")
        record_id = params.get("record_id")
        data = params.get("data", {})

        if not object_type or not record_id:
            return {"error": "object_type and record_id are required"}

        # Store the update
        key = f"{object_type}:{record_id}"
        cls._salesforce_records[key] = {
            "id": record_id,
            "type": object_type,
            "data": data,
            "updated": "2025-11-05T10:30:00Z"
        }

        return {
            "success": True,
            "id": record_id,
            "message": f"Updated {object_type} record {record_id}"
        }

    @classmethod
    def _handle_salesforce_create_record(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new Salesforce record."""
        object_type = params.get("object_type")
        data = params.get("data", {})

        if not object_type:
            return {"error": "object_type is required"}

        # Generate new ID
        new_id = f"00Q5f00000{cls._next_record_id:06d}"
        cls._next_record_id += 1

        # Store the record
        key = f"{object_type}:{new_id}"
        cls._salesforce_records[key] = {
            "id": new_id,
            "type": object_type,
            "data": data,
            "created": "2025-11-05T10:30:00Z"
        }

        return {
            "success": True,
            "id": new_id,
            "message": f"Created {object_type} record {new_id}"
        }


# Global instance for use in tool files
mcp_client = MCPClient()
