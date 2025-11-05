"""
Salesforce - Query Records
==========================
Query Salesforce records using SOQL-like syntax.
"""

from typing import Dict, Any, List
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def query(soql_query: str) -> List[Dict[str, Any]]:
    """
    Query Salesforce records.

    Args:
        soql_query: SOQL query string (e.g., "SELECT Id, Name FROM Lead WHERE Status = 'Open'")

    Returns:
        List of record dictionaries matching the query

    Example:
        >>> leads = query("SELECT Id, Name, Email FROM Lead WHERE Status = 'Open'")
        >>> print(f"Found {len(leads)} open leads")
        >>> for lead in leads[:5]:
        ...     print(f"{lead['Name']} - {lead['Email']}")
    """
    result = mcp_client.call_tool(
        server="salesforce",
        tool="query",
        params={"query": soql_query}
    )
    return result.get("records", [])
