"""
Google Drive - Get Sheet
========================
Retrieves a spreadsheet from Google Drive.

Returns all rows - the agent can filter in code to avoid
loading unnecessary data into context.
"""

from typing import Dict, Any, List
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def get_sheet(sheet_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve a spreadsheet from Google Drive.

    Args:
        sheet_id: The ID of the spreadsheet to retrieve

    Returns:
        List of dictionaries, where each dictionary represents a row.
        Keys are column names, values are cell values.

    Example:
        >>> rows = get_sheet("sheet001")
        >>> # Filter in code instead of loading all rows into context
        >>> pending = [r for r in rows if r.get("Status") == "pending"]
        >>> print(f"Found {len(pending)} pending items")
        >>> print(pending[:5])  # Only show first 5
    """
    result = mcp_client.call_tool(
        server="google_drive",
        tool="get_sheet",
        params={"sheet_id": sheet_id}
    )
    return result.get("rows", [])
