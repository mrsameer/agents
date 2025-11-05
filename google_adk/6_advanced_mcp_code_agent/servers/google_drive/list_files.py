"""
Google Drive - List Files
=========================
Lists files in Google Drive with optional filtering.
"""

from typing import Dict, Any, List, Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def list_files(query: Optional[str] = None, max_results: int = 100) -> List[Dict[str, Any]]:
    """
    List files in Google Drive.

    Args:
        query: Optional search query to filter files
        max_results: Maximum number of results to return (default: 100)

    Returns:
        List of file metadata dictionaries with:
            - id (str): File ID
            - name (str): File name
            - type (str): File type (document, spreadsheet, etc.)
            - modified (str): Last modified date

    Example:
        >>> files = list_files(max_results=10)
        >>> for f in files:
        ...     print(f"{f['name']} ({f['type']})")
    """
    result = mcp_client.call_tool(
        server="google_drive",
        tool="list_files",
        params={"query": query or "", "max_results": max_results}
    )
    return result.get("files", [])
