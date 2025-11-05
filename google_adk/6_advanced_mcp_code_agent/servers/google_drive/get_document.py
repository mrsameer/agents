"""
Google Drive - Get Document
===========================
Retrieves a document from Google Drive.

This file demonstrates progressive disclosure - the agent only reads
this file when it needs to understand how to get documents.
"""

from typing import Dict, Any
import sys
import os

# Add parent directory to path to import mcp_client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def get_document(document_id: str) -> Dict[str, Any]:
    """
    Retrieve a document from Google Drive.

    Args:
        document_id: The ID of the document to retrieve

    Returns:
        Dictionary with:
            - title (str): Document title
            - content (str): Document content
            - metadata (dict): Document metadata (created, modified, owner)

    Example:
        >>> doc = get_document("abc123")
        >>> print(doc["title"])
        >>> print(doc["content"][:100])  # First 100 chars
    """
    return mcp_client.call_tool(
        server="google_drive",
        tool="get_document",
        params={"document_id": document_id}
    )
