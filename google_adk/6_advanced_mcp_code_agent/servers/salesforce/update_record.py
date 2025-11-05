"""
Salesforce - Update Record
==========================
Updates an existing Salesforce record.

Data flows through code execution environment without
entering model context - privacy preserving!
"""

from typing import Dict, Any
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def update_record(object_type: str, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update a Salesforce record.

    Args:
        object_type: Type of Salesforce object (e.g., "Lead", "Contact", "Opportunity")
        record_id: The ID of the record to update
        data: Dictionary of fields to update

    Returns:
        Dictionary with:
            - success (bool): Whether the update succeeded
            - id (str): Record ID
            - message (str): Status message

    Example:
        >>> # Data can flow from one tool to another without entering model context
        >>> doc = get_document("abc123")
        >>> result = update_record(
        ...     object_type="Lead",
        ...     record_id="00Q5f000001abc001",
        ...     data={"Notes": doc["content"]}
        ... )
        >>> print(result["message"])
    """
    return mcp_client.call_tool(
        server="salesforce",
        tool="update_record",
        params={
            "object_type": object_type,
            "record_id": record_id,
            "data": data
        }
    )
