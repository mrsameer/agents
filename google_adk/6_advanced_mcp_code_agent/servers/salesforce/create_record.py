"""
Salesforce - Create Record
==========================
Creates a new Salesforce record.
"""

from typing import Dict, Any
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from mcp_client import mcp_client


def create_record(object_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Salesforce record.

    Args:
        object_type: Type of Salesforce object (e.g., "Lead", "Contact", "Opportunity")
        data: Dictionary of field values for the new record

    Returns:
        Dictionary with:
            - success (bool): Whether the creation succeeded
            - id (str): New record ID
            - message (str): Status message

    Example:
        >>> result = create_record(
        ...     object_type="Lead",
        ...     data={
        ...         "FirstName": "John",
        ...         "LastName": "Doe",
        ...         "Company": "Acme Corp",
        ...         "Email": "john@acme.com"
        ...     }
        ... )
        >>> print(f"Created lead with ID: {result['id']}")
    """
    return mcp_client.call_tool(
        server="salesforce",
        tool="create_record",
        params={
            "object_type": object_type,
            "data": data
        }
    )
