"""
Salesforce MCP Server API
=========================
Progressive disclosure: Import only what you need.

Available tools:
- query: Query Salesforce records
- update_record: Update an existing record
- create_record: Create a new record
"""

from .query import query
from .update_record import update_record
from .create_record import create_record

__all__ = ['query', 'update_record', 'create_record']
