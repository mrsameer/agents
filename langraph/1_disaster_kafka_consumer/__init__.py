"""
Disaster Data Kafka Consumer (LangGraph)

A LangGraph-based consumer that ingests disaster data packets from Kafka
and stores them in PostgreSQL for analysis.
"""

from .agent import (
    run_disaster_consumer,
    initialize_database,
    consume_kafka_messages,
    query_disasters_by_type,
    query_disasters_by_location,
    query_disasters_by_date_range,
    get_statistics_summary,
    create_consumer_workflow,
)

__version__ = "1.0.0"
__author__ = "Disaster Data Team"

__all__ = [
    "run_disaster_consumer",
    "initialize_database",
    "consume_kafka_messages",
    "query_disasters_by_type",
    "query_disasters_by_location",
    "query_disasters_by_date_range",
    "get_statistics_summary",
    "create_consumer_workflow",
]
