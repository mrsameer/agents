"""
Disaster Data Kafka Consumer Agent (LangGraph)

This LangGraph agent consumes disaster data packets from Kafka and stores them in PostgreSQL.

Architecture:
- Consumes from Kafka topic: disaster-data-ingestion
- Parses discrete disaster event packets
- Validates packet schema
- Stores in PostgreSQL database
- Provides monitoring and statistics

LangGraph Workflow:
1. consume_kafka -> Parse messages from Kafka
2. validate_packet -> Validate packet structure and data
3. transform_data -> Transform to DB schema
4. store_in_postgres -> Store in PostgreSQL
5. update_stats -> Update consumption statistics
"""

import json
import datetime
import logging
from typing import TypedDict, List, Dict, Any, Annotated
from langgraph.graph import StateGraph, END
import psycopg2
from psycopg2.extras import execute_values
import os

# Setup logging
def setup_logger():
    """Setup detailed logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_logger()


# ============================================================================
# State Definition for LangGraph
# ============================================================================

class ConsumerState(TypedDict):
    """State for the Kafka consumer workflow"""
    # Input
    kafka_messages: List[Dict[str, Any]]
    batch_size: int

    # Processing
    validated_packets: List[Dict[str, Any]]
    invalid_packets: List[Dict[str, Any]]
    transformed_records: List[Dict[str, Any]]

    # Output
    stored_count: int
    failed_count: int
    statistics: Dict[str, Any]
    errors: List[str]

    # Status
    status: str
    timestamp: str


# ============================================================================
# PostgreSQL Database Functions
# ============================================================================

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "disaster_data"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres")
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise


def initialize_database():
    """Initialize PostgreSQL database schema"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create disaster_events table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS disaster_events (
                id SERIAL PRIMARY KEY,
                packet_id VARCHAR(255) UNIQUE NOT NULL,
                packet_type VARCHAR(100),
                disaster_type VARCHAR(50) NOT NULL,

                -- Temporal fields
                event_start_date DATE,
                event_end_date DATE,
                duration_days INTEGER,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Spatial fields
                primary_location VARCHAR(255),
                affected_locations TEXT[],
                location_count INTEGER,

                -- Impact metrics
                deaths INTEGER DEFAULT 0,
                injured INTEGER DEFAULT 0,
                displaced INTEGER DEFAULT 0,
                affected INTEGER DEFAULT 0,
                damage_amount DECIMAL(15, 2),

                -- Metadata
                severity VARCHAR(20),
                source_url TEXT,
                source_domain VARCHAR(255),
                source_title TEXT,
                relevance_score INTEGER,

                -- Processing info
                priority VARCHAR(20),
                retention_days INTEGER DEFAULT 365,

                -- Raw data
                raw_packet JSONB,

                -- Indexes
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_disaster_type
            ON disaster_events(disaster_type);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_dates
            ON disaster_events(event_start_date, event_end_date);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_locations
            ON disaster_events USING GIN(affected_locations);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_severity
            ON disaster_events(severity);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingestion_timestamp
            ON disaster_events(ingestion_timestamp);
        """)

        # Create statistics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consumption_statistics (
                id SERIAL PRIMARY KEY,
                batch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                messages_consumed INTEGER,
                messages_stored INTEGER,
                messages_failed INTEGER,
                disaster_type_breakdown JSONB,
                severity_breakdown JSONB,
                processing_time_seconds DECIMAL(10, 3)
            );
        """)

        conn.commit()
        logger.info("Database schema initialized successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def store_disaster_events(records: List[Dict[str, Any]]) -> int:
    """
    Store disaster event records in PostgreSQL

    Args:
        records: List of transformed disaster event records

    Returns:
        Number of records stored
    """
    if not records:
        return 0

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Prepare insert query with ON CONFLICT
        insert_query = """
            INSERT INTO disaster_events (
                packet_id, packet_type, disaster_type,
                event_start_date, event_end_date, duration_days,
                primary_location, affected_locations, location_count,
                deaths, injured, displaced, affected, damage_amount,
                severity, source_url, source_domain, source_title,
                relevance_score, priority, retention_days, raw_packet
            ) VALUES %s
            ON CONFLICT (packet_id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP,
                raw_packet = EXCLUDED.raw_packet
        """

        # Prepare values
        values = []
        for record in records:
            values.append((
                record.get('packet_id'),
                record.get('packet_type'),
                record.get('disaster_type'),
                record.get('event_start_date'),
                record.get('event_end_date'),
                record.get('duration_days'),
                record.get('primary_location'),
                record.get('affected_locations', []),
                record.get('location_count', 0),
                record.get('deaths', 0),
                record.get('injured', 0),
                record.get('displaced', 0),
                record.get('affected', 0),
                record.get('damage_amount'),
                record.get('severity'),
                record.get('source_url'),
                record.get('source_domain'),
                record.get('source_title'),
                record.get('relevance_score'),
                record.get('priority'),
                record.get('retention_days', 365),
                json.dumps(record.get('raw_packet', {}))
            ))

        # Execute batch insert
        execute_values(cursor, insert_query, values)
        conn.commit()

        stored_count = len(values)
        logger.info(f"Stored {stored_count} disaster events in PostgreSQL")

        return stored_count

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to store records: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def store_statistics(stats: Dict[str, Any]):
    """Store consumption statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO consumption_statistics (
                messages_consumed, messages_stored, messages_failed,
                disaster_type_breakdown, severity_breakdown,
                processing_time_seconds
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            stats.get('messages_consumed', 0),
            stats.get('messages_stored', 0),
            stats.get('messages_failed', 0),
            json.dumps(stats.get('disaster_type_breakdown', {})),
            json.dumps(stats.get('severity_breakdown', {})),
            stats.get('processing_time_seconds', 0)
        ))

        conn.commit()
        logger.info("Stored consumption statistics")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to store statistics: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# Kafka Consumer Functions
# ============================================================================

def consume_kafka_messages(
    topic: str = "disaster-data-ingestion",
    batch_size: int = 10,
    timeout_ms: int = 5000,
    use_mock: bool = False
) -> List[Dict[str, Any]]:
    """
    Consume messages from Kafka topic

    Args:
        topic: Kafka topic name
        batch_size: Number of messages to consume
        timeout_ms: Consumer timeout in milliseconds
        use_mock: Use mock data for testing

    Returns:
        List of Kafka messages
    """
    if use_mock:
        # Return mock disaster event packets for testing
        return [
            {
                "packet_id": "disaster_event_floods_kerala_20240815_001",
                "packet_type": "discrete_disaster_event",
                "kafka_topic": "disaster-data-ingestion",
                "timestamp": "2024-08-15T10:30:00",
                "temporal": {
                    "start_date": "2024-08-15",
                    "end_date": "2024-08-17",
                    "duration_days": 2,
                    "temporal_description": "August 15-17, 2024"
                },
                "spatial": {
                    "primary_location": "Kerala",
                    "affected_locations": ["Kerala", "Wayanad", "Idukki"],
                    "location_count": 3
                },
                "impact": {
                    "deaths": 25,
                    "injured": 50,
                    "displaced": 1000,
                    "affected": 5000
                },
                "metadata": {
                    "disaster_type": "floods",
                    "severity": "high",
                    "event_name": "Kerala Monsoon Floods 2024",
                    "source": {
                        "url": "https://www.thehindu.com/news/kerala-floods-2024",
                        "domain": "thehindu.com",
                        "title": "Heavy Floods Hit Kerala: 25 Dead"
                    },
                    "relevance_score": 9
                },
                "processing_instructions": {
                    "priority": "high",
                    "retention_days": 365
                }
            },
            {
                "packet_id": "disaster_event_cyclone_odisha_20240820_001",
                "packet_type": "discrete_disaster_event",
                "kafka_topic": "disaster-data-ingestion",
                "timestamp": "2024-08-20T14:00:00",
                "temporal": {
                    "start_date": "2024-08-20",
                    "end_date": "2024-08-22",
                    "duration_days": 2
                },
                "spatial": {
                    "primary_location": "Odisha",
                    "affected_locations": ["Odisha", "Puri", "Bhubaneswar"],
                    "location_count": 3
                },
                "impact": {
                    "deaths": 12,
                    "injured": 35,
                    "displaced": 2000,
                    "affected": 10000
                },
                "metadata": {
                    "disaster_type": "cyclones",
                    "severity": "high",
                    "event_name": "Cyclone Dana",
                    "source": {
                        "url": "https://www.ndtv.com/cyclone-dana-odisha",
                        "domain": "ndtv.com",
                        "title": "Cyclone Dana Makes Landfall in Odisha"
                    },
                    "relevance_score": 8
                },
                "processing_instructions": {
                    "priority": "critical",
                    "retention_days": 365
                }
            },
            {
                "packet_id": "disaster_event_earthquake_delhi_20240825_001",
                "packet_type": "discrete_disaster_event",
                "kafka_topic": "disaster-data-ingestion",
                "timestamp": "2024-08-25T08:15:00",
                "temporal": {
                    "start_date": "2024-08-25",
                    "end_date": "2024-08-25",
                    "duration_days": 0
                },
                "spatial": {
                    "primary_location": "Delhi",
                    "affected_locations": ["Delhi", "NCR", "Gurgaon"],
                    "location_count": 3
                },
                "impact": {
                    "deaths": 0,
                    "injured": 5,
                    "displaced": 0,
                    "affected": 500
                },
                "metadata": {
                    "disaster_type": "earthquakes",
                    "severity": "medium",
                    "event_name": "Delhi Tremors",
                    "source": {
                        "url": "https://www.hindustantimes.com/delhi-earthquake",
                        "domain": "hindustantimes.com",
                        "title": "Earthquake Tremors Felt in Delhi-NCR"
                    },
                    "relevance_score": 6
                },
                "processing_instructions": {
                    "priority": "normal",
                    "retention_days": 365
                }
            }
        ]

    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=os.getenv("KAFKA_CONSUMER_GROUP", "disaster-consumer-group"),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=timeout_ms
        )

        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= batch_size:
                break

        consumer.close()
        logger.info(f"Consumed {len(messages)} messages from Kafka")

        return messages

    except ImportError:
        logger.warning("kafka-python not installed, using mock data")
        return consume_kafka_messages(topic, batch_size, timeout_ms, use_mock=True)
    except Exception as e:
        logger.error(f"Failed to consume from Kafka: {str(e)}")
        logger.warning("Falling back to mock data")
        return consume_kafka_messages(topic, batch_size, timeout_ms, use_mock=True)


# ============================================================================
# LangGraph Node Functions
# ============================================================================

def node_consume_kafka(state: ConsumerState) -> ConsumerState:
    """Node 1: Consume messages from Kafka"""
    logger.info("=== Node 1: Consuming from Kafka ===")

    messages = consume_kafka_messages(
        topic="disaster-data-ingestion",
        batch_size=state.get("batch_size", 10),
        use_mock=False
    )

    state["kafka_messages"] = messages
    state["status"] = "kafka_consumed"
    state["timestamp"] = datetime.datetime.now().isoformat()

    logger.info(f"Consumed {len(messages)} messages")
    return state


def node_validate_packets(state: ConsumerState) -> ConsumerState:
    """Node 2: Validate packet structure and data"""
    logger.info("=== Node 2: Validating Packets ===")

    validated = []
    invalid = []

    required_fields = ["packet_id", "packet_type", "metadata"]

    for packet in state["kafka_messages"]:
        try:
            # Check required fields
            if all(field in packet for field in required_fields):
                # Validate disaster_type exists
                disaster_type = packet.get("metadata", {}).get("disaster_type")
                if disaster_type:
                    validated.append(packet)
                else:
                    invalid.append({
                        "packet": packet,
                        "reason": "Missing disaster_type in metadata"
                    })
            else:
                invalid.append({
                    "packet": packet,
                    "reason": f"Missing required fields: {required_fields}"
                })
        except Exception as e:
            invalid.append({
                "packet": packet,
                "reason": f"Validation error: {str(e)}"
            })

    state["validated_packets"] = validated
    state["invalid_packets"] = invalid
    state["status"] = "packets_validated"

    logger.info(f"Validated: {len(validated)}, Invalid: {len(invalid)}")
    return state


def node_transform_data(state: ConsumerState) -> ConsumerState:
    """Node 3: Transform packets to database schema"""
    logger.info("=== Node 3: Transforming Data ===")

    transformed = []

    for packet in state["validated_packets"]:
        try:
            temporal = packet.get("temporal", {})
            spatial = packet.get("spatial", {})
            impact = packet.get("impact", {})
            metadata = packet.get("metadata", {})
            source = metadata.get("source", {})
            processing = packet.get("processing_instructions", {})

            record = {
                "packet_id": packet.get("packet_id"),
                "packet_type": packet.get("packet_type"),
                "disaster_type": metadata.get("disaster_type"),

                # Temporal
                "event_start_date": temporal.get("start_date"),
                "event_end_date": temporal.get("end_date"),
                "duration_days": temporal.get("duration_days"),

                # Spatial
                "primary_location": spatial.get("primary_location"),
                "affected_locations": spatial.get("affected_locations", []),
                "location_count": spatial.get("location_count", 0),

                # Impact
                "deaths": impact.get("deaths", 0),
                "injured": impact.get("injured", 0),
                "displaced": impact.get("displaced", 0),
                "affected": impact.get("affected", 0),
                "damage_amount": impact.get("damage_amount"),

                # Metadata
                "severity": metadata.get("severity"),
                "source_url": source.get("url"),
                "source_domain": source.get("domain"),
                "source_title": source.get("title"),
                "relevance_score": metadata.get("relevance_score"),

                # Processing
                "priority": processing.get("priority"),
                "retention_days": processing.get("retention_days", 365),

                # Raw
                "raw_packet": packet
            }

            transformed.append(record)

        except Exception as e:
            logger.error(f"Failed to transform packet {packet.get('packet_id')}: {str(e)}")
            state["errors"].append(f"Transform error: {str(e)}")

    state["transformed_records"] = transformed
    state["status"] = "data_transformed"

    logger.info(f"Transformed {len(transformed)} records")
    return state


def node_store_in_postgres(state: ConsumerState) -> ConsumerState:
    """Node 4: Store data in PostgreSQL"""
    logger.info("=== Node 4: Storing in PostgreSQL ===")

    try:
        stored_count = store_disaster_events(state["transformed_records"])
        state["stored_count"] = stored_count
        state["failed_count"] = len(state["validated_packets"]) - stored_count
        state["status"] = "data_stored"

        logger.info(f"Stored {stored_count} records in database")

    except Exception as e:
        logger.error(f"Failed to store data: {str(e)}")
        state["errors"].append(f"Storage error: {str(e)}")
        state["stored_count"] = 0
        state["failed_count"] = len(state["validated_packets"])
        state["status"] = "storage_failed"

    return state


def node_update_statistics(state: ConsumerState) -> ConsumerState:
    """Node 5: Update consumption statistics"""
    logger.info("=== Node 5: Updating Statistics ===")

    # Calculate statistics
    disaster_type_breakdown = {}
    severity_breakdown = {}

    for record in state["transformed_records"]:
        dtype = record.get("disaster_type", "unknown")
        disaster_type_breakdown[dtype] = disaster_type_breakdown.get(dtype, 0) + 1

        severity = record.get("severity", "unknown")
        severity_breakdown[severity] = severity_breakdown.get(severity, 0) + 1

    stats = {
        "messages_consumed": len(state["kafka_messages"]),
        "messages_stored": state["stored_count"],
        "messages_failed": state["failed_count"],
        "disaster_type_breakdown": disaster_type_breakdown,
        "severity_breakdown": severity_breakdown,
        "processing_time_seconds": 0.0  # TODO: Add timing
    }

    state["statistics"] = stats
    state["status"] = "completed"

    try:
        store_statistics(stats)
    except Exception as e:
        logger.error(f"Failed to store statistics: {str(e)}")

    logger.info(f"Statistics: {stats}")
    return state


# ============================================================================
# LangGraph Workflow Definition
# ============================================================================

def create_consumer_workflow() -> StateGraph:
    """Create the LangGraph workflow for disaster data consumption"""

    workflow = StateGraph(ConsumerState)

    # Add nodes
    workflow.add_node("consume_kafka", node_consume_kafka)
    workflow.add_node("validate_packets", node_validate_packets)
    workflow.add_node("transform_data", node_transform_data)
    workflow.add_node("store_in_postgres", node_store_in_postgres)
    workflow.add_node("update_statistics", node_update_statistics)

    # Define edges (flow)
    workflow.set_entry_point("consume_kafka")
    workflow.add_edge("consume_kafka", "validate_packets")
    workflow.add_edge("validate_packets", "transform_data")
    workflow.add_edge("transform_data", "store_in_postgres")
    workflow.add_edge("store_in_postgres", "update_statistics")
    workflow.add_edge("update_statistics", END)

    return workflow.compile()


# ============================================================================
# Main Consumer Function
# ============================================================================

def run_disaster_consumer(batch_size: int = 10) -> Dict[str, Any]:
    """
    Run the disaster data consumer workflow

    Args:
        batch_size: Number of messages to consume in one batch

    Returns:
        Final state with statistics
    """
    logger.info("="*80)
    logger.info("Starting Disaster Data Kafka Consumer")
    logger.info("="*80)

    # Initialize state
    initial_state: ConsumerState = {
        "kafka_messages": [],
        "batch_size": batch_size,
        "validated_packets": [],
        "invalid_packets": [],
        "transformed_records": [],
        "stored_count": 0,
        "failed_count": 0,
        "statistics": {},
        "errors": [],
        "status": "initialized",
        "timestamp": datetime.datetime.now().isoformat()
    }

    # Create and run workflow
    workflow = create_consumer_workflow()
    final_state = workflow.invoke(initial_state)

    logger.info("="*80)
    logger.info("Consumer Workflow Completed")
    logger.info(f"Status: {final_state['status']}")
    logger.info(f"Stored: {final_state['stored_count']}, Failed: {final_state['failed_count']}")
    logger.info("="*80)

    return final_state


# ============================================================================
# Query Functions for Data Retrieval
# ============================================================================

def query_disasters_by_type(disaster_type: str) -> List[Dict[str, Any]]:
    """Query disasters by type"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT * FROM disaster_events
            WHERE disaster_type = %s
            ORDER BY event_start_date DESC
        """, (disaster_type,))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return results
    finally:
        cursor.close()
        conn.close()


def query_disasters_by_location(location: str) -> List[Dict[str, Any]]:
    """Query disasters by location"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT * FROM disaster_events
            WHERE primary_location ILIKE %s
               OR %s = ANY(affected_locations)
            ORDER BY event_start_date DESC
        """, (f"%{location}%", location))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return results
    finally:
        cursor.close()
        conn.close()


def query_disasters_by_date_range(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Query disasters by date range"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT * FROM disaster_events
            WHERE event_start_date >= %s AND event_end_date <= %s
            ORDER BY event_start_date DESC
        """, (start_date, end_date))

        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return results
    finally:
        cursor.close()
        conn.close()


def get_statistics_summary() -> Dict[str, Any]:
    """Get overall statistics summary"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Total events by type
        cursor.execute("""
            SELECT disaster_type, COUNT(*) as count
            FROM disaster_events
            GROUP BY disaster_type
        """)
        by_type = dict(cursor.fetchall())

        # Total casualties
        cursor.execute("""
            SELECT
                SUM(deaths) as total_deaths,
                SUM(injured) as total_injured,
                SUM(displaced) as total_displaced,
                SUM(affected) as total_affected
            FROM disaster_events
        """)
        casualties = cursor.fetchone()

        # Events by severity
        cursor.execute("""
            SELECT severity, COUNT(*) as count
            FROM disaster_events
            GROUP BY severity
        """)
        by_severity = dict(cursor.fetchall())

        return {
            "total_events": sum(by_type.values()),
            "by_disaster_type": by_type,
            "total_deaths": casualties[0] or 0,
            "total_injured": casualties[1] or 0,
            "total_displaced": casualties[2] or 0,
            "total_affected": casualties[3] or 0,
            "by_severity": by_severity
        }
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Initialize database on first run
    try:
        initialize_database()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

    # Run consumer
    result = run_disaster_consumer(batch_size=10)
    print(f"\nFinal Statistics: {json.dumps(result['statistics'], indent=2)}")
