"""
Configuration for Disaster Data Kafka Consumer

All configurable parameters for the consumer:
- Kafka settings
- PostgreSQL settings
- Consumer behavior
- Processing options
"""

import os

# ============================================================================
# Kafka Configuration
# ============================================================================

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "disaster-data-ingestion"),
    "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "disaster-consumer-group"),
    "auto_offset_reset": "earliest",  # Start from beginning if no offset exists
    "enable_auto_commit": True,
    "consumer_timeout_ms": 5000,  # 5 seconds
    "max_poll_records": 100,  # Maximum messages per poll
    "session_timeout_ms": 30000,  # 30 seconds
    "heartbeat_interval_ms": 10000,  # 10 seconds
}

# ============================================================================
# PostgreSQL Configuration
# ============================================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "disaster_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "connect_timeout": 10,
    "options": "-c search_path=public",
}

# Connection pool settings
POSTGRES_POOL_CONFIG = {
    "minconn": 1,
    "maxconn": 10,
}

# ============================================================================
# Consumer Behavior Configuration
# ============================================================================

CONSUMER_CONFIG = {
    "batch_size": 10,  # Number of messages to process in one batch
    "poll_interval_seconds": 5,  # Time between polls
    "max_retries": 3,  # Maximum retry attempts for failed operations
    "retry_delay_seconds": 2,  # Delay between retries
    "enable_mock_mode": False,  # Use mock data for testing
    "continuous_mode": True,  # Run continuously or one-shot
}

# ============================================================================
# Processing Configuration
# ============================================================================

PROCESSING_CONFIG = {
    "validate_schema": True,  # Validate packet schema
    "store_invalid_packets": True,  # Store invalid packets for debugging
    "enable_deduplication": True,  # Check for duplicate packet_ids
    "enable_statistics": True,  # Track and store statistics
    "log_level": "INFO",  # Logging level
}

# Required fields for packet validation
REQUIRED_PACKET_FIELDS = [
    "packet_id",
    "packet_type",
    "metadata",
]

# Expected disaster types
VALID_DISASTER_TYPES = [
    "floods",
    "droughts",
    "cyclones",
    "earthquakes",
    "landslides",
]

# Valid severity levels
VALID_SEVERITY_LEVELS = [
    "low",
    "medium",
    "high",
    "critical",
]

# Valid priority levels
VALID_PRIORITY_LEVELS = [
    "low",
    "normal",
    "high",
    "critical",
]

# ============================================================================
# Database Schema Configuration
# ============================================================================

DB_SCHEMA_CONFIG = {
    "disaster_events_table": "disaster_events",
    "statistics_table": "consumption_statistics",
    "invalid_packets_table": "invalid_packets",
    "enable_partitioning": False,  # Enable table partitioning by date
    "retention_days": 365,  # Default retention for events
}

# ============================================================================
# Monitoring and Alerting Configuration
# ============================================================================

MONITORING_CONFIG = {
    "enable_metrics": True,
    "metrics_interval_seconds": 60,  # Report metrics every minute
    "alert_on_failures": False,  # Enable alerting
    "failure_threshold": 10,  # Alert after N consecutive failures
    "enable_health_check": True,
    "health_check_interval_seconds": 30,
}

# ============================================================================
# Logging Configuration
# ============================================================================

LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "file_logging": True,
    "console_logging": True,
    "log_file": "logs/disaster_consumer.log",
    "max_log_size_mb": 100,
    "backup_count": 5,
}

# ============================================================================
# Feature Flags
# ============================================================================

FEATURES = {
    "enable_kafka_consumption": True,
    "enable_postgres_storage": True,
    "enable_statistics_tracking": True,
    "enable_validation": True,
    "enable_transformation": True,
    "enable_mock_data": False,  # For testing without Kafka
}

# ============================================================================
# Query Configuration
# ============================================================================

QUERY_CONFIG = {
    "default_limit": 100,  # Default limit for queries
    "max_limit": 1000,  # Maximum allowed limit
    "enable_caching": False,  # Enable query result caching
    "cache_ttl_seconds": 300,  # Cache TTL (5 minutes)
}

# ============================================================================
# Environment-Specific Overrides
# ============================================================================

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

if ENVIRONMENT == "production":
    CONSUMER_CONFIG["batch_size"] = 100
    POSTGRES_POOL_CONFIG["maxconn"] = 50
    MONITORING_CONFIG["enable_metrics"] = True
    MONITORING_CONFIG["alert_on_failures"] = True
    LOGGING_CONFIG["level"] = "WARNING"

elif ENVIRONMENT == "testing":
    FEATURES["enable_mock_data"] = True
    CONSUMER_CONFIG["batch_size"] = 5
    CONSUMER_CONFIG["continuous_mode"] = False
    LOGGING_CONFIG["level"] = "DEBUG"

elif ENVIRONMENT == "development":
    CONSUMER_CONFIG["batch_size"] = 10
    LOGGING_CONFIG["level"] = "DEBUG"
    FEATURES["enable_mock_data"] = False
