#!/bin/bash

# Script to run the disaster consumer in continuous mode
# This will keep consuming events from Kafka and storing them in PostgreSQL

echo "Starting Disaster Kafka Consumer in Continuous Mode..."
echo "Press Ctrl+C to stop"
echo ""

cd /Users/shaiksameer/Downloads/vassar/projects/agents/diaster_consumer_agent/1_disaster_kafka_consumer

uv run python run_consumer.py --continuous --batch-size 10 --poll-interval 5 --verbose
