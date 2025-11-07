"""
Example Usage of Disaster Data Kafka Consumer

This script demonstrates different ways to use the consumer:
1. One-shot batch consumption
2. Continuous consumption
3. Querying stored data
4. Viewing statistics
"""

import json
from agent import (
    run_disaster_consumer,
    initialize_database,
    query_disasters_by_type,
    query_disasters_by_location,
    query_disasters_by_date_range,
    get_statistics_summary,
)


def example_1_initialize_database():
    """Example 1: Initialize the database schema"""
    print("=" * 80)
    print("Example 1: Initialize Database")
    print("=" * 80)

    try:
        initialize_database()
        print("✓ Database initialized successfully")
        print("  - disaster_events table created")
        print("  - consumption_statistics table created")
        print("  - Indexes created for optimal querying")
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")


def example_2_one_shot_consumption():
    """Example 2: Run consumer once (batch mode)"""
    print("\n" + "=" * 80)
    print("Example 2: One-Shot Consumption (Batch Mode)")
    print("=" * 80)

    print("\nRunning consumer for 10 messages...")
    result = run_disaster_consumer(batch_size=10)

    print(f"\n✓ Consumption completed")
    print(f"  Status: {result['status']}")
    print(f"  Messages consumed: {len(result['kafka_messages'])}")
    print(f"  Validated: {len(result['validated_packets'])}")
    print(f"  Invalid: {len(result['invalid_packets'])}")
    print(f"  Stored in DB: {result['stored_count']}")
    print(f"  Failed: {result['failed_count']}")

    if result['statistics']:
        print(f"\n  Statistics:")
        stats = result['statistics']
        print(f"    Disaster types: {stats.get('disaster_type_breakdown', {})}")
        print(f"    Severity: {stats.get('severity_breakdown', {})}")


def example_3_continuous_consumption():
    """Example 3: Continuous consumption (daemon mode)"""
    print("\n" + "=" * 80)
    print("Example 3: Continuous Consumption (Daemon Mode)")
    print("=" * 80)
    print("\nNote: This would run continuously. Use Ctrl+C to stop.")
    print("Code example:\n")

    print("""
    import time
    from agent import run_disaster_consumer

    print("Starting continuous consumer...")
    try:
        while True:
            result = run_disaster_consumer(batch_size=10)
            print(f"Batch completed: {result['stored_count']} stored")
            time.sleep(5)  # Wait 5 seconds between batches
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    """)


def example_4_query_by_disaster_type():
    """Example 4: Query disasters by type"""
    print("\n" + "=" * 80)
    print("Example 4: Query Disasters by Type")
    print("=" * 80)

    disaster_types = ["floods", "cyclones", "earthquakes"]

    for disaster_type in disaster_types:
        try:
            results = query_disasters_by_type(disaster_type)
            print(f"\n{disaster_type.upper()}:")
            print(f"  Total events: {len(results)}")

            if results:
                latest = results[0]
                print(f"  Latest event:")
                print(f"    - ID: {latest.get('packet_id')}")
                print(f"    - Date: {latest.get('event_start_date')}")
                print(f"    - Location: {latest.get('primary_location')}")
                print(f"    - Deaths: {latest.get('deaths')}")
                print(f"    - Severity: {latest.get('severity')}")
        except Exception as e:
            print(f"  Error querying {disaster_type}: {e}")


def example_5_query_by_location():
    """Example 5: Query disasters by location"""
    print("\n" + "=" * 80)
    print("Example 5: Query Disasters by Location")
    print("=" * 80)

    locations = ["Kerala", "Odisha", "Delhi"]

    for location in locations:
        try:
            results = query_disasters_by_location(location)
            print(f"\n{location.upper()}:")
            print(f"  Total events: {len(results)}")

            if results:
                # Group by disaster type
                by_type = {}
                for event in results:
                    dtype = event.get('disaster_type', 'unknown')
                    by_type[dtype] = by_type.get(dtype, 0) + 1

                print(f"  By type: {by_type}")

                # Calculate total casualties
                total_deaths = sum(e.get('deaths', 0) for e in results)
                total_injured = sum(e.get('injured', 0) for e in results)
                print(f"  Total casualties: {total_deaths} deaths, {total_injured} injured")
        except Exception as e:
            print(f"  Error querying {location}: {e}")


def example_6_query_by_date_range():
    """Example 6: Query disasters by date range"""
    print("\n" + "=" * 80)
    print("Example 6: Query Disasters by Date Range")
    print("=" * 80)

    start_date = "2024-08-01"
    end_date = "2024-08-31"

    try:
        results = query_disasters_by_date_range(start_date, end_date)
        print(f"\nEvents between {start_date} and {end_date}:")
        print(f"  Total events: {len(results)}")

        if results:
            print(f"\n  Sample events:")
            for idx, event in enumerate(results[:3], 1):
                print(f"\n  {idx}. {event.get('disaster_type', '').upper()}")
                print(f"     Date: {event.get('event_start_date')} to {event.get('event_end_date')}")
                print(f"     Location: {event.get('primary_location')}")
                print(f"     Impact: {event.get('deaths')} deaths, {event.get('displaced')} displaced")
                print(f"     Severity: {event.get('severity')}")
    except Exception as e:
        print(f"  Error: {e}")


def example_7_view_statistics():
    """Example 7: View overall statistics"""
    print("\n" + "=" * 80)
    print("Example 7: Overall Statistics Summary")
    print("=" * 80)

    try:
        stats = get_statistics_summary()

        print(f"\nTotal Events: {stats['total_events']}")

        print(f"\nBy Disaster Type:")
        for dtype, count in stats['by_disaster_type'].items():
            print(f"  - {dtype}: {count}")

        print(f"\nTotal Casualties:")
        print(f"  - Deaths: {stats['total_deaths']}")
        print(f"  - Injured: {stats['total_injured']}")
        print(f"  - Displaced: {stats['total_displaced']}")
        print(f"  - Affected: {stats['total_affected']}")

        print(f"\nBy Severity:")
        for severity, count in stats['by_severity'].items():
            print(f"  - {severity}: {count}")

    except Exception as e:
        print(f"  Error: {e}")


def example_8_custom_query():
    """Example 8: Custom SQL query example"""
    print("\n" + "=" * 80)
    print("Example 8: Custom Query (High Severity Events)")
    print("=" * 80)
    print("\nCode example:\n")

    print("""
    from agent import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()

    # Query high severity events with casualties
    cursor.execute('''
        SELECT disaster_type, primary_location, event_start_date,
               deaths, injured, severity
        FROM disaster_events
        WHERE severity = 'high' AND deaths > 0
        ORDER BY deaths DESC
        LIMIT 10
    ''')

    results = cursor.fetchall()
    for row in results:
        print(f"  {row[0]} in {row[1]}: {row[3]} deaths")

    cursor.close()
    conn.close()
    """)


def example_9_workflow_visualization():
    """Example 9: Visualize the LangGraph workflow"""
    print("\n" + "=" * 80)
    print("Example 9: LangGraph Workflow Visualization")
    print("=" * 80)

    print("""
    The consumer follows this LangGraph workflow:

    ┌─────────────────┐
    │ consume_kafka   │  Node 1: Read messages from Kafka
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │ validate_packets│  Node 2: Validate packet structure
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │ transform_data  │  Node 3: Transform to DB schema
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │store_in_postgres│  Node 4: Store in PostgreSQL
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │update_statistics│  Node 5: Update stats & metrics
    └────────┬────────┘
             │
             ↓
           [END]

    Each node processes the state and passes it to the next node.
    """)


def example_10_error_handling():
    """Example 10: Error handling and invalid packets"""
    print("\n" + "=" * 80)
    print("Example 10: Error Handling")
    print("=" * 80)

    print("\nRunning consumer and checking for errors...")
    result = run_disaster_consumer(batch_size=10)

    if result['invalid_packets']:
        print(f"\n✗ Found {len(result['invalid_packets'])} invalid packets:")
        for idx, invalid in enumerate(result['invalid_packets'][:3], 1):
            print(f"\n  {idx}. Reason: {invalid.get('reason')}")
            print(f"     Packet ID: {invalid.get('packet', {}).get('packet_id', 'N/A')}")

    if result['errors']:
        print(f"\n✗ Processing errors:")
        for error in result['errors']:
            print(f"  - {error}")

    if not result['invalid_packets'] and not result['errors']:
        print("\n✓ No errors or invalid packets!")


def main():
    """Run all examples"""
    print("\n" + "=" * 80)
    print("DISASTER DATA KAFKA CONSUMER - EXAMPLES")
    print("=" * 80)

    examples = [
        example_1_initialize_database,
        example_2_one_shot_consumption,
        example_3_continuous_consumption,
        example_4_query_by_disaster_type,
        example_5_query_by_location,
        example_6_query_by_date_range,
        example_7_view_statistics,
        example_8_custom_query,
        example_9_workflow_visualization,
        example_10_error_handling,
    ]

    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"\nError in {example.__name__}: {e}")

    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()
