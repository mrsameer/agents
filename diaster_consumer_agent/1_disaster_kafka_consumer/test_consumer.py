"""
Test script for Disaster Data Kafka Consumer

This script tests the consumer with mock data (no Kafka required)
"""

import json
from agent import (
    run_disaster_consumer,
    initialize_database,
    query_disasters_by_type,
    query_disasters_by_location,
    get_statistics_summary,
    consume_kafka_messages,
)


def test_initialize_database():
    """Test 1: Database initialization"""
    print("\n" + "=" * 80)
    print("TEST 1: Database Initialization")
    print("=" * 80)

    try:
        initialize_database()
        print("✓ Database initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_mock_data_consumption():
    """Test 2: Mock data consumption"""
    print("\n" + "=" * 80)
    print("TEST 2: Mock Data Consumption")
    print("=" * 80)

    try:
        messages = consume_kafka_messages(use_mock=True)
        print(f"✓ Retrieved {len(messages)} mock messages")

        if messages:
            print(f"\nSample message:")
            print(f"  Packet ID: {messages[0].get('packet_id')}")
            print(f"  Disaster Type: {messages[0].get('metadata', {}).get('disaster_type')}")
            print(f"  Location: {messages[0].get('spatial', {}).get('primary_location')}")
            print(f"  Severity: {messages[0].get('metadata', {}).get('severity')}")

        return len(messages) > 0
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_consumer_workflow():
    """Test 3: Complete consumer workflow with mock data"""
    print("\n" + "=" * 80)
    print("TEST 3: Consumer Workflow (with Mock Data)")
    print("=" * 80)

    try:
        # Run consumer with mock data
        result = run_disaster_consumer(batch_size=10)

        print(f"\nWorkflow Status: {result['status']}")
        print(f"Messages consumed: {len(result['kafka_messages'])}")
        print(f"Validated packets: {len(result['validated_packets'])}")
        print(f"Invalid packets: {len(result['invalid_packets'])}")
        print(f"Stored in DB: {result['stored_count']}")
        print(f"Failed: {result['failed_count']}")

        if result['statistics']:
            print(f"\nStatistics:")
            stats = result['statistics']
            print(f"  Disaster types: {stats.get('disaster_type_breakdown', {})}")
            print(f"  Severity: {stats.get('severity_breakdown', {})}")

        print("\n✓ Consumer workflow completed successfully")
        return result['stored_count'] > 0
    except Exception as e:
        print(f"✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_query_by_type():
    """Test 4: Query disasters by type"""
    print("\n" + "=" * 80)
    print("TEST 4: Query by Disaster Type")
    print("=" * 80)

    try:
        disaster_types = ["floods", "cyclones", "earthquakes"]
        total_found = 0

        for disaster_type in disaster_types:
            results = query_disasters_by_type(disaster_type)
            total_found += len(results)
            print(f"  {disaster_type}: {len(results)} events")

            if results:
                latest = results[0]
                print(f"    Latest: {latest.get('primary_location')} on {latest.get('event_start_date')}")

        print(f"\n✓ Total events found: {total_found}")
        return total_found > 0
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_query_by_location():
    """Test 5: Query disasters by location"""
    print("\n" + "=" * 80)
    print("TEST 5: Query by Location")
    print("=" * 80)

    try:
        locations = ["Kerala", "Odisha", "Delhi"]
        total_found = 0

        for location in locations:
            results = query_disasters_by_location(location)
            total_found += len(results)
            print(f"  {location}: {len(results)} events")

            if results:
                # Calculate casualties
                deaths = sum(e.get('deaths', 0) for e in results)
                injured = sum(e.get('injured', 0) for e in results)
                print(f"    Casualties: {deaths} deaths, {injured} injured")

        print(f"\n✓ Total events found: {total_found}")
        return total_found > 0
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_statistics_summary():
    """Test 6: Get statistics summary"""
    print("\n" + "=" * 80)
    print("TEST 6: Statistics Summary")
    print("=" * 80)

    try:
        stats = get_statistics_summary()

        print(f"\nTotal Events: {stats['total_events']}")
        print(f"\nBy Disaster Type:")
        for dtype, count in stats['by_disaster_type'].items():
            print(f"  {dtype}: {count}")

        print(f"\nTotal Casualties:")
        print(f"  Deaths: {stats['total_deaths']}")
        print(f"  Injured: {stats['total_injured']}")
        print(f"  Displaced: {stats['total_displaced']}")
        print(f"  Affected: {stats['total_affected']}")

        print(f"\nBy Severity:")
        for severity, count in stats['by_severity'].items():
            print(f"  {severity}: {count}")

        print("\n✓ Statistics retrieved successfully")
        return stats['total_events'] > 0
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_validation():
    """Test 7: Packet validation"""
    print("\n" + "=" * 80)
    print("TEST 7: Packet Validation")
    print("=" * 80)

    try:
        from agent import node_validate_packets, ConsumerState

        # Create test state with valid and invalid packets
        state: ConsumerState = {
            "kafka_messages": [
                {
                    "packet_id": "test_001",
                    "packet_type": "discrete_disaster_event",
                    "metadata": {"disaster_type": "floods"}
                },
                {
                    "packet_id": "test_002",
                    "packet_type": "discrete_disaster_event",
                    # Missing metadata
                },
                {
                    "packet_id": "test_003",
                    "packet_type": "discrete_disaster_event",
                    "metadata": {}  # Missing disaster_type
                }
            ],
            "batch_size": 10,
            "validated_packets": [],
            "invalid_packets": [],
            "transformed_records": [],
            "stored_count": 0,
            "failed_count": 0,
            "statistics": {},
            "errors": [],
            "status": "initialized",
            "timestamp": ""
        }

        result = node_validate_packets(state)

        print(f"Valid packets: {len(result['validated_packets'])}")
        print(f"Invalid packets: {len(result['invalid_packets'])}")

        for invalid in result['invalid_packets']:
            print(f"  Reason: {invalid.get('reason')}")

        print("\n✓ Validation logic works correctly")
        return len(result['validated_packets']) == 1 and len(result['invalid_packets']) == 2
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def test_data_transformation():
    """Test 8: Data transformation"""
    print("\n" + "=" * 80)
    print("TEST 8: Data Transformation")
    print("=" * 80)

    try:
        from agent import node_transform_data, ConsumerState

        # Create test state with validated packet
        state: ConsumerState = {
            "kafka_messages": [],
            "batch_size": 10,
            "validated_packets": [
                {
                    "packet_id": "test_transform_001",
                    "packet_type": "discrete_disaster_event",
                    "temporal": {
                        "start_date": "2024-08-15",
                        "end_date": "2024-08-17",
                        "duration_days": 2
                    },
                    "spatial": {
                        "primary_location": "Kerala",
                        "affected_locations": ["Kerala", "Wayanad"],
                        "location_count": 2
                    },
                    "impact": {
                        "deaths": 25,
                        "injured": 50,
                        "displaced": 1000
                    },
                    "metadata": {
                        "disaster_type": "floods",
                        "severity": "high",
                        "source": {
                            "url": "https://example.com",
                            "domain": "example.com",
                            "title": "Test"
                        },
                        "relevance_score": 8
                    },
                    "processing_instructions": {
                        "priority": "high",
                        "retention_days": 365
                    }
                }
            ],
            "invalid_packets": [],
            "transformed_records": [],
            "stored_count": 0,
            "failed_count": 0,
            "statistics": {},
            "errors": [],
            "status": "packets_validated",
            "timestamp": ""
        }

        result = node_transform_data(state)

        print(f"Transformed records: {len(result['transformed_records'])}")

        if result['transformed_records']:
            record = result['transformed_records'][0]
            print(f"\nSample transformed record:")
            print(f"  Packet ID: {record.get('packet_id')}")
            print(f"  Disaster Type: {record.get('disaster_type')}")
            print(f"  Start Date: {record.get('event_start_date')}")
            print(f"  Location: {record.get('primary_location')}")
            print(f"  Deaths: {record.get('deaths')}")
            print(f"  Severity: {record.get('severity')}")

        print("\n✓ Transformation logic works correctly")
        return len(result['transformed_records']) == 1
    except Exception as e:
        print(f"✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("DISASTER DATA KAFKA CONSUMER - TEST SUITE")
    print("=" * 80)

    tests = [
        ("Database Initialization", test_initialize_database),
        ("Mock Data Consumption", test_mock_data_consumption),
        ("Consumer Workflow", test_consumer_workflow),
        ("Query by Type", test_query_by_type),
        ("Query by Location", test_query_by_location),
        ("Statistics Summary", test_statistics_summary),
        ("Packet Validation", test_validation),
        ("Data Transformation", test_data_transformation),
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ Test failed with exception: {e}")
            results[test_name] = False

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for r in results.values() if r)
    total = len(results)

    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
