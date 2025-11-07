"""
Simple script to produce disaster event packets to Kafka
This will generate sample disaster events and send them to the Kafka topic
"""

import json
import datetime
from kafka import KafkaProducer
import time


def create_disaster_events():
    """Create sample disaster event packets"""
    return [
        {
            "packet_id": "disaster_event_floods_kerala_20241107_001",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": "2024-11-07T10:30:00",
            "temporal": {
                "start_date": "2024-11-07",
                "end_date": "2024-11-09",
                "duration_days": 2,
                "temporal_description": "November 7-9, 2024"
            },
            "spatial": {
                "primary_location": "Kerala",
                "affected_locations": ["Kerala", "Wayanad", "Idukki"],
                "location_count": 3
            },
            "impact": {
                "deaths": 35,
                "injured": 75,
                "displaced": 2000,
                "affected": 8000,
                "damage_amount": 5000000.00
            },
            "metadata": {
                "disaster_type": "floods",
                "severity": "high",
                "event_name": "Kerala Monsoon Floods 2024",
                "source": {
                    "url": "https://www.thehindu.com/news/kerala-floods-2024",
                    "domain": "thehindu.com",
                    "title": "Heavy Floods Hit Kerala: 35 Dead, Thousands Displaced"
                },
                "relevance_score": 9
            },
            "processing_instructions": {
                "priority": "high",
                "retention_days": 365
            }
        },
        {
            "packet_id": "disaster_event_cyclone_odisha_20241105_001",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": "2024-11-05T14:00:00",
            "temporal": {
                "start_date": "2024-11-05",
                "end_date": "2024-11-07",
                "duration_days": 2
            },
            "spatial": {
                "primary_location": "Odisha",
                "affected_locations": ["Odisha", "Puri", "Bhubaneswar", "Cuttack"],
                "location_count": 4
            },
            "impact": {
                "deaths": 18,
                "injured": 45,
                "displaced": 3500,
                "affected": 15000,
                "damage_amount": 8000000.00
            },
            "metadata": {
                "disaster_type": "cyclones",
                "severity": "critical",
                "event_name": "Cyclone Dana Strikes Odisha Coast",
                "source": {
                    "url": "https://www.ndtv.com/cyclone-dana-odisha-2024",
                    "domain": "ndtv.com",
                    "title": "Cyclone Dana Makes Landfall: 18 Dead, Massive Evacuation"
                },
                "relevance_score": 9
            },
            "processing_instructions": {
                "priority": "critical",
                "retention_days": 365
            }
        },
        {
            "packet_id": "disaster_event_earthquake_delhi_20241106_001",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": "2024-11-06T08:15:00",
            "temporal": {
                "start_date": "2024-11-06",
                "end_date": "2024-11-06",
                "duration_days": 0
            },
            "spatial": {
                "primary_location": "Delhi",
                "affected_locations": ["Delhi", "NCR", "Gurgaon", "Noida"],
                "location_count": 4
            },
            "impact": {
                "deaths": 2,
                "injured": 12,
                "displaced": 50,
                "affected": 1000,
                "damage_amount": 500000.00
            },
            "metadata": {
                "disaster_type": "earthquakes",
                "severity": "medium",
                "event_name": "Delhi Earthquake 5.2 Magnitude",
                "source": {
                    "url": "https://www.hindustantimes.com/delhi-earthquake-2024",
                    "domain": "hindustantimes.com",
                    "title": "5.2 Magnitude Earthquake Shakes Delhi-NCR"
                },
                "relevance_score": 7
            },
            "processing_instructions": {
                "priority": "normal",
                "retention_days": 365
            }
        },
        {
            "packet_id": "disaster_event_landslide_uttarakhand_20241104_001",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": "2024-11-04T16:30:00",
            "temporal": {
                "start_date": "2024-11-04",
                "end_date": "2024-11-05",
                "duration_days": 1
            },
            "spatial": {
                "primary_location": "Uttarakhand",
                "affected_locations": ["Uttarakhand", "Chamoli", "Joshimath"],
                "location_count": 3
            },
            "impact": {
                "deaths": 8,
                "injured": 15,
                "displaced": 200,
                "affected": 500,
                "damage_amount": 1000000.00
            },
            "metadata": {
                "disaster_type": "landslides",
                "severity": "high",
                "event_name": "Uttarakhand Landslide Tragedy",
                "source": {
                    "url": "https://www.indianexpress.com/uttarakhand-landslide",
                    "domain": "indianexpress.com",
                    "title": "Landslide in Uttarakhand: 8 Dead, Several Missing"
                },
                "relevance_score": 8
            },
            "processing_instructions": {
                "priority": "high",
                "retention_days": 365
            }
        },
        {
            "packet_id": "disaster_event_drought_maharashtra_20241103_001",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": "2024-11-03T09:00:00",
            "temporal": {
                "start_date": "2024-06-01",
                "end_date": "2024-11-03",
                "duration_days": 155
            },
            "spatial": {
                "primary_location": "Maharashtra",
                "affected_locations": ["Maharashtra", "Marathwada", "Vidarbha", "Beed"],
                "location_count": 4
            },
            "impact": {
                "deaths": 5,
                "injured": 0,
                "displaced": 0,
                "affected": 50000,
                "damage_amount": 20000000.00
            },
            "metadata": {
                "disaster_type": "droughts",
                "severity": "high",
                "event_name": "Maharashtra Drought Crisis 2024",
                "source": {
                    "url": "https://www.thehindu.com/maharashtra-drought-2024",
                    "domain": "thehindu.com",
                    "title": "Severe Drought in Maharashtra: 50,000 Affected"
                },
                "relevance_score": 8
            },
            "processing_instructions": {
                "priority": "high",
                "retention_days": 365
            }
        }
    ]


def produce_to_kafka():
    """Produce disaster events to Kafka"""
    print("="*80)
    print("DISASTER EVENT PRODUCER")
    print("="*80)

    # Create Kafka producer
    print("\nConnecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

    print("Connected to Kafka successfully!")

    # Get disaster events
    events = create_disaster_events()
    print(f"\nProducing {len(events)} disaster events to Kafka topic 'disaster-data-ingestion'...")

    # Send each event to Kafka
    success_count = 0
    for event in events:
        try:
            # Use packet_id as the key for partitioning
            future = producer.send(
                'disaster-data-ingestion',
                key=event['packet_id'],
                value=event
            )

            # Wait for confirmation
            record_metadata = future.get(timeout=10)

            print(f"  ✓ Sent: {event['packet_id']} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            print(f"    Type: {event['metadata']['disaster_type']}, Location: {event['spatial']['primary_location']}")

            success_count += 1
            time.sleep(0.5)  # Small delay between messages

        except Exception as e:
            print(f"  ✗ Failed to send {event['packet_id']}: {str(e)}")

    # Flush and close
    producer.flush()
    producer.close()

    print(f"\n{'='*80}")
    print(f"Successfully produced {success_count}/{len(events)} events to Kafka")
    print(f"{'='*80}")


if __name__ == "__main__":
    try:
        produce_to_kafka()
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
