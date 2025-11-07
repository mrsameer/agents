"""
Script to verify disaster events stored in PostgreSQL database
"""

import psycopg2
import json
from datetime import datetime


def connect_db():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="disaster_data",
        user="postgres",
        password="postgres"
    )


def query_all_events():
    """Query all disaster events from database"""
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                id, packet_id, disaster_type, severity,
                event_start_date, event_end_date, duration_days,
                primary_location, affected_locations,
                deaths, injured, displaced, affected, damage_amount,
                source_url, source_title, priority,
                ingestion_timestamp
            FROM disaster_events
            ORDER BY ingestion_timestamp DESC
        """)

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        print("="*80)
        print(f"DISASTER EVENTS IN DATABASE")
        print("="*80)
        print(f"\nTotal events found: {len(rows)}\n")

        for row in rows:
            event = dict(zip(columns, row))
            print(f"{'='*80}")
            print(f"Event ID: {event['id']}")
            print(f"Packet ID: {event['packet_id']}")
            print(f"Type: {event['disaster_type'].upper()}")
            print(f"Severity: {event['severity']}")
            print(f"Location: {event['primary_location']}")
            print(f"Affected Locations: {', '.join(event['affected_locations']) if event['affected_locations'] else 'N/A'}")
            print(f"Date Range: {event['event_start_date']} to {event['event_end_date']} ({event['duration_days']} days)")
            print(f"\nImpact:")
            print(f"  - Deaths: {event['deaths']}")
            print(f"  - Injured: {event['injured']}")
            print(f"  - Displaced: {event['displaced']}")
            print(f"  - Affected: {event['affected']}")
            print(f"  - Damage: ${event['damage_amount']:,.2f}" if event['damage_amount'] else "  - Damage: N/A")
            print(f"\nSource: {event['source_title']}")
            print(f"URL: {event['source_url']}")
            print(f"Priority: {event['priority']}")
            print(f"Ingested: {event['ingestion_timestamp']}")
            print()

    finally:
        cursor.close()
        conn.close()


def query_statistics():
    """Query consumption statistics"""
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Get summary statistics
        cursor.execute("""
            SELECT
                COUNT(*) as total_events,
                SUM(deaths) as total_deaths,
                SUM(injured) as total_injured,
                SUM(displaced) as total_displaced,
                SUM(affected) as total_affected,
                SUM(damage_amount) as total_damage
            FROM disaster_events
        """)

        stats = cursor.fetchone()

        print("="*80)
        print("SUMMARY STATISTICS")
        print("="*80)
        print(f"Total Events: {stats[0]}")
        print(f"Total Deaths: {stats[1]}")
        print(f"Total Injured: {stats[2]}")
        print(f"Total Displaced: {stats[3]}")
        print(f"Total Affected: {stats[4]}")
        print(f"Total Damage: ${stats[5]:,.2f}" if stats[5] else "Total Damage: N/A")

        # Events by disaster type
        cursor.execute("""
            SELECT disaster_type, COUNT(*) as count
            FROM disaster_events
            GROUP BY disaster_type
            ORDER BY count DESC
        """)

        print("\nEvents by Type:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]}")

        # Events by severity
        cursor.execute("""
            SELECT severity, COUNT(*) as count
            FROM disaster_events
            GROUP BY severity
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                END
        """)

        print("\nEvents by Severity:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]}")

        # Consumption statistics
        cursor.execute("""
            SELECT
                messages_consumed, messages_stored, messages_failed,
                disaster_type_breakdown, severity_breakdown,
                batch_timestamp
            FROM consumption_statistics
            ORDER BY batch_timestamp DESC
            LIMIT 5
        """)

        print("\n" + "="*80)
        print("RECENT CONSUMPTION BATCHES")
        print("="*80)

        for row in cursor.fetchall():
            print(f"\nBatch Time: {row[5]}")
            print(f"  Consumed: {row[0]}, Stored: {row[1]}, Failed: {row[2]}")
            print(f"  Types: {row[3]}")
            print(f"  Severity: {row[4]}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    try:
        query_all_events()
        print()
        query_statistics()
        print("\n" + "="*80)
        print("VERIFICATION COMPLETE")
        print("="*80)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
