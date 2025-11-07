#!/usr/bin/env python3
"""
Simple script to run the Disaster Data Kafka Consumer

Usage:
    python run_consumer.py                    # Run once (batch mode)
    python run_consumer.py --continuous       # Run continuously
    python run_consumer.py --batch-size 50    # Custom batch size
    python run_consumer.py --init-db          # Initialize database first
"""

import argparse
import time
import signal
import sys
from agent import run_disaster_consumer, initialize_database


# Flag for graceful shutdown
running = True


def signal_handler(sig, frame):
    """Handle Ctrl+C for graceful shutdown"""
    global running
    print("\n\n⚠️  Shutting down consumer gracefully...")
    running = False


def main():
    parser = argparse.ArgumentParser(description="Run Disaster Data Kafka Consumer")

    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously (daemon mode)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of messages to consume per batch (default: 10)"
    )

    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds between polls in continuous mode (default: 5)"
    )

    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database schema before running"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output"
    )

    args = parser.parse_args()

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("=" * 80)
    print("DISASTER DATA KAFKA CONSUMER")
    print("=" * 80)

    # Initialize database if requested
    if args.init_db:
        print("\n📊 Initializing database schema...")
        try:
            initialize_database()
            print("✓ Database initialized successfully")
        except Exception as e:
            print(f"✗ Database initialization failed: {e}")
            sys.exit(1)

    # Run consumer
    if args.continuous:
        print(f"\n🔄 Running in CONTINUOUS mode")
        print(f"   Batch size: {args.batch_size}")
        print(f"   Poll interval: {args.poll_interval}s")
        print(f"   Press Ctrl+C to stop\n")

        batch_count = 0

        while running:
            try:
                result = run_disaster_consumer(batch_size=args.batch_size)
                batch_count += 1

                # Print summary
                if args.verbose:
                    print(f"\n{'=' * 80}")
                    print(f"Batch #{batch_count} - {result['timestamp']}")
                    print(f"{'=' * 80}")
                    print(f"Status: {result['status']}")
                    print(f"Messages consumed: {len(result['kafka_messages'])}")
                    print(f"Stored: {result['stored_count']}, Failed: {result['failed_count']}")

                    if result['statistics']:
                        stats = result['statistics']
                        if stats.get('disaster_type_breakdown'):
                            print(f"By type: {stats['disaster_type_breakdown']}")
                        if stats.get('severity_breakdown'):
                            print(f"By severity: {stats['severity_breakdown']}")
                else:
                    # Simple progress indicator
                    print(f"Batch #{batch_count}: {result['stored_count']} stored, "
                          f"{result['failed_count']} failed", end='\r')

                # Wait before next poll
                if running:
                    time.sleep(args.poll_interval)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"\n✗ Error in batch #{batch_count}: {e}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()

                # Wait before retry
                if running:
                    time.sleep(args.poll_interval)

        print(f"\n\n✓ Consumer stopped. Processed {batch_count} batches.")

    else:
        print(f"\n▶️  Running in ONE-SHOT mode")
        print(f"   Batch size: {args.batch_size}\n")

        try:
            result = run_disaster_consumer(batch_size=args.batch_size)

            print(f"{'=' * 80}")
            print(f"RESULTS")
            print(f"{'=' * 80}")
            print(f"Status: {result['status']}")
            print(f"Messages consumed: {len(result['kafka_messages'])}")
            print(f"Validated: {len(result['validated_packets'])}")
            print(f"Invalid: {len(result['invalid_packets'])}")
            print(f"Stored in DB: {result['stored_count']}")
            print(f"Failed: {result['failed_count']}")

            if result['statistics']:
                print(f"\nStatistics:")
                stats = result['statistics']
                if stats.get('disaster_type_breakdown'):
                    print(f"  Disaster types: {stats['disaster_type_breakdown']}")
                if stats.get('severity_breakdown'):
                    print(f"  Severity: {stats['severity_breakdown']}")

            if result['invalid_packets'] and args.verbose:
                print(f"\nInvalid Packets:")
                for invalid in result['invalid_packets']:
                    print(f"  - {invalid.get('reason')}")

            if result['errors'] and args.verbose:
                print(f"\nErrors:")
                for error in result['errors']:
                    print(f"  - {error}")

            print(f"{'=' * 80}")

        except Exception as e:
            print(f"✗ Consumer failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
