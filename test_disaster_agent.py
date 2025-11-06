#!/usr/bin/env python3
"""
Test script for the Disaster Data Collection Agent
Tests each module independently to identify issues
"""

import sys
import os
sys.path.insert(0, '/home/user/agents')

print("=" * 80)
print("TESTING DISASTER DATA COLLECTION AGENT")
print("=" * 80)

# Test 1: Import check
print("\n[1/6] Testing imports...")
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "agent",
        "/home/user/agents/google_adk/5_disaster_data_agent/agent.py"
    )
    agent_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(agent_module)

    search_web_for_disaster_data = agent_module.search_web_for_disaster_data
    crawl_urls_with_ai = agent_module.crawl_urls_with_ai
    extract_structured_data = agent_module.extract_structured_data
    generate_kafka_packets = agent_module.generate_kafka_packets
    format_kafka_packets_for_output = agent_module.format_kafka_packets_for_output
    root_agent = agent_module.root_agent

    print("✓ All imports successful")
except Exception as e:
    print(f"✗ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Web Search (US1.1)
print("\n[2/6] Testing Web Search Module (US1.1)...")
try:
    result = search_web_for_disaster_data(disaster_type="floods", max_results=2)
    print(f"Status: {result['status']}")
    print(f"Disaster Type: {result['disaster_type']}")
    print(f"URLs Found: {result['total_count']}")

    if result['status'] == 'success' and result['total_count'] > 0:
        print("✓ Web search successful")
        print(f"  Sample URL: {result['discovered_urls'][0]['url'][:60]}...")
        search_result = result  # Save for later tests
    else:
        print("⚠ Web search returned no results (might be network issue)")
        search_result = None
except Exception as e:
    print(f"✗ Web search failed: {e}")
    import traceback
    traceback.print_exc()
    search_result = None

# Test 3: HTML Extraction (US1.3) - test with sample HTML
print("\n[3/6] Testing Structured Data Extraction Module (US1.3)...")
try:
    sample_html = """
    <html>
    <head><title>Flood Alert in Kerala</title></head>
    <body>
        <h1>Major Flood Warning</h1>
        <p>Heavy flooding reported in Kerala on 15/01/2024 affecting multiple districts.</p>
        <p>The disaster has affected over 10,000 people in the region.</p>
        <table>
            <caption>Flood Statistics</caption>
            <tr><th>District</th><th>Affected</th></tr>
            <tr><td>Ernakulam</td><td>5000</td></tr>
            <tr><td>Kottayam</td><td>5000</td></tr>
        </table>
    </body>
    </html>
    """

    extraction_result = extract_structured_data(sample_html, "https://example.com/test")
    print(f"Status: {extraction_result['status']}")
    print(f"Title: {extraction_result['structured_data']['title']}")
    print(f"Paragraphs: {len(extraction_result['structured_data']['paragraphs'])}")
    print(f"Tables: {len(extraction_result['structured_data']['tables'])}")
    print(f"Locations found: {extraction_result['disaster_entities']['locations']}")
    print(f"Dates found: {extraction_result['disaster_entities']['dates']}")
    print("✓ Structured data extraction successful")
except Exception as e:
    print(f"✗ Extraction failed: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Kafka Packet Generation
print("\n[4/6] Testing Kafka Packet Generation...")
try:
    import json

    # Create mock search results
    mock_search = {
        "status": "success",
        "disaster_type": "floods",
        "discovered_urls": [
            {
                "url": "https://example.com/flood-news",
                "title": "Major Flood Alert",
                "domain": "example.com",
                "snippet": "Flooding reported in multiple states...",
                "query": "India floods",
                "relevance_score": 8,
                "timestamp": "2024-01-15T10:00:00",
            }
        ],
    }

    kafka_result = generate_kafka_packets(search_results=json.dumps(mock_search))
    print(f"Status: {kafka_result['status']}")
    print(f"Packets Generated: {kafka_result['packet_count']}")

    if kafka_result['packet_count'] > 0:
        packet = kafka_result['packets'][0]
        print(f"Packet ID: {packet['packet_id']}")
        print(f"Kafka Topic: {packet['kafka_topic']}")
        print(f"Priority: {packet['processing_instructions']['priority']}")
        print("✓ Kafka packet generation successful")
    else:
        print("⚠ No packets generated")
except Exception as e:
    print(f"✗ Kafka packet generation failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Crawl4AI (US1.2) - Skip if no URLs from search
print("\n[5/6] Testing Crawl4AI Module (US1.2)...")
if search_result and search_result.get('discovered_urls'):
    try:
        test_url = search_result['discovered_urls'][0]['url']
        print(f"Testing crawl on: {test_url[:60]}...")

        crawl_result = crawl_urls_with_ai([test_url])
        print(f"Status: {crawl_result['status']}")
        print(f"Success Count: {crawl_result['success_count']}")
        print(f"Error Count: {crawl_result['error_count']}")
        print(f"Total Size: {crawl_result['total_size_bytes']} bytes")

        if crawl_result['success_count'] > 0:
            print("✓ Crawl4AI successful")
        else:
            print("⚠ Crawl had errors")
    except Exception as e:
        print(f"✗ Crawl4AI failed: {e}")
        import traceback
        traceback.print_exc()
else:
    print("⚠ Skipping crawl test (no URLs from search)")

# Test 6: Agent Integration
print("\n[6/6] Testing Agent Integration...")
try:
    print(f"Agent Name: {root_agent.name}")
    print(f"Agent Model: {root_agent.model}")
    print(f"Agent Tools: {len(root_agent.tools)}")
    for tool in root_agent.tools:
        print(f"  - {tool.__name__}")
    print("✓ Agent initialized successfully")
except Exception as e:
    print(f"✗ Agent integration failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("TESTING COMPLETE")
print("=" * 80)
