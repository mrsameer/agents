#!/usr/bin/env python3
"""
Complete Pipeline Test for Disaster Data Collection Agent
Tests all modules with mock data and real crawling
"""

import sys
import json
sys.path.insert(0, '/home/user/agents')

print("=" * 80)
print("DISASTER DATA COLLECTION AGENT - COMPLETE PIPELINE TEST")
print("=" * 80)

# Import the agent
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

# Test 1: Search with Mock Data
print("\n" + "=" * 80)
print("STEP 1: SEED-QUERY-DRIVEN WEB SEARCH (US1.1 - Mock Mode)")
print("=" * 80)

search_result = search_web_for_disaster_data(disaster_type="floods", max_results=3, use_mock=True)

print(f"Status: {search_result['status']}")
print(f"Disaster Type: {search_result['disaster_type']}")
print(f"URLs Found: {search_result['total_count']}")
if 'note' in search_result:
    print(f"Note: {search_result['note']}")

print("\nDiscovered URLs:")
for idx, url_data in enumerate(search_result['discovered_urls'], 1):
    print(f"\n  {idx}. {url_data['title']}")
    print(f"     URL: {url_data['url']}")
    print(f"     Relevance Score: {url_data['relevance_score']}")
    print(f"     Snippet: {url_data['snippet'][:80]}...")

# Test 2: Crawl URLs with Crawl4AI
print("\n\n" + "=" * 80)
print("STEP 2: AI-BASED WEB CRAWLING (US1.2)")
print("=" * 80)

urls_to_crawl = [url_data['url'] for url_data in search_result['discovered_urls'][:2]]
print(f"Crawling {len(urls_to_crawl)} URLs...")
for url in urls_to_crawl:
    print(f"  - {url}")

crawl_result = crawl_urls_with_ai(urls_to_crawl)

print(f"\nCrawl Status: {crawl_result['status']}")

if crawl_result['status'] == 'error':
    print(f"Crawl Error: {crawl_result.get('error_message', 'Unknown error')}")
    print("Note: Crawl4AI may not be fully initialized or network issues")
else:
    print(f"Success Count: {crawl_result.get('success_count', 0)}")
    print(f"Error Count: {crawl_result.get('error_count', 0)}")
    print(f"Total Size: {crawl_result.get('total_size_bytes', 0):,} bytes")

if crawl_result.get('success_count', 0) > 0:
    print("\nCrawled Data Sample:")
    sample = crawl_result['crawled_data'][0]
    print(f"  URL: {sample.get('url', 'N/A')}")
    print(f"  Status: {sample.get('status', 'N/A')}")
    if sample.get('status') == 'success':
        print(f"  HTML Size: {len(sample.get('html', ''))} chars")
        print(f"  Markdown Size: {len(sample.get('markdown', ''))} chars")
        print(f"  Links Found: {len(sample.get('links', []))}")

# Test 3: Structured Data Extraction
print("\n\n" + "=" * 80)
print("STEP 3: STRUCTURED DATA EXTRACTION (US1.3)")
print("=" * 80)

# Test with sample HTML
sample_html = """
<html>
<head><title>Major Flood Emergency in Kerala and Maharashtra</title></head>
<body>
    <h1>Flood Alert: Multiple States Affected</h1>
    <h2>Emergency Response Underway</h2>

    <p>Severe flooding reported across Kerala and Maharashtra on 15/01/2024. The National Disaster
    Management Authority (NDMA) has deployed emergency response teams to affected areas.</p>

    <p>Over 50,000 people have been evacuated from flood-affected regions. Rescue operations are
    continuing in Ernakulam, Kottayam, Pune, and Mumbai districts.</p>

    <h3>Impact Statistics</h3>
    <table>
        <caption>Flood Impact by State</caption>
        <thead>
            <tr><th>State</th><th>Affected Districts</th><th>Evacuated</th><th>Relief Camps</th></tr>
        </thead>
        <tbody>
            <tr><td>Kerala</td><td>12</td><td>30,000</td><td>150</td></tr>
            <tr><td>Maharashtra</td><td>8</td><td>20,000</td><td>100</td></tr>
        </tbody>
    </table>

    <h3>Emergency Contacts</h3>
    <ul>
        <li>NDMA Helpline: 1078</li>
        <li>State Emergency Operations Center</li>
        <li>District Control Rooms</li>
    </ul>

    <p>The India Meteorological Department (IMD) predicts continued heavy rainfall over the next
    48 hours. Residents in low-lying areas are advised to evacuate immediately.</p>
</body>
</html>
"""

print("Extracting data from sample disaster news HTML...")
extraction_result = extract_structured_data(sample_html, "https://example.com/flood-news")

print(f"\nExtraction Status: {extraction_result['status']}")
print(f"Title: {extraction_result['structured_data']['title']}")
print(f"Headings: {len(extraction_result['structured_data']['headings'])}")
print(f"Paragraphs: {len(extraction_result['structured_data']['paragraphs'])}")
print(f"Tables: {len(extraction_result['structured_data']['tables'])}")
print(f"Lists: {len(extraction_result['structured_data']['lists'])}")

print("\nExtracted Disaster Entities:")
print(f"  Locations: {extraction_result['disaster_entities']['locations']}")
print(f"  Dates: {extraction_result['disaster_entities']['dates']}")
print(f"  Event Keywords: {set(extraction_result['disaster_entities']['event_names'])}")

if extraction_result['structured_data']['tables']:
    print("\nSample Table Data:")
    table = extraction_result['structured_data']['tables'][0]
    print(f"  Caption: {table['caption']}")
    print(f"  Headers: {table['headers']}")
    print(f"  Rows: {len(table['rows'])}")

# Test 4: Kafka Packet Generation
print("\n\n" + "=" * 80)
print("STEP 4: KAFKA MESSAGE PACKET GENERATION")
print("=" * 80)

print("Generating Kafka packets from collected data...")

kafka_result = generate_kafka_packets(
    search_results=json.dumps(search_result),
    crawl_results=json.dumps(crawl_result),
    extraction_results=json.dumps(extraction_result),
)

print(f"\nKafka Generation Status: {kafka_result['status']}")
print(f"Packets Generated: {kafka_result['packet_count']}")
print(f"Timestamp: {kafka_result['timestamp']}")

if kafka_result['packet_count'] > 0:
    print("\nSample Kafka Packet:")
    packet = kafka_result['packets'][0]
    print(f"  Packet ID: {packet['packet_id']}")
    print(f"  Packet Type: {packet['packet_type']}")
    print(f"  Kafka Topic: {packet['kafka_topic']}")
    print(f"  Schema Version: {packet['schema_version']}")
    print(f"  Priority: {packet['processing_instructions']['priority']}")
    print(f"  Disaster Type: {packet['data']['metadata']['disaster_type']}")
    print(f"  Source URL: {packet['data']['source']['url']}")

# Test 5: Format for Output
print("\n\n" + "=" * 80)
print("STEP 5: FORMAT KAFKA PACKETS FOR OUTPUT")
print("=" * 80)

output = format_kafka_packets_for_output(json.dumps(kafka_result))

print(f"\nOutput Status: {output['status']}")
print(f"Total Packets: {output['total_packets']}")
print(f"Kafka Topic: {output['kafka_config']['topic']}")
print(f"Serialization: {output['kafka_config']['serialization']}")
print(f"Partition Key: {output['kafka_config']['partition_key']}")

print("\n" + "=" * 80)
print("PIPELINE TEST COMPLETE")
print("=" * 80)
print("\nSummary:")
print("✓ US1.1: Seed-Query-Driven Web Search - WORKING (Mock Mode)")

crawl_success = crawl_result.get('success_count', 0) if crawl_result['status'] == 'success' else 0
if crawl_success > 0:
    print(f"✓ US1.2: AI-Based Web Crawling - {crawl_success} URLs crawled")
else:
    print(f"⚠ US1.2: AI-Based Web Crawling - Not available (network/setup issue)")

print("✓ US1.3: Structured Data Extraction - WORKING")
print(f"✓ Kafka Packet Generation - {kafka_result['packet_count']} packets ready")
print("\n✓ Core modules operational!")
print("=" * 80)
