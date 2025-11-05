"""
Example usage of the Disaster Data Collection Agent

This script demonstrates the complete workflow:
1. Search for disaster data using seed queries
2. Crawl discovered URLs with Crawl4AI
3. Extract structured data from crawled content
4. Generate Kafka message packets
"""

import json
from agent import (
    root_agent,
    search_web_for_disaster_data,
    crawl_urls_with_ai,
    extract_structured_data,
    generate_kafka_packets,
    format_kafka_packets_for_output,
)


def example_1_search_only():
    """Example 1: Search for disaster data only"""
    print("=" * 80)
    print("Example 1: Search for Flood Data in India")
    print("=" * 80)

    result = search_web_for_disaster_data(disaster_type="floods", max_results=5)

    print(f"\nStatus: {result['status']}")
    print(f"Disaster Type: {result['disaster_type']}")
    print(f"Total URLs Found: {result['total_count']}")
    print(f"\nTop 3 Discovered URLs:")

    for idx, url_data in enumerate(result["discovered_urls"][:3], 1):
        print(f"\n{idx}. Title: {url_data['title']}")
        print(f"   URL: {url_data['url']}")
        print(f"   Domain: {url_data['domain']}")
        print(f"   Relevance Score: {url_data['relevance_score']}")
        print(f"   Snippet: {url_data['snippet'][:100]}...")

    return result


def example_2_search_and_crawl():
    """Example 2: Search and crawl URLs"""
    print("\n\n" + "=" * 80)
    print("Example 2: Search for Cyclone Data and Crawl URLs")
    print("=" * 80)

    # Step 1: Search
    print("\nStep 1: Searching for cyclone data...")
    search_result = search_web_for_disaster_data(disaster_type="cyclones", max_results=3)
    print(f"Found {search_result['total_count']} URLs")

    # Step 2: Crawl
    if search_result["discovered_urls"]:
        print("\nStep 2: Crawling discovered URLs...")
        urls = [item["url"] for item in search_result["discovered_urls"][:2]]
        crawl_result = crawl_urls_with_ai(urls)

        print(f"\nCrawl Results:")
        print(f"Success Count: {crawl_result['success_count']}")
        print(f"Error Count: {crawl_result['error_count']}")
        print(f"Total Size: {crawl_result['total_size_bytes']} bytes")

        return search_result, crawl_result
    else:
        print("No URLs to crawl")
        return search_result, None


def example_3_full_pipeline():
    """Example 3: Complete pipeline with Kafka packet generation"""
    print("\n\n" + "=" * 80)
    print("Example 3: Full Pipeline - Search, Crawl, Extract, Generate Kafka Packets")
    print("=" * 80)

    # Step 1: Search
    print("\nStep 1: Searching for earthquake data...")
    search_result = search_web_for_disaster_data(disaster_type="earthquakes", max_results=3)
    print(f"✓ Found {search_result['total_count']} URLs")

    if not search_result["discovered_urls"]:
        print("No URLs found. Exiting.")
        return

    # Step 2: Crawl
    print("\nStep 2: Crawling top URL...")
    urls = [search_result["discovered_urls"][0]["url"]]
    crawl_result = crawl_urls_with_ai(urls)
    print(f"✓ Crawled {crawl_result['success_count']} pages successfully")

    # Step 3: Extract structured data
    print("\nStep 3: Extracting structured data...")
    if crawl_result["crawled_data"]:
        html_content = crawl_result["crawled_data"][0].get("html", "")
        url = crawl_result["crawled_data"][0].get("url", "")

        extraction_result = extract_structured_data(html_content, url)
        print(f"✓ Extracted {len(extraction_result['structured_data']['paragraphs'])} paragraphs")
        print(f"✓ Found {len(extraction_result['structured_data']['tables'])} tables")
        print(f"✓ Identified {len(extraction_result['disaster_entities']['locations'])} locations")

        # Step 4: Generate Kafka packets
        print("\nStep 4: Generating Kafka message packets...")
        kafka_result = generate_kafka_packets(
            search_results=json.dumps(search_result),
            crawl_results=json.dumps(crawl_result),
            extraction_results=json.dumps(extraction_result),
        )
        print(f"✓ Generated {kafka_result['packet_count']} Kafka packets")

        # Step 5: Format for output
        print("\nStep 5: Formatting packets for output...")
        output = format_kafka_packets_for_output(json.dumps(kafka_result))

        print(f"\n{'=' * 80}")
        print("KAFKA PACKET SUMMARY")
        print(f"{'=' * 80}")
        print(f"Total Packets: {output['total_packets']}")
        print(f"Kafka Topic: {output['kafka_config']['topic']}")
        print(f"Serialization: {output['kafka_config']['serialization']}")

        print(f"\n{'=' * 80}")
        print("SAMPLE KAFKA PACKET")
        print(f"{'=' * 80}")
        print(json.dumps(output["sample_packet"], indent=2)[:1000] + "...")

        return output


def example_4_all_disaster_types():
    """Example 4: Search all disaster types"""
    print("\n\n" + "=" * 80)
    print("Example 4: Search All Disaster Types")
    print("=" * 80)

    result = search_web_for_disaster_data(disaster_type="all", max_results=2)

    print(f"\nTotal URLs Found: {result['total_count']}")
    print(f"\nURLs by Relevance:")

    for idx, url_data in enumerate(result["discovered_urls"][:5], 1):
        print(f"\n{idx}. Score: {url_data['relevance_score']} | {url_data['title'][:60]}...")
        print(f"   Query: {url_data['query']}")


def example_5_using_agent():
    """Example 5: Using the agent directly"""
    print("\n\n" + "=" * 80)
    print("Example 5: Using the Agent Directly")
    print("=" * 80)

    print("\nAgent Information:")
    print(f"Name: {root_agent.name}")
    print(f"Model: {root_agent.model}")
    print(f"Description: {root_agent.description}")

    print(f"\nAvailable Tools: {len(root_agent.tools)}")
    for tool in root_agent.tools:
        print(f"  - {tool.__name__}")

    print("\n" + "-" * 80)
    print("To interact with the agent, use:")
    print("  agent.run('Search for latest flood data in India')")
    print("  agent.run('Generate Kafka packets for cyclone warnings')")
    print("-" * 80)


def main():
    """Run all examples"""
    print("\n" + "=" * 80)
    print("DISASTER DATA COLLECTION AGENT - EXAMPLES")
    print("=" * 80)

    try:
        # Run examples
        example_1_search_only()
        example_2_search_and_crawl()
        example_3_full_pipeline()
        example_4_all_disaster_types()
        example_5_using_agent()

        print("\n\n" + "=" * 80)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)

    except Exception as e:
        print(f"\n\nError running examples: {str(e)}")
        print("Make sure all dependencies are installed:")
        print("  pip install duckduckgo-search crawl4ai beautifulsoup4 kafka-python")


if __name__ == "__main__":
    main()
