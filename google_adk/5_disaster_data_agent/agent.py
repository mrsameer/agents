"""
Disaster Data Collection Agent

This autonomous agent implements a comprehensive disaster data collection system with:
- US1.1: Seed-Query-Driven Web Search Module
- US1.2: Crawl4AI Integration for AI-Based Web Crawling
- US1.3: Firecrawl for Structured Text and Table Extraction
- Kafka Message Packet Generation

The agent collects, processes, and structures disaster-related data from the web
and generates message packets ready for Kafka pipeline ingestion.
"""

import json
import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from google.adk.agents import Agent

# ============================================================================
# US1.1: Seed-Query-Driven Web Search Module
# ============================================================================

# Seed queries for various disaster types in India
SEED_QUERIES = {
    "floods": [
        "India floods latest news",
        "India flood disaster updates",
        "India monsoon flooding",
        "India flood affected areas",
    ],
    "droughts": [
        "India drought conditions",
        "India water scarcity news",
        "India drought affected regions",
        "India rainfall deficit",
    ],
    "cyclones": [
        "India cyclone latest update",
        "India tropical cyclone warning",
        "India Bay of Bengal cyclone",
        "India Arabian Sea cyclone",
    ],
    "earthquakes": [
        "India earthquake latest news",
        "India seismic activity",
        "India earthquake tremors",
        "India earthquake affected areas",
    ],
    "landslides": [
        "India landslide news",
        "India hill slope failure",
        "India landslide disaster",
        "India monsoon landslides",
    ],
}


def search_web_for_disaster_data(
    disaster_type: str = "all", max_results: int = 5, use_mock: bool = False
) -> dict:
    """
    Performs seed-query-driven web search for disaster-related information.

    This function implements US1.1 requirements:
    - Uses predefined seed queries for various disaster types
    - Performs keyword-based web search
    - Filters and ranks URLs based on relevance
    - Returns metadata (title, domain, timestamp)

    Args:
        disaster_type: Type of disaster ('floods', 'droughts', 'cyclones',
                      'earthquakes', 'landslides', or 'all')
        max_results: Maximum number of results per query
        use_mock: Use mock data for testing (when network unavailable)

    Returns:
        dict: Search results with URLs, titles, domains, and metadata
    """
    # Use mock data if requested or if network issues
    if use_mock:
        mock_urls = [
            {
                "url": "https://www.ndma.gov.in/disaster-management/floods",
                "title": "India Floods: NDMA Emergency Response and Relief Operations",
                "domain": "ndma.gov.in",
                "snippet": "The National Disaster Management Authority (NDMA) has issued flood warnings for multiple states. Emergency relief operations are underway in Kerala, Maharashtra, and West Bengal.",
                "query": f"India {disaster_type} latest news",
                "relevance_score": 9,
                "timestamp": datetime.datetime.now().isoformat(),
            },
            {
                "url": "https://www.thehindu.com/news/national/floods-india-2024",
                "title": "Major Flooding Reported Across India: Thousands Displaced",
                "domain": "thehindu.com",
                "snippet": "Heavy monsoon rains have caused severe flooding in India with thousands evacuated. Disaster management teams are on alert across multiple states.",
                "query": f"India {disaster_type} disaster updates",
                "relevance_score": 8,
                "timestamp": datetime.datetime.now().isoformat(),
            },
            {
                "url": "https://www.imd.gov.in/weather-warnings",
                "title": "India Meteorological Department: Severe Weather Alert",
                "domain": "imd.gov.in",
                "snippet": "IMD issues red alert for cyclone warning in Bay of Bengal. Coastal areas of Odisha and Andhra Pradesh on high alert. Emergency preparedness measures activated.",
                "query": f"India {disaster_type} affected areas",
                "relevance_score": 7,
                "timestamp": datetime.datetime.now().isoformat(),
            },
        ]
        return {
            "status": "success",
            "timestamp": datetime.datetime.now().isoformat(),
            "disaster_type": disaster_type,
            "discovered_urls": mock_urls[:max_results],
            "total_count": len(mock_urls[:max_results]),
            "note": "Using mock data (network unavailable)",
        }

    try:
        from ddgs import DDGS
    except ImportError:
        return {
            "status": "error",
            "error_message": "ddgs not installed. Install with: pip install ddgs",
        }

    results = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "disaster_type": disaster_type,
        "discovered_urls": [],
        "total_count": 0,
    }

    # Select queries based on disaster type
    if disaster_type == "all":
        queries = []
        for disaster_queries in SEED_QUERIES.values():
            queries.extend(disaster_queries)
    elif disaster_type in SEED_QUERIES:
        queries = SEED_QUERIES[disaster_type]
    else:
        return {
            "status": "error",
            "error_message": f"Unknown disaster type: {disaster_type}. Valid types: {list(SEED_QUERIES.keys()) + ['all']}",
        }

    # Perform web search using DuckDuckGo
    discovered_urls = []

    try:
        ddgs = DDGS()
    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Failed to initialize DDGS: {str(e)}. Network or SSL issue.",
        }

    for query in queries:
        try:
            search_results = list(ddgs.text(query, max_results=max_results))

            for result in search_results:
                url = result.get("href", "")
                title = result.get("title", "")
                body = result.get("body", "")

                # Extract domain
                domain = urlparse(url).netloc

                # Calculate relevance score (simple heuristic)
                relevance_score = 0
                keywords = ["disaster", "india", "alert", "warning", "emergency", "relief"]
                for keyword in keywords:
                    if keyword.lower() in title.lower() or keyword.lower() in body.lower():
                        relevance_score += 1

                discovered_urls.append({
                    "url": url,
                    "title": title,
                    "domain": domain,
                    "snippet": body[:200],
                    "query": query,
                    "relevance_score": relevance_score,
                    "timestamp": datetime.datetime.now().isoformat(),
                })

        except Exception as e:
            # Log error but continue with other queries
            print(f"Warning: Search failed for query '{query}': {str(e)}")
            continue

    # Sort by relevance score and freshness
    discovered_urls.sort(key=lambda x: x["relevance_score"], reverse=True)

    results["discovered_urls"] = discovered_urls
    results["total_count"] = len(discovered_urls)

    return results


# ============================================================================
# US1.2: Crawl4AI Integration for AI-Based Web Crawling
# ============================================================================

def crawl_urls_with_ai(urls: List[str], max_depth: int = 1) -> dict:
    """
    Performs AI-based web crawling using Crawl4AI framework.

    This function implements US1.2 requirements:
    - Crawls discovered URLs using Crawl4AI
    - Handles crawl scheduling and error handling
    - Extracts content from dynamic and static pages
    - Stores raw HTML/text data

    Args:
        urls: List of URLs to crawl (JSON string or list)
        max_depth: Crawl depth limit

    Returns:
        dict: Crawled content with metadata
    """
    # Parse URLs if provided as JSON string
    if isinstance(urls, str):
        try:
            urls = json.loads(urls)
        except json.JSONDecodeError:
            urls = [urls]

    try:
        from crawl4ai import AsyncWebCrawler
        import asyncio
    except ImportError:
        return {
            "status": "error",
            "error_message": "crawl4ai not installed. Install with: pip install crawl4ai",
        }

    results = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "crawled_data": [],
        "success_count": 0,
        "error_count": 0,
        "total_size_bytes": 0,
    }

    # Define async crawl function
    async def crawl_async():
        async with AsyncWebCrawler(verbose=False) as crawler:
            for url in urls:
                try:
                    # Perform crawl
                    crawl_result = await crawler.arun(url=url)

                    if crawl_result.success:
                        crawled_item = {
                            "url": url,
                            "status": "success",
                            "html": crawl_result.html[:5000] if crawl_result.html else "",  # Truncate for storage
                            "markdown": crawl_result.markdown[:5000] if crawl_result.markdown else "",
                            "extracted_content": crawl_result.extracted_content if crawl_result.extracted_content else "",
                            "links": dict(list(crawl_result.links.items())[:20]) if crawl_result.links else {},  # Limit links
                            "media": crawl_result.media if crawl_result.media else {},
                            "metadata": {
                                "title": getattr(crawl_result, "title", ""),
                                "description": getattr(crawl_result, "description", ""),
                            },
                            "crawl_timestamp": datetime.datetime.now().isoformat(),
                            "content_size_bytes": len(crawl_result.html) if crawl_result.html else 0,
                        }

                        results["crawled_data"].append(crawled_item)
                        results["success_count"] += 1
                        results["total_size_bytes"] += len(crawl_result.html) if crawl_result.html else 0
                    else:
                        results["crawled_data"].append({
                            "url": url,
                            "status": "error",
                            "error_message": crawl_result.error_message if hasattr(crawl_result, 'error_message') else "Crawl failed",
                        })
                        results["error_count"] += 1

                except Exception as e:
                    results["crawled_data"].append({
                        "url": url,
                        "status": "error",
                        "error_message": str(e),
                    })
                    results["error_count"] += 1

    # Run the async crawler
    # Handle both cases: standalone and within an existing event loop (e.g., Google ADK)
    try:
        # Check if there's already a running event loop
        try:
            loop = asyncio.get_running_loop()
            # We're already in an event loop (e.g., Google ADK agent)
            # Run in a separate thread to avoid "asyncio.run() cannot be called from a running event loop"
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, crawl_async())
                future.result(timeout=120)  # 2 minute timeout
        except RuntimeError:
            # No running loop, safe to use asyncio.run()
            asyncio.run(crawl_async())
    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Failed to run async crawler: {str(e)}",
        }

    return results


# ============================================================================
# US1.3: Firecrawl for Structured Text and Table Extraction
# ============================================================================

def extract_structured_data(html_content: str, url: str = "") -> dict:
    """
    Extracts structured text and tables from HTML content using Firecrawl-like parsing.

    This function implements US1.3 requirements:
    - Extracts key data elements (titles, paragraphs, tables, captions)
    - Converts tables to JSON format
    - Maps disaster-related attributes (date, event name, location)
    - Produces clean and usable datasets

    Args:
        html_content: HTML content to parse
        url: Source URL for reference

    Returns:
        dict: Structured data including text, tables, and metadata
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {
            "status": "error",
            "error_message": "beautifulsoup4 not installed. Install with: pip install beautifulsoup4",
        }

    results = {
        "status": "success",
        "url": url,
        "timestamp": datetime.datetime.now().isoformat(),
        "structured_data": {
            "title": "",
            "headings": [],
            "paragraphs": [],
            "tables": [],
            "lists": [],
            "metadata": {},
        },
        "disaster_entities": {
            "dates": [],
            "locations": [],
            "event_names": [],
            "casualties": [],
        },
    }

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract title
        title_tag = soup.find("title")
        if title_tag:
            results["structured_data"]["title"] = title_tag.get_text(strip=True)

        # Extract headings
        for heading_level in ["h1", "h2", "h3", "h4"]:
            for heading in soup.find_all(heading_level):
                results["structured_data"]["headings"].append({
                    "level": heading_level,
                    "text": heading.get_text(strip=True),
                })

        # Extract paragraphs
        for paragraph in soup.find_all("p"):
            text = paragraph.get_text(strip=True)
            if len(text) > 20:  # Filter short paragraphs
                results["structured_data"]["paragraphs"].append(text)

        # Extract tables
        for table_idx, table in enumerate(soup.find_all("table")):
            table_data = {
                "table_id": table_idx,
                "caption": "",
                "headers": [],
                "rows": [],
            }

            # Extract caption
            caption = table.find("caption")
            if caption:
                table_data["caption"] = caption.get_text(strip=True)

            # Extract headers
            headers = table.find_all("th")
            if headers:
                table_data["headers"] = [h.get_text(strip=True) for h in headers]

            # Extract rows
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    table_data["rows"].append(row_data)

            results["structured_data"]["tables"].append(table_data)

        # Extract lists
        for list_tag in soup.find_all(["ul", "ol"]):
            list_items = [li.get_text(strip=True) for li in list_tag.find_all("li")]
            if list_items:
                results["structured_data"]["lists"].append({
                    "type": list_tag.name,
                    "items": list_items,
                })

        # Extract disaster-related entities (simple pattern matching)
        full_text = soup.get_text()

        # Extract dates (simple pattern)
        import re
        date_patterns = [
            r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b",
        ]
        for pattern in date_patterns:
            dates = re.findall(pattern, full_text)
            results["disaster_entities"]["dates"].extend(dates)

        # Extract Indian locations (simple list)
        indian_states = [
            "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
            "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
            "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
            "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana",
            "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Mumbai",
            "Kolkata", "Chennai", "Bangalore", "Hyderabad",
        ]
        for location in indian_states:
            if location.lower() in full_text.lower():
                results["disaster_entities"]["locations"].append(location)

        # Extract disaster event keywords
        disaster_keywords = [
            "flood", "drought", "cyclone", "earthquake", "landslide", "tsunami",
            "disaster", "emergency", "relief", "evacuation", "casualties",
        ]
        for keyword in disaster_keywords:
            if keyword.lower() in full_text.lower():
                results["disaster_entities"]["event_names"].append(keyword)

    except Exception as e:
        results["status"] = "error"
        results["error_message"] = str(e)

    return results


# ============================================================================
# Kafka Message Packet Generation
# ============================================================================

def generate_kafka_packets(
    search_results: Optional[str] = None,
    crawl_results: Optional[str] = None,
    extraction_results: Optional[str] = None,
) -> dict:
    """
    Generates Kafka message packets from processed disaster data.

    This function creates standardized message packets ready for Kafka ingestion.
    Each packet contains structured disaster data with metadata for downstream processing.

    Args:
        search_results: JSON string of search results
        crawl_results: JSON string of crawl results
        extraction_results: JSON string of extraction results

    Returns:
        dict: Kafka message packets ready for pipeline ingestion
    """
    kafka_packets = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "packet_count": 0,
        "packets": [],
    }

    # Parse input data
    search_data = json.loads(search_results) if search_results else {}
    crawl_data = json.loads(crawl_results) if crawl_results else {}
    extraction_data = json.loads(extraction_results) if extraction_results else {}

    # Generate packets for each discovered URL
    discovered_urls = search_data.get("discovered_urls", [])

    for idx, url_data in enumerate(discovered_urls):
        packet = {
            "packet_id": f"disaster_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{idx}",
            "packet_type": "disaster_data_collection",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": datetime.datetime.now().isoformat(),
            "schema_version": "1.0",
            "data": {
                "source": {
                    "url": url_data.get("url", ""),
                    "domain": url_data.get("domain", ""),
                    "title": url_data.get("title", ""),
                    "discovery_timestamp": url_data.get("timestamp", ""),
                },
                "metadata": {
                    "disaster_type": search_data.get("disaster_type", "unknown"),
                    "search_query": url_data.get("query", ""),
                    "relevance_score": url_data.get("relevance_score", 0),
                },
                "content": {
                    "snippet": url_data.get("snippet", ""),
                    "crawl_status": "pending",
                    "extraction_status": "pending",
                },
            },
            "processing_instructions": {
                "priority": "high" if url_data.get("relevance_score", 0) > 3 else "normal",
                "requires_crawl": True,
                "requires_extraction": True,
                "retention_days": 365,
            },
        }

        # Add crawl data if available
        if crawl_data:
            crawled_items = crawl_data.get("crawled_data", [])
            for crawled_item in crawled_items:
                if crawled_item.get("url") == url_data.get("url"):
                    packet["data"]["content"]["crawl_status"] = crawled_item.get("status", "")
                    packet["data"]["content"]["crawled_html_size"] = crawled_item.get("content_size_bytes", 0)
                    packet["data"]["content"]["crawl_timestamp"] = crawled_item.get("crawl_timestamp", "")

        # Add extraction data if available
        if extraction_data:
            packet["data"]["content"]["extraction_status"] = extraction_data.get("status", "")
            packet["data"]["structured_content"] = extraction_data.get("structured_data", {})
            packet["data"]["disaster_entities"] = extraction_data.get("disaster_entities", {})

        kafka_packets["packets"].append(packet)
        kafka_packets["packet_count"] += 1

    return kafka_packets


def format_kafka_packets_for_output(packet_data: str) -> dict:
    """
    Formats Kafka packets for display and export.

    Args:
        packet_data: JSON string of Kafka packet data

    Returns:
        dict: Formatted output with summary and sample packets
    """
    try:
        packets = json.loads(packet_data)
    except (json.JSONDecodeError, TypeError):
        return {
            "status": "error",
            "error_message": "Invalid packet data format",
        }

    summary = {
        "status": "success",
        "total_packets": packets.get("packet_count", 0),
        "timestamp": packets.get("timestamp", ""),
        "summary": f"Generated {packets.get('packet_count', 0)} Kafka message packets ready for ingestion",
        "sample_packet": packets.get("packets", [{}])[0] if packets.get("packets") else {},
        "all_packets": packets.get("packets", []),
        "kafka_config": {
            "topic": "disaster-data-ingestion",
            "key_field": "packet_id",
            "partition_key": "disaster_type",
            "serialization": "json",
        },
    }

    return summary


# ============================================================================
# Agent Definition
# ============================================================================

root_agent = Agent(
    name="disaster_data_collection_agent",
    model="gemini-2.0-flash-exp",
    description=(
        "Autonomous agent for collecting, processing, and structuring disaster-related "
        "data from the web. Implements seed-query-driven search, AI-based crawling, "
        "structured data extraction, and Kafka message packet generation for natural "
        "disasters in India."
    ),
    instruction="""
You are an autonomous disaster data collection agent with expertise in gathering and
processing disaster-related information from the web.

Your capabilities include:

1. **Seed-Query-Driven Web Search (US1.1)**:
   - Search for disaster data using predefined seed queries
   - Cover floods, droughts, cyclones, earthquakes, and landslides
   - Rank and filter URLs based on relevance
   - Provide metadata about discovered sources

2. **AI-Based Web Crawling (US1.2)**:
   - Crawl discovered URLs using Crawl4AI framework
   - Handle dynamic and static web pages
   - Extract raw HTML and markdown content
   - Track crawl statistics and success rates

3. **Structured Data Extraction (US1.3)**:
   - Parse HTML content to extract structured data
   - Convert tables to JSON format
   - Identify disaster-related entities (dates, locations, events)
   - Extract paragraphs, headings, and lists

4. **Kafka Message Packet Generation**:
   - Transform processed data into Kafka message packets
   - Include metadata, timestamps, and processing instructions
   - Format packets for downstream pipeline ingestion

**Usage Examples**:

- "Search for latest flood data in India"
- "Crawl the top 5 cyclone-related URLs"
- "Extract structured data from disaster news articles"
- "Generate Kafka packets for all collected disaster data"

**Important Notes**:
- All search functionality uses DuckDuckGo (no API key required)
- Focus on disaster data relevant to India
- Provide detailed metadata for pipeline processing
- Generate standardized Kafka packets for data ingestion

When users ask you to collect disaster data, guide them through the complete pipeline:
search → crawl → extract → generate packets.
""",
    tools=[
        search_web_for_disaster_data,
        crawl_urls_with_ai,
        extract_structured_data,
        generate_kafka_packets,
        format_kafka_packets_for_output,
    ],
)
