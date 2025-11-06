# Disaster Data Collection Agent

An autonomous agent for collecting, processing, and structuring disaster-related data from the web, generating Kafka message packets for pipeline ingestion.

## Overview

This agent implements a comprehensive disaster data collection system focused on natural disasters in India. It covers the complete data pipeline from discovery to structured output.

## Features

### US1.1: Seed-Query-Driven Web Search Module ✅

**Objective**: Find new data sources about natural disasters in India using predefined seed queries.

**Implementation**:
- Predefined seed queries for 5 disaster types:
  - Floods
  - Droughts
  - Cyclones
  - Earthquakes
  - Landslides
- Keyword-based web search using DuckDuckGo (free, no API key required)
- URL filtering and ranking based on relevance score
- Metadata collection (title, domain, timestamp)
- Central repository storage ready

**Functions**:
- `search_web_for_disaster_data(disaster_type, max_results)`: Performs web search with seed queries

### US1.2: AI-Based Web Crawling with Crawl4AI ✅

**Objective**: Perform AI-based web crawling from discovered URLs.

**Implementation**:
- Crawl4AI AsyncWebCrawler integration with async/await
- Event loop compatibility (works both standalone and within Google ADK)
- Crawl scheduling and error handling
- Support for both dynamic and static pages
- Configurable crawl depth and limits
- Raw HTML/text storage
- Crawl statistics and logging (duration, success rate, content size)

**Functions**:
- `crawl_urls_with_ai(urls, max_depth)`: Crawls URLs using Crawl4AI (async-safe)

### US1.3: Structured Text and Table Extraction ✅

**Objective**: Extract structured data from crawled web pages using Firecrawl-like parsing.

**Implementation**:
- BeautifulSoup4 for HTML parsing (open source alternative to Firecrawl)
- Key data element extraction:
  - Titles, headings (H1-H4)
  - Paragraphs and text content
  - Tables with captions
  - Lists (ordered and unordered)
- Table-to-JSON conversion
- Disaster attribute mapping:
  - Dates (multiple formats)
  - Locations (Indian states and cities)
  - Event names and types
  - Casualties and impact data

**Functions**:
- `extract_structured_data(html_content, url)`: Extracts structured data from HTML

### Kafka Message Packet Generation ✅

**Objective**: Generate standardized Kafka message packets for downstream processing.

**Implementation**:
- Standardized packet format with schema version
- Metadata inclusion (timestamps, source info, disaster type)
- Processing instructions (priority, retention)
- Ready for Kafka ingestion pipeline
- Topic: `disaster-data-ingestion`
- Serialization: JSON

**Functions**:
- `generate_kafka_packets(search_results, crawl_results, extraction_results)`: Generates Kafka packets
- `format_kafka_packets_for_output(packet_data)`: Formats packets for display

## Installation

### Dependencies

All dependencies are defined in `pyproject.toml`. Install with:

```bash
pip install -e .
```

**Key Dependencies** (all open source and free except Google API):
- `ddgs`: Free web search via DuckDuckGo/Brave (no API key)
- `crawl4ai`: AI-powered web crawling with Playwright
- `beautifulsoup4`: HTML parsing and data extraction
- `kafka-python`: Kafka client for message generation
- `google-adk`: Google Agent Development Kit (requires GOOGLE_API_KEY)

**Installation with uv (recommended)**:
```bash
uv pip install --system ddgs crawl4ai beautifulsoup4 lxml kafka-python google-adk
python -m playwright install chromium --with-deps
```

### Environment Setup

1. Set up your `.env` file with Google API key:
```bash
GOOGLE_API_KEY=your_google_api_key_here
```

2. No other API keys required! All other tools are open source and free.

## Usage

### Basic Usage

```python
from google_adk.5_disaster_data_agent.agent import root_agent

# Initialize agent
agent = root_agent

# Example 1: Search for disaster data
agent.run("Search for latest flood data in India")

# Example 2: Complete pipeline
agent.run("Search for cyclone data, crawl the top 5 URLs, extract structured data, and generate Kafka packets")

# Example 3: Specific disaster type
agent.run("Find earthquake news from the past week and generate Kafka packets")
```

### Example Workflow (with Mock Mode for Testing)

```python
# Step 1: Search for disaster data (use mock=True for testing/restricted networks)
search_results = search_web_for_disaster_data(disaster_type="floods", max_results=10, use_mock=True)

# Step 2: Crawl discovered URLs
urls = [item["url"] for item in search_results["discovered_urls"][:5]]
crawl_results = crawl_urls_with_ai(urls)

# Step 3: Extract structured data
html_content = crawl_results["crawled_data"][0]["html"]
extraction_results = extract_structured_data(html_content)

# Step 4: Generate Kafka packets
kafka_packets = generate_kafka_packets(
    search_results=json.dumps(search_results),
    crawl_results=json.dumps(crawl_results),
    extraction_results=json.dumps(extraction_results)
)

# Step 5: Format for output
output = format_kafka_packets_for_output(json.dumps(kafka_packets))
```

## Agent Capabilities

The agent understands natural language commands:

- **Search**: "Find flood data in India", "Search for cyclone warnings"
- **Crawl**: "Crawl these URLs", "Fetch content from disaster news sites"
- **Extract**: "Extract structured data", "Parse tables from HTML"
- **Generate**: "Create Kafka packets", "Prepare data for pipeline ingestion"

## Data Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    Disaster Data Pipeline                       │
└─────────────────────────────────────────────────────────────────┘

1. Seed Query Search (US1.1)
   ├─ Predefined queries for disaster types
   ├─ DuckDuckGo free search
   ├─ Relevance ranking
   └─ URL discovery
        │
        ↓
2. AI-Based Crawling (US1.2)
   ├─ Crawl4AI framework
   ├─ Dynamic/static page support
   ├─ Error handling & retries
   └─ Raw content extraction
        │
        ↓
3. Structured Extraction (US1.3)
   ├─ HTML parsing (BeautifulSoup)
   ├─ Table-to-JSON conversion
   ├─ Entity extraction (dates, locations)
   └─ Disaster attribute mapping
        │
        ↓
4. Kafka Packet Generation
   ├─ Standardized packet format
   ├─ Metadata enrichment
   ├─ Processing instructions
   └─ Ready for Kafka ingestion
```

## Kafka Message Packet Format

```json
{
  "packet_id": "disaster_data_20240115_120000_0",
  "packet_type": "disaster_data_collection",
  "kafka_topic": "disaster-data-ingestion",
  "timestamp": "2024-01-15T12:00:00",
  "schema_version": "1.0",
  "data": {
    "source": {
      "url": "https://example.com/flood-news",
      "domain": "example.com",
      "title": "Major Flood Alert in India",
      "discovery_timestamp": "2024-01-15T11:00:00"
    },
    "metadata": {
      "disaster_type": "floods",
      "search_query": "India floods latest news",
      "relevance_score": 8
    },
    "content": {
      "snippet": "Heavy rainfall causes flooding...",
      "crawl_status": "success",
      "extraction_status": "success"
    },
    "structured_content": {
      "title": "Major Flood Alert",
      "headings": [...],
      "paragraphs": [...],
      "tables": [...]
    },
    "disaster_entities": {
      "dates": ["15/01/2024"],
      "locations": ["Kerala", "Mumbai"],
      "event_names": ["flood", "disaster"],
      "casualties": []
    }
  },
  "processing_instructions": {
    "priority": "high",
    "requires_crawl": true,
    "requires_extraction": true,
    "retention_days": 365
  }
}
```

## Testing

Run unit tests:

```bash
cd google_adk/5_disaster_data_agent
python -m pytest test_agent.py -v
```

Or:

```bash
python test_agent.py
```

### Test Coverage

- ✅ Seed query validation
- ✅ Web search functionality
- ✅ URL filtering and ranking
- ✅ HTML parsing and extraction
- ✅ Table-to-JSON conversion
- ✅ Entity extraction (dates, locations)
- ✅ Kafka packet generation
- ✅ Packet structure validation
- ✅ End-to-end pipeline

## Architecture

### Components

1. **Search Module** (US1.1)
   - Seed query manager
   - DuckDuckGo search interface
   - URL ranking algorithm
   - Metadata collector

2. **Crawling Module** (US1.2)
   - Crawl4AI integration
   - Scheduler and retry logic
   - Content extractor
   - Statistics tracker

3. **Extraction Module** (US1.3)
   - HTML parser (BeautifulSoup)
   - Table converter
   - Entity recognizer
   - Data mapper

4. **Kafka Module**
   - Packet generator
   - Schema manager
   - Priority assigner
   - Output formatter

### Design Principles

- **Autonomous**: Runs independently with minimal intervention
- **Modular**: Each user story is a separate, testable module
- **Extensible**: Easy to add new disaster types or data sources
- **Reliable**: Error handling and retry mechanisms throughout
- **Observable**: Comprehensive logging and statistics

## Configuration

### Disaster Types

Add new disaster types in `agent.py`:

```python
SEED_QUERIES = {
    "floods": [...],
    "new_disaster_type": [
        "query 1",
        "query 2",
    ],
}
```

### Search Parameters

Customize search behavior:
- `max_results`: Number of results per query (default: 5)
- `disaster_type`: Specific type or "all"
- Relevance keywords in ranking algorithm

### Crawl Parameters

Adjust crawling behavior:
- `max_depth`: Crawl depth limit (default: 1)
- Content size limits
- Retry attempts

## Kafka Integration

### Next Steps for Kafka Pipeline

1. **Install Kafka** (if not already):
   ```bash
   # Docker
   docker run -d --name kafka -p 9092:9092 apache/kafka
   ```

2. **Create Topic**:
   ```bash
   kafka-topics --create --topic disaster-data-ingestion --bootstrap-server localhost:9092
   ```

3. **Send Packets**:
   ```python
   from kafka import KafkaProducer

   producer = KafkaProducer(
       bootstrap_servers=['localhost:9092'],
       value_serializer=lambda v: json.dumps(v).encode('utf-8')
   )

   for packet in kafka_packets["packets"]:
       producer.send('disaster-data-ingestion', packet)
   ```

## Limitations

1. **Network Restrictions**: In restricted network environments (corporate proxies, SSL interception), web search and crawling may fail. Use **mock mode** for testing: `search_web_for_disaster_data(disaster_type="floods", use_mock=True)`
2. **Search Rate Limits**: DuckDuckGo/Brave search may rate-limit excessive searches
3. **Crawl Politeness**: Respects robots.txt and rate limits
4. **Entity Extraction**: Simple pattern matching (can be enhanced with NER models)
5. **Language**: Primarily English content

## Mock Mode for Testing

When web search or crawling is unavailable (network restrictions, testing), use mock mode:

```python
# Use mock data for search
search_result = search_web_for_disaster_data(
    disaster_type="floods",
    max_results=5,
    use_mock=True  # Returns realistic mock data
)
```

Mock mode provides:
- 3 realistic disaster-related URLs
- Proper metadata and relevance scores
- Indian government and news sources
- Perfect for testing extraction and Kafka generation without network access

## Future Enhancements

- [ ] Add Redis/MongoDB for URL repository
- [ ] Implement advanced entity extraction with NLP models
- [ ] Add support for PDF and document parsing
- [ ] Implement distributed crawling
- [ ] Add real-time alerting for critical disasters
- [ ] Integrate with GIS for location mapping
- [ ] Add duplicate detection
- [ ] Implement incremental crawling

## Troubleshooting

### Issue: Web search returns no results or SSL/TLS errors
**Solution**: Use mock mode for testing:
```python
search_web_for_disaster_data(disaster_type="floods", use_mock=True)
```
OR check network/proxy settings

### Issue: Crawl4AI "Playwright not installed" error
**Solution**: Install Playwright browsers:
```bash
python -m playwright install chromium --with-deps
```

### Issue: Crawl4AI network errors (ERR_TUNNEL_CONNECTION_FAILED)
**Solution**: This occurs in restricted networks. The agent is working correctly; network access is restricted. Extraction and Kafka generation will still work with mock/cached data.

### Issue: "asyncio.run() cannot be called from a running event loop"
**Solution**: Already fixed! The crawl function now detects if it's running within an existing event loop (like Google ADK) and handles it correctly using ThreadPoolExecutor. No action needed.

### Issue: BeautifulSoup parsing errors
**Solution**: Try different parser (`lxml`, `html.parser`, `html5lib`)

### Issue: Package renamed warning for duckduckgo_search
**Solution**: Already fixed. We use `ddgs` package now. If you see this, run:
```bash
pip uninstall duckduckgo-search
pip install ddgs
```

## Contributing

To extend or modify the agent:

1. Add new disaster types to `SEED_QUERIES`
2. Enhance relevance scoring algorithm
3. Add new data extraction patterns
4. Improve entity recognition
5. Add new Kafka packet fields

## License

Part of the agents project.

## Support

For issues or questions:
- Check test cases in `test_agent.py`
- Review agent logs
- Validate dependencies are installed

## Acknowledgments

- **DuckDuckGo**: Free search API
- **Crawl4AI**: AI-powered crawling framework
- **BeautifulSoup**: HTML parsing library
- **Google ADK**: Agent framework
