"""
Unit tests for the Disaster Data Collection Agent

Tests cover all three user stories:
- US1.1: Seed-Query-Driven Web Search Module
- US1.2: Crawl4AI Integration
- US1.3: Structured Data Extraction
- Kafka Packet Generation
"""

import json
import unittest
from unittest.mock import Mock, patch
from agent import (
    search_web_for_disaster_data,
    extract_structured_data,
    generate_kafka_packets,
    format_kafka_packets_for_output,
    SEED_QUERIES,
)


class TestSeedQuerySearch(unittest.TestCase):
    """Tests for US1.1: Seed-Query-Driven Web Search Module"""

    def test_seed_queries_defined(self):
        """Test that seed queries are properly defined for all disaster types"""
        expected_types = ["floods", "droughts", "cyclones", "earthquakes", "landslides"]
        for disaster_type in expected_types:
            self.assertIn(disaster_type, SEED_QUERIES)
            self.assertTrue(len(SEED_QUERIES[disaster_type]) > 0)

    def test_query_relevance_keywords(self):
        """Test that seed queries contain relevant keywords"""
        for disaster_type, queries in SEED_QUERIES.items():
            for query in queries:
                # Check if query contains either disaster type or 'India'
                self.assertTrue(
                    disaster_type.lower() in query.lower() or "india" in query.lower(),
                    f"Query '{query}' should contain '{disaster_type}' or 'India'",
                )

    @patch("agent.DDGS")
    def test_search_web_for_disaster_data_success(self, mock_ddgs):
        """Test successful web search for disaster data"""
        # Mock search results
        mock_results = [
            {
                "href": "https://example.com/flood-news",
                "title": "India Flood Disaster Alert",
                "body": "Major flooding reported in India...",
            }
        ]
        mock_ddgs.return_value.text.return_value = mock_results

        # Perform search
        result = search_web_for_disaster_data(disaster_type="floods", max_results=5)

        # Verify results
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["disaster_type"], "floods")
        self.assertIsInstance(result["discovered_urls"], list)
        self.assertIn("timestamp", result)

    def test_search_invalid_disaster_type(self):
        """Test search with invalid disaster type"""
        result = search_web_for_disaster_data(disaster_type="invalid_type")
        self.assertEqual(result["status"], "error")
        self.assertIn("Unknown disaster type", result["error_message"])


class TestStructuredDataExtraction(unittest.TestCase):
    """Tests for US1.3: Structured Data Extraction"""

    def test_extract_title_from_html(self):
        """Test extraction of title from HTML"""
        html = "<html><head><title>Test Title</title></head><body></body></html>"
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["structured_data"]["title"], "Test Title")

    def test_extract_headings_from_html(self):
        """Test extraction of headings from HTML"""
        html = """
        <html><body>
            <h1>Main Heading</h1>
            <h2>Sub Heading</h2>
            <h3>Section</h3>
        </body></html>
        """
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        headings = result["structured_data"]["headings"]
        self.assertEqual(len(headings), 3)
        self.assertEqual(headings[0]["level"], "h1")
        self.assertEqual(headings[0]["text"], "Main Heading")

    def test_extract_paragraphs_from_html(self):
        """Test extraction of paragraphs from HTML"""
        html = """
        <html><body>
            <p>This is a paragraph about floods in India.</p>
            <p>Another paragraph with disaster information.</p>
        </body></html>
        """
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        paragraphs = result["structured_data"]["paragraphs"]
        self.assertEqual(len(paragraphs), 2)
        self.assertIn("floods", paragraphs[0].lower())

    def test_extract_tables_from_html(self):
        """Test extraction of tables from HTML"""
        html = """
        <html><body>
            <table>
                <caption>Flood Statistics</caption>
                <tr><th>Date</th><th>Location</th><th>Casualties</th></tr>
                <tr><td>2024-01-15</td><td>Mumbai</td><td>10</td></tr>
                <tr><td>2024-01-20</td><td>Kerala</td><td>5</td></tr>
            </table>
        </body></html>
        """
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        tables = result["structured_data"]["tables"]
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]["caption"], "Flood Statistics")
        self.assertEqual(len(tables[0]["rows"]), 3)  # Including header row

    def test_extract_disaster_entities_dates(self):
        """Test extraction of disaster-related dates"""
        html = """
        <html><body>
            <p>The flood occurred on 15/01/2024 and continued until Jan 20, 2024.</p>
        </body></html>
        """
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        dates = result["disaster_entities"]["dates"]
        self.assertTrue(len(dates) > 0)

    def test_extract_disaster_entities_locations(self):
        """Test extraction of Indian locations"""
        html = """
        <html><body>
            <p>Flooding reported in Kerala, Maharashtra, and West Bengal.</p>
        </body></html>
        """
        result = extract_structured_data(html, "https://example.com")

        self.assertEqual(result["status"], "success")
        locations = result["disaster_entities"]["locations"]
        self.assertIn("Kerala", locations)
        self.assertIn("Maharashtra", locations)
        self.assertIn("West Bengal", locations)


class TestKafkaPacketGeneration(unittest.TestCase):
    """Tests for Kafka Message Packet Generation"""

    def test_generate_kafka_packets_structure(self):
        """Test that generated Kafka packets have correct structure"""
        search_results = json.dumps({
            "status": "success",
            "disaster_type": "floods",
            "discovered_urls": [
                {
                    "url": "https://example.com/flood",
                    "title": "Flood Alert",
                    "domain": "example.com",
                    "snippet": "Flooding in India...",
                    "query": "India floods",
                    "relevance_score": 5,
                    "timestamp": "2024-01-15T10:00:00",
                }
            ],
        })

        result = generate_kafka_packets(search_results=search_results)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["packet_count"], 1)
        self.assertIsInstance(result["packets"], list)

        packet = result["packets"][0]
        self.assertIn("packet_id", packet)
        self.assertIn("packet_type", packet)
        self.assertIn("kafka_topic", packet)
        self.assertIn("timestamp", packet)
        self.assertIn("data", packet)
        self.assertIn("processing_instructions", packet)

    def test_kafka_packet_data_fields(self):
        """Test that Kafka packet data fields are correctly populated"""
        search_results = json.dumps({
            "status": "success",
            "disaster_type": "cyclones",
            "discovered_urls": [
                {
                    "url": "https://example.com/cyclone",
                    "title": "Cyclone Warning",
                    "domain": "example.com",
                    "snippet": "Cyclone approaching...",
                    "query": "India cyclone",
                    "relevance_score": 8,
                    "timestamp": "2024-01-15T10:00:00",
                }
            ],
        })

        result = generate_kafka_packets(search_results=search_results)
        packet = result["packets"][0]

        self.assertEqual(packet["data"]["source"]["url"], "https://example.com/cyclone")
        self.assertEqual(packet["data"]["source"]["title"], "Cyclone Warning")
        self.assertEqual(packet["data"]["metadata"]["disaster_type"], "cyclones")
        self.assertEqual(packet["data"]["metadata"]["relevance_score"], 8)

    def test_kafka_packet_priority_assignment(self):
        """Test that high relevance scores result in high priority"""
        search_results = json.dumps({
            "status": "success",
            "disaster_type": "earthquakes",
            "discovered_urls": [
                {
                    "url": "https://example.com/high-priority",
                    "title": "Major Earthquake",
                    "domain": "example.com",
                    "snippet": "Earthquake alert...",
                    "query": "India earthquake",
                    "relevance_score": 6,
                    "timestamp": "2024-01-15T10:00:00",
                },
                {
                    "url": "https://example.com/normal-priority",
                    "title": "Earthquake News",
                    "domain": "example.com",
                    "snippet": "Minor tremors...",
                    "query": "India earthquake",
                    "relevance_score": 2,
                    "timestamp": "2024-01-15T10:00:00",
                },
            ],
        })

        result = generate_kafka_packets(search_results=search_results)

        self.assertEqual(result["packets"][0]["processing_instructions"]["priority"], "high")
        self.assertEqual(result["packets"][1]["processing_instructions"]["priority"], "normal")

    def test_format_kafka_packets_for_output(self):
        """Test formatting of Kafka packets for output"""
        packet_data = json.dumps({
            "status": "success",
            "packet_count": 2,
            "timestamp": "2024-01-15T10:00:00",
            "packets": [
                {"packet_id": "packet_1", "data": {}},
                {"packet_id": "packet_2", "data": {}},
            ],
        })

        result = format_kafka_packets_for_output(packet_data)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["total_packets"], 2)
        self.assertIn("kafka_config", result)
        self.assertEqual(result["kafka_config"]["topic"], "disaster-data-ingestion")


class TestEndToEndPipeline(unittest.TestCase):
    """Integration tests for the complete pipeline"""

    def test_packet_generation_with_extraction(self):
        """Test packet generation with extraction results"""
        search_results = json.dumps({
            "status": "success",
            "disaster_type": "landslides",
            "discovered_urls": [
                {
                    "url": "https://example.com/landslide",
                    "title": "Landslide Alert",
                    "domain": "example.com",
                    "snippet": "Landslides reported...",
                    "query": "India landslides",
                    "relevance_score": 5,
                    "timestamp": "2024-01-15T10:00:00",
                }
            ],
        })

        extraction_results = json.dumps({
            "status": "success",
            "url": "https://example.com/landslide",
            "structured_data": {
                "title": "Landslide Alert",
                "paragraphs": ["Heavy rains caused landslides."],
            },
            "disaster_entities": {
                "locations": ["Uttarakhand"],
                "dates": ["15/01/2024"],
            },
        })

        result = generate_kafka_packets(
            search_results=search_results, extraction_results=extraction_results
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["packet_count"], 1)

        packet = result["packets"][0]
        self.assertIn("structured_content", packet["data"])
        self.assertIn("disaster_entities", packet["data"])


if __name__ == "__main__":
    unittest.main()
