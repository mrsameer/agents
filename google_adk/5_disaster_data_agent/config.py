"""
Configuration file for the Disaster Data Collection Agent

This file contains all configurable parameters for the agent:
- Seed queries
- Search parameters
- Crawl settings
- Extraction rules
- Kafka configuration
"""

# ============================================================================
# Disaster Types and Seed Queries
# ============================================================================

DISASTER_TYPES = ["floods", "droughts", "cyclones", "earthquakes", "landslides"]

SEED_QUERIES = {
    "floods": [
        "India floods latest news",
        "India flood disaster updates",
        "India monsoon flooding",
        "India flood affected areas",
        "India flood relief operations",
    ],
    "droughts": [
        "India drought conditions",
        "India water scarcity news",
        "India drought affected regions",
        "India rainfall deficit",
        "India agricultural drought",
    ],
    "cyclones": [
        "India cyclone latest update",
        "India tropical cyclone warning",
        "India Bay of Bengal cyclone",
        "India Arabian Sea cyclone",
        "India storm surge warning",
    ],
    "earthquakes": [
        "India earthquake latest news",
        "India seismic activity",
        "India earthquake tremors",
        "India earthquake affected areas",
        "India earthquake magnitude",
    ],
    "landslides": [
        "India landslide news",
        "India hill slope failure",
        "India landslide disaster",
        "India monsoon landslides",
        "India mountain slope collapse",
    ],
}

# ============================================================================
# Search Configuration
# ============================================================================

SEARCH_CONFIG = {
    "default_max_results": 5,
    "relevance_keywords": [
        "disaster",
        "india",
        "alert",
        "warning",
        "emergency",
        "relief",
        "evacuation",
        "casualties",
        "damage",
        "rescue",
    ],
    "high_priority_keywords": [
        "breaking",
        "urgent",
        "critical",
        "severe",
        "major",
        "emergency",
    ],
    "domains_whitelist": [
        # Indian government sites
        "ndma.gov.in",
        "mha.gov.in",
        "imd.gov.in",
        # News sources
        "thehindu.com",
        "indianexpress.com",
        "hindustantimes.com",
        "ndtv.com",
        # International
        "bbc.com",
        "reuters.com",
    ],
    "domains_blacklist": [
        "example.com",
        "test.com",
    ],
}

# ============================================================================
# Crawl Configuration
# ============================================================================

CRAWL_CONFIG = {
    "max_depth": 1,
    "max_pages_per_domain": 10,
    "timeout_seconds": 30,
    "retry_attempts": 3,
    "retry_delay_seconds": 2,
    "user_agent": "DisasterDataBot/1.0 (Educational Research)",
    "respect_robots_txt": True,
    "max_content_size_mb": 10,
    "allowed_content_types": [
        "text/html",
        "application/xhtml+xml",
        "text/plain",
    ],
}

# ============================================================================
# Extraction Configuration
# ============================================================================

EXTRACTION_CONFIG = {
    "min_paragraph_length": 20,
    "max_paragraphs": 100,
    "max_tables": 20,
    "max_list_items": 50,
    "extract_metadata": True,
    "extract_images": False,
    "extract_videos": False,
}

# Indian states and major cities for location extraction
INDIAN_LOCATIONS = [
    # States
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chhattisgarh",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jharkhand",
    "Karnataka",
    "Kerala",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "Nagaland",
    "Odisha",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
    # Union Territories
    "Delhi",
    "Jammu and Kashmir",
    "Ladakh",
    "Puducherry",
    "Andaman and Nicobar Islands",
    # Major Cities
    "Mumbai",
    "Kolkata",
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Ahmedabad",
    "Pune",
    "Surat",
    "Jaipur",
    "Lucknow",
    "Kanpur",
    "Nagpur",
    "Indore",
    "Thane",
    "Bhopal",
    "Visakhapatnam",
    "Patna",
    "Vadodara",
    "Ghaziabad",
    "Ludhiana",
    "Agra",
    "Nashik",
    "Faridabad",
    "Meerut",
    "Rajkot",
    "Varanasi",
]

# Disaster-related keywords
DISASTER_KEYWORDS = [
    "flood",
    "flooding",
    "inundation",
    "drought",
    "water scarcity",
    "cyclone",
    "hurricane",
    "typhoon",
    "earthquake",
    "tremor",
    "seismic",
    "landslide",
    "mudslide",
    "rockfall",
    "tsunami",
    "storm surge",
    "disaster",
    "emergency",
    "relief",
    "evacuation",
    "casualties",
    "damage",
    "rescue",
    "shelter",
    "aid",
]

# Date patterns for extraction
DATE_PATTERNS = [
    r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",  # DD/MM/YYYY or DD-MM-YYYY
    r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b",  # YYYY/MM/DD or YYYY-MM-DD
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b",  # Month DD, YYYY
    r"\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}\b",  # DD Month YYYY
]

# ============================================================================
# Kafka Configuration
# ============================================================================

KAFKA_CONFIG = {
    "topic": "disaster-data-ingestion",
    "key_field": "packet_id",
    "partition_key": "disaster_type",
    "serialization": "json",
    "compression": "gzip",
    "batch_size": 100,
    "schema_version": "1.0",
    "retention_days_default": 365,
    "priority_levels": ["low", "normal", "high", "critical"],
}

# Kafka packet structure template
KAFKA_PACKET_TEMPLATE = {
    "packet_id": "",
    "packet_type": "disaster_data_collection",
    "kafka_topic": "disaster-data-ingestion",
    "timestamp": "",
    "schema_version": "1.0",
    "data": {
        "source": {
            "url": "",
            "domain": "",
            "title": "",
            "discovery_timestamp": "",
        },
        "metadata": {
            "disaster_type": "",
            "search_query": "",
            "relevance_score": 0,
        },
        "content": {
            "snippet": "",
            "crawl_status": "",
            "extraction_status": "",
        },
    },
    "processing_instructions": {
        "priority": "normal",
        "requires_crawl": True,
        "requires_extraction": True,
        "retention_days": 365,
    },
}

# ============================================================================
# Agent Configuration
# ============================================================================

AGENT_CONFIG = {
    "name": "disaster_data_collection_agent",
    "model": "gemini-2.0-flash-exp",
    "temperature": 0.7,
    "max_iterations": 10,
    "timeout_seconds": 300,
}

# ============================================================================
# Storage Configuration
# ============================================================================

STORAGE_CONFIG = {
    "output_dir": "./data/disaster_data",
    "search_results_dir": "./data/disaster_data/search",
    "crawl_results_dir": "./data/disaster_data/crawl",
    "extraction_results_dir": "./data/disaster_data/extraction",
    "kafka_packets_dir": "./data/disaster_data/kafka_packets",
    "logs_dir": "./logs",
}

# ============================================================================
# Logging Configuration
# ============================================================================

LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "file_logging": True,
    "console_logging": True,
}

# ============================================================================
# Rate Limiting
# ============================================================================

RATE_LIMIT_CONFIG = {
    "search_delay_seconds": 2,
    "crawl_delay_seconds": 1,
    "max_requests_per_minute": 30,
    "max_requests_per_hour": 500,
}

# ============================================================================
# Feature Flags
# ============================================================================

FEATURES = {
    "enable_search": True,
    "enable_crawl": True,
    "enable_extraction": True,
    "enable_kafka_generation": True,
    "enable_duplicate_detection": False,
    "enable_geolocation": False,
    "enable_sentiment_analysis": False,
    "enable_image_extraction": False,
}
