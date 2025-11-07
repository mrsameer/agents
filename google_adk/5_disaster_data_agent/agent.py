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
import logging

def setup_logger():
    """Setup detailed logging for debugging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

logger = setup_logger()

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
    
    EXCLUDED_DOMAINS = [
        'tiktok.com', 'twitter.com', 'facebook.com', 'instagram.com',
        'youtube.com', 'reddit.com', 'quora.com', 'pinterest.com'
    ]
    
    PREFERRED_DOMAINS = [
        'ndma.gov.in', 'imd.gov.in', 'thehindu.com', 'indianexpress.com',
        'timesofindia.com', 'ndtv.com', 'hindustantimes.com', 'deccanherald.com',
        'tribuneindia.com', 'thequint.com', 'scroll.in', 'bbc.com/news',
        'reuters.com', 'apnews.com'
    ]
    
    for query in queries:
        try:
            search_results = list(ddgs.text(query, max_results=max_results * 2))  # Get more results

            for result in search_results:
                url = result.get("href", "")
                title = result.get("title", "")
                body = result.get("body", "")

                # Extract domain
                domain = urlparse(url).netloc
                
                # FIX: Skip excluded domains
                if any(excluded in domain.lower() for excluded in EXCLUDED_DOMAINS):
                    continue
                
                # FIX: Prefer news and government sites
                domain_bonus = 3 if any(preferred in domain.lower() for preferred in PREFERRED_DOMAINS) else 0

                # Calculate relevance score
                relevance_score = domain_bonus
                keywords = ["disaster", "india", "alert", "warning", "emergency", "relief", 
                           "flood", "cyclone", "earthquake", "deaths", "affected", "evacuated"]
                for keyword in keywords:
                    if keyword.lower() in title.lower() or keyword.lower() in body.lower():
                        relevance_score += 1

                # FIX: Only add if relevance score is decent
                if relevance_score >= 2:
                    discovered_urls.append({
                        "url": url,
                        "title": title,
                        "domain": domain,
                        "snippet": body[:200],
                        "query": query,
                        "relevance_score": relevance_score,
                        "timestamp": datetime.datetime.now().isoformat(),
                    })
                
                # Stop if we have enough good results
                if len(discovered_urls) >= max_results:
                    break

        except Exception as e:
            print(f"Warning: Search failed for query '{query}': {str(e)}")
            continue

  
    discovered_urls.sort(key=lambda x: x["relevance_score"], reverse=True)

    results["discovered_urls"] = discovered_urls
    results["total_count"] = len(discovered_urls)

    return results




# def collect_and_process_disaster_data(
#     disaster_type: str = "floods",
#     max_urls: int = 1,
#     use_mock: bool = False
# ) -> dict:
#     """Complete end-to-end disaster data collection workflow."""
#     results = {
#         "status": "success",
#         "timestamp": datetime.datetime.now().isoformat(),
#         "disaster_type": disaster_type,
#         "workflow_steps": {},
#         "summary": {},
#         "final_packets": [],
#     }
    
#     # Step 1: Search
#     search_results = search_web_for_disaster_data(
#         disaster_type=disaster_type,
#         max_results=max_urls,
#         use_mock=use_mock
#     )
#     results["workflow_steps"]["1_search"] = {
#         "status": search_results.get("status"),
#         "urls_found": search_results.get("total_count", 0),
#     }
    
#     if search_results.get("status") != "success" or not search_results.get("discovered_urls"):
#         results["status"] = "failed"
#         results["error"] = "No URLs found"
#         return results
    
#     # Step 2: Crawl
#     urls_to_crawl = [item["url"] for item in search_results["discovered_urls"]]
#     crawl_results = crawl_urls_with_ai(urls_to_crawl, use_simple_fallback=True)
#     results["workflow_steps"]["2_crawl"] = {
#         "status": crawl_results.get("status"),
#         "success": crawl_results.get("success_count", 0),
#         "errors": crawl_results.get("error_count", 0),
#     }
    
#     if crawl_results.get("success_count", 0) == 0:
#         results["status"] = "failed"
#         results["error"] = "All crawls failed"
#         return results
    
#     # Step 3: Validate and Extract
#     validated_results = validate_and_extract(json.dumps(crawl_results))
#     results["workflow_steps"]["3_extraction"] = {
#         "status": validated_results.get("status"),
#         "extracted": validated_results.get("extracted_count", 0),
#         "skipped": validated_results.get("skipped_count", 0),
#     }
    
#     # Step 4: Generate DISCRETE Kafka Packets (ONE PER EVENT)
#     kafka_packets = generate_discrete_event_packets(
#         search_data=search_results,
#         extraction_data=validated_results
#     )
#     results["workflow_steps"]["4_kafka"] = {
#         "status": kafka_packets.get("status"),
#         "packets": kafka_packets.get("packet_count", 0),
#     }
#     results["final_packets"] = kafka_packets.get("packets", [])
    
#     # Create summary
#     total_locs = sum(len(p.get("spatial", {}).get("affected_locations", [])) for p in results["final_packets"])
#     total_events = len([p for p in results["final_packets"] if p.get("packet_type") == "discrete_disaster_event"])
    
#     results["summary"] = {
#         "urls_searched": search_results.get("total_count", 0),
#         "urls_crawled": crawl_results.get("success_count", 0),
#         "pages_extracted": validated_results.get("extracted_count", 0),
#         "discrete_events_found": total_events,
#         "kafka_packets": kafka_packets.get("packet_count", 0),
#         "total_locations": total_locs,
#     }
    
#     return results




def collect_and_process_disaster_data(
    disaster_type: str = "floods",
    max_urls: int = 1,
    use_mock: bool = False,
    user_query: str = ""  # NEW PARAMETER
) -> dict:
    """Complete end-to-end disaster data collection workflow with time filtering."""
    results = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "disaster_type": disaster_type,
        "workflow_steps": {},
        "summary": {},
        "final_packets": [],
        "time_bounds": {},  # NEW
    }
    
    # NEW STEP 0: Extract time bounds from user query
    time_bounds = extract_time_bounds_from_query(user_query or f"Find {disaster_type} news")
    results["time_bounds"] = time_bounds
    results["workflow_steps"]["0_time_extraction"] = {
        "status": "success",
        "start_date": time_bounds.get("start_date"),
        "end_date": time_bounds.get("end_date"),
        "description": time_bounds.get("temporal_description"),
    }
    
    # Step 1: Search
    search_results = search_web_for_disaster_data(
        disaster_type=disaster_type,
        max_results=max_urls * 3,  # Get more URLs for filtering
        use_mock=use_mock
    )
    results["workflow_steps"]["1_search"] = {
        "status": search_results.get("status"),
        "urls_found": search_results.get("total_count", 0),
    }
    
    if search_results.get("status") != "success" or not search_results.get("discovered_urls"):
        results["status"] = "failed"
        results["error"] = "No URLs found"
        return results
    
    # NEW STEP 1.5: Filter URLs by date
    filtered_urls = filter_urls_by_date_relevance(
        search_results.get("discovered_urls", []),
        time_bounds
    )
    results["workflow_steps"]["1.5_url_filtering"] = {
        "status": "success",
        "urls_before": len(search_results.get("discovered_urls", [])),
        "urls_after": len(filtered_urls),
    }
    
    # Take only max_urls after filtering
    urls_to_crawl = [item["url"] for item in filtered_urls[:max_urls]]
    
    # Step 2: Crawl
    crawl_results = crawl_urls_with_ai(urls_to_crawl, use_simple_fallback=True)
    results["workflow_steps"]["2_crawl"] = {
        "status": crawl_results.get("status"),
        "success": crawl_results.get("success_count", 0),
        "errors": crawl_results.get("error_count", 0),
    }
    
    if crawl_results.get("success_count", 0) == 0:
        results["status"] = "failed"
        results["error"] = "All crawls failed"
        return results
    
    # Step 3: Validate and Extract
    validated_results = validate_and_extract(json.dumps(crawl_results))
    results["workflow_steps"]["3_extraction"] = {
        "status": validated_results.get("status"),
        "extracted": validated_results.get("extracted_count", 0),
        "skipped": validated_results.get("skipped_count", 0),
    }
    
    # Step 4: Generate DISCRETE Kafka Packets with TIME FILTERING
    kafka_packets = generate_discrete_event_packets(
        search_data=search_results,
        extraction_data=validated_results,
        time_bounds=time_bounds  # NEW PARAMETER
    )
    results["workflow_steps"]["4_kafka"] = {
        "status": kafka_packets.get("status"),
        "packets": kafka_packets.get("packet_count", 0),
    }
    results["final_packets"] = kafka_packets.get("packets", [])
    
    # Create summary
    total_locs = sum(len(p.get("spatial", {}).get("affected_locations", [])) for p in results["final_packets"])
    total_events = len([p for p in results["final_packets"] if p.get("packet_type") == "discrete_disaster_event"])
    
    results["summary"] = {
        "time_bounds": f"{time_bounds.get('start_date')} to {time_bounds.get('end_date')}",
        "urls_searched": search_results.get("total_count", 0),
        "urls_filtered": len(filtered_urls),
        "urls_crawled": crawl_results.get("success_count", 0),
        "pages_extracted": validated_results.get("extracted_count", 0),
        "discrete_events_found": total_events,
        "kafka_packets": kafka_packets.get("packet_count", 0),
        "total_locations": total_locs,
    }
    
    return results





def generate_enhanced_kafka_packets(
    search_data: dict,
    extraction_data: dict
) -> dict:
    """Generate Kafka packets with full structured content"""
    packets = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "packet_count": 0,
        "packets": [],
    }
    
    for extraction in extraction_data.get("validated_extractions", []):
        if extraction.get("status") != "extracted":
            continue
            
        url = extraction.get("url", "")
        
        # Find search item
        search_item = next(
            (item for item in search_data.get("discovered_urls", []) if item.get("url") == url),
            {"url": url, "domain": urlparse(url).netloc, "title": "", "query": "", "relevance_score": 0}
        )
        
        # Get extraction details
        ext = extraction.get("extraction", {})
        struct = ext.get("structured_data", {})
        entities = ext.get("disaster_entities", {})
        
        packet = {
            "packet_id": f"disaster_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{packets['packet_count']}",
            "packet_type": "disaster_data_collection",
            "kafka_topic": "disaster-data-ingestion",
            "timestamp": datetime.datetime.now().isoformat(),
            "schema_version": "1.0",
            "source": {
                "url": url,
                "domain": search_item.get("domain", ""),
                "title": struct.get("title", "") or search_item.get("title", ""),
            },
            "metadata": {
                "disaster_type": search_data.get("disaster_type", "unknown"),
                "relevance_score": search_item.get("relevance_score", 0),
            },
            "structured_content": {
                "title": struct.get("title", ""),
                "headings_count": len(struct.get("headings", [])),
                "headings": struct.get("headings", [])[:5],  # ADDED: Show sample headings
                "paragraphs_count": len(struct.get("paragraphs", [])),
                "paragraphs": struct.get("paragraphs", [])[:5],  # ADDED: Show sample paragraphs
                "tables_count": len(struct.get("tables", [])),
                "tables": struct.get("tables", []),
                "lists_count": len(struct.get("lists", [])),
                "metadata": struct.get("metadata", {}),
            },
            "disaster_entities": {
                "dates": entities.get("dates", []),
                "locations": entities.get("locations", []),
                "event_names": entities.get("event_names", []),
                "casualties": entities.get("casualties", []),
            },
            "processing_status": {
                "status": "success",
                "timestamp": datetime.datetime.now().isoformat(),
            },
        }
        
        packets["packets"].append(packet)
        packets["packet_count"] += 1
    
    return packets


# def generate_discrete_event_packets(
#     search_data: dict,
#     extraction_data: dict
# ) -> dict:
#     """Generate discrete Kafka packets - one per disaster event."""
#     import re
#     from datetime import datetime
    
#     packets = {
#         "status": "success",
#         "timestamp": datetime.now().isoformat(),
#         "packet_count": 0,
#         "packets": [],
#     }
    
#     for extraction in extraction_data.get("validated_extractions", []):
#         if extraction.get("status") != "extracted":
#             continue
            
#         url = extraction.get("url", "")
        
#         search_item = next(
#             (item for item in search_data.get("discovered_urls", []) if item.get("url") == url),
#             {"url": url, "domain": urlparse(url).netloc, "title": "", "query": "", "relevance_score": 0}
#         )
        
#         ext = extraction.get("extraction", {})
#         struct = ext.get("structured_data", {})
#         entities = ext.get("disaster_entities", {})
        
#         all_paragraphs = struct.get("paragraphs", [])
#         all_tables = struct.get("tables", [])  # ADD THIS
        
#         # PASS TABLES to extract_discrete_events
#         discrete_events = extract_discrete_events(
#             paragraphs=all_paragraphs,
#             entities=entities,
#             disaster_type=search_data.get("disaster_type", "unknown"),
#             tables=all_tables  # ADD THIS
#         )
        
#         if not discrete_events:
#             packet = create_single_event_packet(
#                 url=url,
#                 search_item=search_item,
#                 struct=struct,
#                 entities=entities,
#                 disaster_type=search_data.get("disaster_type", "unknown"),
#                 packet_index=packets['packet_count']
#             )
#             packets["packets"].append(packet)
#             packets["packet_count"] += 1
#         else:
#             for event in discrete_events:
#                 packet = create_single_event_packet(
#                     url=url,
#                     search_item=search_item,
#                     struct=struct,
#                     entities=entities,
#                     disaster_type=search_data.get("disaster_type", "unknown"),
#                     packet_index=packets['packet_count'],
#                     event_data=event
#                 )
#                 packets["packets"].append(packet)
#                 packets["packet_count"] += 1
    
#     return packets


def create_packet_from_llm_event(
    event: dict,
    url: str,
    search_item: dict,
    struct: dict,
    disaster_type: str,
    packet_index: int
) -> dict:
    """Create Kafka packet from LLM-extracted event."""
    from datetime import datetime
    
    logger.info(f"\n{'='*80}\nCREATING PACKET {packet_index}\n{'='*80}")
    logger.info(f"Event ID: {event.get('event_id')}")
    logger.info(f"Event Type: {event.get('event_type')}")
    logger.info(f"Start Date: {event.get('start_date')}")
    logger.info(f"End Date: {event.get('end_date')}")
    logger.info(f"Primary Location: {event.get('primary_location')}")
    logger.info(f"Content IDs: {event.get('content_ids', [])}")
    
    # Handle relative dates
    start_date = event.get('start_date')
    end_date = event.get('end_date')
    
    if start_date and start_date.startswith('RELATIVE:'):
        # Convert relative date using article publish date
        publish_date = struct.get('metadata', {}).get('publish_date')
        if publish_date:
            # TODO: Implement relative date conversion
            start_date = publish_date
        else:
            start_date = datetime.now().strftime('%Y-%m-%d')
    
    casualties = event.get('casualties', {})
    
    packet = {
        "packet_id": f"disaster_event_{datetime.now().strftime('%Y%m%d%H%M%S')}_{packet_index}",
        "packet_type": "discrete_disaster_event",
        "kafka_topic": "disaster-events-discrete",
        "timestamp": datetime.now().isoformat(),
        "schema_version": "2.1",
        
        "event": {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "event_name": event.get("description", ""),
            "description": event.get("description", ""),
            "severity": event.get("severity", "unknown"),
        },
        
        "temporal": {
            "start_date": start_date,
            "end_date": end_date,
            "all_dates_mentioned": [start_date] if start_date else [],
            "is_ongoing": end_date is None and start_date is not None,
        },
        
        "spatial": {
            "primary_location": event.get("primary_location"),
            "affected_locations": event.get("locations", []),
            "num_locations": len(event.get("locations", [])),
        },
        
        "impact": {
            "deaths": casualties.get("deaths", 0),
            "injured": casualties.get("injured", 0),
            "displaced": casualties.get("displaced", 0),
            "total_affected": sum(casualties.values()) if casualties else 0,
        },
        
        "source": {
            "url": url,
            "domain": search_item.get("domain", ""),
            "title": struct.get("title", ""),
            "collection_timestamp": datetime.now().isoformat(),
            "content_ids": event.get("content_ids", []),
        },
        
        "metadata": {
            "disaster_type": disaster_type,
            "relevance_score": search_item.get("relevance_score", 0),
            "confidence": "high" if start_date and event.get("primary_location") else "medium",
            "extraction_method": "llm_clustering",
        },
        
        "processing_instructions": {
            "priority": "high" if event.get("severity") == "high" else "normal",
            "requires_nlp": False,  # Already processed by LLM
            "requires_geo_coding": bool(event.get("locations")),
            "requires_time_normalization": "RELATIVE:" in str(start_date),
            "retention_days": 365,
        },
    }
    
    logger.info(f"Packet created with temporal: {packet['temporal']}")
    logger.info(f"{'='*80}\n")
    
    return packet

# def generate_discrete_event_packets(
#     search_data: dict,
#     extraction_data: dict
# ) -> dict:
#     """Generate discrete Kafka packets using LLM-based event clustering."""
    
#     packets = {
#         "status": "success",
#         "timestamp": datetime.datetime.now().isoformat(),
#         "packet_count": 0,
#         "packets": [],
#     }
    
#     for extraction in extraction_data.get("validated_extractions", []):
#         if extraction.get("status") != "extracted":
#             continue
            
#         url = extraction.get("url", "")
        
#         search_item = next(
#             (item for item in search_data.get("discovered_urls", []) if item.get("url") == url),
#             {"url": url, "domain": urlparse(url).netloc, "title": "", "query": "", "relevance_score": 0}
#         )
        
#         ext = extraction.get("extraction", {})
#         struct = ext.get("structured_data", {})
#         entities = ext.get("disaster_entities", {})
        
#         all_paragraphs = struct.get("paragraphs", [])
#         all_tables = struct.get("tables", [])
        
#         # Prepare URL metadata for LLM
#         url_metadata = {
#             "url": url,
#             "title": struct.get("title", ""),
#             "publish_date": struct.get("metadata", {}).get("publish_date"),
#             "domain": search_item.get("domain", "")
#         }
        
#         # Use LLM to cluster content into discrete events
#         discrete_events = cluster_related_content_with_llm(
#             paragraphs=all_paragraphs,
#             entities=entities,
#             tables=all_tables,
#             url_metadata=url_metadata
#         )
        
#         # Fallback: Use old method if LLM fails
#         if not discrete_events:
#             logger.info("LLM clustering failed, using fallback method")
#             discrete_events = extract_discrete_events_fallback(
#                 paragraphs=all_paragraphs,
#                 entities=entities,
#                 tables=all_tables,
#                 disaster_type=search_data.get("disaster_type", "unknown")
#             )
        
#         # Create packets from events
#         if not discrete_events:
#             # No events found - create summary packet
#             packet = create_single_event_packet(
#                 url=url,
#                 search_item=search_item,
#                 struct=struct,
#                 entities=entities,
#                 disaster_type=search_data.get("disaster_type", "unknown"),
#                 packet_index=packets['packet_count']
#             )
#             packets["packets"].append(packet)
#             packets["packet_count"] += 1
#         else:
#             # Create packet for each discrete event
#             for event in discrete_events:
#                 packet = create_packet_from_llm_event(
#                     event=event,
#                     url=url,
#                     search_item=search_item,
#                     struct=struct,
#                     disaster_type=search_data.get("disaster_type", "unknown"),
#                     packet_index=packets['packet_count']
#                 )
#                 packets["packets"].append(packet)
#                 packets["packet_count"] += 1
    
#     return packets

def generate_discrete_event_packets(
    search_data: dict,
    extraction_data: dict,
    time_bounds: Optional[dict] = None # NEW PARAMETER
) -> dict:
    """Generate discrete Kafka packets using LLM-based event clustering WITH TIME FILTERING."""
    
    packets = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "packet_count": 0,
        "packets": [],
    }
    
    for extraction in extraction_data.get("validated_extractions", []):
        if extraction.get("status") != "extracted":
            continue
            
        url = extraction.get("url", "")
        
        search_item = next(
            (item for item in search_data.get("discovered_urls", []) if item.get("url") == url),
            {"url": url, "domain": urlparse(url).netloc, "title": "", "query": "", "relevance_score": 0}
        )
        
        ext = extraction.get("extraction", {})
        struct = ext.get("structured_data", {})
        entities = ext.get("disaster_entities", {})
        
        all_paragraphs = struct.get("paragraphs", [])
        all_tables = struct.get("tables", [])
        
        url_metadata = {
            "url": url,
            "title": struct.get("title", ""),
            "publish_date": struct.get("metadata", {}).get("publish_date"),
            "domain": search_item.get("domain", "")
        }
        
        # Use LLM to cluster content with TIME BOUNDS
        discrete_events = cluster_related_content_with_llm(
            paragraphs=all_paragraphs,
            entities=entities,
            tables=all_tables,
            url_metadata=url_metadata,
            time_bounds=time_bounds  # NEW
        )
        
        # Fallback if LLM fails
        if not discrete_events:
            logger.info("LLM clustering failed, using fallback method")
            discrete_events = extract_discrete_events_fallback(
                paragraphs=all_paragraphs,
                entities=entities,
                tables=all_tables,
                disaster_type=search_data.get("disaster_type", "unknown")
            )
        
        # NEW: Post-filter events by time bounds
        if time_bounds:
            discrete_events = filter_events_by_time_bounds(discrete_events, time_bounds)
        
        # Create packets from filtered events
        if not discrete_events:
            logger.info("No events passed time filtering - skipping packet creation")
        else:
            for event in discrete_events:
                packet = create_packet_from_llm_event(
                    event=event,
                    url=url,
                    search_item=search_item,
                    struct=struct,
                    disaster_type=search_data.get("disaster_type", "unknown"),
                    packet_index=packets['packet_count']
                )
                packets["packets"].append(packet)
                packets["packet_count"] += 1
    
    return packets



def extract_discrete_events_fallback(
    paragraphs: list,
    entities: dict,
    tables: list,
    disaster_type: str
) -> list:
    """
    Fallback method if LLM fails - improved version of old extract_discrete_events.
    Now properly maps dates to paragraphs.
    """
    import re
    
    discrete_events = []
    
    logger.info(f"\n{'='*80}\nFALLBACK: EXTRACTING EVENTS WITHOUT LLM\n{'='*80}")
    
    # Build a date-location-paragraph mapping
    all_dates = entities.get('dates', [])
    all_locations = entities.get('locations', [])
    
    for para_idx, paragraph in enumerate(paragraphs[:30]):
        para_lower = paragraph.lower()
        
        # Find which global dates appear in THIS paragraph
        para_dates = [d for d in all_dates if d.lower() in para_lower]
        
        # Find which locations appear in THIS paragraph
        para_locations = [loc for loc in all_locations if loc.lower() in para_lower]
        
        # Extract casualties
        casualty_patterns = [
            r"(\d+)\s+(?:people\s+)?(?:killed|dead|deaths|died)",
            r"(\d+)\s+(?:people\s+)?(?:injured|wounded)",
            r"(\d+)\s+(?:people\s+)?(?:displaced|evacuated|affected)",
        ]
        casualties = []
        for pattern in casualty_patterns:
            casualties.extend(re.findall(pattern, paragraph, re.IGNORECASE))
        
        # Create event if meaningful
        if (para_dates or para_locations) and len(paragraph) > 50:
            event = {
                "event_id": f"para_{para_idx}",
                "event_type": disaster_type,
                "description": paragraph[:200],
                "dates": para_dates,
                "start_date": para_dates[0] if para_dates else None,
                "end_date": para_dates[-1] if len(para_dates) > 1 else None,
                "locations": para_locations,
                "primary_location": para_locations[0] if para_locations else None,
                "casualties": {
                    "deaths": int(casualties[0]) if casualties else 0,
                    "injured": 0,
                    "displaced": 0
                },
                "severity": "high" if casualties and int(casualties[0]) > 50 else "medium",
                "source": "fallback_paragraph",
            }
            discrete_events.append(event)
            logger.info(f"Fallback event {para_idx}: {event.get('start_date')} at {event.get('primary_location')}")
    
    return discrete_events


def extract_time_bounds_from_query(user_query: str) -> dict:
    """
    Use LLM to extract time bounds from user query.
    
    Args:
        user_query: User's search query
        
    Returns:
        dict with start_date, end_date, and temporal_description
    """
    from google import genai
    import os
    from datetime import datetime
    
    today = datetime.now()
    
    prompt = f"""You are a date extraction expert. Extract the time bounds from this user query.

**TODAY'S DATE**: {today.strftime('%Y-%m-%d')} ({today.strftime('%A, %B %d, %Y')})

**USER QUERY**: "{user_query}"

**TASK**: Determine the START DATE and END DATE for the search based on the query.

**EXAMPLES**:
- "past week" → start: 7 days ago, end: today
- "yesterday" → start: yesterday, end: yesterday
- "last month" → start: first day of last month, end: last day of last month
- "past 3 days" → start: 3 days ago, end: today
- "latest news" or "recent" → start: 7 days ago, end: today (default)
- "2024" → start: 2024-01-01, end: 2024-12-31
- "no time mentioned" → start: 30 days ago, end: today (default)

**OUTPUT FORMAT (JSON ONLY)**:
{{
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD",
  "temporal_description": "past week",
  "has_time_constraint": true
}}

Return ONLY valid JSON."""

    try:
        client = genai.Client(api_key=os.environ.get('GOOGLE_API_KEY'))
        response = client.models.generate_content(
            model='gemini-2.0-flash-exp',
            contents=prompt
        )
        
        response_text = response.text.strip()
        if response_text.startswith('```json'):
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif response_text.startswith('```'):
            response_text = response_text.split('```')[1].split('```')[0].strip()
        
        time_bounds = json.loads(response_text)
        
        logger.info(f"\n{'='*80}\nEXTRACTED TIME BOUNDS FROM QUERY\n{'='*80}")
        logger.info(f"User Query: {user_query}")
        logger.info(f"Start Date: {time_bounds.get('start_date')}")
        logger.info(f"End Date: {time_bounds.get('end_date')}")
        logger.info(f"Description: {time_bounds.get('temporal_description')}")
        logger.info(f"Has Time Constraint: {time_bounds.get('has_time_constraint')}")
        logger.info(f"{'='*80}\n")
        
        return time_bounds
        
    except Exception as e:
        logger.error(f"Time bound extraction failed: {str(e)}")
        # Default: past 7 days
        from datetime import timedelta
        start = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        return {
            "start_date": start,
            "end_date": end,
            "temporal_description": "past week (default)",
            "has_time_constraint": False
        }


def filter_urls_by_date_relevance(urls: list, time_bounds: dict) -> list:
    """
    Filter URLs to only crawl those likely within time bounds.
    
    Args:
        urls: List of URL dicts with metadata
        time_bounds: Time bounds from extract_time_bounds_from_query
        
    Returns:
        Filtered list of URLs
    """
    from datetime import datetime
    import re
    
    if not time_bounds.get('has_time_constraint'):
        logger.info("No time constraint - crawling all URLs")
        return urls
    
    logger.info(f"\n{'='*80}\nFILTERING URLS BY DATE RELEVANCE\n{'='*80}")
    logger.info(f"Time Bound: {time_bounds.get('start_date')} to {time_bounds.get('end_date')}")
    logger.info(f"Total URLs before filtering: {len(urls)}")
    
    start_date = datetime.strptime(time_bounds['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(time_bounds['end_date'], '%Y-%m-%d')
    
    filtered_urls = []
    
    for url_item in urls:
        url = url_item.get('url', '')
        title = url_item.get('title', '').lower()
        snippet = url_item.get('snippet', '').lower()
        
        # Check if URL/title/snippet contains dates
        date_patterns = [
            r'20\d{2}[/-]\d{2}[/-]\d{2}',  # 2024-11-06
            r'\d{2}[/-]\d{2}[/-]20\d{2}',  # 11-06-2024
           # r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[- ]?\d{1,2}[,- ]?20\d{2}',  # Nov 6, 2024
           r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[- ]?\d{1,2}[,\- ]?20\d{2}'

        ]
        
        found_dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, url + ' ' + title + ' ' + snippet, re.IGNORECASE)
            found_dates.extend(matches)
        
        # If dates found, check if any are within bounds
        if found_dates:
            for date_str in found_dates:
                try:
                    # Parse date
                    parsed_date = None
                    if '-' in date_str and len(date_str.split('-')[0]) == 4:
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                    elif '/' in date_str:
                        parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
                    # Add more parsing logic as needed
                    
                    if parsed_date and start_date <= parsed_date <= end_date:
                        filtered_urls.append(url_item)
                        logger.info(f"✓ INCLUDED: {url} (found date: {date_str})")
                        break
                except:
                    continue
        else:
            # No explicit dates - include if keywords suggest recency
            recency_keywords = ['latest', 'recent', 'today', 'breaking', 'live', 'update']
            if any(kw in title or kw in snippet for kw in recency_keywords):
                filtered_urls.append(url_item)
                logger.info(f"✓ INCLUDED: {url} (recency keywords found)")
            else:
                logger.info(f"✗ EXCLUDED: {url} (no date match)")
    
    logger.info(f"Total URLs after filtering: {len(filtered_urls)}")
    logger.info(f"{'='*80}\n")
    
    return filtered_urls if filtered_urls else urls[:2]  # Fallback to first 2 if all filtered out

def filter_events_by_time_bounds(events: list, time_bounds: dict) -> list:
    """
    Filter extracted events to only include those within time bounds.
    
    Args:
        events: List of event dicts
        time_bounds: Time bounds from extract_time_bounds_from_query
        
    Returns:
        Filtered list of events
    """
    from datetime import datetime
    
    if not time_bounds.get('has_time_constraint'):
        logger.info("No time constraint - keeping all events")
        return events
    
    logger.info(f"\n{'='*80}\nFILTERING EVENTS BY TIME BOUNDS\n{'='*80}")
    logger.info(f"Time Bound: {time_bounds.get('start_date')} to {time_bounds.get('end_date')}")
    logger.info(f"Total events before filtering: {len(events)}")
    
    start_date = datetime.strptime(time_bounds['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(time_bounds['end_date'], '%Y-%m-%d')
    
    filtered_events = []
    
    for event in events:
        event_start = event.get('start_date')
        
        if not event_start:
            logger.info(f"✗ EXCLUDED: {event.get('event_id')} (no start date)")
            continue
        
        # Parse event date
        try:
            if event_start.startswith('RELATIVE:'):
                # Already handled in create_packet_from_llm_event
                event_date = datetime.now()
            else:
                event_date = datetime.strptime(event_start, '%Y-%m-%d')
            
            # Check if within bounds
            if start_date <= event_date <= end_date:
                filtered_events.append(event)
                logger.info(f"✓ INCLUDED: {event.get('event_id')} at {event.get('primary_location')} on {event_start}")
            else:
                logger.info(f"✗ EXCLUDED: {event.get('event_id')} on {event_start} (outside bounds)")
                
        except Exception as e:
            logger.info(f"✗ EXCLUDED: {event.get('event_id')} (date parse error: {str(e)})")
    
    logger.info(f"Total events after filtering: {len(filtered_events)}")
    logger.info(f"{'='*80}\n")
    
    return filtered_events

def extract_discrete_events(paragraphs: list, entities: dict, disaster_type: str, tables: list = None) -> list:
    """
    Extract discrete disaster events from paragraphs AND tables.
    Each event has specific date, location, and details.
    """
    import re
    from datetime import datetime
    
    discrete_events = []
    
    logger.info(f"\n{'='*80}\nEXTRACTING DISCRETE EVENTS\n{'='*80}")
    logger.info(f"Input: {len(paragraphs)} paragraphs, {len(tables) if tables else 0} tables")
    logger.info(f"Available dates from entities: {entities.get('dates', [])}")
    logger.info(f"Available locations from entities: {entities.get('locations', [])}")
    
    # PART 1: Extract from TABLES (where most earthquake data lives)
    if tables:
        logger.info(f"\n--- EXTRACTING FROM TABLES ---")
        for table_idx, table in enumerate(tables):
            logger.info(f"\nTable {table_idx}:")
            logger.info(f"  Caption: {table.get('caption', 'N/A')}")
            logger.info(f"  Headers: {table.get('headers', [])}")
            logger.info(f"  Row count: {len(table.get('rows', []))}")
            
            headers = [h.lower() for h in table.get('headers', [])]
            rows = table.get('rows', [])
            
            # Find column indices
            date_col = None
            location_col = None
            magnitude_col = None
            deaths_col = None
            
            for idx, header in enumerate(headers):
                if 'date' in header or 'time' in header or 'year' in header:
                    date_col = idx
                if 'location' in header or 'place' in header or 'region' in header or 'state' in header:
                    location_col = idx
                if 'magnitude' in header or 'mw' in header or 'richter' in header:
                    magnitude_col = idx
                if 'death' in header or 'casualt' in header or 'killed' in header:
                    deaths_col = idx
            
            logger.info(f"  Column mapping: date={date_col}, location={location_col}, magnitude={magnitude_col}, deaths={deaths_col}")
            
            # Extract events from rows
            if date_col is not None or location_col is not None:
                for row_idx, row in enumerate(rows[:20]):  # Limit to 20 rows
                    if len(row) <= max(filter(lambda x: x is not None, [date_col, location_col, magnitude_col, deaths_col] or [0])):
                        continue
                    
                    event_date = row[date_col].strip() if date_col is not None and len(row) > date_col else None
                    event_location = row[location_col].strip() if location_col is not None and len(row) > location_col else None
                    event_magnitude = row[magnitude_col].strip() if magnitude_col is not None and len(row) > magnitude_col else None
                    event_deaths = row[deaths_col].strip() if deaths_col is not None and len(row) > deaths_col else None
                    
                    # Skip if both date and location are empty
                    if not event_date and not event_location:
                        continue
                    
                    # Extract casualties
                    casualties = []
                    if event_deaths:
                        death_numbers = re.findall(r'\d+', event_deaths)
                        casualties.extend(death_numbers)
                    
                    # Create event from table row
                    event = {
                        "event_id": f"table_{table_idx}_row_{row_idx}",
                        "event_type": disaster_type if disaster_type != "all" else "earthquake",
                        "description": f"{disaster_type.title()} event" + (f" (magnitude {event_magnitude})" if event_magnitude else ""),
                        "dates": [event_date] if event_date else [],
                        "start_date": event_date,
                        "end_date": None,
                        "locations": [event_location] if event_location else [],
                        "primary_location": event_location,
                        "casualties": {
                            "reported": casualties,
                            "total": sum(int(c) for c in casualties if c.isdigit()) if casualties else 0
                        },
                        "severity": calculate_severity(casualties, 1 if event_location else 0),
                        "source": "table",
                    }
                    
                    discrete_events.append(event)
                    logger.info(f"  ✓ Created table event: date={event_date}, location={event_location}")
    
    # PART 2: Extract from PARAGRAPHS (as before)
    logger.info(f"\n--- EXTRACTING FROM PARAGRAPHS ---")
    
    date_patterns = [
        r"(?:on|in|during)\s+(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})",
        r"(?:on|in|during)\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})",
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
    ]
    
    for para_idx, paragraph in enumerate(paragraphs):
        para_lower = paragraph.lower()
        
        # Extract dates from THIS specific paragraph
        para_dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, paragraph, re.IGNORECASE)
            para_dates.extend(matches)
        
        # Deduplicate
        para_dates = list(set(para_dates))
        
        # Extract locations mentioned in THIS paragraph
        para_locations = []
        for loc in entities.get("locations", []):
            if loc.lower() in para_lower:
                para_locations.append(loc)
        
        # Extract casualties from THIS paragraph
        casualty_patterns = [
            r"(\d+)\s+(?:people\s+)?(?:killed|dead|deaths|died)",
            r"(\d+)\s+(?:people\s+)?(?:injured|wounded)",
            r"(\d+)\s+(?:people\s+)?(?:displaced|evacuated|affected)",
        ]
        casualties = []
        for pattern in casualty_patterns:
            matches = re.findall(pattern, paragraph, re.IGNORECASE)
            casualties.extend(matches)
        
        # Detect event types in THIS paragraph
        event_types = []
        disaster_keywords = {
            "flood": ["flood", "flooding", "inundation", "deluge"],
            "cyclone": ["cyclone", "hurricane", "typhoon", "storm"],
            "earthquake": ["earthquake", "tremor", "quake", "seismic"],
            "landslide": ["landslide", "mudslide", "slope failure"],
            "drought": ["drought", "water crisis", "dry spell"],
        }
        
        for event_type, keywords in disaster_keywords.items():
            if any(keyword in para_lower for keyword in keywords):
                event_types.append(event_type)
        
        # Create event if we have meaningful data
        has_date = len(para_dates) > 0
        has_location = len(para_locations) > 0
        has_content = len(paragraph) > 50
        
        logger.info(f"\nParagraph {para_idx}:")
        logger.info(f"  Text: {paragraph[:100]}...")
        logger.info(f"  Dates found: {para_dates}")
        logger.info(f"  Locations found: {para_locations}")
        logger.info(f"  Casualties: {casualties}")
        logger.info(f"  Has date: {has_date}, Has location: {has_location}, Has content: {has_content}")
        
        if (has_date or has_location) and has_content:
            event = {
                "event_id": f"para_{para_idx}",
                "event_type": event_types[0] if event_types else disaster_type,
                "description": paragraph,
                "dates": para_dates,
                "start_date": para_dates[0] if para_dates else None,
                "end_date": para_dates[-1] if len(para_dates) > 1 else None,
                "locations": para_locations,
                "primary_location": para_locations[0] if para_locations else None,
                "casualties": {
                    "reported": casualties,
                    "total": sum(int(c) for c in casualties if c.isdigit()) if casualties else 0
                },
                "severity": calculate_severity(casualties, len(para_locations)),
                "source": "paragraph",
            }
            discrete_events.append(event)
            logger.info(f"  ✓ Created paragraph event with start_date={event['start_date']}, end_date={event['end_date']}")
        else:
            logger.info(f"  ✗ Skipped (insufficient data)")
    
    logger.info(f"\nTotal discrete events found: {len(discrete_events)}")
    logger.info(f"  From tables: {len([e for e in discrete_events if e.get('source') == 'table'])}")
    logger.info(f"  From paragraphs: {len([e for e in discrete_events if e.get('source') == 'paragraph'])}")
    logger.info(f"{'='*80}\n")
    
    return discrete_events


def calculate_severity(casualties: list, num_locations: int) -> str:
    """Calculate event severity based on casualties and spread"""
    if not casualties:
        return "low"
    
    total = sum(int(c) for c in casualties if c.isdigit())
    
    if total > 100 or num_locations > 3:
        return "high"
    elif total > 10 or num_locations > 1:
        return "medium"
    else:
        return "low"


def create_single_event_packet(
    url: str,
    search_item: dict,
    struct: dict,
    entities: dict,
    disaster_type: str,
    packet_index: int,
    event_data: dict = None
) -> dict:
    """Create a single Kafka packet for one discrete event."""
    from datetime import datetime
    
    if event_data:
        # Discrete event packet
        logger.info(f"\n{'='*80}\nCREATING PACKET {packet_index}\n{'='*80}")
        logger.info(f"Event ID: {event_data.get('event_id')}")
        logger.info(f"Start Date: {event_data.get('start_date')}")
        logger.info(f"End Date: {event_data.get('end_date')}")
        logger.info(f"Primary Location: {event_data.get('primary_location')}")
        logger.info(f"All Locations: {event_data.get('locations')}")
        
        packet = {
            "packet_id": f"disaster_event_{datetime.now().strftime('%Y%m%d%H%M%S')}_{packet_index}",
            "packet_type": "discrete_disaster_event",
            "kafka_topic": "disaster-events-discrete",
            "timestamp": datetime.now().isoformat(),
            "schema_version": "2.0",
            
            "event": {
                "event_id": event_data.get("event_id"),
                "event_type": event_data.get("event_type"),
                "event_name": f"{event_data.get('event_type', '').title()} in {event_data.get('primary_location', 'Unknown')}",
                "description": event_data.get("description", ""),
                "severity": event_data.get("severity", "unknown"),
            },
            
            "temporal": {
                "start_date": event_data.get("start_date"),
                "end_date": event_data.get("end_date"),
                "all_dates_mentioned": event_data.get("dates", []),
                "is_ongoing": event_data.get("end_date") is None and event_data.get("start_date") is not None,
            },
            
            "spatial": {
                "primary_location": event_data.get("primary_location"),
                "affected_locations": event_data.get("locations", []),
                "num_locations": len(event_data.get("locations", [])),
            },
            
            "impact": {
                "casualties": event_data.get("casualties", {}),
                "deaths": next((int(c) for c in event_data.get("casualties", {}).get("reported", []) if c.isdigit()), 0),
                "affected_population": event_data.get("casualties", {}).get("total", 0),
            },
            
            "source": {
                "url": url,
                "domain": search_item.get("domain", ""),
                "title": struct.get("title", ""),
                "collection_timestamp": datetime.now().isoformat(),
            },
            
            "metadata": {
                "disaster_type": disaster_type,
                "relevance_score": search_item.get("relevance_score", 0),
                "confidence": "high" if event_data.get("dates") and event_data.get("locations") else "medium",
            },
            
            "processing_instructions": {
                "priority": "high" if event_data.get("severity") == "high" else "normal",
                "requires_nlp": True,
                "requires_geo_coding": bool(event_data.get("locations")),
                "requires_time_normalization": bool(event_data.get("dates")),
                "retention_days": 365,
            },
        }
        
        logger.info(f"Packet temporal data: {packet['temporal']}")
        logger.info(f"{'='*80}\n")
    else:
        # Fallback: Single packet with all entities
        logger.info(f"\n{'='*80}\nCREATING SUMMARY PACKET {packet_index}\n{'='*80}")
        logger.info(f"No discrete events found, creating summary packet")
        
        packet = {
            "packet_id": f"disaster_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}_{packet_index}",
            "packet_type": "disaster_summary",
            "kafka_topic": "disaster-events-discrete",
            "timestamp": datetime.now().isoformat(),
            "schema_version": "2.0",
            
            "event": {
                "event_type": disaster_type,
                "event_name": f"{disaster_type.title()} - Multiple Events",
                "description": struct.get("metadata", {}).get("summary", ""),
                "severity": "unknown",
            },
            
            "temporal": {
                "all_dates_mentioned": entities.get("dates", [])[:10],
                "date_range": f"{entities.get('dates', ['Unknown'])[0]} - {entities.get('dates', ['Unknown'])[-1]}" if entities.get("dates") else "Unknown",
                "start_date": entities.get("dates", [None])[0] if entities.get("dates") else None,
                "end_date": entities.get("dates", [None])[-1] if len(entities.get("dates", [])) > 1 else None,
                "is_ongoing": True,
            },
            
            "spatial": {
                "affected_locations": entities.get("locations", []),
                "num_locations": len(entities.get("locations", [])),
            },
            
            "impact": {
                "casualties_reported": entities.get("casualties", []),
            },
            
            "source": {
                "url": url,
                "domain": search_item.get("domain", ""),
                "title": struct.get("title", ""),
                "collection_timestamp": datetime.now().isoformat(),
            },
            
            "metadata": {
                "disaster_type": disaster_type,
                "relevance_score": search_item.get("relevance_score", 0),
                "note": "Summary packet - contains multiple events",
            },
            
            "processing_instructions": {
                "priority": "normal",
                "requires_event_separation": True,
                "retention_days": 365,
            },
        }
        
        logger.info(f"{'='*80}\n")
    
    return packet


def crawl_urls_with_ai(urls: List[str], max_depth: int = 1, use_simple_fallback: bool = True) -> dict:
    """
    Performs AI-based web crawling using Crawl4AI framework with robust error handling.
    
    Args:
        urls: List of URLs to crawl (JSON string or list)
        max_depth: Crawl depth limit
        use_simple_fallback: If True, fall back to simple HTTP requests on failure
    
    Returns:
        dict: Crawled content with metadata
    """
    # Parse URLs if provided as JSON string
    if isinstance(urls, str):
        try:
            urls = json.loads(urls)
        except json.JSONDecodeError:
            urls = [urls]

    results = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "crawled_data": [],
        "success_count": 0,
        "error_count": 0,
        "total_size_bytes": 0,
    }

    # FIX 1: Try Crawl4AI first, but with better configuration
    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
        import asyncio
        
        # FIX 2: Configure browser with more conservative settings
        browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            extra_args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-software-rasterizer",
                "--no-sandbox",
            ]
        )
        
        crawler_config = CrawlerRunConfig(
            page_timeout=30000,  # 30 second timeout
            wait_until="domcontentloaded",  # Don't wait for everything
        )
        
        async def crawl_async():
            # FIX 3: Create new crawler instance for each batch to avoid context issues
            async with AsyncWebCrawler(config=browser_config, verbose=False) as crawler:
                for idx, url in enumerate(urls):
                    try:
                        # Add delay between requests to avoid rate limiting
                        if idx > 0:
                            await asyncio.sleep(2)
                        
                        # Perform crawl with timeout
                        crawl_result = await asyncio.wait_for(
                            crawler.arun(url=url, config=crawler_config),
                            timeout=45  # 45 second overall timeout
                        )

                        if crawl_result.success:
                            html_content = crawl_result.html if crawl_result.html else ""
                            markdown_content = crawl_result.markdown if crawl_result.markdown else ""
                            
                            crawled_item = {
                                "url": url,
                                "status": "success",
                                "html": html_content,  # FIX 4: Don't truncate
                                "html_size": len(html_content),
                                "markdown": markdown_content[:10000],  # Keep reasonable markdown
                                "markdown_size": len(markdown_content),
                                "extracted_content": crawl_result.extracted_content if crawl_result.extracted_content else "",
                                "links": dict(list(crawl_result.links.items())[:20]) if crawl_result.links else {},
                                "media": crawl_result.media if crawl_result.media else {},
                                "metadata": {
                                    "title": getattr(crawl_result, "title", ""),
                                    "description": getattr(crawl_result, "description", ""),
                                },
                                "crawl_timestamp": datetime.datetime.now().isoformat(),
                                "content_size_bytes": len(html_content),
                                "method": "crawl4ai",
                            }

                            results["crawled_data"].append(crawled_item)
                            results["success_count"] += 1
                            results["total_size_bytes"] += len(html_content)
                        else:
                            error_msg = crawl_result.error_message if hasattr(crawl_result, 'error_message') else "Crawl failed"
                            results["crawled_data"].append({
                                "url": url,
                                "status": "error",
                                "error_message": error_msg,
                                "method": "crawl4ai",
                            })
                            results["error_count"] += 1

                    except asyncio.TimeoutError:
                        results["crawled_data"].append({
                            "url": url,
                            "status": "error",
                            "error_message": "Crawl timeout (45s exceeded)",
                            "method": "crawl4ai",
                        })
                        results["error_count"] += 1
                    except Exception as e:
                        results["crawled_data"].append({
                            "url": url,
                            "status": "error",
                            "error_message": f"Crawl4AI error: {str(e)}",
                            "method": "crawl4ai",
                        })
                        results["error_count"] += 1

        # FIX 5: Better async execution handling
        try:
            loop = asyncio.get_running_loop()
            # Already in event loop - use thread pool
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(asyncio.run, crawl_async())
                future.result(timeout=180)  # 3 minute total timeout
        except RuntimeError:
            # No running loop
            asyncio.run(crawl_async())
            
    except ImportError:
        results["status"] = "partial"
        results["note"] = "crawl4ai not available, using fallback method"
        use_simple_fallback = True
    except Exception as e:
        results["status"] = "partial"
        results["note"] = f"Crawl4AI failed: {str(e)}, using fallback method"
        use_simple_fallback = True

    # FIX 6: Fallback to simple HTTP requests for failed URLs
    if use_simple_fallback and results["error_count"] > 0:
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            # Configure retry strategy
            session = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            # Set headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            # Retry failed URLs with simple HTTP
            for item in results["crawled_data"]:
                if item["status"] == "error":
                    url = item["url"]
                    try:
                        response = session.get(url, headers=headers, timeout=30)
                        response.raise_for_status()
                        
                        html_content = response.text
                        
                        # Update the failed item
                        item.update({
                            "status": "success",
                            "html": html_content,
                            "html_size": len(html_content),
                            "markdown": "",
                            "extracted_content": "",
                            "links": {},
                            "media": {},
                            "metadata": {
                                "title": "",
                                "description": "",
                            },
                            "crawl_timestamp": datetime.datetime.now().isoformat(),
                            "content_size_bytes": len(html_content),
                            "method": "requests_fallback",
                        })
                        del item["error_message"]
                        
                        results["success_count"] += 1
                        results["error_count"] -= 1
                        results["total_size_bytes"] += len(html_content)
                        
                    except Exception as e:
                        item["error_message"] = f"Fallback also failed: {str(e)}"
                        item["method"] = "requests_fallback_failed"
                        
        except ImportError:
            results["note"] = results.get("note", "") + " | requests library not available for fallback"

    return results



def extract_structured_data(html_content: str, url: str = "") -> dict:
    """Enhanced structured data extraction with aggressive content finding"""
    try:
        from bs4 import BeautifulSoup
        import re
    except ImportError:
        return {
            "status": "error",
            "error_message": "beautifulsoup4 not installed",
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
        
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()

        # Get ALL text first for entity extraction
        full_text = soup.get_text(separator=' ', strip=True)
        logger.info(f"\n{'='*80}\nEXTRACTING FROM URL: {url}\n{'='*80}")
        logger.info(f"Full text length: {len(full_text)} characters")

        # Title extraction
        title_tag = soup.find("title")
        if title_tag:
            results["structured_data"]["title"] = title_tag.get_text(strip=True)
            logger.info(f"Title: {results['structured_data']['title']}")
        
        # Meta title fallback
        if not results["structured_data"]["title"]:
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                results["structured_data"]["title"] = og_title.get("content")

        # Target main content area
        main_content = (
            soup.find("main") or 
            soup.find("article") or 
            soup.find("div", class_=re.compile(r"article|content|story|post|main", re.I)) or
            soup.find("div", id=re.compile(r"article|content|story|post|main", re.I)) or
            soup.body or
            soup
        )

        # Extract ALL paragraphs from entire page
        all_paragraphs = soup.find_all("p")
        for paragraph in all_paragraphs:
            text = paragraph.get_text(strip=True)
            if len(text) > 20 and not text.startswith("Skip to"):
                results["structured_data"]["paragraphs"].append(text)

        logger.info(f"Extracted {len(results['structured_data']['paragraphs'])} paragraphs")
        if results['structured_data']['paragraphs']:
            logger.info(f"First paragraph sample: {results['structured_data']['paragraphs'][0][:200]}...")

        # Extract headings
        for heading_level in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            for heading in main_content.find_all(heading_level):
                text = heading.get_text(strip=True)
                if text and len(text) > 2:
                    results["structured_data"]["headings"].append({
                        "level": heading_level,
                        "text": text,
                    })

        # Extract tables
        for table_idx, table in enumerate(soup.find_all("table")):
            table_data = {
                "table_id": table_idx,
                "caption": "",
                "headers": [],
                "rows": [],
            }

            caption = table.find("caption")
            if caption:
                table_data["caption"] = caption.get_text(strip=True)

            headers = table.find_all("th")
            if headers:
                table_data["headers"] = [h.get_text(strip=True) for h in headers]

            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    if any(row_data):
                        table_data["rows"].append(row_data)

            if table_data["rows"] or table_data["headers"]:
                results["structured_data"]["tables"].append(table_data)

        # Extract lists
        for list_tag in main_content.find_all(["ul", "ol"]):
            list_items = [li.get_text(strip=True) for li in list_tag.find_all("li")]
            if list_items and len(list_items) > 1:
                results["structured_data"]["lists"].append({
                    "type": list_tag.name,
                    "items": list_items[:20],
                })

        # ENTITY EXTRACTION with detailed logging
        logger.info("\n--- ENTITY EXTRACTION ---")
        
        # Date extraction
        date_patterns = [
            r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
            r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b",
            r"\b\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[a-z]*\.?\s+\d{4}\b",
        ]
        found_dates = set()
        for pattern in date_patterns:
            dates = re.findall(pattern, full_text, re.IGNORECASE)
            found_dates.update(dates)
        
        valid_dates = [d for d in found_dates if not (d.isdigit() and (int(d) < 2000 or int(d) > 2030))]
        results["disaster_entities"]["dates"] = list(set(valid_dates))[:20]
        logger.info(f"Found {len(results['disaster_entities']['dates'])} dates: {results['disaster_entities']['dates']}")

        # Location extraction
        indian_locations = [
            "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
            "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
            "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
            "Nagaland", "Odisha", "Orissa", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", 
            "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
            "Delhi", "Mumbai", "Kolkata", "Chennai", "Bangalore", "Bengaluru", "Hyderabad",
            "Pune", "Ahmedabad", "Jaipur", "Surat", "Lucknow", "Kanpur", "Nagpur",
            "Indore", "Bhopal", "Patna", "Visakhapatnam", "Vadodara", "Ghaziabad",
            "Ludhiana", "Agra", "Nashik", "Faridabad", "Meerut", "Rajkot", "Varanasi",
            "Bay of Bengal", "Arabian Sea", "Indian Ocean"
        ]
        found_locations = set()
        full_text_lower = full_text.lower()
        for location in indian_locations:
            pattern = r'\b' + re.escape(location.lower()) + r'\b'
            if re.search(pattern, full_text_lower):
                found_locations.add(location)
        results["disaster_entities"]["locations"] = sorted(list(found_locations))
        logger.info(f"Found {len(results['disaster_entities']['locations'])} locations: {results['disaster_entities']['locations']}")

        # Event extraction
        disaster_patterns = [
            (r"(?:cyclone|hurricane|typhoon)\s+(\w+)", "cyclone"),
            (r"(?:flood|flooding)\s+(?:in|at|near)\s+([\w\s]+?)(?:\.|,|$)", "flood"),
            (r"(?:earthquake|quake|tremor)(?:\s+of\s+)?(?:magnitude\s+)?([\d.]+)", "earthquake"),
            (r"(?:landslide|mudslide)s?\s+(?:in|at|near)\s+([\w\s]+?)(?:\.|,|$)", "landslide"),
            (r"(?:drought|water crisis)\s+(?:in|at)\s+([\w\s]+?)(?:\.|,|$)", "drought"),
        ]
        found_events = set()
        
        for pattern, event_type in disaster_patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    found_events.add(f"{event_type}: {match[0]}")
                else:
                    found_events.add(f"{event_type}: {match}")
        
        disaster_keywords = [
            "flood", "drought", "cyclone", "earthquake", "landslide", "tsunami",
            "disaster", "emergency", "evacuation", "relief", "storm", "monsoon"
        ]
        for keyword in disaster_keywords:
            if keyword in full_text_lower:
                found_events.add(keyword)
        
        results["disaster_entities"]["event_names"] = list(found_events)[:20]
        logger.info(f"Found {len(results['disaster_entities']['event_names'])} events: {results['disaster_entities']['event_names']}")

        # Casualty extraction
        casualty_patterns = [
            r"(\d+)\s+(?:people\s+)?(?:killed|dead|deaths|died|fatalities)",
            r"death\s+toll\s+(?:of\s+)?(\d+)",
            r"(\d+)\s+(?:people\s+)?(?:injured|wounded|hurt)",
            r"(\d+)\s+(?:people\s+)?(?:missing|displaced|evacuated|affected)",
        ]
        found_casualties = []
        for pattern in casualty_patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            found_casualties.extend(matches)
        results["disaster_entities"]["casualties"] = found_casualties[:10]
        logger.info(f"Found {len(results['disaster_entities']['casualties'])} casualty numbers: {results['disaster_entities']['casualties']}")

        # Add metadata
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            results["structured_data"]["metadata"]["description"] = meta_desc.get("content")
        
        if results["structured_data"]["paragraphs"]:
            results["structured_data"]["metadata"]["summary"] = results["structured_data"]["paragraphs"][0][:300]

        logger.info(f"{'='*80}\n")

    except Exception as e:
        results["status"] = "error"
        results["error_message"] = str(e)
        import traceback
        logger.error(f"Extraction error: {traceback.format_exc()}")

    return results


# ============================================================================
# Kafka Message Packet Generation
# ============================================================================

UPDATED_AGENT_INSTRUCTION = """
You are an autonomous disaster data collection agent with expertise in gathering and
processing disaster-related information from the web.

**Primary Function**: Use `collect_and_process_disaster_data()` for complete workflows

When users request disaster data collection, follow this approach:

1. **For Complete Workflows**: Use `collect_and_process_disaster_data(disaster_type, max_urls)`
   - This handles: search → crawl → validate → extract → generate kafka packets
   - Example: "Search for flood data" → call collect_and_process_disaster_data("floods", 1)

2. **For Individual Steps** (advanced users):
   - search_web_for_disaster_data() - for just searching
   - crawl_urls_with_ai() - for just crawling
   - validate_and_extract() - for validation + extraction
   - generate_kafka_packets_v2() - for packet generation

**Response Guidelines**:
- Always show the workflow progress (Step 1, Step 2, etc.)
- Display extracted entity counts (dates, locations, events)
- Show packet generation summary
- Provide sample packet structure

**Disaster Types**: floods, droughts, cyclones, earthquakes, landslides, all

Example responses:
- "I found 3 flood-related articles, crawled successfully, extracted 15 locations and 8 dates, generated 3 Kafka packets"
- Show a sample packet structure for reference
"""


def validate_disaster_content(html_content: str) -> dict:
    """
    Validates if HTML content contains disaster-related information
    
    Returns:
        dict with is_valid, confidence_score, and reason
    """
    from bs4 import BeautifulSoup
    
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text().lower()
        
        # Check for disaster keywords
        disaster_keywords = {
            'high': ['flood', 'cyclone', 'earthquake', 'landslide', 'tsunami', 'drought'],
            'medium': ['disaster', 'emergency', 'evacuation', 'relief', 'casualties', 'deaths'],
            'low': ['india', 'alert', 'warning', 'affected']
        }
        
        score = 0
        for keyword in disaster_keywords['high']:
            if keyword in text:
                score += 3
        for keyword in disaster_keywords['medium']:
            if keyword in text:
                score += 2
        for keyword in disaster_keywords['low']:
            if keyword in text:
                score += 1
        
        # Check for blocked/error pages
        error_phrases = ['page not found', 'blocked', 'banned', 'not available', 
                        'access denied', '404', '403', 'error']
        is_error_page = any(phrase in text for phrase in error_phrases)
        
        # Check minimum content length
        has_content = len(text) > 500
        
        is_valid = score >= 5 and has_content and not is_error_page
        
        return {
            "is_valid": is_valid,
            "confidence_score": score,
            "has_content": has_content,
            "is_error_page": is_error_page,
            "reason": f"Score: {score}, Content: {has_content}, Error: {is_error_page}"
        }
        
    except Exception as e:
        return {
            "is_valid": False,
            "confidence_score": 0,
            "reason": f"Validation error: {str(e)}"
        }


def validate_and_extract(crawl_results: str) -> dict:
    """
    Validates crawled content and extracts structured data only from valid pages
    
    Args:
        crawl_results: JSON string of crawl results
        
    Returns:
        dict with validation and extraction results
    """
    try:
        crawl_data = json.loads(crawl_results)
    except (json.JSONDecodeError, TypeError):
        return {
            "status": "error",
            "error_message": "Invalid crawl results format"
        }
    
    results = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "validated_extractions": [],
        "skipped_count": 0,
        "extracted_count": 0,
    }
    
    crawled_items = crawl_data.get("crawled_data", [])
    
    for item in crawled_items:
        if item.get("status") != "success":
            results["skipped_count"] += 1
            continue
            
        url = item.get("url", "")
        html = item.get("html", "")
        
        # Validate content first
        validation = validate_disaster_content(html)
        
        if validation["is_valid"]:
            # Extract structured data
            extraction = extract_structured_data(html, url)
            
            results["validated_extractions"].append({
                "url": url,
                "validation": validation,
                "extraction": extraction,
                "status": "extracted"
            })
            results["extracted_count"] += 1
        else:
            results["validated_extractions"].append({
                "url": url,
                "validation": validation,
                "status": "skipped",
                "reason": validation["reason"]
            })
            results["skipped_count"] += 1
    
    return results

# def cluster_related_content_with_llm(paragraphs: list, entities: dict, tables: list, url_metadata: dict) -> list:
#     """
#     Use LLM to cluster related paragraphs/tables into discrete events.
    
#     Args:
#         paragraphs: List of paragraph texts
#         entities: Extracted entities (dates, locations, etc.)
#         tables: Extracted tables
#         url_metadata: Metadata like publish date, title
        
#     Returns:
#         List of discrete events with clustered content
#     """
#     from google import genai
#     import os
    
#     logger.info(f"\n{'='*80}\nCLUSTERING CONTENT INTO DISCRETE EVENTS\n{'='*80}")
    
#     # Prepare content for LLM
#     content_blocks = []
    
#     # Add paragraphs with IDs
#     for idx, para in enumerate(paragraphs[:50]):  # Limit to first 50 paragraphs
#         if len(para) > 50:
#             content_blocks.append(f"PARAGRAPH_{idx}: {para}")
    
#     # Add tables with IDs
#     for idx, table in enumerate(tables[:10]):  # Limit to first 10 tables
#         table_str = f"TABLE_{idx} (Caption: {table.get('caption', 'N/A')})\n"
#         table_str += f"Headers: {table.get('headers', [])}\n"
#         for row_idx, row in enumerate(table.get('rows', [])[:20]):
#             table_str += f"Row {row_idx}: {row}\n"
#         content_blocks.append(table_str)
    
#     # Prepare prompt
#     prompt = f"""You are a disaster event extraction expert. Analyze this web content and extract DISCRETE disaster events.

# **ARTICLE METADATA:**
# - URL: {url_metadata.get('url', 'Unknown')}
# - Title: {url_metadata.get('title', 'Unknown')}
# - Published: {url_metadata.get('publish_date', 'Unknown')}
# - Current Date: {datetime.datetime.now().strftime('%Y-%m-%d')}

# **AVAILABLE ENTITIES:**
# - Dates found: {entities.get('dates', [])}
# - Locations found: {entities.get('locations', [])}
# - Events mentioned: {entities.get('event_names', [])}
# - Casualties: {entities.get('casualties', [])}

# **CONTENT BLOCKS:**
# {chr(10).join(content_blocks[:30])}  # Limit to avoid token limits

# **TASK:**
# Extract discrete disaster events. Each event should have:
# 1. **Event ID**: Unique identifier
# 2. **Event Type**: flood/earthquake/cyclone/drought/landslide
# 3. **Description**: Brief summary (max 200 chars)
# 4. **Start Date**: When event started (YYYY-MM-DD format, use "RELATIVE:today" if mentioned as "today")
# 5. **End Date**: When event ended (YYYY-MM-DD or null if ongoing)
# 6. **Locations**: List of affected locations
# 7. **Primary Location**: Main affected area
# 8. **Content IDs**: Which PARAGRAPH_X or TABLE_X IDs describe this event
# 9. **Casualties**: Deaths, injured, displaced (numbers only)
# 10. **Severity**: low/medium/high

# **RULES:**
# - If multiple paragraphs describe the SAME event (same date+location), cluster them into ONE event
# - Use relative dates like "today", "yesterday" and convert using publish date
# - Extract dates from tables if paragraph lacks dates
# - Mark event as "ongoing" if no end date mentioned
# - Set severity based on casualties (>100 deaths = high, >10 = medium, else low)

# **OUTPUT FORMAT (JSON):**
# {{
#   "events": [
#     {{
#       "event_id": "event_1",
#       "event_type": "earthquake",
#       "description": "6.5 magnitude earthquake in Delhi-NCR",
#       "start_date": "2025-02-17",
#       "end_date": null,
#       "locations": ["Delhi", "Faridabad", "Ghaziabad"],
#       "primary_location": "Delhi",
#       "content_ids": ["PARAGRAPH_3", "PARAGRAPH_5", "PARAGRAPH_7"],
#       "casualties": {{"deaths": 4, "injured": 20, "displaced": 0}},
#       "severity": "medium"
#     }}
#   ]
# }}

# Return ONLY valid JSON. No explanations."""

#     try:
#         client = genai.Client(api_key=os.environ.get('GOOGLE_API_KEY'))
#         response = client.models.generate_content(
#             model='gemini-2.0-flash-exp',
#             contents=prompt
#         )
        
#         # Parse LLM response
#         response_text = response.text.strip()
#         if response_text.startswith('```json'):
#             response_text = response_text.split('```json')[1].split('```')[0].strip()
#         elif response_text.startswith('```'):
#             response_text = response_text.split('```')[1].split('```')[0].strip()
        
#         events_data = json.loads(response_text)
#         events = events_data.get('events', [])
        
#         logger.info(f"LLM extracted {len(events)} discrete events")
#         for event in events:
#             logger.info(f"  - {event.get('event_id')}: {event.get('event_type')} at {event.get('primary_location')} on {event.get('start_date')}")
        
#         return events
        
#     except Exception as e:
#         logger.error(f"LLM clustering failed: {str(e)}")
#         import traceback
#         logger.error(traceback.format_exc())
#         return []

def cluster_related_content_with_llm(
    paragraphs: list, 
    entities: dict, 
    tables: list, 
    url_metadata: dict,
    time_bounds: dict = None  # NEW PARAMETER
) -> list:
    """
    Use LLM to cluster related paragraphs/tables into discrete events.
    NOW WITH TIME FILTERING.
    """
    from google import genai
    import os
    
    logger.info(f"\n{'='*80}\nCLUSTERING CONTENT INTO DISCRETE EVENTS\n{'='*80}")
    
    # Log time bounds
    if time_bounds:
        logger.info(f"TIME CONSTRAINT: {time_bounds.get('start_date')} to {time_bounds.get('end_date')}")
        logger.info(f"Description: {time_bounds.get('temporal_description')}")
    
    # Prepare content for LLM
    content_blocks = []
    
    for idx, para in enumerate(paragraphs[:50]):
        if len(para) > 50:
            content_blocks.append(f"PARAGRAPH_{idx}: {para}")
    
    for idx, table in enumerate(tables[:10]):
        table_str = f"TABLE_{idx} (Caption: {table.get('caption', 'N/A')})\n"
        table_str += f"Headers: {table.get('headers', [])}\n"
        for row_idx, row in enumerate(table.get('rows', [])[:20]):
            table_str += f"Row {row_idx}: {row}\n"
        content_blocks.append(table_str)
    
    # Build time constraint prompt
    time_constraint_text = ""
    if time_bounds and time_bounds.get('has_time_constraint'):
        time_constraint_text = f"""
**CRITICAL TIME CONSTRAINT**:
- User requested: {time_bounds.get('temporal_description')}
- Start Date: {time_bounds.get('start_date')}
- End Date: {time_bounds.get('end_date')}
- TODAY: {datetime.datetime.now().strftime('%Y-%m-%d')}

**YOU MUST**:
1. ONLY extract events that occurred between {time_bounds.get('start_date')} and {time_bounds.get('end_date')}
2. IGNORE all events outside this date range
3. If an event has no clear date, try to infer from context
4. If you cannot determine the date, do NOT include the event
"""
    else:
        time_constraint_text = f"**TODAY'S DATE**: {datetime.datetime.now().strftime('%Y-%m-%d')}"
    
    # Prepare prompt
    prompt = f"""You are a disaster event extraction expert. Analyze this web content and extract DISCRETE disaster events.

**ARTICLE METADATA:**
- URL: {url_metadata.get('url', 'Unknown')}
- Title: {url_metadata.get('title', 'Unknown')}
- Published: {url_metadata.get('publish_date', 'Unknown')}

{time_constraint_text}

**AVAILABLE ENTITIES:**
- Dates found: {entities.get('dates', [])}
- Locations found: {entities.get('locations', [])}
- Events mentioned: {entities.get('event_names', [])}
- Casualties: {entities.get('casualties', [])}

**CONTENT BLOCKS:**
{chr(10).join(content_blocks[:30])}

**TASK:**
Extract discrete disaster events. Each event should have:
1. **Event ID**: Unique identifier
2. **Event Type**: flood/earthquake/cyclone/drought/landslide
3. **Description**: Brief summary (max 200 chars)
4. **Start Date**: When event started (YYYY-MM-DD format)
5. **End Date**: When event ended (YYYY-MM-DD or null if ongoing)
6. **Locations**: List of affected locations
7. **Primary Location**: Main affected area
8. **Content IDs**: Which PARAGRAPH_X or TABLE_X IDs describe this event
9. **Casualties**: Deaths, injured, displaced (numbers only)
10. **Severity**: low/medium/high

**RULES:**
- If multiple paragraphs describe the SAME event (same date+location), cluster them into ONE event
- Use relative dates like "today", "yesterday" and convert using current date
- Extract dates from tables if paragraph lacks dates
- Set severity based on casualties (>100 deaths = high, >10 = medium, else low)
- **FILTER OUT events outside the time constraint**

**OUTPUT FORMAT (JSON):**
{{
  "events": [
    {{
      "event_id": "event_1",
      "event_type": "earthquake",
      "description": "6.5 magnitude earthquake in Delhi-NCR",
      "start_date": "2025-11-06",
      "end_date": null,
      "locations": ["Delhi", "Faridabad"],
      "primary_location": "Delhi",
      "content_ids": ["PARAGRAPH_3", "PARAGRAPH_5"],
      "casualties": {{"deaths": 4, "injured": 20, "displaced": 0}},
      "severity": "medium"
    }}
  ]
}}

Return ONLY valid JSON. No explanations."""

    try:
        client = genai.Client(api_key=os.environ.get('GOOGLE_API_KEY'))
        response = client.models.generate_content(
            model='gemini-2.0-flash-exp',
            contents=prompt
        )
        
        response_text = response.text.strip()
        if response_text.startswith('```json'):
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif response_text.startswith('```'):
            response_text = response_text.split('```')[1].split('```')[0].strip()
        
        events_data = json.loads(response_text)
        events = events_data.get('events', [])
        
        logger.info(f"LLM extracted {len(events)} discrete events (before post-filtering)")
        for event in events:
            logger.info(f"  - {event.get('event_id')}: {event.get('event_type')} at {event.get('primary_location')} on {event.get('start_date')}")
        
        return events
        
    except Exception as e:
        logger.error(f"LLM clustering failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []
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
        "Autonomous disaster data collection agent for India. Creates discrete "
        "Kafka packets for individual disaster events with specific dates and locations."
    ),
    instruction="""
You are a disaster data collection agent for India.

**PRIMARY FUNCTION**: Use collect_and_process_disaster_data() for complete workflows.

This function now generates DISCRETE EVENT PACKETS:
- One packet per disaster event
- Each packet has specific date/time (start_date, end_date)
- Each packet has specific location
- Separate impact metrics per event

When users request disaster data:
1. Call collect_and_process_disaster_data(disaster_type, max_urls)
2. Show how many discrete events were found
3. Display sample event packet showing temporal and spatial fields

**Disaster Types**: floods, droughts, cyclones, earthquakes, landslides, all

**Example**:
User: "Search for flood data"
You: "Found 3 discrete flood events: 
- Event 1: Kerala floods (Aug 15, 2024) - 25 deaths
- Event 2: Maharashtra floods (Aug 18-20, 2024) - 12 affected
- Event 3: Assam floods (Aug 22, 2024) - ongoing
Generated 3 separate Kafka packets."
""",
    tools=[
        collect_and_process_disaster_data,
        search_web_for_disaster_data,
        crawl_urls_with_ai,
        validate_and_extract,
        generate_discrete_event_packets,      # NEW: Primary packet generator
        generate_enhanced_kafka_packets,      # Keep for fallback
        format_kafka_packets_for_output,
    ],
)
