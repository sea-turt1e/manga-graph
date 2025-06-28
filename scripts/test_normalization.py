#!/usr/bin/env python3
"""
Test normalization with sample data
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_current_state():
    """Check current database state for specific examples"""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Check ONE PIECE variations
        logger.info("\n=== Checking ONE PIECE variations ===")
        result = session.run("""
            MATCH (w:Work)
            WHERE w.title =~ '(?i).*one\\s*piece.*'
            RETURN w.title as title, w.type as type
            ORDER BY w.title
            LIMIT 20
        """)
        
        one_piece_titles = []
        for record in result:
            title = record["title"]
            one_piece_titles.append(title)
            logger.info(f"Found: {title} (type: {record['type']})")
        
        # Check author variations (e.g., 尾田栄一郎)
        logger.info("\n=== Checking author variations (尾田栄一郎) ===")
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name CONTAINS '尾田栄一郎'
            RETURN a.name as name
        """)
        
        for record in result:
            logger.info(f"Found author: {record['name']}")
        
        # Check works with volume numbers
        logger.info("\n=== Checking works with volume numbers ===")
        result = session.run("""
            MATCH (w:Work {type: 'Manga'})
            WHERE w.title =~ '.*\\d+$' OR w.title =~ '.*[(（]\\d+[)）]$'
            RETURN w.title as title
            ORDER BY w.title
            LIMIT 10
        """)
        
        for record in result:
            logger.info(f"Found volume: {record['title']}")
        
        # Count total authors and works
        result = session.run("MATCH (a:Author) RETURN count(a) as count")
        author_count = result.single()["count"]
        
        result = session.run("MATCH (w:Work {type: 'Manga'}) RETURN count(w) as count")
        manga_count = result.single()["count"]
        
        logger.info(f"\n=== Current totals ===")
        logger.info(f"Total authors: {author_count}")
        logger.info(f"Total manga: {manga_count}")
    
    driver.close()

if __name__ == "__main__":
    test_current_state()