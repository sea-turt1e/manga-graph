#!/usr/bin/env python3
"""
Check the current status of normalization
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def check_status():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Check authors with prefixes
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name =~ '^\\[.*?\\].*'
            RETURN count(a) as count
        """)
        prefix_count = result.single()["count"]
        logger.info(f"Authors with role prefixes: {prefix_count}")
        
        # Check duplicate ONE PIECE
        result = session.run("""
            MATCH (w:Work)
            WHERE w.title =~ '(?i).*one\\s*piece.*'
            RETURN w.title as title, count(w) as count
            ORDER BY count DESC
            LIMIT 10
        """)
        logger.info("\nONE PIECE variations:")
        for record in result:
            logger.info(f"  {record['title']}: {record['count']} instances")
        
        # Check specific author duplicates
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name CONTAINS '尾田栄一郎'
            RETURN a.name as name, count(a) as count
        """)
        logger.info("\n尾田栄一郎 variations:")
        for record in result:
            logger.info(f"  {record['name']}: {record['count']} instances")
        
        # Check works with volume numbers
        result = session.run("""
            MATCH (w:Work {type: 'Manga'})
            WHERE w.title =~ '.*\\d+$' OR w.title =~ '.*[(（]\\d+[)）]$'
            RETURN count(w) as count
        """)
        volume_count = result.single()["count"]
        logger.info(f"\nWorks with volume numbers: {volume_count}")
    
    driver.close()

if __name__ == "__main__":
    check_status()