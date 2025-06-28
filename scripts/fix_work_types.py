#!/usr/bin/env python3
"""
Fix work types in the database
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

def fix_work_types():
    """Set all Work nodes to have type='Manga'"""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Count works without type
        result = session.run("""
            MATCH (w:Work)
            WHERE w.type IS NULL
            RETURN count(w) as count
        """)
        count = result.single()["count"]
        logger.info(f"Found {count} works without type")
        
        if count > 0:
            # Update all works to have type='Manga'
            logger.info("Setting type='Manga' for all works...")
            result = session.run("""
                MATCH (w:Work)
                WHERE w.type IS NULL
                SET w.type = 'Manga'
                RETURN count(w) as updated
            """)
            updated = result.single()["updated"]
            logger.info(f"Updated {updated} works")
    
    driver.close()

if __name__ == "__main__":
    fix_work_types()