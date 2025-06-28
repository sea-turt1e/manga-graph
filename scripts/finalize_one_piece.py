#!/usr/bin/env python3
"""
Finalize ONE PIECE normalization
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def finalize_one_piece():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Merge "One piece" into "ONE PIECE"
        logger.info("Merging 'One piece' into 'ONE PIECE'...")
        
        # First, ensure canonical exists
        session.run("""
            MERGE (w:Work {title: 'ONE PIECE', type: 'Manga'})
        """)
        
        # Move all relationships from "One piece" to "ONE PIECE"
        # First, move author relationships
        result = session.run("""
            MATCH (old:Work {title: 'One piece'})
            MATCH (new:Work {title: 'ONE PIECE'})
            MATCH (a:Author)-[r:CREATED]->(old)
            MERGE (a)-[:CREATED]->(new)
            DELETE r
            RETURN count(r) as moved
        """)
        author_moved = result.single()["moved"]
        logger.info(f"Moved {author_moved} author relationships")
        
        # Move publisher relationships
        result = session.run("""
            MATCH (old:Work {title: 'One piece'})
            MATCH (new:Work {title: 'ONE PIECE'})
            MATCH (p:Publisher)-[r:PUBLISHED]->(old)
            MERGE (p)-[:PUBLISHED]->(new)
            DELETE r
            RETURN count(r) as moved
        """)
        pub_moved = result.single()["moved"]
        logger.info(f"Moved {pub_moved} publisher relationships")
        
        # Move magazine relationships
        result = session.run("""
            MATCH (old:Work {title: 'One piece'})
            MATCH (new:Work {title: 'ONE PIECE'})
            MATCH (m:Magazine)-[r:CONTAINS]->(old)
            MERGE (m)-[:CONTAINS]->(new)
            DELETE r
            RETURN count(r) as moved
        """)
        mag_moved = result.single()["moved"]
        logger.info(f"Moved {mag_moved} magazine relationships")
        
        # Delete all "One piece" nodes
        result = session.run("""
            MATCH (w:Work {title: 'One piece'})
            DETACH DELETE w
            RETURN count(w) as deleted
        """)
        
        deleted = result.single()["deleted"]
        logger.info(f"Deleted {deleted} 'One piece' nodes")
        
        # Check final state
        result = session.run("""
            MATCH (w:Work)
            WHERE w.title =~ '(?i).*one\\s*piece.*'
            RETURN w.title as title, count(w) as count
            ORDER BY count DESC
            LIMIT 5
        """)
        
        logger.info("\nFinal ONE PIECE variations:")
        for record in result:
            logger.info(f"  {record['title']}: {record['count']} instances")
    
    driver.close()

if __name__ == "__main__":
    finalize_one_piece()