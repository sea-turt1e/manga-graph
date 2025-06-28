#!/usr/bin/env python3
"""
Final cleanup of author duplicates
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def final_cleanup():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Merge duplicate 尾田栄一郎
        logger.info("Merging 尾田栄一郎 duplicates...")
        
        # First merge the two plain 尾田栄一郎 instances
        result = session.run("""
            MATCH (a:Author {name: '尾田栄一郎'})
            WITH collect(a) as authors
            WHERE size(authors) > 1
            WITH authors[0] as keep, authors[1..] as remove, size(authors[1..]) as remove_count
            UNWIND remove as old
            OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
            WITH keep, old, collect(w) as works, remove_count
            FOREACH (w IN works | 
                MERGE (keep)-[:CREATED]->(w)
            )
            DETACH DELETE old
            RETURN remove_count as merged
        """)
        result_data = result.single()
        merged = result_data["merged"] if result_data else 0
        logger.info(f"Merged {merged} duplicate 尾田栄一郎 nodes")
        
        # Check for any remaining duplicates across all authors
        logger.info("\nChecking for remaining author duplicates...")
        result = session.run("""
            MATCH (a:Author)
            WITH a.name as name, count(a) as count
            WHERE count > 1
            RETURN name, count
            ORDER BY count DESC
            LIMIT 20
        """)
        
        duplicates = [(r["name"], r["count"]) for r in result]
        if duplicates:
            logger.info(f"Found {len(duplicates)} duplicate author groups:")
            for name, count in duplicates[:5]:
                logger.info(f"  {name}: {count} instances")
            
            # Merge all duplicates
            for name, count in duplicates:
                session.run("""
                    MATCH (a:Author {name: $name})
                    WITH collect(a) as authors
                    WITH authors[0] as keep, authors[1..] as remove
                    UNWIND remove as old
                    OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
                    WITH keep, old, collect(w) as works
                    FOREACH (w IN works | 
                        MERGE (keep)-[:CREATED]->(w)
                    )
                    DETACH DELETE old
                """, name=name)
            
            logger.info(f"Merged all {len(duplicates)} duplicate author groups")
        
        # Final statistics
        result = session.run("MATCH (a:Author) RETURN count(a) as count")
        author_count = result.single()["count"]
        
        result = session.run("MATCH (w:Work {type: 'Manga'}) RETURN count(w) as count")
        manga_count = result.single()["count"]
        
        logger.info(f"\nFinal database statistics:")
        logger.info(f"  Total authors: {author_count}")
        logger.info(f"  Total manga: {manga_count}")
    
    driver.close()

if __name__ == "__main__":
    final_cleanup()