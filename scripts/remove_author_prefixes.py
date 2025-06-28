#!/usr/bin/env python3
"""
Remove author role prefixes efficiently
"""

import os
import re
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def remove_prefixes():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        # Process authors with single brackets
        logger.info("Processing authors with [role] prefixes...")
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name =~ '^\\[.*?\\].*' AND NOT a.name =~ '^\\[\\[.*?\\]\\].*'
            WITH a, 
                 substring(a.name, size(split(a.name, ']')[0]) + 1) as new_name,
                 a.name as old_name
            WHERE trim(new_name) <> ''
            SET a.name = trim(new_name)
            RETURN count(a) as updated
        """)
        count1 = result.single()["updated"]
        logger.info(f"Updated {count1} authors with single bracket prefixes")
        
        # Process authors with double brackets
        logger.info("Processing authors with [[role]] prefixes...")
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name =~ '^\\[\\[.*?\\]\\].*'
            WITH a,
                 substring(a.name, size(split(a.name, ']]')[0]) + 2) as new_name,
                 a.name as old_name
            WHERE trim(new_name) <> ''
            SET a.name = trim(new_name)
            RETURN count(a) as updated
        """)
        count2 = result.single()["updated"]
        logger.info(f"Updated {count2} authors with double bracket prefixes")
        
        # Handle multiple authors in one field
        logger.info("Processing multiple authors in single field...")
        result = session.run("""
            MATCH (a:Author)
            WHERE a.name CONTAINS ',' AND (a.name CONTAINS '[' OR a.name CONTAINS ']')
            RETURN a.name as name
        """)
        
        multi_authors = [r["name"] for r in result]
        logger.info(f"Found {len(multi_authors)} multi-author entries to process")
        
        for old_name in multi_authors:
            # Clean each part
            parts = old_name.split(',')
            cleaned_parts = []
            for part in parts:
                part = part.strip()
                # Remove brackets
                part = re.sub(r'^\[.*?\]\s*', '', part)
                part = re.sub(r'^\[\[.*?\]\]\s*', '', part)
                if part:
                    cleaned_parts.append(part)
            
            if cleaned_parts:
                new_name = ', '.join(cleaned_parts)
                if new_name != old_name:
                    session.run("""
                        MATCH (a:Author {name: $old_name})
                        SET a.name = $new_name
                    """, old_name=old_name, new_name=new_name)
        
        # Merge duplicates created by prefix removal
        logger.info("Merging duplicates created by prefix removal...")
        result = session.run("""
            MATCH (a:Author)
            WITH a.name as name, collect(a) as authors
            WHERE size(authors) > 1
            RETURN name, size(authors) as count
            ORDER BY count DESC
            LIMIT 100
        """)
        
        duplicates = [(r["name"], r["count"]) for r in result]
        logger.info(f"Found {len(duplicates)} duplicate author groups")
        
        for name, count in duplicates:
            # Keep first, merge others
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
        
        logger.info("Prefix removal complete!")
    
    driver.close()

if __name__ == "__main__":
    remove_prefixes()