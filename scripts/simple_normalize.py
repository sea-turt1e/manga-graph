#!/usr/bin/env python3
"""
Simple normalization approach
"""

import os
import re
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class SimpleNormalizer:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

    def close(self):
        self.driver.close()

    def normalize_one_piece(self):
        """Normalize ONE PIECE titles"""
        logger.info("Normalizing ONE PIECE variations...")
        
        with self.driver.session() as session:
            # Create canonical ONE PIECE if not exists
            session.run("""
                MERGE (w:Work {title: 'ONE PIECE', type: 'Manga'})
            """)
            
            # Get all variations
            result = session.run("""
                MATCH (w:Work)
                WHERE w.title =~ '(?i).*one\\s*piece.*' AND w.title <> 'ONE PIECE'
                RETURN DISTINCT w.title as title
            """)
            
            variations = [r["title"] for r in result]
            logger.info(f"Found {len(variations)} ONE PIECE variations")
            
            for title in variations:
                logger.info(f"Processing: {title}")
                
                # Move authors
                session.run("""
                    MATCH (a:Author)-[r:CREATED]->(w:Work {title: $title})
                    MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                    MERGE (a)-[:CREATED]->(canonical)
                    DELETE r
                """, title=title)
                
                # Move publishers
                session.run("""
                    MATCH (p:Publisher)-[r:PUBLISHED]->(w:Work {title: $title})
                    MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                    MERGE (p)-[:PUBLISHED]->(canonical)
                    DELETE r
                """, title=title)
                
                # Move magazines
                session.run("""
                    MATCH (m:Magazine)-[r:CONTAINS]->(w:Work {title: $title})
                    MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                    MERGE (m)-[:CONTAINS]->(canonical)
                    DELETE r
                """, title=title)
                
                # Delete the duplicate (with any remaining relationships)
                session.run("""
                    MATCH (w:Work {title: $title})
                    DETACH DELETE w
                """, title=title)

    def normalize_author_prefixes(self):
        """Remove role prefixes from author names"""
        logger.info("Removing author role prefixes...")
        
        with self.driver.session() as session:
            # Find all authors with prefixes
            result = session.run("""
                MATCH (a:Author)
                WHERE a.name =~ '^\\[.*?\\].*'
                RETURN a.name as name
                LIMIT 5000
            """)
            
            authors_with_prefixes = [r["name"] for r in result]
            logger.info(f"Found {len(authors_with_prefixes)} authors with prefixes")
            
            processed = 0
            for old_name in authors_with_prefixes:
                # Remove prefix
                new_name = re.sub(r'^\[.*?\]\s*', '', old_name).strip()
                
                if new_name and new_name != old_name:
                    # Check if normalized version exists
                    check = session.run("""
                        MATCH (a:Author {name: $name})
                        RETURN count(a) as count
                    """, name=new_name)
                    
                    exists = check.single()["count"] > 0
                    
                    if exists:
                        # Merge into existing
                        session.run("""
                            MATCH (old:Author {name: $old_name})
                            MATCH (new:Author {name: $new_name})
                            OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
                            WITH old, new, w, r
                            WHERE r IS NOT NULL
                            MERGE (new)-[:CREATED]->(w)
                            DELETE r
                            WITH old
                            DELETE old
                        """, old_name=old_name, new_name=new_name)
                    else:
                        # Just update the name
                        session.run("""
                            MATCH (a:Author {name: $old_name})
                            SET a.name = $new_name
                        """, old_name=old_name, new_name=new_name)
                    
                    processed += 1
                    if processed % 100 == 0:
                        logger.info(f"Processed {processed} authors")

    def run(self):
        """Run all normalizations"""
        try:
            self.normalize_one_piece()
            self.normalize_author_prefixes()
            
            # Report final counts
            with self.driver.session() as session:
                result = session.run("MATCH (a:Author) RETURN count(a) as count")
                author_count = result.single()["count"]
                
                result = session.run("MATCH (w:Work {type: 'Manga'}) RETURN count(w) as count")
                manga_count = result.single()["count"]
                
                logger.info(f"\nFinal counts - Authors: {author_count}, Manga: {manga_count}")
                
        except Exception as e:
            logger.error(f"Error during normalization: {e}")
            raise


def main():
    normalizer = SimpleNormalizer()
    try:
        normalizer.run()
    finally:
        normalizer.close()


if __name__ == "__main__":
    main()