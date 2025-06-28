#!/usr/bin/env python3
"""
Batch normalization for large databases
"""

import os
import re
from typing import Dict, Set, Optional, List
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class BatchNormalizer:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        self.batch_size = 1000

    def close(self):
        self.driver.close()

    def normalize_author_name(self, name: str) -> str:
        """Remove role prefixes from author names"""
        # Remove role prefixes like [著], [原作], [作画] etc.
        name = re.sub(r'^\[.*?\]\s*', '', name)
        # Remove double brackets
        name = re.sub(r'^\[\[.*?\]\]\s*', '', name)
        # Handle multiple authors (e.g., "[著]武井宏文, 尾田栄一郎")
        if ',' in name:
            parts = name.split(',')
            normalized_parts = []
            for part in parts:
                part = part.strip()
                part = re.sub(r'^\[.*?\]\s*', '', part)
                if part:
                    normalized_parts.append(part)
            return ', '.join(normalized_parts)
        
        return name.strip()

    def normalize_one_piece_titles(self):
        """Specifically handle ONE PIECE variations"""
        logger.info("Normalizing ONE PIECE titles...")
        
        with self.driver.session() as session:
            # Find all ONE PIECE variations
            result = session.run("""
                MATCH (w:Work)
                WHERE w.title =~ '(?i).*one\\s*piece.*' AND w.title <> 'ONE PIECE'
                RETURN w.title as title, count(w) as count
                ORDER BY count DESC
            """)
            
            variations = [(r["title"], r["count"]) for r in result]
            logger.info(f"Found {len(variations)} ONE PIECE variations")
            
            # Merge all variations into "ONE PIECE"
            for title, count in variations:
                if title != "ONE PIECE":
                    logger.info(f"Merging {count} instances of '{title}' -> 'ONE PIECE'")
                    
                    # First ensure canonical exists
                    session.run("""
                        MERGE (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                    """)
                    
                    # Move relationships - one at a time
                    # Move CREATED relationships
                    session.run("""
                        MATCH (old:Work {title: $old_title})
                        MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                        OPTIONAL MATCH (a:Author)-[r:CREATED]->(old)
                        WITH old, canonical, collect({author: a, rel: r}) as rels
                        FOREACH (rel IN rels |
                            MERGE (rel.author)-[:CREATED]->(canonical)
                            DELETE rel.rel
                        )
                        RETURN count(rels) as moved
                    """, old_title=title)
                    
                    # Move PUBLISHED relationships
                    session.run("""
                        MATCH (old:Work {title: $old_title})
                        MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                        OPTIONAL MATCH (p:Publisher)-[r:PUBLISHED]->(old)
                        WITH old, canonical, collect({publisher: p, rel: r}) as rels
                        FOREACH (rel IN rels |
                            MERGE (rel.publisher)-[:PUBLISHED]->(canonical)
                            DELETE rel.rel
                        )
                        RETURN count(rels) as moved
                    """, old_title=title)
                    
                    # Move CONTAINS relationships
                    session.run("""
                        MATCH (old:Work {title: $old_title})
                        MATCH (canonical:Work {title: 'ONE PIECE', type: 'Manga'})
                        OPTIONAL MATCH (m:Magazine)-[r:CONTAINS]->(old)
                        WITH old, canonical, collect({magazine: m, rel: r}) as rels
                        FOREACH (rel IN rels |
                            MERGE (rel.magazine)-[:CONTAINS]->(canonical)
                            DELETE rel.rel
                        )
                        RETURN count(rels) as moved
                    """, old_title=title)
                    
                    # Delete old node
                    session.run("""
                        MATCH (old:Work {title: $old_title})
                        DELETE old
                    """, old_title=title)

    def normalize_authors_batch(self):
        """Normalize authors in batches"""
        logger.info("Starting batch author normalization...")
        
        with self.driver.session() as session:
            # Get total count
            result = session.run("MATCH (a:Author) RETURN count(a) as count")
            total = result.single()["count"]
            logger.info(f"Total authors to process: {total}")
            
            # Process in batches
            offset = 0
            merged_count = 0
            
            while offset < total:
                # Get batch of authors
                result = session.run("""
                    MATCH (a:Author)
                    RETURN a.name as name
                    ORDER BY a.name
                    SKIP $offset
                    LIMIT $limit
                """, offset=offset, limit=self.batch_size)
                
                batch_names = [r["name"] for r in result]
                
                # Group by normalized name
                groups = {}
                for name in batch_names:
                    normalized = self.normalize_author_name(name)
                    if normalized and normalized != name:  # Only process if different
                        if normalized not in groups:
                            groups[normalized] = []
                        groups[normalized].append(name)
                
                # Merge within batch
                for normalized, originals in groups.items():
                    if len(originals) > 0:
                        # Check if normalized version exists
                        check_result = session.run("""
                            MATCH (a:Author {name: $name})
                            RETURN a
                            LIMIT 1
                        """, name=normalized)
                        
                        canonical_exists = check_result.single() is not None
                        
                        if canonical_exists:
                            # Merge into existing
                            for original in originals:
                                try:
                                    session.run("""
                                        MATCH (old:Author {name: $old_name})
                                        MATCH (canonical:Author {name: $canonical_name})
                                        OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
                                        WITH old, canonical, collect({work: w, rel: r}) as rels
                                        FOREACH (rel IN rels |
                                            MERGE (canonical)-[:CREATED]->(rel.work)
                                            DELETE rel.rel
                                        )
                                        DELETE old
                                    """, old_name=original, canonical_name=normalized)
                                    merged_count += 1
                                except Exception as e:
                                    logger.error(f"Error merging {original}: {e}")
                        else:
                            # Create new normalized node from first original
                            try:
                                session.run("""
                                    MATCH (old:Author {name: $old_name})
                                    CREATE (new:Author {name: $new_name})
                                    WITH old, new
                                    OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
                                    WITH old, new, collect({work: w, rel: r}) as rels
                                    FOREACH (rel IN rels |
                                        CREATE (new)-[:CREATED]->(rel.work)
                                        DELETE rel.rel
                                    )
                                    DELETE old
                                """, old_name=originals[0], new_name=normalized)
                                merged_count += 1
                                
                                # Merge rest into the new node
                                for original in originals[1:]:
                                    session.run("""
                                        MATCH (old:Author {name: $old_name})
                                        MATCH (canonical:Author {name: $canonical_name})
                                        OPTIONAL MATCH (old)-[r:CREATED]->(w:Work)
                                        WITH old, canonical, collect({work: w, rel: r}) as rels
                                        FOREACH (rel IN rels |
                                            MERGE (canonical)-[:CREATED]->(rel.work)
                                            DELETE rel.rel
                                        )
                                        DELETE old
                                    """, old_name=original, canonical_name=normalized)
                                    merged_count += 1
                            except Exception as e:
                                logger.error(f"Error creating normalized author: {e}")
                
                offset += self.batch_size
                logger.info(f"Progress: {min(offset, total)}/{total} authors processed, {merged_count} merged")
            
            logger.info(f"Author normalization complete. Total merged: {merged_count}")

    def run(self):
        """Run all normalizations"""
        try:
            # First handle specific cases
            self.normalize_one_piece_titles()
            
            # Then batch process authors
            self.normalize_authors_batch()
            
            # Report final statistics
            with self.driver.session() as session:
                result = session.run("MATCH (a:Author) RETURN count(a) as count")
                author_count = result.single()["count"]
                
                result = session.run("MATCH (w:Work {type: 'Manga'}) RETURN count(w) as count")
                manga_count = result.single()["count"]
                
                logger.info(f"\nFinal counts - Authors: {author_count}, Manga: {manga_count}")
                
        except KeyboardInterrupt:
            logger.info("\nNormalization interrupted by user")
            sys.exit(0)


def main():
    normalizer = BatchNormalizer()
    try:
        normalizer.run()
    finally:
        normalizer.close()


if __name__ == "__main__":
    main()