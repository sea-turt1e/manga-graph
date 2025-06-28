#!/usr/bin/env python3
"""
Normalize author and manga names in Neo4j database
"""

import os
import re
from typing import Dict, Set, Optional, Tuple
from neo4j import GraphDatabase
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class NameNormalizer:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

    def close(self):
        self.driver.close()

    def normalize_author_name(self, name: str) -> str:
        """
        Normalize author name by removing role prefixes and standardizing format
        """
        # Remove role prefixes like [著], [原作], [作画] etc.
        name = re.sub(r'^\[.*?\]\s*', '', name)
        
        # Remove trailing whitespace
        name = name.strip()
        
        return name

    def normalize_manga_title(self, title: str) -> str:
        """
        Normalize manga title by standardizing format
        """
        # Save original for debugging
        original = title
        
        # Standardize full-width/half-width characters
        # Convert full-width alphanumeric to half-width
        title = title.translate(str.maketrans(
            'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９　',
            'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 '
        ))
        
        # Remove volume numbers at the end
        title = re.sub(r'\s*[\(（]\d+[\)）]$', '', title)
        title = re.sub(r'\s*第?\d+巻?$', '', title)
        title = re.sub(r'\s*vol\.?\s*\d+$', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\s*\d+$', '', title)
        
        # Standardize spaces
        title = re.sub(r'\s+', ' ', title)
        
        # Remove trailing whitespace
        title = title.strip()
        
        # Normalize case for specific known titles
        # ONE PIECE -> ONE PIECE (standardize)
        if title.upper() == 'ONE PIECE':
            title = 'ONE PIECE'
        
        return title

    def get_author_duplicates(self, session) -> Dict[str, Set[str]]:
        """
        Get all author name variations that should be merged
        """
        result = session.run("""
            MATCH (a:Author)
            RETURN a.name as name
        """)
        
        # Group by normalized name
        name_groups = {}
        for record in result:
            original_name = record["name"]
            normalized_name = self.normalize_author_name(original_name)
            
            if normalized_name not in name_groups:
                name_groups[normalized_name] = set()
            name_groups[normalized_name].add(original_name)
        
        # Return only groups with duplicates
        return {k: v for k, v in name_groups.items() if len(v) > 1}

    def get_manga_duplicates(self, session) -> Dict[str, Set[str]]:
        """
        Get all manga title variations that should be merged
        """
        result = session.run("""
            MATCH (w:Work)
            WHERE w.type = 'Manga'
            RETURN w.title as title
        """)
        
        # Group by normalized title
        title_groups = {}
        for record in result:
            original_title = record["title"]
            normalized_title = self.normalize_manga_title(original_title)
            
            if normalized_title not in title_groups:
                title_groups[normalized_title] = set()
            title_groups[normalized_title].add(original_title)
        
        # Return only groups with duplicates
        return {k: v for k, v in title_groups.items() if len(v) > 1}

    def merge_author_nodes(self, session, normalized_name: str, original_names: Set[str]):
        """
        Merge multiple author nodes into one
        """
        # Choose the canonical name (prefer kanji, then original without prefix)
        canonical_name = normalized_name
        for name in original_names:
            if name == normalized_name:
                canonical_name = name
                break
        
        # Create or get the canonical author node
        session.run("""
            MERGE (canonical:Author {name: $canonical_name})
        """, canonical_name=canonical_name)
        
        # For each duplicate, move all relationships to canonical node
        for original_name in original_names:
            if original_name != canonical_name:
                # Move all CREATED relationships
                session.run("""
                    MATCH (old:Author {name: $original_name})-[r:CREATED]->(w:Work)
                    MATCH (canonical:Author {name: $canonical_name})
                    MERGE (canonical)-[:CREATED]->(w)
                    DELETE r
                """, original_name=original_name, canonical_name=canonical_name)
                
                # Delete the old node
                session.run("""
                    MATCH (old:Author {name: $original_name})
                    DELETE old
                """, original_name=original_name)

    def merge_manga_nodes(self, session, normalized_title: str, original_titles: Set[str]):
        """
        Merge multiple manga nodes into one (for series)
        """
        # Choose the canonical title (shortest version without volume number)
        canonical_title = min(original_titles, key=len)
        
        logger.info(f"Merging manga: {original_titles} -> {canonical_title}")
        
        # For each duplicate, update relationships
        for original_title in original_titles:
            if original_title != canonical_title:
                # Get all properties from old node
                result = session.run("""
                    MATCH (old:Work {title: $original_title, type: 'Manga'})
                    RETURN old
                """, original_title=original_title)
                
                old_nodes = list(result)
                if not old_nodes:
                    continue
                    
                old_node = old_nodes[0]['old']
                
                # Create or merge canonical node with series info
                session.run("""
                    MATCH (old:Work {title: $original_title, type: 'Manga'})
                    MERGE (canonical:Work {title: $canonical_title, type: 'Manga'})
                    ON CREATE SET 
                        canonical.startDate = old.startDate,
                        canonical.endDate = old.endDate,
                        canonical.isSeries = true
                    ON MATCH SET
                        canonical.isSeries = true,
                        canonical.startDate = CASE 
                            WHEN canonical.startDate IS NULL OR old.startDate < canonical.startDate 
                            THEN old.startDate 
                            ELSE canonical.startDate 
                        END,
                        canonical.endDate = CASE 
                            WHEN canonical.endDate IS NULL OR old.endDate > canonical.endDate 
                            THEN old.endDate 
                            ELSE canonical.endDate 
                        END
                """, original_title=original_title, canonical_title=canonical_title)
                
                # Move all relationships to canonical node
                # Move CREATED relationships
                session.run("""
                    MATCH (a:Author)-[r:CREATED]->(old:Work {title: $original_title, type: 'Manga'})
                    MATCH (canonical:Work {title: $canonical_title, type: 'Manga'})
                    MERGE (a)-[:CREATED]->(canonical)
                    DELETE r
                """, original_title=original_title, canonical_title=canonical_title)
                
                # Move PUBLISHED relationships
                session.run("""
                    MATCH (p:Publisher)-[r:PUBLISHED]->(old:Work {title: $original_title, type: 'Manga'})
                    MATCH (canonical:Work {title: $canonical_title, type: 'Manga'})
                    MERGE (p)-[:PUBLISHED]->(canonical)
                    DELETE r
                """, original_title=original_title, canonical_title=canonical_title)
                
                # Move CONTAINS relationships
                session.run("""
                    MATCH (m:Magazine)-[r:CONTAINS]->(old:Work {title: $original_title, type: 'Manga'})
                    MATCH (canonical:Work {title: $canonical_title, type: 'Manga'})
                    MERGE (m)-[:CONTAINS]->(canonical)
                    DELETE r
                """, original_title=original_title, canonical_title=canonical_title)
                
                # Delete the old node
                session.run("""
                    MATCH (old:Work {title: $original_title, type: 'Manga'})
                    DELETE old
                """, original_title=original_title)

    def normalize_database(self):
        """
        Main function to normalize the database
        """
        with self.driver.session() as session:
            # Get duplicates
            logger.info("Finding author duplicates...")
            author_duplicates = self.get_author_duplicates(session)
            logger.info(f"Found {len(author_duplicates)} groups of duplicate authors")
            
            logger.info("Finding manga duplicates...")
            manga_duplicates = self.get_manga_duplicates(session)
            logger.info(f"Found {len(manga_duplicates)} groups of duplicate manga")
            
            # Merge authors
            logger.info("Merging duplicate authors...")
            count = 0
            total = len(author_duplicates)
            for normalized_name, original_names in author_duplicates.items():
                try:
                    self.merge_author_nodes(session, normalized_name, original_names)
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"Progress: {count}/{total} author groups merged")
                except Exception as e:
                    logger.error(f"Error merging authors {original_names}: {e}")
            
            # Merge manga
            logger.info("Merging duplicate manga...")
            count = 0
            total = len(manga_duplicates)
            for normalized_title, original_titles in manga_duplicates.items():
                try:
                    self.merge_manga_nodes(session, normalized_title, original_titles)
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"Progress: {count}/{total} manga groups merged")
                except Exception as e:
                    logger.error(f"Error merging manga {original_titles}: {e}")
            
            logger.info("Normalization complete!")
            
            # Report statistics
            result = session.run("MATCH (a:Author) RETURN count(a) as count")
            author_count = result.single()["count"]
            
            result = session.run("MATCH (w:Work {type: 'Manga'}) RETURN count(w) as count")
            manga_count = result.single()["count"]
            
            logger.info(f"Final counts - Authors: {author_count}, Manga: {manga_count}")


def main():
    normalizer = NameNormalizer()
    try:
        normalizer.normalize_database()
    finally:
        normalizer.close()


if __name__ == "__main__":
    main()