import json
import os
from neo4j import GraphDatabase
from typing import List, Dict, Any
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jDataImporter:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()

    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")

    def create_constraints(self):
        """Create constraints and indexes"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT work_uri IF NOT EXISTS FOR (w:Work) REQUIRE w.uri IS UNIQUE",
                "CREATE CONSTRAINT author_uri IF NOT EXISTS FOR (a:Author) REQUIRE a.uri IS UNIQUE", 
                "CREATE CONSTRAINT magazine_uri IF NOT EXISTS FOR (m:Magazine) REQUIRE m.uri IS UNIQUE",
                "CREATE INDEX work_title IF NOT EXISTS FOR (w:Work) ON (w.title)",
                "CREATE INDEX author_name IF NOT EXISTS FOR (a:Author) ON (a.name)",
                "CREATE INDEX magazine_name IF NOT EXISTS FOR (m:Magazine) ON (m.name)"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint/index: {constraint}")
                except Exception as e:
                    logger.warning(f"Constraint/index may already exist: {e}")

    def import_authors(self, authors: List[Dict]):
        """Import authors into Neo4j"""
        with self.driver.session() as session:
            for author in authors:
                if not author.get('name'):
                    continue
                    
                query = """
                MERGE (a:Author {uri: $uri})
                SET a.name = $name,
                    a.birth_date = $birth_date,
                    a.death_date = $death_date,
                    a.nationality = $nationality,
                    a.updated_at = datetime()
                """
                
                session.run(query, 
                    uri=author.get('uri', ''),
                    name=author.get('name', ''),
                    birth_date=author.get('birth_date', ''),
                    death_date=author.get('death_date', ''),
                    nationality=author.get('nationality', '')
                )
                
        logger.info(f"Imported {len(authors)} authors")

    def import_magazines(self, magazines: List[Dict]):
        """Import magazines into Neo4j"""
        with self.driver.session() as session:
            for magazine in magazines:
                if not magazine.get('name'):
                    continue
                    
                query = """
                MERGE (m:Magazine {uri: $uri})
                SET m.name = $name,
                    m.publisher = $publisher,
                    m.start_date = $start_date,
                    m.end_date = $end_date,
                    m.updated_at = datetime()
                """
                
                session.run(query,
                    uri=magazine.get('uri', ''),
                    name=magazine.get('name', ''),
                    publisher=magazine.get('publisher', ''),
                    start_date=magazine.get('start_date', ''),
                    end_date=magazine.get('end_date', '')
                )
                
        logger.info(f"Imported {len(magazines)} magazines")

    def import_works(self, works: List[Dict]):
        """Import works into Neo4j"""
        with self.driver.session() as session:
            for work in works:
                if not work.get('title'):
                    continue
                    
                # Create work node
                query = """
                MERGE (w:Work {uri: $uri})
                SET w.title = $title,
                    w.publisher = $publisher,
                    w.publication_date = $publication_date,
                    w.genre = $genre,
                    w.updated_at = datetime()
                """
                
                session.run(query,
                    uri=work.get('uri', ''),
                    title=work.get('title', ''),
                    publisher=work.get('publisher', ''),
                    publication_date=work.get('publication_date', ''),
                    genre=work.get('genre', '')
                )
                
                # Create relationship with author if exists
                if work.get('creator_uri') and work.get('creator_name'):
                    author_query = """
                    MERGE (a:Author {uri: $creator_uri})
                    ON CREATE SET a.name = $creator_name
                    WITH a
                    MATCH (w:Work {uri: $work_uri})
                    MERGE (a)-[:CREATED]->(w)
                    """
                    
                    session.run(author_query,
                        creator_uri=work.get('creator_uri', ''),
                        creator_name=work.get('creator_name', ''),
                        work_uri=work.get('uri', '')
                    )
                    
        logger.info(f"Imported {len(works)} works")

    def create_same_magazine_relationships(self):
        """Create relationships between works published in the same magazine"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work), (w2:Work)
            WHERE w1.publisher = w2.publisher 
            AND w1.uri <> w2.uri
            AND w1.publisher <> ''
            AND w2.publisher <> ''
            MERGE (w1)-[:SAME_PUBLISHER]->(w2)
            """
            
            result = session.run(query)
            logger.info("Created same publisher relationships")

    def create_mentor_relationships(self):
        """Create mentor relationships based on common patterns"""
        with self.driver.session() as session:
            # This is a simplified example - in reality you'd need more sophisticated logic
            # or additional data to determine mentor relationships
            query = """
            MATCH (a1:Author)-[:CREATED]->(w1:Work)
            MATCH (a2:Author)-[:CREATED]->(w2:Work)
            WHERE w1.publisher = w2.publisher
            AND a1.uri <> a2.uri
            AND a1.birth_date < a2.birth_date
            WITH a1, a2, COUNT(*) as common_publishers
            WHERE common_publishers >= 2
            MERGE (a1)-[:MENTOR_OF]->(a2)
            """
            
            result = session.run(query)
            logger.info("Created potential mentor relationships")

    def load_json_data(self, filename: str) -> List[Dict]:
        """Load data from JSON file"""
        filepath = f'../data/{filename}'
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return []
            
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        logger.info(f"Loaded {len(data)} records from {filename}")
        return data

    def import_all_data(self, clear_db: bool = True):
        """Import all data into Neo4j"""
        if clear_db:
            self.clear_database()
            
        self.create_constraints()
        
        # Load data from JSON files
        authors = self.load_json_data('manga_authors.json')
        magazines = self.load_json_data('manga_magazines.json')
        works = self.load_json_data('manga_works.json')
        
        # Import data
        self.import_authors(authors)
        self.import_magazines(magazines)
        self.import_works(works)
        
        # Create relationships
        self.create_same_magazine_relationships()
        self.create_mentor_relationships()
        
        # Log statistics
        with self.driver.session() as session:
            stats = session.run("""
                RETURN count{(n:Author)} as authors,
                       count{(n:Work)} as works,
                       count{(n:Magazine)} as magazines,
                       count{()-[r]-()} as relationships
            """).single()
            
            logger.info(f"Import completed - Authors: {stats['authors']}, "
                       f"Works: {stats['works']}, Magazines: {stats['magazines']}, "
                       f"Relationships: {stats['relationships']}")

if __name__ == "__main__":
    importer = Neo4jDataImporter()
    try:
        importer.import_all_data()
    finally:
        importer.close()