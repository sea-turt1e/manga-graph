from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
import logging
from domain.entities import Work, Author, Magazine
from domain.repositories import MangaRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Neo4jMangaRepository(MangaRepository):
    def __init__(self, uri: str, user: str, password: str):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        """Connect to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def test_connection(self):
        """Test database connection"""
        def test_tx(tx):
            result = tx.run("RETURN 1 as test")
            return result.single()["test"]
        
        with self.driver.session() as session:
            return session.execute_read(test_tx) == 1

    def _serialize_value(self, value):
        """Convert Neo4j types to JSON serializable types"""
        if hasattr(value, 'iso_format'):  # DateTime
            return value.iso_format()
        elif hasattr(value, '__dict__'):
            return str(value)
        else:
            return value

    def _serialize_properties(self, properties: dict) -> dict:
        """Serialize all properties to JSON-safe types"""
        return {k: self._serialize_value(v) for k, v in properties.items()}

    def search_graph(self, search_term: str, depth: int = 2) -> Dict[str, List]:
        """Search for nodes and their relationships"""
        def search_tx(tx):
            # First get matching nodes
            node_query = """
            MATCH (n)
            WHERE toLower(n.title) CONTAINS toLower($term) 
               OR toLower(n.name) CONTAINS toLower($term)
            RETURN n
            LIMIT 10
            """
            
            node_result = tx.run(node_query, term=search_term)
            found_nodes = []
            nodes = {}
            
            for record in node_result:
                node = record["n"]
                node_id = str(node.element_id)
                found_nodes.append(node_id)
                
                nodes[node_id] = {
                    "id": node_id,
                    "label": node.get("title", node.get("name", "Unknown")),
                    "type": list(node.labels)[0].lower() if node.labels else "unknown",
                    "properties": self._serialize_properties(dict(node))
                }
            
            # Get relationships for found nodes
            edges = []
            if found_nodes:
                rel_query = """
                MATCH (n)-[r]-(connected)
                WHERE elementId(n) IN $nodeIds
                RETURN n, r, connected
                LIMIT 50
                """
                
                rel_result = tx.run(rel_query, nodeIds=found_nodes)
                
                for record in rel_result:
                    source_node = record["n"]
                    relationship = record["r"]
                    target_node = record["connected"]
                    
                    source_id = str(source_node.element_id)
                    target_id = str(target_node.element_id)
                    
                    # Add target node if not already present
                    if target_id not in nodes:
                        nodes[target_id] = {
                            "id": target_id,
                            "label": target_node.get("title", target_node.get("name", "Unknown")),
                            "type": list(target_node.labels)[0].lower() if target_node.labels else "unknown",
                            "properties": self._serialize_properties(dict(target_node))
                        }
                    
                    # Add edge
                    edge_id = f"{source_id}-{relationship.type}-{target_id}"
                    edge = {
                        "id": edge_id,
                        "source": source_id,
                        "target": target_id,
                        "type": relationship.type.lower(),
                        "properties": self._serialize_properties(dict(relationship))
                    }
                    edges.append(edge)
            
            return {
                "nodes": list(nodes.values()),
                "edges": edges
            }
        
        with self.driver.session() as session:
            return session.execute_read(search_tx)

    def get_all_authors(self) -> List[Author]:
        """Get all authors"""
        def authors_tx(tx):
            result = tx.run("MATCH (a:Author) RETURN a LIMIT 100")
            authors = []
            for record in result:
                node = record["a"]
                author = Author(
                    id=str(node.element_id),
                    name=node.get("name", "Unknown"),
                    birth_date=node.get("birth_date"),
                    biography=node.get("biography")
                )
                authors.append(author)
            return authors
        
        with self.driver.session() as session:
            return session.execute_read(authors_tx)

    def get_all_works(self) -> List[Work]:
        """Get all works"""
        def works_tx(tx):
            result = tx.run("MATCH (w:Work) RETURN w LIMIT 100")
            works = []
            for record in result:
                node = record["w"]
                work = Work(
                    id=str(node.element_id),
                    title=node.get("title", "Unknown"),
                    authors=[],  # TODO: Fetch related authors
                    magazines=[],  # TODO: Fetch related magazines
                    publication_date=node.get("publication_date"),
                    genre=node.get("genre"),
                    description=node.get("description")
                )
                works.append(work)
            return works
        
        with self.driver.session() as session:
            return session.execute_read(works_tx)

    def get_all_magazines(self) -> List[Magazine]:
        """Get all magazines"""
        def magazines_tx(tx):
            result = tx.run("MATCH (m:Magazine) RETURN m LIMIT 100")
            magazines = []
            for record in result:
                node = record["m"]
                magazine = Magazine(
                    id=str(node.element_id),
                    name=node.get("name", "Unknown"),
                    publisher=node.get("publisher", "Unknown"),
                    established_date=node.get("established_date")
                )
                magazines.append(magazine)
            return magazines
        
        with self.driver.session() as session:
            return session.execute_read(magazines_tx)

    def get_author_by_id(self, author_id: str) -> Optional[Author]:
        """Get author by ID"""
        # TODO: Implement
        pass

    def get_work_by_id(self, work_id: str) -> Optional[Work]:
        """Get work by ID"""
        # TODO: Implement
        pass

    def get_magazine_by_id(self, magazine_id: str) -> Optional[Magazine]:
        """Get magazine by ID"""
        # TODO: Implement
        pass