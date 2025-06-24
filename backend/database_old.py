from neo4j import GraphDatabase
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jConnection:
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
        with self.driver.session() as session:
            result = session.run("RETURN 1 as test")
            return result.single()["test"] == 1

    def search_graph(self, user_search_term: str, graph_depth: int = 2) -> Dict[str, List]:
        """Search for nodes and their relationships"""
        def run_search(tx):
            # Search for works that match the search term and get related nodes
            cypher_statement = """
            MATCH (n)
            WHERE toLower(n.title) CONTAINS toLower($searchTerm) 
               OR toLower(n.name) CONTAINS toLower($searchTerm)
            OPTIONAL MATCH (n)-[r1]-(connected1)
            OPTIONAL MATCH (connected1)-[r2]-(connected2)
            RETURN n, r1, connected1, r2, connected2
            LIMIT 50
            """
            
            result = tx.run(cypher_statement, searchTerm=user_search_term)
            
            nodes = {}
            edges = []
            
            for record in result:
                # Add main node
                if record["n"]:
                    main_node = record["n"]
                    node_id = str(main_node.element_id)
                    if node_id not in nodes:
                        nodes[node_id] = {
                            "id": node_id,
                            "label": main_node.get("title", main_node.get("name", "Unknown")),
                            "type": list(main_node.labels)[0].lower() if main_node.labels else "unknown",
                            "properties": dict(main_node)
                        }
                
                # Add connected node 1
                if record["connected1"]:
                    connected_node = record["connected1"]
                    connected_id = str(connected_node.element_id)
                    if connected_id not in nodes:
                        nodes[connected_id] = {
                            "id": connected_id,
                            "label": connected_node.get("title", connected_node.get("name", "Unknown")),
                            "type": list(connected_node.labels)[0].lower() if connected_node.labels else "unknown",
                            "properties": dict(connected_node)
                        }
                
                # Add connected node 2
                if record["connected2"]:
                    connected_node2 = record["connected2"]
                    connected_id2 = str(connected_node2.element_id)
                    if connected_id2 not in nodes:
                        nodes[connected_id2] = {
                            "id": connected_id2,
                            "label": connected_node2.get("title", connected_node2.get("name", "Unknown")),
                            "type": list(connected_node2.labels)[0].lower() if connected_node2.labels else "unknown",
                            "properties": dict(connected_node2)
                        }
                
                # Add relationship 1
                if record["r1"] and record["n"] and record["connected1"]:
                    rel = record["r1"]
                    edge_id = f"{rel.start_node.element_id}-{rel.type}-{rel.end_node.element_id}"
                    edge = {
                        "id": edge_id,
                        "source": str(rel.start_node.element_id),
                        "target": str(rel.end_node.element_id),
                        "type": rel.type.lower(),
                        "properties": dict(rel)
                    }
                    if edge not in edges:
                        edges.append(edge)
                
                # Add relationship 2
                if record["r2"] and record["connected1"] and record["connected2"]:
                    rel2 = record["r2"]
                    edge_id2 = f"{rel2.start_node.element_id}-{rel2.type}-{rel2.end_node.element_id}"
                    edge2 = {
                        "id": edge_id2,
                        "source": str(rel2.start_node.element_id),
                        "target": str(rel2.end_node.element_id),
                        "type": rel2.type.lower(),
                        "properties": dict(rel2)
                    }
                    if edge2 not in edges:
                        edges.append(edge2)
            
            return {
                "nodes": list(nodes.values()),
                "edges": edges
            }
        
        with self.driver.session() as session:
            return session.execute_read(run_search)

    def get_all_authors(self) -> List[Dict]:
        """Get all authors"""
        with self.driver.session() as session:
            result = session.run("MATCH (a:Author) RETURN a LIMIT 100")
            return [{"id": str(record["a"].id), "name": record["a"]["name"], "properties": dict(record["a"])} 
                   for record in result]

    def get_all_works(self) -> List[Dict]:
        """Get all works"""
        with self.driver.session() as session:
            result = session.run("MATCH (w:Work) RETURN w LIMIT 100")
            return [{"id": str(record["w"].id), "title": record["w"]["title"], "properties": dict(record["w"])} 
                   for record in result]

    def get_all_magazines(self) -> List[Dict]:
        """Get all magazines"""
        with self.driver.session() as session:
            result = session.run("MATCH (m:Magazine) RETURN m LIMIT 100")
            return [{"id": str(record["m"].id), "name": record["m"]["name"], "properties": dict(record["m"])} 
                   for record in result]