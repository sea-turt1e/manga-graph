"""
Neo4j-based media arts service for faster graph data retrieval
"""
import logging
from typing import Any, Dict, List, Optional

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

logger = logging.getLogger(__name__)


class Neo4jMediaArtsService:
    """Neo4j-based media arts data service for improved performance"""

    def __init__(self, neo4j_repository: Optional[Neo4jMangaRepository] = None):
        self.neo4j_repository = neo4j_repository or Neo4jMangaRepository()

    def search_manga_data(self, search_term: str, limit: int = 20) -> Dict[str, List]:
        """
        Search manga data using Neo4j for fast retrieval
        
        Args:
            search_term: Search term
            limit: Result limit
            
        Returns:
            Dictionary containing nodes and edges lists
        """
        try:
            result = self.neo4j_repository.search_manga_data_with_related(
                search_term, limit, include_related=False
            )
            
            # Convert Neo4j format to expected format
            return self._convert_neo4j_to_graph_format(result)
            
        except Exception as e:
            logger.error(f"Error searching manga data: {e}")
            return {"nodes": [], "edges": []}

    def search_manga_data_with_related(self, search_term: str, limit: int = 20, include_related: bool = True) -> Dict[str, List]:
        """
        Search manga data with related works using Neo4j
        
        Args:
            search_term: Search term
            limit: Result limit
            include_related: Whether to include related works
            
        Returns:
            Dictionary containing nodes and edges lists
        """
        try:
            result = self.neo4j_repository.search_manga_data_with_related(
                search_term, limit, include_related
            )
            
            return self._convert_neo4j_to_graph_format(result)
            
        except Exception as e:
            logger.error(f"Error searching manga data with related: {e}")
            return {"nodes": [], "edges": []}

    def get_creator_works(self, creator_name: str, limit: int = 50) -> Dict[str, List]:
        """
        Get creator's works using Neo4j
        
        Args:
            creator_name: Creator's name
            limit: Result limit
            
        Returns:
            Dictionary containing nodes and edges lists
        """
        try:
            # Search for works by creator
            works = self.neo4j_repository.search_manga_works(creator_name, limit)
            
            nodes = []
            edges = []
            processed_ids = set()
            
            # Create author node
            if works:
                author_id = f"author_{abs(hash(creator_name))}"
                author_node = {
                    "id": author_id,
                    "label": creator_name,
                    "type": "author",
                    "properties": {
                        "name": creator_name,
                        "source": "neo4j"
                    }
                }
                nodes.append(author_node)
                processed_ids.add(author_id)
                
                # Add works and relationships
                for work in works:
                    work_id = work['work_id']
                    if work_id not in processed_ids:
                        work_node = {
                            "id": work_id,
                            "label": work['title'],
                            "type": "work",
                            "properties": {
                                "title": work['title'],
                                "published_date": work['published_date'],
                                "genre": work['genre'],
                                "source": "neo4j"
                            }
                        }
                        nodes.append(work_node)
                        processed_ids.add(work_id)
                        
                        # Add relationship
                        edge = {
                            "id": f"{author_id}-created-{work_id}",
                            "source": author_id,
                            "target": work_id,
                            "type": "created",
                            "properties": {"source": "neo4j"}
                        }
                        edges.append(edge)
            
            return {"nodes": nodes, "edges": edges}
            
        except Exception as e:
            logger.error(f"Error getting creator works: {e}")
            return {"nodes": [], "edges": []}

    def get_database_statistics(self) -> Dict[str, int]:
        """Get Neo4j database statistics"""
        try:
            return self.neo4j_repository.get_database_statistics()
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {}

    def _convert_neo4j_to_graph_format(self, neo4j_result: Dict[str, Any]) -> Dict[str, List]:
        """
        Convert Neo4j repository format to expected graph format
        
        Args:
            neo4j_result: Result from Neo4j repository
            
        Returns:
            Converted graph format
        """
        nodes = []
        edges = []
        
        # Convert nodes
        for node in neo4j_result.get('nodes', []):
            converted_node = {
                "id": node['id'],
                "label": node['label'],
                "type": node['type'],
                "properties": node.get('data', {})
            }
            
            # Add source info
            if 'properties' not in converted_node:
                converted_node['properties'] = {}
            converted_node['properties']['source'] = 'neo4j'
            
            nodes.append(converted_node)
        
        # Convert edges
        for edge in neo4j_result.get('edges', []):
            converted_edge = {
                "id": f"{edge['from']}-{edge['type']}-{edge['to']}",
                "source": edge['from'],
                "target": edge['to'],
                "type": edge['type'],
                "properties": {"source": "neo4j"}
            }
            edges.append(converted_edge)
        
        return {"nodes": nodes, "edges": edges}

    def close(self):
        """Close the Neo4j connection"""
        if self.neo4j_repository:
            self.neo4j_repository.close()