"""
Neo4j-based media arts service for faster graph data retrieval
"""

import logging
from typing import Any, Dict, List, Optional

from infrastructure.external.neo4j_repository import Neo4jMangaRepository
from domain.services.mock_neo4j_service import MockNeo4jService

logger = logging.getLogger(__name__)


class Neo4jMediaArtsService:
    """Neo4j-based media arts data service for improved performance"""

    def __init__(self, neo4j_repository: Optional[Neo4jMangaRepository] = None):
        import os
        
        # Force mock mode if USE_MOCK_NEO4J is set or Neo4j connection fails
        use_mock_env = os.getenv("USE_MOCK_NEO4J", "false").lower() == "true"
        
        if use_mock_env:
            logger.info("USE_MOCK_NEO4J is set, using mock service")
            self.neo4j_repository = MockNeo4jService()
            self.use_mock = True
        else:
            try:
                self.neo4j_repository = neo4j_repository or Neo4jMangaRepository()
                self.use_mock = False
                # Test connection by getting stats
                stats = self.neo4j_repository.get_database_statistics()
                if not stats or stats.get('work_count', 0) == 0:
                    logger.warning("Neo4j database appears to be empty, switching to mock service")
                    self.neo4j_repository = MockNeo4jService()
                    self.use_mock = True
            except Exception as e:
                logger.error(f"Failed to initialize Neo4j repository: {e}")
                logger.info("Using mock service for Neo4j data")
                self.neo4j_repository = MockNeo4jService()
                self.use_mock = True

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
            result = self.neo4j_repository.search_manga_data_with_related(search_term, limit, include_related=False)

            # Convert Neo4j format to expected format
            return self._convert_neo4j_to_graph_format(result)

        except Exception as e:
            logger.error(f"Error searching manga data: {e}")
            return {"nodes": [], "edges": []}

    def search_manga_data_with_related(
        self, search_term: str, limit: int = 20, include_related: bool = True
    ) -> Dict[str, List]:
        """
        Search manga data with related works using Neo4j

        Args:
            search_term: Search term
            limit: Result limit
            include_related: Whether to include related works

        Returns:
            Dictionary containing nodes and edges lists
        """
        if not self.neo4j_repository:
            logger.warning("Neo4j repository is not available")
            return {"nodes": [], "edges": []}
            
        try:
            result = self.neo4j_repository.search_manga_data_with_related(search_term, limit, include_related)

            if self.use_mock:
                # Mock service returns data in the correct format already
                return result
            else:
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
                    "properties": {"name": creator_name, "source": "neo4j"},
                }
                nodes.append(author_node)
                processed_ids.add(author_id)

                # Add works and relationships
                for work in works:
                    work_id = work["work_id"]
                    if work_id not in processed_ids:
                        work_node = {
                            "id": work_id,
                            "label": work["title"],
                            "type": "work",
                            "properties": {
                                "title": work["title"],
                                "published_date": work["published_date"],
                                "genre": work["genre"],
                                "source": "neo4j",
                            },
                        }
                        nodes.append(work_node)
                        processed_ids.add(work_id)

                        # Add relationship
                        edge = {
                            "id": f"{author_id}-created-{work_id}",
                            "source": author_id,
                            "target": work_id,
                            "type": "created",
                            "properties": {"source": "neo4j"},
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
        for node in neo4j_result.get("nodes", []):
            # Extract properties from data field if it exists
            node_data = node.get("data", {})

            # Build properties object
            properties = {"source": "neo4j"}

            # Add work-specific properties
            if node["type"] == "work":
                properties.update(
                    {
                        "title": node_data.get("title", node["label"]),
                        "published_date": node_data.get("published_date", ""),
                        "genre": node_data.get("genre", ""),
                        "isbn": node_data.get("isbn", ""),
                        "volume": node_data.get("volume", ""),
                        "is_series": node_data.get("is_series", False),
                        "work_count": node_data.get("work_count", 1),
                    }
                )

                # For series, include volume information
                if node_data.get("is_series"):
                    properties["series_volumes"] = node_data.get("volume", "")
                    properties["date_range"] = node_data.get("published_date", "")

                # Publishers and creators
                if node_data.get("publishers"):
                    properties["publishers"] = node_data["publishers"]
                if node_data.get("creators"):
                    properties["creators"] = node_data["creators"]

            elif node["type"] == "author":
                properties["name"] = node["label"]
            elif node["type"] == "publisher":
                properties["name"] = node["label"]

            converted_node = {"id": node["id"], "label": node["label"], "type": node["type"], "properties": properties}

            nodes.append(converted_node)

        # Convert edges
        for edge in neo4j_result.get("edges", []):
            converted_edge = {
                "id": f"{edge['from']}-{edge['type']}-{edge['to']}",
                "source": edge["from"],
                "target": edge["to"],
                "type": edge["type"],
                "properties": {"source": "neo4j"},
            }
            edges.append(converted_edge)

        return {"nodes": nodes, "edges": edges}

    def close(self):
        """Close the Neo4j connection"""
        if self.neo4j_repository:
            self.neo4j_repository.close()
