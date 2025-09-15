"""
Neo4j-based media arts service for faster graph data retrieval
"""

import logging
from typing import Any, Dict, List, Optional

from domain.services.mock_neo4j_service import MockNeo4jService
from infrastructure.external.neo4j_repository import Neo4jMangaRepository

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
                logger.info("Neo4j repository initialized (real mode)")
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
        self,
        search_term: str,
        limit: int = 20,
        include_related: bool = True,
        include_same_publisher_other_magazines: Optional[bool] = False,
        same_publisher_other_magazines_limit: Optional[int] = 5,
        sort_total_volumes: Optional[str] = None,
        min_total_volumes: Optional[int] = None,
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
            result = self.neo4j_repository.search_manga_data_with_related(
                search_term,
                limit,
                include_related,
                include_same_publisher_other_magazines=include_same_publisher_other_magazines,
                same_publisher_other_magazines_limit=same_publisher_other_magazines_limit,
                sort_total_volumes=sort_total_volumes,
                min_total_volumes=min_total_volumes,
            )

            if self.use_mock:
                return result
            else:
                return self._convert_neo4j_to_graph_format(result)

        except Exception as e:
            logger.error(f"Error searching manga data with related: {e}")
            # On error, provide mock fallback so API still returns something meaningful
            mock = MockNeo4jService()
            return mock.search_manga_data_with_related(
                search_term,
                limit=limit,
                include_related=include_related,
                include_same_publisher_other_magazines=include_same_publisher_other_magazines,
                same_publisher_other_magazines_limit=same_publisher_other_magazines_limit,
                sort_total_volumes=sort_total_volumes,
                min_total_volumes=min_total_volumes,
            )

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

    def get_work_by_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """Get work details by ID"""
        try:
            return self.neo4j_repository.get_work_by_id(work_id)
        except Exception as e:
            logger.error(f"Error getting work by ID {work_id}: {e}")
            return None

    def update_work_cover_image(self, work_id: str, cover_url: str) -> bool:
        """Update work cover image URL"""
        try:
            return self.neo4j_repository.update_work_cover_image(work_id, cover_url)
        except Exception as e:
            logger.error(f"Error updating cover image for work {work_id}: {e}")
            return False

    def get_works_needing_covers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get works that have ISBN but no cover image"""
        try:
            return self.neo4j_repository.get_works_needing_covers(limit)
        except Exception as e:
            logger.error(f"Error getting works needing covers: {e}")
            return []

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
            # Extract properties from data or properties field
            node_data = node.get("properties", node.get("data", {}))

            # Build properties object
            properties = {"source": "neo4j"}

            # Add work-specific properties
            if node["type"] == "work":
                is_series = node_data.get("is_series", False)
                volume = node_data.get("volume", "")

                # Log for debugging
                logger.debug(
                    f"Converting work node: {node['label']}, is_series: {is_series}, original volume: {volume}"
                )

                properties.update(
                    {
                        "title": node_data.get("title", node["label"]),
                        "published_date": node_data.get("published_date", ""),
                        "genre": node_data.get("genre", ""),
                        "isbn": node_data.get("isbn", ""),
                        "volume": "1" if is_series else volume,  # Series always show volume 1
                        "is_series": is_series,
                        "work_count": node_data.get("work_count", 1),
                        "total_volumes": node_data.get("total_volumes", node_data.get("work_count", 1)),
                    }
                )

                # For series, include additional series information
                if is_series:
                    properties["series_volumes"] = node_data.get("series_volumes", node_data.get("volume", ""))
                    properties["date_range"] = node_data.get("published_date", "")

                # Publishers and creators
                if node_data.get("publishers"):
                    # Normalize publisher names to handle parenthetical annotations
                    from domain.services.name_normalizer import normalize_publisher_name

                    normalized_publishers = []
                    for publisher in node_data["publishers"]:
                        normalized = normalize_publisher_name(publisher)
                        if normalized and normalized not in normalized_publishers:
                            normalized_publishers.append(normalized)
                    properties["publishers"] = normalized_publishers
                if node_data.get("creators"):
                    properties["creators"] = node_data["creators"]

            elif node["type"] == "author":
                properties["name"] = node["label"]
            elif node["type"] == "publisher":
                properties["name"] = node["label"]
            elif node["type"] == "magazine":
                properties["name"] = node["label"]
            elif node["type"] == "publication":
                properties.update(
                    {
                        "title": node_data.get("title", node["label"]),
                        "publication_date": node_data.get("publication_date", ""),
                        "genre": node_data.get("genre", ""),
                        "creators": node_data.get("creators", []),
                        "magazines": node_data.get("magazines", []),
                    }
                )

            converted_node = {"id": node["id"], "label": node["label"], "type": node["type"], "properties": properties}

            nodes.append(converted_node)

        # Convert edges
        for edge in neo4j_result.get("edges", []):
            # Handle both old and new edge formats
            source = edge.get("source", edge.get("from"))
            target = edge.get("target", edge.get("to"))
            edge_id = edge.get("id", f"{source}-{edge['type']}-{target}")

            converted_edge = {
                "id": edge_id,
                "source": source,
                "target": target,
                "type": edge["type"],
                "properties": edge.get("properties", {"source": "neo4j"}),
            }
            edges.append(converted_edge)

        return {"nodes": nodes, "edges": edges}

    def close(self):
        """Close the Neo4j connection"""
        if self.neo4j_repository:
            self.neo4j_repository.close()
