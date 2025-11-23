"""Service layer for querying the manga_anime_list Neo4j database."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from neo4j import Driver, GraphDatabase

logger = logging.getLogger(__name__)


class MangaAnimeNeo4jService:
    """Lightweight Neo4j accessor tailored for the MyAnimeList-derived dataset."""

    def __init__(
        self,
        driver: Optional[Driver] = None,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        if driver is not None:
            self.driver = driver
            return

        uri = uri or os.getenv("MANGA_ANIME_NEO4J_URI") or os.getenv("NEO4J_URI")
        user = user or os.getenv("MANGA_ANIME_NEO4J_USER") or os.getenv("NEO4J_USER", "neo4j")
        password = password or os.getenv("MANGA_ANIME_NEO4J_PASSWORD") or os.getenv("NEO4J_PASSWORD")

        if not uri or not password:
            raise ValueError(
                "MangaAnimeNeo4jService requires Neo4j connection settings. "
                "Set MANGA_ANIME_NEO4J_* or reuse the default NEO4J_* variables."
            )

        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def fetch_graph(self, query: Optional[str], limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch a graph slice of works plus their adjacent nodes."""
        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_graph_tx, query, limit)

        return self._convert_to_graph(record)

    def fetch_work_subgraph(self, work_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch a focused subgraph centered on a specific work."""
        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_work_subgraph_tx, work_id)

        return self._convert_to_graph(record)

    def close(self) -> None:
        if self.driver:
            self.driver.close()

    # ------------------------------------------------------------------
    # Cypher helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _fetch_graph_tx(tx, query: Optional[str], limit: int):
        cypher = """
        MATCH (w:Work)
        WHERE $query IS NULL
           OR toLower(coalesce(w.title_name, w.title, '')) CONTAINS toLower($query)
           OR toLower(coalesce(w.english_name, '')) CONTAINS toLower($query)
        WITH w
        ORDER BY coalesce(toInteger(w.members), 0) DESC, toInteger(w.id) ASC
        LIMIT $limit
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships
        """
        return tx.run(cypher, query=query, limit=limit).single()

    @staticmethod
    def _fetch_work_subgraph_tx(tx, work_id: str):
        cypher = """
        MATCH (w:Work {id: $work_id})
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships
        """
        return tx.run(cypher, work_id=work_id).single()

    # ------------------------------------------------------------------
    # Result conversion helpers
    # ------------------------------------------------------------------
    def _convert_to_graph(self, record) -> Dict[str, List[Dict[str, Any]]]:
        if record is None:
            return {"nodes": [], "edges": []}

        raw_nodes = (record.get("work_nodes") or []) + (record.get("neighbor_nodes") or [])
        raw_edges = record.get("relationships") or []

        nodes: List[Dict[str, Any]] = []
        seen_node_ids = set()

        for entry in raw_nodes:
            if not entry:
                continue
            node_id = entry["id"]
            if node_id in seen_node_ids:
                continue
            seen_node_ids.add(node_id)
            nodes.append(self._format_node(entry))

        edges: List[Dict[str, Any]] = []
        seen_edge_ids = set()
        for entry in raw_edges:
            if not entry:
                continue
            edge_id = entry.get("id")
            if edge_id in seen_edge_ids:
                continue
            seen_edge_ids.add(edge_id)
            edges.append(
                {
                    "id": edge_id,
                    "source": entry.get("source"),
                    "target": entry.get("target"),
                    "type": entry.get("type"),
                    "properties": {"source": "neo4j-manga-anime", **(entry.get("properties") or {})},
                }
            )

        return {"nodes": nodes, "edges": edges}

    def _format_node(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        labels = entry.get("labels", [])
        props = entry.get("properties", {})
        node_type = self._infer_type(labels)
        label = self._derive_label(node_type, props)

        properties = {**props, "source": "neo4j-manga-anime"}

        return {"id": entry.get("id"), "label": label, "type": node_type, "properties": properties}

    @staticmethod
    def _infer_type(labels: List[str]) -> str:
        priority = [
            ("work", {"Work"}),
            ("author", {"Author"}),
            ("magazine", {"Magazine"}),
            ("publisher", {"Publisher"}),
        ]
        label_set = set(labels or [])
        for inferred, candidates in priority:
            if label_set & candidates:
                return inferred
        return (labels[0] if labels else "unknown").lower()

    @staticmethod
    def _derive_label(node_type: str, props: Dict[str, Any]) -> str:
        if node_type == "work":
            return props.get("title_name") or props.get("english_name") or props.get("title") or props.get("id") or "Work"
        if node_type == "author":
            return props.get("name") or props.get("english_name") or "Author"
        if node_type == "magazine":
            return props.get("name") or props.get("title") or "Magazine"
        if node_type == "publisher":
            return props.get("name") or "Publisher"
        return props.get("name") or props.get("title") or "Node"