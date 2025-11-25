"""Service layer for querying the manga_anime_list Neo4j database."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Literal, Optional

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import Neo4jError

logger = logging.getLogger(__name__)


SearchMode = Literal["simple", "fulltext", "ranked"]
SearchLanguage = Literal["english", "japanese"]


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
        self.fulltext_index = os.getenv("MANGA_ANIME_FULLTEXT_INDEX", "work_titles_fulltext")
        self.fulltext_candidate_limit = int(os.getenv("MANGA_ANIME_FULLTEXT_CANDIDATE_LIMIT", "200"))
        self.rank_threshold = float(os.getenv("MANGA_ANIME_RANK_THRESHOLD", "0.45"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def fetch_graph(
        self,
        query: Optional[str],
        limit: int = 50,
        *,
        language: SearchLanguage = "english",
        mode: SearchMode = "simple",
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch a graph slice of works plus their adjacent nodes using the specified search mode."""

        normalized_mode = mode if query else "simple"
        with self.driver.session() as session:
            try:
                if normalized_mode == "fulltext":
                    record = session.read_transaction(
                        self._fetch_graph_tx_fulltext,
                        query,
                        limit,
                        language,
                        self.fulltext_index,
                    )
                elif normalized_mode == "ranked":
                    record = session.read_transaction(
                        self._fetch_graph_tx_ranked,
                        query,
                        limit,
                        language,
                        self.fulltext_index,
                        self.fulltext_candidate_limit,
                        self.rank_threshold,
                    )
                else:
                    record = session.read_transaction(self._fetch_graph_tx_simple, query, limit, language)
            except Neo4jError as exc:
                logger.warning("Falling back to simple search due to Neo4j error: %s", exc)
                record = session.read_transaction(self._fetch_graph_tx_simple, query, limit, language)

        return self._convert_to_graph(record)

    def fetch_graph_by_japanese(
        self, query: Optional[str], limit: int = 50, mode: SearchMode = "simple"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Backward-compatible helper focused on japanese_name queries."""
        return self.fetch_graph(query=query, limit=limit, language="japanese", mode=mode)

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
    def _fetch_graph_tx_simple(tx, query: Optional[str], limit: int, language: SearchLanguage):
        clauses = MangaAnimeNeo4jService._build_simple_where(language)
        cypher = f"""
        MATCH (w:Work)
        WHERE $searchTerm IS NULL OR {clauses}
        WITH w
        ORDER BY coalesce(toInteger(w.members), 0) DESC, toInteger(w.id) ASC
        LIMIT $limitCount
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {{id: elementId(w), labels: labels(w), properties: properties(w)}}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {{id: elementId(n), labels: labels(n), properties: properties(n)}} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {{id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)}} END) AS relationships
        """
        params = {"searchTerm": query, "limitCount": limit}
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_graph_tx_fulltext(
        tx,
        query: Optional[str],
        limit: int,
        language: SearchLanguage,
        index_name: str,
    ):
        lucene_query = MangaAnimeNeo4jService._build_lucene_query(query)
        cypher = """
        CALL db.index.fulltext.queryNodes($indexName, $luceneQuery)
        YIELD node AS w, score
        WHERE $language <> 'japanese' OR coalesce(w.japanese_name, '') <> ''
        WITH w, score
        ORDER BY score DESC
        LIMIT $limitCount
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships
        """
        params = {
            "indexName": index_name,
            "luceneQuery": lucene_query,
            "limitCount": limit,
            "language": language,
        }
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_graph_tx_ranked(
        tx,
        query: Optional[str],
        limit: int,
        language: SearchLanguage,
        index_name: str,
        candidate_limit: int,
        rank_threshold: float,
    ):
        lucene_query = MangaAnimeNeo4jService._build_lucene_query(query)
        cypher = """
        CALL db.index.fulltext.queryNodes($indexName, $luceneQuery)
        YIELD node AS w, score
        WHERE $language <> 'japanese' OR coalesce(w.japanese_name, '') <> ''
        WITH w, score
        ORDER BY score DESC
        LIMIT $candidateLimit
        WITH w, score,
             apoc.text.levenshteinSimilarity(
                 toLower(coalesce(
                     CASE WHEN $language = 'japanese' THEN toString(w.japanese_name)
                          ELSE toString(coalesce(w.title_name, w.title, w.english_name))
                     END,
                     ''
                 )),
                 toLower($rawQuery)
             ) AS levSim
        WITH w, score, levSim,
             (0.5 * score + 0.5 * coalesce(levSim, 0.0)) AS finalScore
        WHERE finalScore >= $rankThreshold
        ORDER BY finalScore DESC
        LIMIT $limitCount
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships
        """
        params = {
            "indexName": index_name,
            "luceneQuery": lucene_query,
            "candidateLimit": candidate_limit,
            "rankThreshold": rank_threshold,
            "limitCount": limit,
            "language": language,
            "rawQuery": query or "",
        }
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_work_subgraph_tx(tx, work_id: str):
     cypher = """
     MATCH (w:Work {id: $work_id})
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships
        """
     return tx.run(cypher, parameters={"work_id": work_id}).single()

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

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _build_simple_where(language: SearchLanguage) -> str:
        if language == "japanese":
            clauses = [
                "toLower(toString(coalesce(w.japanese_name, ''))) CONTAINS toLower($searchTerm)",
                "toLower(toString(coalesce(w.title_name, ''))) CONTAINS toLower($searchTerm)",
                "toLower(toString(coalesce(w.title, ''))) CONTAINS toLower($searchTerm)",
            ]
        else:
            clauses = [
                "toLower(toString(coalesce(w.title_name, w.title, ''))) CONTAINS toLower($searchTerm)",
                "toLower(toString(coalesce(w.english_name, ''))) CONTAINS toLower($searchTerm)",
            ]
        return " OR ".join(clauses)

    @staticmethod
    def _build_lucene_query(term: Optional[str]) -> str:
        if not term:
            return "*"
        escaped = MangaAnimeNeo4jService._escape_lucene_specials(term.strip())
        return f"{escaped}~1"

    @staticmethod
    def _escape_lucene_specials(value: str) -> str:
        specials = set(r"+-&|!(){}[]^\"~*?:\\/")
        return "".join(f"\\{ch}" if ch in specials else ch for ch in value)