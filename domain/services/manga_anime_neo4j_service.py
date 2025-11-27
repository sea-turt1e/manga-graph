"""Service layer for querying the manga_anime_list Neo4j database."""

import logging
import os
from typing import Any, Dict, List, Literal, Optional

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

SearchLanguage = Literal["english", "japanese"]
SearchMode = Literal["simple", "fulltext", "ranked"]
VectorProperty = Literal["embedding_title_en", "embedding_title_ja", "embedding_description"]


class MangaAnimeNeo4jService:
    """Service for querying the manga_anime_list Neo4j database."""

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        resolved_uri = uri or os.getenv("MANGA_ANIME_NEO4J_URI") or os.getenv("NEO4J_URI")
        resolved_user = user or os.getenv("MANGA_ANIME_NEO4J_USER") or os.getenv("NEO4J_USER", "neo4j")
        resolved_password = password or os.getenv("MANGA_ANIME_NEO4J_PASSWORD") or os.getenv("NEO4J_PASSWORD")

        if not resolved_uri:
            raise ValueError("Neo4j URI must be provided via arguments or environment variables")
        if resolved_password is None:
            raise ValueError("Neo4j password must be provided via arguments or environment variables")

        self.driver = GraphDatabase.driver(resolved_uri, auth=(resolved_user, resolved_password))
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
            except Exception as exc:
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

    def fetch_similar_by_embedding(
        self,
        query_embedding: List[float],
        *,
        property_name: VectorProperty,
        limit: int = 20,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Run cosine similarity search against a stored embedding property."""

        if property_name not in {"embedding_title_en", "embedding_title_ja", "embedding_description"}:
            raise ValueError("Unsupported embedding property")

        with self.driver.session() as session:
            record = session.read_transaction(
                self._vector_similarity_tx,
                property_name,
                query_embedding,
                limit,
            )

        return self._convert_to_graph(record)

    def fetch_author_related_works(self, author_element_id: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works associated with a specific author node (identified by elementId)."""

        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_author_related_works_tx, author_element_id, limit)

        return self._convert_to_graph(record)

    def fetch_magazine_related_works(self, magazine_element_id: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works published in a specific magazine node (identified by elementId)."""

        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_magazine_related_works_tx, magazine_element_id, limit)

        return self._convert_to_graph(record)

    def fetch_publisher_magazines(
        self,
        publisher_element_id: str,
        limit: int = 50,
        *,
        exclude_magazine_id: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch magazines linked to a publisher (optionally excluding a source magazine)."""

        with self.driver.session() as session:
            record = session.read_transaction(
                self._fetch_publisher_magazines_tx,
                publisher_element_id,
                limit,
                exclude_magazine_id,
            )

        return self._convert_to_graph(record)

    def fetch_magazines_work_graph(
        self,
        magazine_element_ids: List[str],
        *,
        work_limit: int = 50,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works and edges for the provided magazine elementIds."""

        if not magazine_element_ids:
            return {"nodes": [], "edges": []}

        with self.driver.session() as session:
            record = session.read_transaction(
                self._fetch_magazines_work_graph_tx,
                magazine_element_ids,
                work_limit,
            )

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
        OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(:Magazine)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
        WITH collect(DISTINCT {{id: elementId(w), labels: labels(w), properties: properties(w)}}) AS work_nodes,
             collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {{id: elementId(n), labels: labels(n), properties: properties(n)}} END) AS neighbor_nodes,
             collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {{id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)}} END) AS relationships,
             collect(DISTINCT CASE WHEN pub IS NULL THEN NULL ELSE {{id: elementId(pub), labels: labels(pub), properties: properties(pub)}} END) AS publisher_nodes,
             collect(DISTINCT CASE WHEN pub_rel IS NULL THEN NULL ELSE {{id: elementId(pub_rel), source: elementId(startNode(pub_rel)), target: elementId(endNode(pub_rel)), type: type(pub_rel), properties: properties(pub_rel)}} END) AS publisher_relationships
        RETURN work_nodes,
               neighbor_nodes + publisher_nodes AS neighbor_nodes,
               relationships + publisher_relationships AS relationships
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
     OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(:Magazine)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
     WITH collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
          collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
          collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships,
          collect(DISTINCT CASE WHEN pub IS NULL THEN NULL ELSE {id: elementId(pub), labels: labels(pub), properties: properties(pub)} END) AS publisher_nodes,
          collect(DISTINCT CASE WHEN pub_rel IS NULL THEN NULL ELSE {id: elementId(pub_rel), source: elementId(startNode(pub_rel)), target: elementId(endNode(pub_rel)), type: type(pub_rel), properties: properties(pub_rel)} END) AS publisher_relationships
     RETURN work_nodes,
         neighbor_nodes + publisher_nodes AS neighbor_nodes,
         relationships + publisher_relationships AS relationships
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
     OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(:Magazine)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
     WITH collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
          collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
          collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships,
          collect(DISTINCT CASE WHEN pub IS NULL THEN NULL ELSE {id: elementId(pub), labels: labels(pub), properties: properties(pub)} END) AS publisher_nodes,
          collect(DISTINCT CASE WHEN pub_rel IS NULL THEN NULL ELSE {id: elementId(pub_rel), source: elementId(startNode(pub_rel)), target: elementId(endNode(pub_rel)), type: type(pub_rel), properties: properties(pub_rel)} END) AS publisher_relationships
     RETURN work_nodes,
         neighbor_nodes + publisher_nodes AS neighbor_nodes,
         relationships + publisher_relationships AS relationships
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

    @staticmethod
    def _fetch_author_related_works_tx(tx, author_element_id: str, limit: int):
        cypher = """
        MATCH (a:Author)
        WHERE elementId(a) = $authorElementId
        MATCH (w:Work)-[rel:CREATED_BY]->(a)
        WITH a, w, rel
    ORDER BY toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
        LIMIT $limitCount
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               [{id: elementId(a), labels: labels(a), properties: properties(a)}] AS neighbor_nodes,
               collect(DISTINCT {id: elementId(rel), source: elementId(w), target: elementId(a), type: type(rel), properties: properties(rel)}) AS relationships
        """
        params = {"authorElementId": author_element_id, "limitCount": limit}
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_magazine_related_works_tx(tx, magazine_element_id: str, limit: int):
        cypher = """
        MATCH (m:Magazine)
        WHERE elementId(m) = $magazineElementId
        MATCH (w:Work)-[rel:PUBLISHED_IN]->(m)
        WITH m, w, rel
    ORDER BY toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
        LIMIT $limitCount
        RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
               [{id: elementId(m), labels: labels(m), properties: properties(m)}] AS neighbor_nodes,
               collect(DISTINCT {id: elementId(rel), source: elementId(w), target: elementId(m), type: type(rel), properties: properties(rel)}) AS relationships
        """
        params = {"magazineElementId": magazine_element_id, "limitCount": limit}
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_publisher_magazines_tx(tx, publisher_element_id: str, limit: int, exclude_magazine_id: Optional[str]):
        cypher = """
        MATCH (p:Publisher)
        WHERE elementId(p) = $publisherElementId
        MATCH (m:Magazine)-[rel:PUBLISHED_BY]->(p)
        WHERE $excludeMagazineId IS NULL OR elementId(m) <> $excludeMagazineId
        WITH p, m, rel
        ORDER BY toLower(coalesce(m.name, ''))
        LIMIT $limitCount
        RETURN collect(DISTINCT {id: elementId(m), labels: labels(m), properties: properties(m)}) AS work_nodes,
               [{id: elementId(p), labels: labels(p), properties: properties(p)}] AS neighbor_nodes,
               collect(DISTINCT {id: elementId(rel), source: elementId(m), target: elementId(p), type: type(rel), properties: properties(rel)}) AS relationships
        """
        params = {
            "publisherElementId": publisher_element_id,
            "limitCount": limit,
            "excludeMagazineId": exclude_magazine_id,
        }
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _fetch_magazines_work_graph_tx(tx, magazine_element_ids: List[str], work_limit: int):
        cypher = """
        MATCH (m:Magazine)
        WHERE elementId(m) IN $magazineElementIds
        WITH m
        CALL {
            WITH m, $workLimit AS limitCount
            OPTIONAL MATCH (m)<-[rel:PUBLISHED_IN]-(w:Work)
            ORDER BY toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
            LIMIT limitCount
            RETURN collect(DISTINCT CASE WHEN w IS NULL THEN NULL ELSE {id: elementId(w), labels: labels(w), properties: properties(w)} END) AS works,
                   collect(DISTINCT CASE WHEN rel IS NULL THEN NULL ELSE {id: elementId(rel), source: elementId(w), target: elementId(m), type: type(rel), properties: properties(rel)} END) AS rels
        }
        WITH collect(DISTINCT {id: elementId(m), labels: labels(m), properties: properties(m)}) AS magazine_nodes,
             collect(works) AS works_lists,
             collect(rels) AS rels_lists
        WITH magazine_nodes,
             REDUCE(acc = [], wl IN works_lists | acc + wl) AS flattened_works,
             REDUCE(acc = [], rl IN rels_lists | acc + rl) AS flattened_rels
        WITH magazine_nodes, flattened_works, flattened_rels
        UNWIND (CASE WHEN size(flattened_works) = 0 THEN [NULL] ELSE flattened_works END) AS work_entry
        WITH magazine_nodes, flattened_rels, collect(DISTINCT work_entry) AS work_nodes_raw
        WITH magazine_nodes, flattened_rels, [w IN work_nodes_raw WHERE w IS NOT NULL] AS work_nodes
        UNWIND (CASE WHEN size(flattened_rels) = 0 THEN [NULL] ELSE flattened_rels END) AS rel_entry
        WITH magazine_nodes, work_nodes, collect(DISTINCT rel_entry) AS rel_nodes_raw
        WITH magazine_nodes, work_nodes, [r IN rel_nodes_raw WHERE r IS NOT NULL] AS relationships
        RETURN work_nodes,
               magazine_nodes AS neighbor_nodes,
               relationships
        """
        params = {
            "magazineElementIds": magazine_element_ids,
            "workLimit": work_limit,
        }
        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _vector_similarity_tx(tx, property_name: str, query_embedding: List[float], limit: int):
        cypher = f"""
        MATCH (w:Work)
        WHERE w.{property_name} IS NOT NULL
        WITH w, gds.similarity.cosine($queryEmbedding, w.{property_name}) AS score
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT $limitCount
        OPTIONAL MATCH (w)-[r]-(n)
        RETURN collect(DISTINCT {{id: elementId(w), labels: labels(w), properties: properties(w) + {{similarity_score: score}}}}) AS work_nodes,
               collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {{id: elementId(n), labels: labels(n), properties: properties(n)}} END) AS neighbor_nodes,
               collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {{id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)}} END) AS relationships
        """
        params = {"queryEmbedding": query_embedding, "limitCount": limit}
        return tx.run(cypher, parameters=params).single()

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
        def pick(*candidates: Any, default: str) -> str:
            for candidate in candidates:
                if candidate is None:
                    continue
                text = str(candidate).strip()
                if text:
                    return text
            return default

        if node_type == "work":
            return pick(
                props.get("title_name"),
                props.get("english_name"),
                props.get("title"),
                props.get("id"),
                default="Work",
            )
        if node_type == "author":
            return pick(props.get("name"), props.get("english_name"), default="Author")
        if node_type == "magazine":
            return pick(props.get("name"), props.get("title"), default="Magazine")
        if node_type == "publisher":
            return pick(props.get("name"), default="Publisher")
        return pick(props.get("name"), props.get("title"), default="Node")

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