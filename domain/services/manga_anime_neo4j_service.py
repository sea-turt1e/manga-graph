"""Service layer for querying the manga_anime_list Neo4j database."""

import logging
import os
from typing import Any, Dict, List, Literal, Optional, Tuple

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
        include_hentai: bool = False,
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

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_graph_by_japanese(
        self,
        query: Optional[str],
        limit: int = 50,
        mode: SearchMode = "simple",
        include_hentai: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Backward-compatible helper focused on japanese_name queries."""
        return self.fetch_graph(query=query, limit=limit, language="japanese", mode=mode, include_hentai=include_hentai)

    def fetch_work_subgraph(self, work_id: str, *, include_hentai: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch a focused subgraph centered on a specific work."""
        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_work_subgraph_tx, work_id)

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_similar_by_embedding(
        self,
        query_embedding: List[float],
        *,
        property_name: VectorProperty,
        limit: int = 20,
        include_hentai: bool = False,
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

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def search_similar_works(
        self,
        query_embedding: List[float],
        *,
        property_name: VectorProperty,
        limit: int = 5,
        threshold: float = 0.5,
        include_hentai: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Run cosine similarity search against a stored embedding property with threshold filtering.

        Returns a list of work dictionaries with similarity scores.
        """

        if property_name not in {"embedding_title_en", "embedding_title_ja", "embedding_description"}:
            raise ValueError("Unsupported embedding property")

        with self.driver.session() as session:
            records = session.read_transaction(
                self._vector_similarity_search_tx,
                property_name,
                query_embedding,
                limit,
                threshold,
                include_hentai,
            )

        return records

    def fetch_author_related_works(
        self, author_element_id: str, limit: int = 50, *, include_hentai: bool = False
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works associated with a specific author node (identified by elementId)."""

        with self.driver.session() as session:
            record = session.read_transaction(self._fetch_author_related_works_tx, author_element_id, limit)

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_magazine_related_works(
        self,
        magazine_element_id: str,
        limit: int = 50,
        *,
        include_hentai: bool = False,
        reference_work_id: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works published in a specific magazine node (identified by elementId).

        If reference_work_id is provided, results are sorted by similarity:
        1. Same demographic
        2. Overlapping themes
        3. Publishing date overlap (Jaccard similarity)
        """

        with self.driver.session() as session:
            record = session.read_transaction(
                self._fetch_magazine_related_works_tx, magazine_element_id, limit, reference_work_id
            )

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_publisher_magazines(
        self,
        publisher_element_id: str,
        limit: int = 50,
        *,
        exclude_magazine_id: Optional[str] = None,
        include_hentai: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch magazines linked to a publisher (optionally excluding a source magazine)."""

        with self.driver.session() as session:
            record = session.read_transaction(
                self._fetch_publisher_magazines_tx,
                publisher_element_id,
                limit,
                exclude_magazine_id,
            )

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_magazines_work_graph(
        self,
        magazine_element_ids: List[str],
        *,
        work_limit: int = 50,
        include_hentai: bool = False,
        reference_work_id: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch works and edges for the provided magazine elementIds.

        If reference_work_id is provided, results are sorted by similarity:
        1. Same demographic
        2. Overlapping themes
        3. Publishing date overlap (Jaccard similarity)
        """

        if not magazine_element_ids:
            return {"nodes": [], "edges": []}

        with self.driver.session() as session:
            record = session.read_transaction(
                self._fetch_magazines_work_graph_tx,
                magazine_element_ids,
                work_limit,
                reference_work_id,
            )

        return self._convert_to_graph(record, include_hentai=include_hentai)

    def fetch_graph_cascade(
        self,
        query: Optional[str],
        limit: int = 3,
        *,
        languages: Optional[List[SearchLanguage]] = None,
        include_hentai: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Cascade search: Try ranked → fulltext → simple for each language.

        This method attempts to find works using the following strategy:
        For each language in order:
        1. Try 'ranked' mode (fulltext + Levenshtein re-ranking) - highest precision
        2. Try 'fulltext' mode (Lucene fuzzy) - catches cases filtered by ranked threshold
        3. Try 'simple' mode (substring match) - final fallback for exact matches

        Returns as soon as results are found.

        This consolidates multiple API calls into a single endpoint for efficiency.

        Args:
            query: Search term (title or partial title)
            limit: Maximum number of works to return
            languages: List of languages to search, defaults to ["japanese", "english"]
            include_hentai: Whether to include hentai content

        Returns:
            Graph data with nodes and edges
        """
        if languages is None:
            languages = ["japanese", "english"]

        # Define search modes in priority order
        search_modes: List[SearchMode] = ["ranked", "fulltext", "simple"]

        for lang in languages:
            for mode in search_modes:
                result = self.fetch_graph(
                    query=query,
                    limit=limit,
                    language=lang,
                    mode=mode,
                    include_hentai=include_hentai,
                )
                if result.get("nodes"):
                    logger.debug("Cascade search found results with %s mode (lang=%s)", mode, lang)
                    return result

        logger.debug("Cascade search found no results for query: %s", query)
        return {"nodes": [], "edges": []}

    def search_similar_works_multi(
        self,
        query_embedding: List[float],
        *,
        property_names: List[VectorProperty],
        limit: int = 10,
        threshold: float = 0.3,
        include_hentai: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Run cosine similarity search against multiple embedding properties in parallel.

        Searches against all specified embedding properties and returns merged,
        deduplicated results sorted by similarity score.

        Args:
            query_embedding: The query vector
            property_names: List of embedding properties to search (e.g., ["embedding_title_en", "embedding_title_ja"])
            limit: Maximum number of results to return
            threshold: Minimum similarity score
            include_hentai: Whether to include hentai content

        Returns:
            Merged and deduplicated list of work dictionaries with similarity scores
        """
        all_results: Dict[str, Dict[str, Any]] = {}

        with self.driver.session() as session:
            for property_name in property_names:
                if property_name not in {"embedding_title_en", "embedding_title_ja", "embedding_description"}:
                    logger.warning("Skipping unsupported embedding property: %s", property_name)
                    continue

                try:
                    records = session.read_transaction(
                        self._vector_similarity_search_tx,
                        property_name,
                        query_embedding,
                        limit,
                        threshold,
                        include_hentai,
                    )

                    # Merge results, keeping the highest similarity score for each work
                    for record in records:
                        work_id = record["work_id"]
                        if work_id not in all_results or record["similarity_score"] > all_results[work_id]["similarity_score"]:
                            all_results[work_id] = record

                except Exception as exc:
                    logger.warning("Error searching %s: %s", property_name, exc)
                    continue

        # Sort by similarity score and limit results
        sorted_results = sorted(all_results.values(), key=lambda x: x["similarity_score"], reverse=True)
        return sorted_results[:limit]

    def fetch_related_graphs_batch(
        self,
        *,
        author_element_id: Optional[str] = None,
        magazine_element_id: Optional[str] = None,
        publisher_element_id: Optional[str] = None,
        author_limit: int = 5,
        magazine_limit: int = 5,
        publisher_limit: int = 3,
        reference_work_id: Optional[str] = None,
        exclude_magazine_id: Optional[str] = None,
        include_hentai: bool = False,
    ) -> Dict[str, Optional[Dict[str, List[Dict[str, Any]]]]]:
        """Fetch author, magazine, and publisher related graphs in a single session.

        This method consolidates multiple graph queries into a single API call,
        reducing round-trips and improving performance.

        Args:
            author_element_id: Author node elementId (optional)
            magazine_element_id: Magazine node elementId (optional)
            publisher_element_id: Publisher node elementId (optional)
            author_limit: Max works to return for author
            magazine_limit: Max works to return for magazine
            publisher_limit: Max magazines to return for publisher
            reference_work_id: Reference work for magazine sorting
            exclude_magazine_id: Magazine to exclude from publisher results
            include_hentai: Whether to include hentai content

        Returns:
            Dict with keys 'author_graph', 'magazine_graph', 'publisher_graph'
            Each value is either a graph dict or None if not requested
        """
        results: Dict[str, Optional[Dict[str, List[Dict[str, Any]]]]] = {
            "author_graph": None,
            "magazine_graph": None,
            "publisher_graph": None,
        }

        with self.driver.session() as session:
            # Fetch author related works
            if author_element_id:
                try:
                    record = session.read_transaction(
                        self._fetch_author_related_works_tx, author_element_id, author_limit
                    )
                    results["author_graph"] = self._convert_to_graph(record, include_hentai=include_hentai)
                    logger.debug("Fetched author graph for %s", author_element_id)
                except Exception as exc:
                    logger.warning("Error fetching author graph: %s", exc)

            # Fetch magazine related works
            if magazine_element_id:
                try:
                    record = session.read_transaction(
                        self._fetch_magazine_related_works_tx, magazine_element_id, magazine_limit, reference_work_id
                    )
                    results["magazine_graph"] = self._convert_to_graph(record, include_hentai=include_hentai)
                    logger.debug("Fetched magazine graph for %s", magazine_element_id)
                except Exception as exc:
                    logger.warning("Error fetching magazine graph: %s", exc)

            # Fetch publisher magazines
            if publisher_element_id:
                try:
                    record = session.read_transaction(
                        self._fetch_publisher_magazines_tx,
                        publisher_element_id,
                        publisher_limit,
                        exclude_magazine_id,
                    )
                    results["publisher_graph"] = self._convert_to_graph(record, include_hentai=include_hentai)
                    logger.debug("Fetched publisher graph for %s", publisher_element_id)
                except Exception as exc:
                    logger.warning("Error fetching publisher graph: %s", exc)

        return results

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
    def _fetch_magazine_related_works_tx(tx, magazine_element_id: str, limit: int, reference_work_id: Optional[str]):
        """Fetch magazine related works with priority-based sorting when reference_work_id is provided."""
        if reference_work_id:
            # Priority-based sorting:
            # 1. demographic match (full match > partial match)
            # 2. themes overlap (full overlap > partial overlap)
            # 3. publishing_date Jaccard similarity
            cypher = """
            // Get the reference work's attributes
            MATCH (ref:Work)
            WHERE elementId(ref) = $referenceWorkId
            WITH ref,
                 coalesce(ref.demographic, []) AS ref_demo,
                 coalesce(ref.themes, []) AS ref_themes,
                 ref.publishing_date AS ref_pub_date

            // Get magazine and its works
            MATCH (m:Magazine)
            WHERE elementId(m) = $magazineElementId
            MATCH (w:Work)-[rel:PUBLISHED_IN]->(m)
            WHERE elementId(w) <> $referenceWorkId  // Exclude the reference work

            WITH m, w, rel, ref_demo, ref_themes, ref_pub_date,
                 coalesce(w.demographic, []) AS w_demo,
                 coalesce(w.themes, []) AS w_themes,
                 w.publishing_date AS w_pub_date

            // Calculate demographic score (0-2: 2=full match, 1=partial, 0=none)
            WITH m, w, rel, ref_demo, ref_themes, ref_pub_date, w_demo, w_themes, w_pub_date,
                 CASE
                     WHEN size(ref_demo) = 0 OR size(w_demo) = 0 THEN 0
                     WHEN ref_demo = w_demo THEN 2
                     WHEN size([d IN ref_demo WHERE d IN w_demo]) > 0 THEN 1
                     ELSE 0
                 END AS demo_score

            // Calculate themes score (count of overlapping themes)
            WITH m, w, rel, ref_themes, ref_pub_date, w_themes, w_pub_date, demo_score,
                 CASE
                     WHEN size(ref_themes) = 0 OR size(w_themes) = 0 THEN 0
                     ELSE size([t IN ref_themes WHERE t IN w_themes])
                 END AS themes_score,
                 CASE
                     WHEN size(ref_themes) = 0 OR size(w_themes) = 0 THEN 0
                     ELSE toFloat(size([t IN ref_themes WHERE t IN w_themes])) / toFloat(size(ref_themes))
                 END AS themes_ratio

            // Parse publishing_date to calculate Jaccard similarity
            // Format: "MMM DD, YYYY to MMM DD, YYYY" or "MMM DD, YYYY to ?"
            // Extract years safely using toString() to handle mixed types
            WITH m, w, rel, demo_score, themes_score, themes_ratio,
                 toString(ref_pub_date) AS ref_pub_str,
                 toString(w_pub_date) AS w_pub_str

            // Extract start and end years
            WITH m, w, rel, demo_score, themes_score, themes_ratio, ref_pub_str, w_pub_str,
                 CASE
                     WHEN ref_pub_str IS NULL OR ref_pub_str = '' OR NOT ref_pub_str CONTAINS ',' THEN null
                     ELSE toInteger(trim(split(split(ref_pub_str, ' to ')[0], ',')[1]))
                 END AS ref_start_year,
                 CASE
                     WHEN ref_pub_str IS NULL OR ref_pub_str = '' THEN date().year
                     WHEN ref_pub_str CONTAINS '?' THEN date().year
                     WHEN NOT ref_pub_str CONTAINS ' to ' THEN date().year
                     WHEN NOT split(ref_pub_str, ' to ')[1] CONTAINS ',' THEN date().year
                     ELSE toInteger(trim(split(split(ref_pub_str, ' to ')[1], ',')[1]))
                 END AS ref_end_year,
                 CASE
                     WHEN w_pub_str IS NULL OR w_pub_str = '' OR NOT w_pub_str CONTAINS ',' THEN null
                     ELSE toInteger(trim(split(split(w_pub_str, ' to ')[0], ',')[1]))
                 END AS w_start_year,
                 CASE
                     WHEN w_pub_str IS NULL OR w_pub_str = '' THEN date().year
                     WHEN w_pub_str CONTAINS '?' THEN date().year
                     WHEN NOT w_pub_str CONTAINS ' to ' THEN date().year
                     WHEN NOT split(w_pub_str, ' to ')[1] CONTAINS ',' THEN date().year
                     ELSE toInteger(trim(split(split(w_pub_str, ' to ')[1], ',')[1]))
                 END AS w_end_year

            // Calculate Jaccard similarity for date overlap
            WITH m, w, rel, demo_score, themes_score, themes_ratio,
                 ref_start_year, ref_end_year, w_start_year, w_end_year,
                 CASE
                     WHEN ref_start_year IS NULL OR w_start_year IS NULL THEN 0.0
                     ELSE
                         // overlap = max(0, min(end1, end2) - max(start1, start2) + 1)
                         // union = (end1 - start1 + 1) + (end2 - start2 + 1) - overlap
                         // jaccard = overlap / union
                         CASE
                             WHEN CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                  < CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                             THEN 0.0
                             ELSE
                                 toFloat(
                                     CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                     - CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                                     + 1
                                 ) / toFloat(
                                     (ref_end_year - ref_start_year + 1)
                                     + (w_end_year - w_start_year + 1)
                                     - (CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                        - CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                                        + 1)
                                 )
                         END
                 END AS date_jaccard

            // Sort by priority: demographic > themes > date_jaccard
            ORDER BY demo_score DESC, themes_score DESC, themes_ratio DESC, date_jaccard DESC,
                     toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
            LIMIT $limitCount

            RETURN collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w)}) AS work_nodes,
                   [{id: elementId(m), labels: labels(m), properties: properties(m)}] AS neighbor_nodes,
                   collect(DISTINCT {id: elementId(rel), source: elementId(w), target: elementId(m), type: type(rel), properties: properties(rel)}) AS relationships
            """
            params = {
                "magazineElementId": magazine_element_id,
                "limitCount": limit,
                "referenceWorkId": reference_work_id,
            }
        else:
            # Original simple query without priority sorting
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
    def _fetch_magazines_work_graph_tx(tx, magazine_element_ids: List[str], work_limit: int, reference_work_id: Optional[str]):
        """Fetch works from multiple magazines with priority-based sorting when reference_work_id is provided."""
        if reference_work_id:
            # Priority-based sorting
            cypher = """
            // Get the reference work's attributes
            MATCH (ref:Work)
            WHERE elementId(ref) = $referenceWorkId
            WITH ref,
                 coalesce(ref.demographic, []) AS ref_demo,
                 coalesce(ref.themes, []) AS ref_themes,
                 ref.publishing_date AS ref_pub_date

            // Get magazines and their works
            MATCH (m:Magazine)
            WHERE elementId(m) IN $magazineElementIds
            WITH m, ref_demo, ref_themes, ref_pub_date
            CALL {
                WITH m, ref_demo, ref_themes, ref_pub_date
                OPTIONAL MATCH (m)<-[rel:PUBLISHED_IN]-(w:Work)
                WHERE elementId(w) <> $referenceWorkId  // Exclude reference work

                WITH m, rel, w, ref_demo, ref_themes, ref_pub_date,
                     coalesce(w.demographic, []) AS w_demo,
                     coalesce(w.themes, []) AS w_themes,
                     w.publishing_date AS w_pub_date

                // Calculate demographic score
                WITH m, rel, w, ref_demo, ref_themes, ref_pub_date, w_demo, w_themes, w_pub_date,
                     CASE
                         WHEN w IS NULL THEN 0
                         WHEN size(ref_demo) = 0 OR size(w_demo) = 0 THEN 0
                         WHEN ref_demo = w_demo THEN 2
                         WHEN size([d IN ref_demo WHERE d IN w_demo]) > 0 THEN 1
                         ELSE 0
                     END AS demo_score

                // Calculate themes score
                WITH m, rel, w, ref_themes, ref_pub_date, w_themes, w_pub_date, demo_score,
                     CASE
                         WHEN w IS NULL OR size(ref_themes) = 0 OR size(w_themes) = 0 THEN 0
                         ELSE size([t IN ref_themes WHERE t IN w_themes])
                     END AS themes_score,
                     CASE
                         WHEN w IS NULL OR size(ref_themes) = 0 OR size(w_themes) = 0 THEN 0.0
                         ELSE toFloat(size([t IN ref_themes WHERE t IN w_themes])) / toFloat(size(ref_themes))
                     END AS themes_ratio

                // Calculate date Jaccard similarity - use toString() for safety
                WITH m, rel, w, demo_score, themes_score, themes_ratio,
                     toString(ref_pub_date) AS ref_pub_str,
                     toString(w_pub_date) AS w_pub_str

                WITH m, rel, w, demo_score, themes_score, themes_ratio, ref_pub_str, w_pub_str,
                     CASE
                         WHEN ref_pub_str IS NULL OR ref_pub_str = '' OR NOT ref_pub_str CONTAINS ',' THEN null
                         ELSE toInteger(trim(split(split(ref_pub_str, ' to ')[0], ',')[1]))
                     END AS ref_start_year,
                     CASE
                         WHEN ref_pub_str IS NULL OR ref_pub_str = '' THEN date().year
                         WHEN ref_pub_str CONTAINS '?' THEN date().year
                         WHEN NOT ref_pub_str CONTAINS ' to ' THEN date().year
                         WHEN NOT split(ref_pub_str, ' to ')[1] CONTAINS ',' THEN date().year
                         ELSE toInteger(trim(split(split(ref_pub_str, ' to ')[1], ',')[1]))
                     END AS ref_end_year,
                     CASE
                         WHEN w_pub_str IS NULL OR w_pub_str = '' OR NOT w_pub_str CONTAINS ',' THEN null
                         ELSE toInteger(trim(split(split(w_pub_str, ' to ')[0], ',')[1]))
                     END AS w_start_year,
                     CASE
                         WHEN w_pub_str IS NULL OR w_pub_str = '' THEN date().year
                         WHEN w_pub_str CONTAINS '?' THEN date().year
                         WHEN NOT w_pub_str CONTAINS ' to ' THEN date().year
                         WHEN NOT split(w_pub_str, ' to ')[1] CONTAINS ',' THEN date().year
                         ELSE toInteger(trim(split(split(w_pub_str, ' to ')[1], ',')[1]))
                     END AS w_end_year

                WITH m, rel, w, demo_score, themes_score, themes_ratio,
                     ref_start_year, ref_end_year, w_start_year, w_end_year,
                     CASE
                         WHEN ref_start_year IS NULL OR w_start_year IS NULL THEN 0.0
                         ELSE
                             CASE
                                 WHEN CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                      < CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                                 THEN 0.0
                                 ELSE
                                     toFloat(
                                         CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                         - CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                                         + 1
                                     ) / toFloat(
                                         (ref_end_year - ref_start_year + 1)
                                         + (w_end_year - w_start_year + 1)
                                         - (CASE WHEN ref_end_year < w_end_year THEN ref_end_year ELSE w_end_year END
                                            - CASE WHEN ref_start_year > w_start_year THEN ref_start_year ELSE w_start_year END
                                            + 1)
                                     )
                             END
                     END AS date_jaccard

                ORDER BY demo_score DESC, themes_score DESC, themes_ratio DESC, date_jaccard DESC,
                         toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
                LIMIT $workLimit

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
                "referenceWorkId": reference_work_id,
            }
        else:
            # Original simple query
            cypher = """
            MATCH (m:Magazine)
            WHERE elementId(m) IN $magazineElementIds
            WITH m
            CALL {
                WITH m
                OPTIONAL MATCH (m)<-[rel:PUBLISHED_IN]-(w:Work)
                ORDER BY toLower(toString(coalesce(w.title_name, w.english_name, w.title, w.id, '')))
                LIMIT $workLimit
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
        """Vector similarity search using Neo4j vector index for fast retrieval."""
        # ベクトルインデックス名のマッピング
        index_map = {
            "embedding_title_ja": "work_embedding_title_ja",
            "embedding_title_en": "work_embedding_title_en",
            "embedding_description": "work_embedding_description",
        }
        index_name = index_map.get(property_name)

        if index_name:
            # ネイティブベクトルインデックスを使用（高速）
            cypher = """
            CALL db.index.vector.queryNodes($indexName, $limitCount, $queryEmbedding)
            YIELD node AS w, score
            OPTIONAL MATCH (w)-[r]-(n)
            OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(:Magazine)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
            WITH w, score, r, n, pub_rel, pub
            WITH collect(DISTINCT {id: elementId(w), labels: labels(w), properties: properties(w) + {similarity_score: score}}) AS work_nodes,
                 collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {id: elementId(n), labels: labels(n), properties: properties(n)} END) AS neighbor_nodes,
                 collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)} END) AS relationships,
                 collect(DISTINCT CASE WHEN pub IS NULL THEN NULL ELSE {id: elementId(pub), labels: labels(pub), properties: properties(pub)} END) AS publisher_nodes,
                 collect(DISTINCT CASE WHEN pub_rel IS NULL THEN NULL ELSE {id: elementId(pub_rel), source: elementId(startNode(pub_rel)), target: elementId(endNode(pub_rel)), type: type(pub_rel), properties: properties(pub_rel)} END) AS publisher_relationships
            RETURN work_nodes,
                   [n IN neighbor_nodes + publisher_nodes WHERE n IS NOT NULL] AS neighbor_nodes,
                   [r IN relationships + publisher_relationships WHERE r IS NOT NULL] AS relationships
            """
            params = {
                "indexName": index_name,
                "queryEmbedding": query_embedding,
                "limitCount": limit,
            }
        else:
            # フォールバック：インデックスがない場合は全スキャン（遅い）
            logger.warning(f"No vector index found for {property_name}, using full scan")
            cypher = f"""
            MATCH (w:Work)
            WHERE w.{property_name} IS NOT NULL
            WITH w, vector.similarity.cosine($queryEmbedding, w.{property_name}) AS score
            WHERE score IS NOT NULL
            ORDER BY score DESC
            LIMIT $limitCount
            OPTIONAL MATCH (w)-[r]-(n)
            OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(:Magazine)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
            WITH w, score, r, n, pub_rel, pub
            WITH collect(DISTINCT {{id: elementId(w), labels: labels(w), properties: properties(w) + {{similarity_score: score}}}}) AS work_nodes,
                 collect(DISTINCT CASE WHEN n IS NULL THEN NULL ELSE {{id: elementId(n), labels: labels(n), properties: properties(n)}} END) AS neighbor_nodes,
                 collect(DISTINCT CASE WHEN r IS NULL THEN NULL ELSE {{id: elementId(r), source: elementId(startNode(r)), target: elementId(endNode(r)), type: type(r), properties: properties(r)}} END) AS relationships,
                 collect(DISTINCT CASE WHEN pub IS NULL THEN NULL ELSE {{id: elementId(pub), labels: labels(pub), properties: properties(pub)}} END) AS publisher_nodes,
                 collect(DISTINCT CASE WHEN pub_rel IS NULL THEN NULL ELSE {{id: elementId(pub_rel), source: elementId(startNode(pub_rel)), target: elementId(endNode(pub_rel)), type: type(pub_rel), properties: properties(pub_rel)}} END) AS publisher_relationships
            RETURN work_nodes,
                   [n IN neighbor_nodes + publisher_nodes WHERE n IS NOT NULL] AS neighbor_nodes,
                   [r IN relationships + publisher_relationships WHERE r IS NOT NULL] AS relationships
            """
            params = {
                "queryEmbedding": query_embedding,
                "limitCount": limit,
            }

        return tx.run(cypher, parameters=params).single()

    @staticmethod
    def _vector_similarity_search_tx(
        tx, property_name: str, query_embedding: List[float], limit: int, threshold: float, include_hentai: bool
    ):
        """Vector similarity search with threshold filtering using Neo4j vector index."""
        # ベクトルインデックス名のマッピング
        index_map = {
            "embedding_title_ja": "work_embedding_title_ja",
            "embedding_title_en": "work_embedding_title_en",
            "embedding_description": "work_embedding_description",
        }
        index_name = index_map.get(property_name)

        # hentai フィルタ条件
        hentai_filter = "" if include_hentai else "AND NOT 'Hentai' IN coalesce(w.genres, [])"

        if index_name:
            # ネイティブベクトルインデックスを使用
            # インデックスから多めに取得してからフィルタリング（hentai除外の場合）
            fetch_limit = limit * 3 if not include_hentai else limit

            cypher = f"""
            CALL db.index.vector.queryNodes($indexName, $fetchLimit, $queryEmbedding)
            YIELD node AS w, score
            WHERE score >= $threshold {hentai_filter}
            WITH w, score
            ORDER BY score DESC
            LIMIT $limitCount
            RETURN w.id AS work_id,
                   coalesce(w.title_name, w.title, w.english_name) AS title_en,
                   w.japanese_name AS title_ja,
                   w.synopsis AS description,
                   score AS similarity_score,
                   w.media_type AS media_type,
                   w.genres AS genres
            """
            params = {
                "indexName": index_name,
                "queryEmbedding": query_embedding,
                "fetchLimit": fetch_limit,
                "threshold": threshold,
                "limitCount": limit,
            }
        else:
            # フォールバック：全スキャン（遅い）
            logger.warning(f"No vector index found for {property_name}, using full scan")
            cypher = f"""
            MATCH (w:Work)
            WHERE w.{property_name} IS NOT NULL {hentai_filter}
            WITH w, vector.similarity.cosine($queryEmbedding, w.{property_name}) AS score
            WHERE score IS NOT NULL AND score >= $threshold
            ORDER BY score DESC
            LIMIT $limitCount
            RETURN w.id AS work_id,
                   coalesce(w.title_name, w.title, w.english_name) AS title_en,
                   w.japanese_name AS title_ja,
                   w.synopsis AS description,
                   score AS similarity_score,
                   w.media_type AS media_type,
                   w.genres AS genres
            """
            params = {
                "queryEmbedding": query_embedding,
                "limitCount": limit,
                "threshold": threshold,
            }

        result = tx.run(cypher, parameters=params)
        return [dict(record) for record in result]

    # ------------------------------------------------------------------
    # Result conversion helpers
    # ------------------------------------------------------------------
    def _convert_to_graph(self, record, *, include_hentai: bool = False) -> Dict[str, List[Dict[str, Any]]]:
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

        if not include_hentai:
            nodes, edges = self._filter_hentai_content(nodes, edges)

        return {"nodes": nodes, "edges": edges}

    def _format_node(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        labels = entry.get("labels", [])
        props = entry.get("properties", {})
        node_type = self._infer_type(labels)
        label = self._derive_label(node_type, props)

        properties = {**props, "source": "neo4j-manga-anime"}

        return {"id": entry.get("id"), "label": label, "type": node_type, "properties": properties}

    def _filter_hentai_content(
        self, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        excluded_ids = {
            node["id"]
            for node in nodes
            if node.get("type") == "work" and self._node_contains_hentai(node.get("properties") or {})
        }
        if not excluded_ids:
            return nodes, edges

        filtered_nodes = [node for node in nodes if node.get("id") not in excluded_ids]
        filtered_edges = [
            edge
            for edge in edges
            if edge.get("source") not in excluded_ids and edge.get("target") not in excluded_ids
        ]
        return filtered_nodes, filtered_edges

    @staticmethod
    def _node_contains_hentai(props: Dict[str, Any]) -> bool:
        genres_value = props.get("genres") or props.get("genre")
        if not genres_value:
            return False
        keyword = "hentai"
        return MangaAnimeNeo4jService._value_contains_keyword(genres_value, keyword)

    @staticmethod
    def _value_contains_keyword(value: Any, keyword: str) -> bool:
        keyword = keyword.lower()
        if isinstance(value, str):
            return keyword in value.lower()
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if isinstance(item, str) and keyword in item.lower():
                    return True
        if isinstance(value, dict):
            for key in value.keys():
                if isinstance(key, str) and keyword in key.lower():
                    return True
            for val in value.values():
                if isinstance(val, str) and keyword in val.lower():
                    return True
        return False

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