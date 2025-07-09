"""
Neo4j database repository for manga graph data
"""

import logging
import os
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase

from domain.services.name_normalizer import (
    generate_normalized_id,
    normalize_and_split_creators,
    normalize_creator_name,
    normalize_publisher_name,
)

logger = logging.getLogger(__name__)


class Neo4jMangaRepository:
    """Neo4j-based manga data repository"""

    def __init__(self, driver=None, uri: str = None, user: str = None, password: str = None):
        if driver is not None:
            # Use existing driver
            self.driver = driver
            self.uri = "provided_driver"
            self.user = "provided_driver"
            self.password = "provided_driver"
            logger.info("Using provided Neo4j driver")
        else:
            # Create new driver
            self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
            self.user = user or os.getenv("NEO4J_USER", "neo4j")
            self.password = password or os.getenv("NEO4J_PASSWORD", "password")
            logger.info(f"Attempting to connect to Neo4j at {self.uri} with user {self.user}")
            try:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                logger.info(f"Successfully connected to Neo4j at {self.uri}")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j at {self.uri}: {e}")
                raise

    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()

    def search_manga_works(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for manga works by title, grouping by series"""
        logger.info(f"Searching for manga works with term: '{search_term}', limit: {limit}")

        with self.driver.session() as session:
            # First, get all matching works and publications
            query = """
            MATCH (w:Work)
            WHERE toLower(w.title) CONTAINS toLower($search_term)
            OPTIONAL MATCH (w)-[:CREATED_BY]->(a:Author)
            OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(m:Magazine)
            OPTIONAL MATCH (m)-[:PUBLISHED_BY]->(p:Publisher)
            OPTIONAL MATCH (s:Series)-[:CONTAINS]->(w)
            OPTIONAL MATCH (pub:Publication)-[:RELATED_TO]->(w)
            OPTIONAL MATCH (pub)-[:PUBLISHED_IN_MAGAZINE]->(m2:Magazine)
            RETURN w.id as work_id, w.title as title, w.published_date as published_date,
                   w.first_published as first_published, w.last_published as last_published,
                   collect(DISTINCT a.name) as creators,
                   collect(DISTINCT p.name) as publishers,
                   collect(DISTINCT m.title) as magazines,
                   w.genre as genre, w.isbn as isbn, w.volume as volume,
                   s.id as series_id, s.name as series_name
            ORDER BY w.title, w.published_date
            """

            logger.debug(f"Running query with search_term: {search_term}")
            result = session.run(query, search_term=search_term)
            all_works = []

            for record in result:
                work = {
                    "work_id": record["work_id"],
                    "title": record["title"],
                    "published_date": record["published_date"],
                    "first_published": record["first_published"],
                    "last_published": record["last_published"],
                    "creators": [c for c in record["creators"] if c],
                    "publishers": [p for p in record["publishers"] if p],
                    "magazines": [m for m in record["magazines"] if m],
                    "genre": record["genre"],
                    "isbn": record["isbn"],
                    "volume": record["volume"],
                    "series_id": record["series_id"],
                    "series_name": record["series_name"],
                }
                all_works.append(work)

            logger.info(f"Found {len(all_works)} works matching '{search_term}'")

            # Group works by series or base title
            series_groups = {}
            series_name_to_key = {}  # シリーズ名からキーへのマッピング
            standalone_works = []

            for work in all_works:
                if work["series_id"]:
                    # シリーズ名を取得
                    series_name = work["series_name"] or self._extract_base_title(work["title"])

                    # 既存のシリーズ名と一致するかチェック
                    if series_name in series_name_to_key:
                        # 既存のグループに追加
                        series_key = series_name_to_key[series_name]
                    else:
                        # 新しいシリーズキーとして登録
                        series_key = work["series_id"]
                        series_name_to_key[series_name] = series_key
                        series_groups[series_key] = {
                            "series_id": work["series_id"],
                            "series_name": series_name,
                            "works": [],
                            "creators": set(),
                            "publishers": set(),
                            "earliest_date": work["published_date"],
                            "latest_date": work["published_date"],
                            "volumes": [],
                        }

                    series_groups[series_key]["works"].append(work)
                    series_groups[series_key]["creators"].update(work["creators"])
                    series_groups[series_key]["publishers"].update(work["publishers"])
                    if "magazines" not in series_groups[series_key]:
                        series_groups[series_key]["magazines"] = set()
                    series_groups[series_key]["magazines"].update(work["magazines"])
                    if work["volume"]:
                        series_groups[series_key]["volumes"].append(work["volume"])
                    # 最も古い日付と新しい日付を更新
                    if work["published_date"] and work["published_date"] < series_groups[series_key]["earliest_date"]:
                        series_groups[series_key]["earliest_date"] = work["published_date"]
                    if work["published_date"] and work["published_date"] > series_groups[series_key]["latest_date"]:
                        series_groups[series_key]["latest_date"] = work["published_date"]
                else:
                    # シリーズIDがない場合は、タイトルから基本タイトルを抽出してグループ化を試みる
                    base_title = self._extract_base_title(work["title"])

                    # 既存のシリーズ名とマッチするかチェック
                    found_group = False
                    for existing_series_name, series_key in series_name_to_key.items():
                        if base_title == existing_series_name or base_title == self._extract_base_title(
                            existing_series_name
                        ):
                            # 既存のグループに追加
                            series_groups[series_key]["works"].append(work)
                            series_groups[series_key]["creators"].update(work["creators"])
                            series_groups[series_key]["publishers"].update(work["publishers"])
                            if "magazines" not in series_groups[series_key]:
                                series_groups[series_key]["magazines"] = set()
                            series_groups[series_key]["magazines"].update(work["magazines"])
                            if work["volume"]:
                                series_groups[series_key]["volumes"].append(work["volume"])
                            # 日付を更新
                            if (
                                work["published_date"]
                                and work["published_date"] < series_groups[series_key]["earliest_date"]
                            ):
                                series_groups[series_key]["earliest_date"] = work["published_date"]
                            if (
                                work["published_date"]
                                and work["published_date"] > series_groups[series_key]["latest_date"]
                            ):
                                series_groups[series_key]["latest_date"] = work["published_date"]
                            found_group = True
                            break

                    if not found_group:
                        # 新しいグループを作成
                        series_key = f"series_{abs(hash(base_title))}"
                        series_name_to_key[base_title] = series_key
                        series_groups[series_key] = {
                            "series_id": series_key,
                            "series_name": base_title,
                            "works": [work],
                            "creators": set(work["creators"]),
                            "publishers": set(work["publishers"]),
                            "magazines": set(work["magazines"]),
                            "earliest_date": work["published_date"],
                            "latest_date": work["published_date"],
                            "volumes": [work["volume"]] if work["volume"] else [],
                        }

            # Convert groups to single works representing the series
            consolidated_works = []
            for group_data in series_groups.values():
                # 複数の作品がある場合は最初の巻を返す
                if len(group_data["works"]) > 1:
                    # 巻数でソートして最初の巻を選択、巻数がない場合は出版日でソート
                    def sort_key(work):
                        volume = work.get("volume", "")
                        if volume and str(volume).strip():
                            # 巻数を数値として抽出
                            import re

                            volume_match = re.search(r"(\d+)", str(volume))
                            if volume_match:
                                volume_num = int(volume_match.group(1))
                                # 1巻がある場合は最優先
                                if volume_num == 1:
                                    return (0, 1)
                                return (0, volume_num)  # 巻数がある場合は優先度0
                        # 巻数がない場合は出版日を使用し、最も古い日付を優先
                        published_date = work.get("published_date", "9999-99-99")
                        if not published_date:
                            published_date = "9999-99-99"
                        return (1, published_date)  # 巻数がない場合は優先度1

                    sorted_works = sorted(group_data["works"], key=sort_key)

                    # 第1巻を探す（なければ最初の巻を使用）
                    first_volume_work = None
                    for work in sorted_works:
                        volume = work.get("volume", "")
                        if volume and str(volume).strip():
                            import re

                            volume_match = re.search(r"(\d+)", str(volume))
                            if volume_match and int(volume_match.group(1)) == 1:
                                first_volume_work = work
                                logger.debug(f"Found volume 1 for series: {work['title']} (ID: {work['work_id']})")
                                break

                    # 第1巻が見つからない場合は、ソート済みリストの最初を使用
                    if not first_volume_work:
                        first_volume_work = sorted_works[0]
                        logger.debug(
                            f"Volume 1 not found, using first sorted work: {first_volume_work['title']} "
                            f"(ID: {first_volume_work['work_id']}, volume: {first_volume_work.get('volume', 'N/A')})"
                        )

                    # タイトルから巻数表記を除去
                    base_title = self._extract_base_title(first_volume_work["title"])

                    # シリーズ全体のfirst_publishedとlast_publishedを計算
                    all_first_dates = [w["first_published"] for w in group_data["works"] if w.get("first_published")]
                    all_last_dates = [w["last_published"] for w in group_data["works"] if w.get("last_published")]

                    series_first_published = (
                        min(all_first_dates) if all_first_dates else first_volume_work.get("first_published")
                    )
                    series_last_published = (
                        max(all_last_dates) if all_last_dates else first_volume_work.get("last_published")
                    )

                    series_work = {
                        "work_id": first_volume_work["work_id"],  # 第1巻のIDを使用
                        "title": base_title,  # 巻数を除去したタイトル
                        "published_date": first_volume_work["published_date"],
                        "first_published": series_first_published,
                        "last_published": series_last_published,
                        "creators": list(group_data["creators"]),  # シリーズ全体のクリエイター
                        "publishers": list(group_data["publishers"]),  # シリーズ全体の出版社
                        "magazines": list(group_data["magazines"]),  # シリーズ全体の雑誌
                        "genre": first_volume_work["genre"],
                        "isbn": first_volume_work["isbn"],
                        "volume": "1",  # シリーズは常に第1巻として表示
                        "is_series": True,
                        "work_count": len(group_data["works"]),
                        "series_volumes": f"{len(group_data['works'])}巻",  # シリーズ全体の巻数情報
                        "individual_works": group_data["works"],  # 個別作品の情報を保持
                    }
                    consolidated_works.append(series_work)
                else:
                    # 単一作品の場合はそのまま使用
                    single_work = group_data["works"][0]
                    single_work["is_series"] = False
                    single_work["work_count"] = 1
                    consolidated_works.append(single_work)

            # Add standalone works
            consolidated_works.extend(standalone_works)

            # Sort by title and limit results
            consolidated_works.sort(key=lambda x: x["title"])
            return consolidated_works[:limit]

    def _extract_base_title(self, title: str) -> str:
        """Extract base title by removing volume numbers and other suffixes"""
        import re

        if not title:
            return title

        # パターンで巻数や番号を除去
        patterns = [
            r"\s*\d+$",  # 末尾の数字
            r"\s*第\d+巻?$",  # 第X巻
            r"\s*\(\d+\)$",  # (数字)
            r"\s*vol\.\s*\d+$",  # vol. X
            r"\s*VOLUME\s*\d+$",  # VOLUME X
            r"\s*巻\d+$",  # 巻X
            r"\s*その\d+$",  # そのX
            r"\s*メガ盛り.*$",  # メガ盛りmenu等の特殊版
            r"\s*完全版.*$",  # 完全版
            r"\s*新装版.*$",  # 新装版
            r"\s*愛蔵版.*$",  # 愛蔵版
            r"\s*文庫版.*$",  # 文庫版
        ]

        base = title
        for pattern in patterns:
            base = re.sub(pattern, "", base, flags=re.IGNORECASE)

        return base.strip()

    def get_related_works_by_author(self, work_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get related works by the same author"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work {id: $work_id})-[:CREATED_BY]->(a:Author)<-[:CREATED_BY]-(w2:Work)
            WHERE w1.id <> w2.id
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   a.name as author_name, 500 as relevance_score
            LIMIT $limit
            """

            result = session.run(query, work_id=work_id, limit=limit)
            return [dict(record) for record in result]

    def get_related_works_by_publisher(self, work_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get related works by the same publisher"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work {id: $work_id})-[:PUBLISHED_IN]->(m:Magazine)<-[:PUBLISHED_IN]-(w2:Work)
            WHERE w1.id <> w2.id
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   m.title as magazine_name
            LIMIT $limit
            """

            result = session.run(query, work_id=work_id, limit=limit)
            return [dict(record) for record in result]

    def get_related_works_by_publication_period(
        self, work_id: str, year_range: int = 5, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get works published in the same period"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work {id: $work_id})
            WHERE w1.published_date IS NOT NULL AND w1.published_date <> ''
            WITH w1, toInteger(substring(w1.published_date, 0, 4)) as year1
            MATCH (w2:Work)
            WHERE w2.published_date IS NOT NULL AND w2.published_date <> ''
            AND w1.id <> w2.id
            WITH w1, w2, year1, toInteger(substring(w2.published_date, 0, 4)) as year2
            WHERE abs(year1 - year2) <= $year_range
            OPTIONAL MATCH (w2)-[:CREATED_BY]->(a:Author)
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   collect(DISTINCT a.name) as creators,
                   abs(year1 - year2) as year_diff, 100 as relevance_score
            ORDER BY year_diff ASC
            LIMIT $limit
            """

            result = session.run(query, work_id=work_id, year_range=year_range, limit=limit)
            return [dict(record) for record in result]

    def get_related_works_by_magazine_and_period(
        self, work_id: str, year_range: int = 2, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get works published in the same magazine during overlapping periods"""
        logger.info(f"get_related_works_by_magazine_and_period called with work_id: {work_id}, limit: {limit}")
        with self.driver.session() as session:
            # 同じ雑誌で連載期間が重複する作品を取得
            query = """
            MATCH (w1:Work {id: $work_id})-[:PUBLISHED_IN]->(m:Magazine)
            WITH w1, m,
                 toInteger(coalesce(substring(w1.first_published, 0, 4), substring(w1.last_published, 0, 4), "1900")) as start_year1,
                 toInteger(coalesce(substring(w1.last_published, 0, 4), substring(w1.first_published, 0, 4), "2100")) as end_year1
            MATCH (w2:Work)-[:PUBLISHED_IN]->(m)
            WHERE w1.id <> w2.id
            WITH w1, w2, m, start_year1, end_year1,
                 toInteger(coalesce(substring(w2.first_published, 0, 4), substring(w2.last_published, 0, 4), "1900")) as start_year2,
                 toInteger(coalesce(substring(w2.last_published, 0, 4), substring(w2.first_published, 0, 4), "2100")) as end_year2
            WHERE start_year2 <= end_year1 AND end_year2 >= start_year1
            WITH w2, m, start_year1, end_year1, start_year2, end_year2,
                 CASE
                   WHEN start_year2 >= start_year1 AND end_year2 <= end_year1 THEN end_year2 - start_year2 + 1
                   WHEN start_year1 >= start_year2 AND end_year1 <= end_year2 THEN end_year1 - start_year1 + 1
                   WHEN start_year2 <= start_year1 AND end_year2 >= start_year1 THEN end_year2 - start_year1 + 1
                   WHEN start_year1 <= start_year2 AND end_year1 >= start_year2 THEN end_year1 - start_year2 + 1
                   ELSE 0
                 END as overlap_years,
                 CASE
                   WHEN end_year2 - start_year2 + 1 > 0 THEN
                     toFloat(CASE
                       WHEN start_year2 >= start_year1 AND end_year2 <= end_year1 THEN end_year2 - start_year2 + 1
                       WHEN start_year1 >= start_year2 AND end_year1 <= end_year2 THEN end_year1 - start_year1 + 1
                       WHEN start_year2 <= start_year1 AND end_year2 >= start_year1 THEN end_year2 - start_year1 + 1
                       WHEN start_year1 <= start_year2 AND end_year1 >= start_year2 THEN end_year1 - start_year2 + 1
                       ELSE 0
                     END) / toFloat(end_year2 - start_year2 + 1)
                   ELSE 0.0
                 END as overlap_ratio,
                 CASE
                   WHEN (end_year1 - start_year1 + 1) + (end_year2 - start_year2 + 1) > 0 THEN
                     toFloat(CASE
                       WHEN start_year2 >= start_year1 AND end_year2 <= end_year1 THEN end_year2 - start_year2 + 1
                       WHEN start_year1 >= start_year2 AND end_year1 <= end_year2 THEN end_year1 - start_year1 + 1
                       WHEN start_year2 <= start_year1 AND end_year2 >= start_year1 THEN end_year2 - start_year1 + 1
                       WHEN start_year1 <= start_year2 AND end_year1 >= start_year2 THEN end_year1 - start_year2 + 1
                       ELSE 0
                     END) / toFloat((end_year1 - start_year1 + 1) + (end_year2 - start_year2 + 1) - CASE
                       WHEN start_year2 >= start_year1 AND end_year2 <= end_year1 THEN end_year2 - start_year2 + 1
                       WHEN start_year1 >= start_year2 AND end_year1 <= end_year2 THEN end_year1 - start_year1 + 1
                       WHEN start_year2 <= start_year1 AND end_year2 >= start_year1 THEN end_year2 - start_year1 + 1
                       WHEN start_year1 <= start_year2 AND end_year1 >= start_year2 THEN end_year1 - start_year2 + 1
                       ELSE 0
                     END)
                   ELSE 0.0
                 END as jaccard_similarity
            WHERE overlap_years > 0
            OPTIONAL MATCH (w2)-[:CREATED_BY]->(a:Author)
            OPTIONAL MATCH (m)-[:PUBLISHED_BY]->(p:Publisher)
            RETURN w2.id as work_id, w2.title as title,
                   w2.first_published as first_published, w2.last_published as last_published,
                   collect(DISTINCT a.name) as creators,
                   m.title as magazine_name,
                   p.name as publisher_name,
                   overlap_years, overlap_ratio, jaccard_similarity,
                   start_year2 as start_year, end_year2 as end_year,
                   toInteger(1000 + (jaccard_similarity * 1000)) as relevance_score
            ORDER BY relevance_score DESC, jaccard_similarity DESC, overlap_years DESC, start_year2 ASC
            LIMIT $limit
            """

            result = session.run(query, work_id=work_id, year_range=year_range, limit=limit)
            results = [dict(record) for record in result]
            logger.info(f"Query returned {len(results)} results")
            for idx, r in enumerate(results[:5]):  # Log first 5 results
                logger.info(
                    f"Result {idx}: {r['title']} (overlap: {r['overlap_years']} years, ratio: {r.get('overlap_ratio', 0):.2f}, jaccard: {r.get('jaccard_similarity', 0):.3f}, score: {r['relevance_score']})"
                )
            return results

    def search_manga_publications(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for manga publications (magazine serializations) by title"""
        logger.info(f"Searching for manga publications with term: '{search_term}', limit: {limit}")

        with self.driver.session() as session:
            query = """
            MATCH (p:Publication)
            WHERE toLower(p.title) CONTAINS toLower($search_term)
            OPTIONAL MATCH (a:Author)-[:CREATED_PUBLICATION]->(p)
            OPTIONAL MATCH (p)-[:PUBLISHED_IN_MAGAZINE]->(m:Magazine)
            OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(mi:MagazineIssue)-[:ISSUE_OF]->(m2:Magazine)
            RETURN p.id as publication_id, p.title as title, p.publication_date as publication_date,
                   collect(DISTINCT a.name) as creators,
                   collect(DISTINCT coalesce(m.name, m2.name)) as magazines,
                   p.genre as genre
            ORDER BY p.title, p.publication_date
            LIMIT $limit
            """

            logger.debug(f"Running publication query with search_term: {search_term}")
            result = session.run(query, search_term=search_term, limit=limit)
            publications = []

            for record in result:
                publication = {
                    "publication_id": record["publication_id"],
                    "title": record["title"],
                    "publication_date": record["publication_date"],
                    "creators": [c for c in record["creators"] if c],
                    "magazines": [m for m in record["magazines"] if m],
                    "genre": record["genre"],
                }
                publications.append(publication)

            logger.info(f"Found {len(publications)} publications matching '{search_term}'")
            return publications

    def search_manga_data_with_related(
        self, search_term: str, limit: int = 20, include_related: bool = True
    ) -> Dict[str, Any]:
        """Search manga data and include related works for graph visualization"""
        logger.info(
            f"search_manga_data_with_related called with term: '{search_term}', limit: {limit}, include_related: {include_related}"
        )

        main_works = self.search_manga_works(search_term, limit)

        # Also search for publications (magazine serializations)
        publications = self.search_manga_publications(search_term, limit)

        if not main_works and not publications:
            logger.warning(f"No works or publications found for search term: '{search_term}'")
            return {"nodes": [], "edges": []}

        nodes = []
        edges = []
        node_ids_seen = set()  # Track node IDs to prevent duplicates
        edge_ids_seen = set()  # Track edge IDs to prevent duplicates

        # Add main works as nodes
        for work in main_works:
            if work["work_id"] not in node_ids_seen:
                work_data = {**work, "source": "neo4j"}
                node = {"id": work["work_id"], "label": work["title"], "type": "work", "properties": work_data}
                nodes.append(node)
                node_ids_seen.add(work["work_id"])

        # Add publications as nodes
        for publication in publications:
            if publication["publication_id"] not in node_ids_seen:
                pub_data = {**publication, "source": "neo4j"}
                node = {
                    "id": publication["publication_id"],
                    "label": publication["title"],
                    "type": "publication",
                    "properties": pub_data,
                }
                nodes.append(node)
                node_ids_seen.add(publication["publication_id"])

            # Add authors and magazines for publications
            for creator in publication["creators"]:
                if creator:
                    normalized_creators = normalize_and_split_creators(creator)
                    for normalized_creator in normalized_creators:
                        if normalized_creator:
                            author_id = generate_normalized_id(normalized_creator, "author")
                            if author_id not in node_ids_seen:
                                author_node = {
                                    "id": author_id,
                                    "label": normalized_creator,
                                    "type": "author",
                                    "properties": {"source": "neo4j", "name": normalized_creator},
                                }
                                nodes.append(author_node)
                                node_ids_seen.add(author_id)

                            edge_id = f"{author_id}-created-{publication['publication_id']}"
                            if edge_id not in edge_ids_seen:
                                edge = {
                                    "id": edge_id,
                                    "source": author_id,
                                    "target": publication["publication_id"],
                                    "type": "created",
                                    "properties": {"source": "neo4j"},
                                }
                                edges.append(edge)
                                edge_ids_seen.add(edge_id)

            # Add magazines for publications
            for magazine in publication["magazines"]:
                if magazine:
                    magazine_id = generate_normalized_id(magazine, "magazine")
                    if magazine_id not in node_ids_seen:
                        magazine_node = {
                            "id": magazine_id,
                            "label": magazine,
                            "type": "magazine",
                            "properties": {"source": "neo4j", "name": magazine},
                        }
                        nodes.append(magazine_node)
                        node_ids_seen.add(magazine_id)

                    edge_id = f"{magazine_id}-published-{publication['publication_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            "id": edge_id,
                            "source": magazine_id,
                            "target": publication["publication_id"],
                            "type": "published",
                            "properties": {"source": "neo4j"},
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)

        # Process main works
        for work in main_works:
            # Add authors as nodes and create edges
            for creator in work["creators"]:
                if creator:
                    # Split multiple creators and normalize each one
                    normalized_creators = normalize_and_split_creators(creator)
                    for normalized_creator in normalized_creators:
                        if normalized_creator:
                            author_id = generate_normalized_id(normalized_creator, "author")
                            if author_id not in node_ids_seen:
                                author_node = {
                                    "id": author_id,
                                    "label": normalized_creator,
                                    "type": "author",
                                    "properties": {"source": "neo4j", "name": normalized_creator},
                                }
                                nodes.append(author_node)
                                node_ids_seen.add(author_id)

                            edge_id = f"{author_id}-created-{work['work_id']}"
                            if edge_id not in edge_ids_seen:
                                edge = {
                                    "id": edge_id,
                                    "source": author_id,
                                    "target": work["work_id"],
                                    "type": "created",
                                    "properties": {"source": "neo4j"},
                                }
                                edges.append(edge)
                                edge_ids_seen.add(edge_id)

            # Add magazines as nodes and create edges (prioritize magazines over publishers)
            for magazine in work["magazines"]:
                if magazine:
                    magazine_id = generate_normalized_id(magazine, "magazine")
                    if magazine_id not in node_ids_seen:
                        magazine_node = {
                            "id": magazine_id,
                            "label": magazine,
                            "type": "magazine",
                            "properties": {"source": "neo4j", "name": magazine},
                        }
                        nodes.append(magazine_node)
                        node_ids_seen.add(magazine_id)

                    edge_id = f"{magazine_id}-published-{work['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            "id": edge_id,
                            "source": magazine_id,
                            "target": work["work_id"],
                            "type": "published",
                            "properties": {"source": "neo4j"},
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)
                # Add publishers as nodes and create edges (only if no magazines)
                if not work["magazines"]:
                    for publisher in work["publishers"]:
                        if publisher:
                            normalized_publisher = normalize_publisher_name(publisher)
                            if normalized_publisher:
                                publisher_id = generate_normalized_id(normalized_publisher, "publisher")
                                if publisher_id not in node_ids_seen:
                                    publisher_node = {
                                        "id": publisher_id,
                                        "label": normalized_publisher,
                                        "type": "publisher",
                                        "properties": {"source": "neo4j", "name": normalized_publisher},
                                    }
                                    nodes.append(publisher_node)
                                    node_ids_seen.add(publisher_id)
                            edge_id = f"{publisher_id}-published-{work['work_id']}"
                            if edge_id not in edge_ids_seen:
                                edge = {
                                    "id": edge_id,
                                    "source": publisher_id,
                                    "target": work["work_id"],
                                    "type": "published",
                                    "properties": {"source": "neo4j"},
                                }
                                edges.append(edge)
                                edge_ids_seen.add(edge_id)

        # Add related works if requested
        if include_related and main_works:
            # 最も多くの雑誌関係を持つ作品を選択（通常はメインシリーズ）
            main_work = None
            max_magazines = 0
            for work in main_works:
                magazine_count = len(work.get("magazines", []))
                if magazine_count > max_magazines:
                    max_magazines = magazine_count
                    main_work = work

            # 雑誌がない場合は最初の作品を使用
            if main_work is None:
                main_work = main_works[0]

            main_work_id = main_work["work_id"]
            logger.info(f"Selected main work for related search: {main_work['title']} (ID: {main_work_id})")
            logger.info(
                f"Main work details - first_published: {main_work.get('first_published')}, last_published: {main_work.get('last_published')}"
            )
            logger.info(f"Main work magazines: {main_work.get('magazines', [])}")

            # Add works by same author
            author_related = self.get_related_works_by_author(main_work_id, 5)
            for related in author_related:
                if related["work_id"] not in node_ids_seen:
                    related_node = {
                        "id": related["work_id"],
                        "label": related["title"],
                        "type": "work",
                        "properties": {**related, "source": "neo4j"},
                        "relevance_score": related.get("relevance_score", 500),
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related["work_id"])

                # Create author relationship edge
                normalized_author = normalize_creator_name(related["author_name"])
                author_id = generate_normalized_id(normalized_author, "author")
                if author_id in node_ids_seen:
                    edge_id = f"{author_id}-created-{related['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            "id": edge_id,
                            "source": author_id,
                            "target": related["work_id"],
                            "type": "created",
                            "properties": {"source": "neo4j"},
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)

            # Add works from same magazine and period
            logger.info(f"Getting magazine period related works for: {main_work_id}")
            magazine_period_related = self.get_related_works_by_magazine_and_period(main_work_id, 2, 50)
            logger.info(f"Found {len(magazine_period_related)} magazine period related works")

            for idx, related in enumerate(magazine_period_related):
                logger.info(
                    f"Magazine related work {idx}: {related['title']} (overlap: {related.get('overlap_years', 0)} years)"
                )

            for related in magazine_period_related:
                if related["work_id"] not in node_ids_seen:
                    related_node = {
                        "id": related["work_id"],
                        "label": related["title"],
                        "type": "work",
                        "properties": {**related, "source": "neo4j"},
                        "relevance_score": related.get("relevance_score", 1000),
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related["work_id"])

                    # Add creators
                    for creator in related["creators"]:
                        if creator:
                            # Split multiple creators and normalize each one
                            normalized_creators = normalize_and_split_creators(creator)
                            for normalized_creator in normalized_creators:
                                if normalized_creator:
                                    author_id = generate_normalized_id(normalized_creator, "author")
                                    author_node = {
                                        "id": author_id,
                                        "label": normalized_creator,
                                        "type": "author",
                                        "properties": {"source": "neo4j", "name": normalized_creator},
                                    }
                                if author_id not in node_ids_seen:
                                    nodes.append(author_node)
                                    node_ids_seen.add(author_id)

                                edge_id = f"{author_id}-created-{related['work_id']}"
                                if edge_id not in edge_ids_seen:
                                    edge = {
                                        "id": edge_id,
                                        "source": author_id,
                                        "target": related["work_id"],
                                        "type": "created",
                                        "properties": {"source": "neo4j"},
                                    }
                                    edges.append(edge)
                                    edge_ids_seen.add(edge_id)

                    # Add magazines (missing part - this was causing the issue)
                    if related.get("magazine_name"):
                        magazine_id = generate_normalized_id(related["magazine_name"], "magazine")
                        if magazine_id not in node_ids_seen:
                            magazine_node = {
                                "id": magazine_id,
                                "label": related["magazine_name"],
                                "type": "magazine",
                                "properties": {"source": "neo4j", "name": related["magazine_name"]},
                            }
                            nodes.append(magazine_node)
                            node_ids_seen.add(magazine_id)

                        edge_id = f"{magazine_id}-published-{related['work_id']}"
                        if edge_id not in edge_ids_seen:
                            edge = {
                                "id": edge_id,
                                "source": magazine_id,
                                "target": related["work_id"],
                                "type": "published",
                                "properties": {"source": "neo4j"},
                            }
                            edges.append(edge)
                            edge_ids_seen.add(edge_id)

                        # Add publisher for this magazine if available
                        if related.get("publisher_name"):
                            normalized_publisher = normalize_publisher_name(related["publisher_name"])
                            if normalized_publisher:
                                publisher_id = generate_normalized_id(normalized_publisher, "publisher")
                                if publisher_id not in node_ids_seen:
                                    publisher_node = {
                                        "id": publisher_id,
                                        "label": normalized_publisher,
                                        "type": "publisher",
                                        "properties": {"source": "neo4j", "name": normalized_publisher},
                                    }
                                    nodes.append(publisher_node)
                                    node_ids_seen.add(publisher_id)

                                # Create magazine -> publisher edge
                                mag_pub_edge_id = f"{magazine_id}-published_by-{publisher_id}"
                                if mag_pub_edge_id not in edge_ids_seen:
                                    mag_pub_edge = {
                                        "id": mag_pub_edge_id,
                                        "source": magazine_id,
                                        "target": publisher_id,
                                        "type": "published_by",
                                        "properties": {"source": "neo4j"},
                                    }
                                    edges.append(mag_pub_edge)
                                    edge_ids_seen.add(mag_pub_edge_id)

                    # Note: publisher nodes are now only created through magazine relationships
                    # Direct work -> publisher edges are removed as requested

                # Create "same_publisher_period" edge between main work and related work
                if related.get("publisher_name"):
                    edge_id = f"{main_work_id}-same_publisher_period-{related['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            "id": edge_id,
                            "source": main_work_id,
                            "target": related["work_id"],
                            "type": "same_publisher_period",
                            "properties": {
                                "source": "neo4j",
                                "description": f"同じ出版社({related['publisher_name']})・同時期",
                            },
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)

            # Add works from same publication period (without magazine constraint)
            period_related = self.get_related_works_by_publication_period(main_work_id, 3, 5)
            for related in period_related:
                if related["work_id"] not in node_ids_seen:
                    related_node = {
                        "id": related["work_id"],
                        "label": related["title"],
                        "type": "work",
                        "properties": {**related, "source": "neo4j"},
                        "relevance_score": related.get("relevance_score", 100),
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related["work_id"])

                    # Add creators of period-related works
                    for creator in related["creators"]:
                        if creator:
                            # Split multiple creators and normalize each one
                            normalized_creators = normalize_and_split_creators(creator)
                            for normalized_creator in normalized_creators:
                                if normalized_creator:
                                    author_id = generate_normalized_id(normalized_creator, "author")
                                    author_node = {
                                        "id": author_id,
                                        "label": normalized_creator,
                                        "type": "author",
                                        "properties": {"source": "neo4j", "name": normalized_creator},
                                    }
                                if author_id not in node_ids_seen:
                                    nodes.append(author_node)
                                    node_ids_seen.add(author_id)

                                edge_id = f"{author_id}-created-{related['work_id']}"
                                if edge_id not in edge_ids_seen:
                                    edge = {
                                        "id": edge_id,
                                        "source": author_id,
                                        "target": related["work_id"],
                                        "type": "created",
                                        "properties": {"source": "neo4j"},
                                    }
                                    edges.append(edge)
                                    edge_ids_seen.add(edge_id)

        # Final deduplication to ensure no duplicate nodes exist
        unique_nodes = []
        seen_work_titles = {}  # For work nodes, track by title to avoid duplicates
        unique_node_ids = set()  # Track by ID

        for node in nodes:
            if node["type"] == "work":
                # For work nodes, prioritize by keeping the one with more complete data
                title = node["label"]
                if title in seen_work_titles:
                    # Keep the node with more complete data (more properties) and higher relevance_score
                    existing_node = seen_work_titles[title]
                    existing_data_count = len(existing_node.get("data", {}))
                    current_data_count = len(node.get("data", {}))
                    existing_score = existing_node.get("relevance_score", 0)
                    current_score = node.get("relevance_score", 0)

                    # Prefer node with higher relevance_score, or more data if scores are equal
                    if current_score > existing_score or (
                        current_score == existing_score and current_data_count > existing_data_count
                    ):
                        # Replace with current node (has higher score or more data)
                        unique_nodes = [n for n in unique_nodes if n["label"] != title or n["type"] != "work"]
                        unique_nodes.append(node)
                        seen_work_titles[title] = node
                        unique_node_ids.add(node["id"])
                    # Otherwise keep the existing one
                else:
                    seen_work_titles[title] = node
                    unique_nodes.append(node)
                    unique_node_ids.add(node["id"])
            else:
                # For non-work nodes, use ID-based deduplication
                if node["id"] not in unique_node_ids:
                    unique_nodes.append(node)
                    unique_node_ids.add(node["id"])

        # Final deduplication for edges, ensuring they reference existing nodes
        valid_node_ids = {node["id"] for node in unique_nodes}

        unique_edges = []
        unique_edge_keys = set()

        for edge in edges:
            from_id = edge.get("source", edge.get("from"))
            to_id = edge.get("target", edge.get("to"))

            # If this edge references a work node that was deduplicated, update the reference
            # Check if the from/to IDs exist in our final node list
            if from_id not in valid_node_ids:
                # Try to find the correct node ID by matching with work titles
                found_replacement = False
                for node in unique_nodes:
                    if node["type"] == "work" and node["id"] != from_id:
                        # Check if this might be the same work by looking at original edges
                        original_from_node = next((n for n in nodes if n["id"] == from_id), None)
                        if original_from_node and original_from_node["label"] == node["label"]:
                            from_id = node["id"]
                            found_replacement = True
                            break
                if not found_replacement:
                    continue  # Skip this edge if we can't find a valid from node

            if to_id not in valid_node_ids:
                # Try to find the correct node ID by matching with work titles
                found_replacement = False
                for node in unique_nodes:
                    if node["type"] == "work" and node["id"] != to_id:
                        # Check if this might be the same work by looking at original edges
                        original_to_node = next((n for n in nodes if n["id"] == to_id), None)
                        if original_to_node and original_to_node["label"] == node["label"]:
                            to_id = node["id"]
                            found_replacement = True
                            break
                if not found_replacement:
                    continue  # Skip this edge if we can't find a valid to node

            # Only add edge if both nodes exist
            if from_id in valid_node_ids and to_id in valid_node_ids:
                edge_key = (from_id, to_id, edge["type"])
                if edge_key not in unique_edge_keys:
                    updated_edge = edge.copy()
                    updated_edge["source"] = from_id
                    updated_edge["target"] = to_id
                    # Remove old format keys if they exist
                    updated_edge.pop("from", None)
                    updated_edge.pop("to", None)
                    unique_edges.append(updated_edge)
                    unique_edge_keys.add(edge_key)

        # Sort nodes by relevance_score (work nodes only, excluding the main search results)
        work_nodes = []
        other_nodes = []
        main_work_nodes = []

        for node in unique_nodes:
            if node["type"] == "work":
                # Check if this is one of the main search results
                is_main_result = any(node["id"] == work["work_id"] for work in main_works)
                if is_main_result:
                    main_work_nodes.append(node)
                else:
                    work_nodes.append(node)
            else:
                other_nodes.append(node)

        # Sort related work nodes by relevance_score in descending order
        work_nodes.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

        # Combine: main results first, then sorted related works, then other nodes
        unique_nodes = main_work_nodes + work_nodes + other_nodes

        logger.info(
            f"After deduplication: {len(unique_nodes)} nodes and {len(unique_edges)} edges for search term: '{search_term}'"
        )
        return {"nodes": unique_nodes, "edges": unique_edges}

    def get_database_statistics(self) -> Dict[str, int]:
        """Get database statistics"""
        try:
            with self.driver.session() as session:
                stats = {}

                # Count nodes
                for label in ["Work", "Author", "Publisher", "Magazine"]:
                    result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    stats[f"{label.lower()}_count"] = result.single()["count"]

                # Count relationships
                for rel_type in ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]:
                    result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                    stats[f"{rel_type.lower()}_relationships"] = result.single()["count"]

                logger.info(f"Database statistics: {stats}")
                return stats
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {}

    def get_work_by_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """Get work details by ID"""
        logger.info(f"Getting work by ID: {work_id}")

        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            OPTIONAL MATCH (w)-[:CREATED_BY]->(a:Author)
            OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(m:Magazine)-[:PUBLISHED_BY]->(p:Publisher)
            RETURN w,
                   collect(DISTINCT a.name) as authors,
                   collect(DISTINCT p.name) as publishers
            """

            result = session.run(query, work_id=work_id)
            record = result.single()

            if record:
                work = record["w"]
                return {
                    "id": work["id"],
                    "title": work.get("title", ""),
                    "isbn": work.get("isbn", ""),
                    "genre": work.get("genre", ""),
                    "published_date": work.get("published_date", ""),
                    "cover_image_url": work.get("cover_image_url", ""),
                    "publisher": record["publishers"][0] if record["publishers"] else "",
                    "authors": record["authors"],
                }

            return None

    def update_work_cover_image(self, work_id: str, cover_url: str) -> bool:
        """Update work cover image URL"""
        logger.info(f"Updating cover image for work {work_id}: {cover_url}")

        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            SET w.cover_image_url = $cover_url
            RETURN w.id as updated_id
            """

            result = session.run(query, work_id=work_id, cover_url=cover_url)
            record = result.single()

            return record is not None

    def get_works_needing_covers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get works that have ISBN but no cover image"""
        logger.info(f"Getting works needing covers, limit: {limit}")

        with self.driver.session() as session:
            query = """
            MATCH (w:Work)
            WHERE w.isbn IS NOT NULL
              AND w.isbn <> ''
              AND (w.cover_image_url IS NULL OR w.cover_image_url = '')
            RETURN w.id as id, w.title as title, w.isbn as isbn
            LIMIT $limit
            """

            result = session.run(query, limit=limit)

            works = []
            for record in result:
                works.append({"id": record["id"], "title": record["title"], "isbn": record["isbn"]})

            return works
