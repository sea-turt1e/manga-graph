"""
Neo4j database repository for manga graph data (external)
"""

import logging
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
    def __init__(self, driver: Optional[Any] = None) -> None:
        import os

        if driver is not None:
            self.driver = driver
        else:
            uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            user = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "password")
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        try:
            self.driver.close()
        except Exception:
            pass

    # ---------------------- Low-level helpers ----------------------
    def _run(self, query: str, **params):
        # Consume results inside the session to avoid using a lazy Result after session closes
        with self.driver.session() as session:
            result = session.run(query, **params)
            return list(result)

    # ---------------------- Public queries ------------------------
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
                   w.total_volumes as total_volumes,
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
                    "total_volumes": record.get("total_volumes"),
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
                        # total_volumes が個々のノードに設定されている場合は最大値、なければ作品数
                        "total_volumes": max(
                            [int(v) for v in [w.get("total_volumes") for w in group_data["works"]] if str(v).isdigit()]
                            or [len(group_data["works"])]
                        ),
                    }
                    consolidated_works.append(series_work)
                else:
                    # 単一作品の場合はそのまま使用
                    single_work = group_data["works"][0]
                    single_work["is_series"] = False
                    single_work["work_count"] = 1
                    if "total_volumes" not in single_work or single_work.get("total_volumes") in (None, ""):
                        # total_volumes が無ければ 単巻の場合は 1 とする
                        single_work["total_volumes"] = 1
                    consolidated_works.append(single_work)

            # Add standalone works
            consolidated_works.extend(standalone_works)

            # Sort by title and limit results
            consolidated_works.sort(key=lambda x: x["title"])
            return consolidated_works[:limit]

    def search_manga_publications(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        query = """
        MATCH (p:Publication)
        WHERE toLower(p.title) CONTAINS toLower($term)
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
        result = self._run(query, term=search_term, limit=limit)
        publications: List[Dict[str, Any]] = []
        for record in result:
            publications.append(
                {
                    "publication_id": record["publication_id"],
                    "title": record["title"],
                    "publication_date": record.get("publication_date"),
                    "creators": [c for c in (record.get("creators") or []) if c],
                    "magazines": [m for m in (record.get("magazines") or []) if m],
                    "genre": record.get("genre"),
                }
            )
        return publications

    # ------------- Related helpers (author/period/magazine) -------------
    def get_related_works_by_author(self, work_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        query = """
        MATCH (w:Work {id: $work_id})-[:CREATED_BY]->(a:Author)
        MATCH (a)<-[:CREATED_BY]-(other:Work)
        WHERE other.id <> $work_id
        OPTIONAL MATCH (other)-[:PUBLISHED_IN]->(m:Magazine)-[:PUBLISHED_BY]->(p:Publisher)
        OPTIONAL MATCH (other)-[:CREATED_BY]->(ca:Author)
        RETURN other.id AS work_id,
               other.title AS title,
               other.total_volumes AS total_volumes,
               head(collect(DISTINCT a.name)) AS author_name,
               head(collect(DISTINCT m.title)) AS magazine_name,
               head(collect(DISTINCT p.name)) AS publisher_name,
               collect(DISTINCT ca.name) AS creators,
               500 AS relevance_score
        LIMIT $limit
        """
        result = self._run(query, work_id=work_id, limit=limit)
        return [dict(r) for r in result]

    def get_related_works_by_magazine_and_period(
        self, work_id: str, year_window: int, limit: int
    ) -> List[Dict[str, Any]]:
        query = """
        MATCH (w:Work {id: $work_id})-[:PUBLISHED_IN]->(m:Magazine)
        OPTIONAL MATCH (w)<-[:CREATED_BY]-(aw:Author)
        WITH w, m, collect(DISTINCT aw) AS authors
        MATCH (other:Work)-[:PUBLISHED_IN]->(m)
        WHERE other.id <> w.id
        WITH w, m, other, authors
        OPTIONAL MATCH (other)-[:CREATED_BY]->(oa:Author)
        WITH w, m, other, authors, collect(DISTINCT oa.name) AS creators
        RETURN other.id AS work_id,
               other.title AS title,
               other.total_volumes AS total_volumes,
               m.title AS magazine_name,
               creators AS creators,
               1000 AS relevance_score,
               0 AS overlap_years,
               0.0 AS overlap_ratio,
               0.0 AS jaccard_similarity,
               head(creators) AS publisher_name
        LIMIT $limit
        """
        result = self._run(query, work_id=work_id, year_window=year_window, limit=limit)
        return [dict(r) for r in result]

    def get_related_works_by_publication_period(self, work_id: str, before: int, after: int) -> List[Dict[str, Any]]:
        query = """
        MATCH (w:Work {id: $work_id})
        MATCH (other:Work)
        WHERE other.id <> w.id
        OPTIONAL MATCH (other)-[:CREATED_BY]->(a:Author)
        RETURN other.id AS work_id,
               other.title AS title,
               other.total_volumes AS total_volumes,
               collect(DISTINCT a.name) AS creators,
               100 AS relevance_score
        LIMIT 20
        """
        result = self._run(query, work_id=work_id, before=before, after=after)
        return [dict(r) for r in result]

    # ---------------------- Main graph query ----------------------
    def search_manga_data_with_related(
        self,
        search_term: str,
        limit: int = 20,
        include_related: bool = True,
        include_same_publisher_other_magazines: Optional[bool] = False,
        same_publisher_other_magazines_limit: Optional[int] = 5,
        sort_total_volumes: Optional[str] = None,
        min_total_volumes: Optional[int] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "search_manga_data_with_related term=%s limit=%s include_related=%s include_same_pub_other_mag=%s same_pub_other_mag_limit=%s",
            search_term,
            limit,
            include_related,
            include_same_publisher_other_magazines,
            same_publisher_other_magazines_limit,
        )

        main_works = self.search_manga_works(search_term, limit)
        if min_total_volumes is not None:
            filtered = [
                w for w in main_works if (int(w.get("total_volumes", w.get("work_count", 0)) or 0) >= min_total_volumes)
            ]
            if filtered:
                main_works = filtered

        if sort_total_volumes in ("asc", "desc"):

            def _extract_total(w: Dict[str, Any]) -> int:
                tv = w.get("total_volumes") or w.get("work_count") or 0
                try:
                    return int(tv)
                except Exception:
                    import re

                    m = re.search(r"(\d+)", str(tv))
                    return int(m.group(1)) if m else 0

            main_works.sort(key=_extract_total, reverse=sort_total_volumes == "desc")

        publications = self.search_manga_publications(search_term, limit)

        # (reserved) Keep track of main work ids for potential ordering if needed in future
        # main_work_ids = {w["work_id"] for w in main_works}

        if not main_works and not publications:
            return {"nodes": [], "edges": []}

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        node_ids_seen = set()
        edge_ids_seen = set()

        # main work nodes
        for work in main_works:
            if work["work_id"] not in node_ids_seen:
                nodes.append(
                    {
                        "id": work["work_id"],
                        "label": work["title"],
                        "type": "work",
                        "properties": {**work, "source": "neo4j"},
                    }
                )
                node_ids_seen.add(work["work_id"])

        # publication nodes + edges
        for pub in publications:
            if pub["publication_id"] not in node_ids_seen:
                nodes.append(
                    {
                        "id": pub["publication_id"],
                        "label": pub["title"],
                        "type": "publication",
                        "properties": {**pub, "source": "neo4j"},
                    }
                )
                node_ids_seen.add(pub["publication_id"])

            all_creators: List[str] = []
            for c in pub["creators"]:
                if c:
                    all_creators.extend(normalize_and_split_creators(c))
            for creator in all_creators:
                if not creator:
                    continue
                aid = generate_normalized_id(creator, "author")
                if aid not in node_ids_seen:
                    nodes.append(
                        {
                            "id": aid,
                            "label": creator,
                            "type": "author",
                            "properties": {"source": "neo4j", "name": creator},
                        }
                    )
                    node_ids_seen.add(aid)
                eid = f"{aid}-created-{pub['publication_id']}"
                if eid not in edge_ids_seen:
                    edges.append(
                        {
                            "id": eid,
                            "source": aid,
                            "target": pub["publication_id"],
                            "type": "created",
                            "properties": {"source": "neo4j"},
                        }
                    )
                    edge_ids_seen.add(eid)
            for m in pub["magazines"]:
                if not m:
                    continue
                mid = generate_normalized_id(m, "magazine")
                if mid not in node_ids_seen:
                    nodes.append(
                        {
                            "id": mid,
                            "label": m,
                            "type": "magazine",
                            "properties": {"source": "neo4j", "name": m},
                        }
                    )
                    node_ids_seen.add(mid)
                eid = f"{mid}-published-{pub['publication_id']}"
                if eid not in edge_ids_seen:
                    edges.append(
                        {
                            "id": eid,
                            "source": mid,
                            "target": pub["publication_id"],
                            "type": "published",
                            "properties": {"source": "neo4j"},
                        }
                    )
                    edge_ids_seen.add(eid)

        # enrich main works (authors, magazines)
        for work in main_works:
            # authors
            all_creators: List[str] = []
            for c in work["creators"]:
                if c:
                    all_creators.extend(normalize_and_split_creators(c))
            for creator in all_creators:
                if not creator:
                    continue
                aid = generate_normalized_id(creator, "author")
                if aid not in node_ids_seen:
                    nodes.append(
                        {
                            "id": aid,
                            "label": creator,
                            "type": "author",
                            "properties": {"source": "neo4j", "name": creator},
                        }
                    )
                    node_ids_seen.add(aid)
                eid = f"{aid}-created-{work['work_id']}"
                if eid not in edge_ids_seen:
                    edges.append(
                        {
                            "id": eid,
                            "source": aid,
                            "target": work["work_id"],
                            "type": "created",
                            "properties": {"source": "neo4j"},
                        }
                    )
                    edge_ids_seen.add(eid)
            # magazines
            for m in work["magazines"]:
                if not m:
                    continue
                mid = generate_normalized_id(m, "magazine")
                if mid not in node_ids_seen:
                    nodes.append(
                        {
                            "id": mid,
                            "label": m,
                            "type": "magazine",
                            "properties": {"source": "neo4j", "name": m},
                        }
                    )
                    node_ids_seen.add(mid)
                eid = f"{mid}-published-{work['work_id']}"
                if eid not in edge_ids_seen:
                    edges.append(
                        {
                            "id": eid,
                            "source": mid,
                            "target": work["work_id"],
                            "type": "published",
                            "properties": {"source": "neo4j"},
                        }
                    )
                    edge_ids_seen.add(eid)
            # publishers fallback when no magazines
            if not work["magazines"]:
                for p in work["publishers"]:
                    if not p:
                        continue
                    np = normalize_publisher_name(p)
                    if not np:
                        continue
                    pid = generate_normalized_id(np, "publisher")
                    if pid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": pid,
                                "label": np,
                                "type": "publisher",
                                "properties": {"source": "neo4j", "name": np},
                            }
                        )
                        node_ids_seen.add(pid)
                    eid = f"{pid}-published-{work['work_id']}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": pid,
                                "target": work["work_id"],
                                "type": "published",
                                "properties": {"source": "neo4j"},
                            }
                        )
                        edge_ids_seen.add(eid)

        # include related
        if include_related and main_works:
            main_work = max(main_works, key=lambda w: len(w.get("magazines", []))) if main_works else main_works[0]
            main_work_id = main_work["work_id"]

            author_related = self.get_related_works_by_author(main_work_id, 5)
            if min_total_volumes is not None:
                author_related = [r for r in author_related if int(r.get("total_volumes") or 0) >= min_total_volumes]
            for r in author_related:
                if r["work_id"] not in node_ids_seen:
                    nodes.append(
                        {
                            "id": r["work_id"],
                            "label": r["title"],
                            "type": "work",
                            "properties": {**r, "source": "neo4j"},
                            "relevance_score": r.get("relevance_score", 500),
                        }
                    )
                    node_ids_seen.add(r["work_id"])
                norm_author = normalize_creator_name(r.get("author_name", ""))
                aid = generate_normalized_id(norm_author, "author")
                if aid in node_ids_seen:
                    eid = f"{aid}-created-{r['work_id']}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": aid,
                                "target": r["work_id"],
                                "type": "created",
                                "properties": {"source": "neo4j"},
                            }
                        )
                        edge_ids_seen.add(eid)

            period_related = self.get_related_works_by_magazine_and_period(main_work_id, 2, 50)
            if min_total_volumes is not None:
                period_related = [r for r in period_related if int(r.get("total_volumes") or 0) >= min_total_volumes]

            period_magazine_ids: set = set()
            related_work_ids: set = set()
            for r in period_related:
                if r["work_id"] not in node_ids_seen:
                    nodes.append(
                        {
                            "id": r["work_id"],
                            "label": r["title"],
                            "type": "work",
                            "properties": {**r, "source": "neo4j"},
                            "relevance_score": r.get("relevance_score", 1000),
                        }
                    )
                    node_ids_seen.add(r["work_id"])
                related_work_ids.add(r["work_id"])
                # authors
                all_creators: List[str] = []
                for c in r.get("creators", []) or []:
                    if c:
                        all_creators.extend(normalize_and_split_creators(c))
                for creator in all_creators:
                    if not creator:
                        continue
                    aid = generate_normalized_id(creator, "author")
                    if aid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": aid,
                                "label": creator,
                                "type": "author",
                                "properties": {"source": "neo4j", "name": creator},
                            }
                        )
                        node_ids_seen.add(aid)
                    eid = f"{aid}-created-{r['work_id']}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": aid,
                                "target": r["work_id"],
                                "type": "created",
                                "properties": {"source": "neo4j"},
                            }
                        )
                        edge_ids_seen.add(eid)
                # magazine
                if r.get("magazine_name"):
                    mid = generate_normalized_id(r["magazine_name"], "magazine")
                    if mid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": mid,
                                "label": r["magazine_name"],
                                "type": "magazine",
                                "properties": {"source": "neo4j", "name": r["magazine_name"]},
                            }
                        )
                        node_ids_seen.add(mid)
                    period_magazine_ids.add(mid)
                    eid = f"{mid}-published-{r['work_id']}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": mid,
                                "target": r["work_id"],
                                "type": "published",
                                "properties": {
                                    "source": "neo4j",
                                    "is_period_overlap": True,
                                    "description": "同時期の掲載誌",
                                },
                            }
                        )
                        edge_ids_seen.add(eid)

        # same publisher other magazines
        if include_same_publisher_other_magazines and main_works:
            main_publishers = set()
            main_magazines = set()
            for w in main_works:
                main_publishers.update([normalize_publisher_name(p) for p in w.get("publishers", []) if p])
                main_magazines.update([m for m in w.get("magazines", []) if m])

            query = """
            UNWIND $publisher_names AS pubName
            MATCH (p:Publisher)
            WHERE toLower(p.name) = toLower(pubName)
            MATCH (m:Magazine)-[:PUBLISHED_BY]->(p)
            WHERE NOT toLower(m.title) IN [x IN $main_magazines | toLower(x)]
            MATCH (w:Work)-[:PUBLISHED_IN]->(m)
            OPTIONAL MATCH (w)-[:CREATED_BY]->(a:Author)
            RETURN w.id AS work_id, w.title AS title,
                   w.first_published AS first_published, w.last_published AS last_published,
                   w.total_volumes AS total_volumes,
                   collect(DISTINCT a.name) AS creators,
                   m.title AS magazine_name,
                   p.name AS publisher_name
            LIMIT $limit
            """
            result = self._run(
                query,
                publisher_names=list(main_publishers),
                main_magazines=list(main_magazines),
                limit=same_publisher_other_magazines_limit or 5,
            )
            same_pub_others = [dict(r) for r in result]
            for r in same_pub_others:
                if min_total_volumes is not None and int(r.get("total_volumes") or 0) < min_total_volumes:
                    continue
                if r["work_id"] not in node_ids_seen:
                    nodes.append(
                        {
                            "id": r["work_id"],
                            "label": r["title"],
                            "type": "work",
                            "properties": {**r, "source": "neo4j", "relation": "same_publisher_other_magazine"},
                            "relevance_score": 750,
                        }
                    )
                    node_ids_seen.add(r["work_id"])
                for c in r.get("creators", []) or []:
                    for creator in normalize_and_split_creators(c):
                        if not creator:
                            continue
                        aid = generate_normalized_id(creator, "author")
                        if aid not in node_ids_seen:
                            nodes.append(
                                {
                                    "id": aid,
                                    "label": creator,
                                    "type": "author",
                                    "properties": {"source": "neo4j", "name": creator},
                                }
                            )
                            node_ids_seen.add(aid)
                        eid = f"{aid}-created-{r['work_id']}"
                        if eid not in edge_ids_seen:
                            edges.append(
                                {
                                    "id": eid,
                                    "source": aid,
                                    "target": r["work_id"],
                                    "type": "created",
                                    "properties": {"source": "neo4j"},
                                }
                            )
                            edge_ids_seen.add(eid)
                if r.get("magazine_name"):
                    mid = generate_normalized_id(r["magazine_name"], "magazine")
                    if mid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": mid,
                                "label": r["magazine_name"],
                                "type": "magazine",
                                "properties": {
                                    "source": "neo4j",
                                    "name": r["magazine_name"],
                                    "is_same_publisher_other_magazine": True,
                                },
                            }
                        )
                        node_ids_seen.add(mid)
                    eid = f"{mid}-published-{r['work_id']}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": mid,
                                "target": r["work_id"],
                                "type": "published",
                                "properties": {"source": "neo4j", "description": "同一出版社の別雑誌"},
                            }
                        )
                        edge_ids_seen.add(eid)

        # Add Publisher nodes and Magazine->Publisher edges based on actual DB relationships for included works
        try:
            work_ids_for_mapping = [n["id"] for n in nodes if n.get("type") == "work"]
            if work_ids_for_mapping:
                map_query = (
                    "UNWIND $work_ids AS wid\n"
                    "MATCH (w:Work {id: wid})-[:PUBLISHED_IN]->(m:Magazine)-[:PUBLISHED_BY]->(p:Publisher)\n"
                    "RETURN w.id AS work_id, m.title AS magazine_name, p.name AS publisher_name"
                )
                mp_result = self._run(map_query, work_ids=work_ids_for_mapping)
                for rec in mp_result:
                    mname = rec.get("magazine_name")
                    pname = rec.get("publisher_name")
                    if not mname or not pname:
                        continue
                    # Normalize and generate ids
                    npub = normalize_publisher_name(pname)
                    if not npub:
                        continue
                    mid = generate_normalized_id(mname, "magazine")
                    pid = generate_normalized_id(npub, "publisher")

                    # Ensure Magazine node exists
                    if mid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": mid,
                                "label": mname,
                                "type": "magazine",
                                "properties": {"source": "neo4j", "name": mname},
                            }
                        )
                        node_ids_seen.add(mid)

                    # Ensure Publisher node exists
                    if pid not in node_ids_seen:
                        nodes.append(
                            {
                                "id": pid,
                                "label": npub,
                                "type": "publisher",
                                "properties": {"source": "neo4j", "name": npub},
                            }
                        )
                        node_ids_seen.add(pid)

                    # Add magazine -> publisher edge
                    eid = f"{mid}-published_by-{pid}"
                    if eid not in edge_ids_seen:
                        edges.append(
                            {
                                "id": eid,
                                "source": mid,
                                "target": pid,
                                "type": "published_by",
                                "properties": {"source": "neo4j"},
                            }
                        )
                        edge_ids_seen.add(eid)
        except Exception as e:
            logger.error("Failed to add magazine->publisher mappings: %s", e)

        # Deduplicate nodes by work title preference and edges by key
        unique_nodes: List[Dict[str, Any]] = []
        seen_work_titles: Dict[str, Dict[str, Any]] = {}
        unique_node_ids = set()
        for n in nodes:
            if n["type"] == "work":
                title = n["label"]
                if title in seen_work_titles:
                    existing = seen_work_titles[title]
                    existing_score = existing.get("relevance_score", 0)
                    current_score = n.get("relevance_score", 0)
                    if current_score > existing_score:
                        unique_nodes = [x for x in unique_nodes if not (x["type"] == "work" and x["label"] == title)]
                        unique_nodes.append(n)
                        seen_work_titles[title] = n
                        unique_node_ids.add(n["id"])
                else:
                    seen_work_titles[title] = n
                    unique_nodes.append(n)
                    unique_node_ids.add(n["id"])
            else:
                if n["id"] not in unique_node_ids:
                    unique_nodes.append(n)
                    unique_node_ids.add(n["id"])

        valid_node_ids = {n["id"] for n in unique_nodes}
        unique_edges: List[Dict[str, Any]] = []
        edge_keys = set()
        for e in edges:
            s = e.get("source", e.get("from"))
            t = e.get("target", e.get("to"))
            if s in valid_node_ids and t in valid_node_ids:
                key = (s, t, e["type"])
                if key not in edge_keys:
                    unique_edges.append({**e, "source": s, "target": t})
                    edge_keys.add(key)

        # Order work nodes by total_volumes if requested
        work_nodes = [n for n in unique_nodes if n["type"] == "work"]
        other_nodes = [n for n in unique_nodes if n["type"] != "work"]

        def _extract_total_for_node(n: Dict[str, Any]) -> int:
            props = n.get("properties", n.get("data", {})) or {}
            tv = props.get("total_volumes") or props.get("work_count") or 0
            try:
                return int(tv)
            except Exception:
                import re

                m = re.search(r"(\d+)", str(tv))
                return int(m.group(1)) if m else 0

        if sort_total_volumes in ("asc", "desc"):
            work_nodes.sort(key=_extract_total_for_node, reverse=sort_total_volumes == "desc")
        else:
            work_nodes.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        unique_nodes = work_nodes + other_nodes

        # Apply limit to work nodes only, and keep all neighbor nodes connected to those works
        if limit and isinstance(limit, int):
            top_works = work_nodes[:limit]
            kept_ids = {n["id"] for n in top_works}

            # Include neighbor nodes that are connected to the kept works via edges
            for e in unique_edges:
                s = e.get("source", e.get("from"))
                t = e.get("target", e.get("to"))
                if s in kept_ids or t in kept_ids:
                    kept_ids.add(s)
                    kept_ids.add(t)

            # Filter nodes and edges to those within the kept id set
            unique_nodes = [n for n in unique_nodes if n["id"] in kept_ids]
            unique_edges = [
                e
                for e in unique_edges
                if (e.get("source", e.get("from")) in kept_ids and e.get("target", e.get("to")) in kept_ids)
            ]

        return {"nodes": unique_nodes, "edges": unique_edges}

    # ---------------------- Admin utilities ----------------------
    def get_database_statistics(self) -> Dict[str, int]:
        try:
            stats: Dict[str, int] = {}
            with self.driver.session() as session:
                for label in ["Work", "Author", "Publisher", "Magazine"]:
                    r = session.run(f"MATCH (n:{label}) RETURN count(n) AS c")
                    stats[f"{label.lower()}_count"] = r.single()["c"]
                for rel in ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]:
                    r = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c")
                    stats[f"{rel.lower()}_relationships"] = r.single()["c"]
            return stats
        except Exception as e:
            logger.error("Error getting database statistics: %s", e)
            return {}

    def get_work_by_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            OPTIONAL MATCH (w)-[:CREATED_BY]->(a:Author)
            OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(m:Magazine)-[:PUBLISHED_BY]->(p:Publisher)
            RETURN w,
                   collect(DISTINCT a.name) as authors,
                   collect(DISTINCT p.name) as publishers
            """
            rec = session.run(query, work_id=work_id).single()
            if not rec:
                return None
            w = rec["w"]
            return {
                "id": w["id"],
                "title": w.get("title", ""),
                "isbn": w.get("isbn", ""),
                "genre": w.get("genre", ""),
                "published_date": w.get("published_date", ""),
                "cover_image_url": w.get("cover_image_url", ""),
                "publisher": (rec["publishers"] or [""])[0],
                "authors": rec["authors"] or [],
            }

    def update_work_cover_image(self, work_id: str, cover_url: str) -> bool:
        with self.driver.session() as session:
            r = session.run(
                """
                MATCH (w:Work {id: $work_id})
                SET w.cover_image_url = $cover_url
                RETURN w.id AS updated_id
                """,
                work_id=work_id,
                cover_url=cover_url,
            ).single()
            return r is not None

    def get_works_needing_covers(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.driver.session() as session:
            r = session.run(
                """
                MATCH (w:Work)
                WHERE w.isbn IS NOT NULL AND w.isbn <> '' AND (w.cover_image_url IS NULL OR w.cover_image_url = '')
                RETURN w.id as id, w.title as title, w.isbn as isbn
                LIMIT $limit
                """,
                limit=limit,
            )
            return [{"id": rec["id"], "title": rec["title"], "isbn": rec["isbn"]} for rec in r]

    # ---------------------- Vector search utilities ----------------------
    def create_vector_index(
        self,
        label: str,
        property_name: str = "embedding",
        dimension: int = 1536,
        similarity: str = "cosine",
    ) -> None:
        """Create a vector index if it doesn't exist."""
        index_name = f"idx_{label.lower()}_{property_name}_vector"
        session = self.driver.session()
        try:
            check_query = """
                CALL db.indexes() YIELD name
                WITH name WHERE name = $indexName
                RETURN count(*) AS count
                """
            rec = session.run(check_query, indexName=index_name).single()
            exists = (rec or {}).get("count", 0) > 0
            if exists:
                logger.info("Vector index already exists: %s", index_name)
                return

            create_query = (
                "CALL db.index.vector.createNodeIndex($indexName, $label, $property_name, $dimension, "
                "{similarityFunction: $similarity})"
            )
            session.run(
                create_query,
                indexName=index_name,
                label=label,
                property_name=property_name,
                dimension=int(dimension),
                similarity=similarity,
            )
            logger.info("Created vector index: %s", index_name)
        except Exception as e:
            logger.error("Failed to create vector index: %s", e)

    def search_by_vector(
        self, embedding: List[float], label: str = "Work", property_name: str = "embedding", limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Search nodes by vector similarity"""
        logger.info(f"Searching by vector similarity for {label} nodes, limit: {limit}")
        with self.driver.session() as session:
            query = """
            CALL db.index.vector.queryNodes($index_name, $limit, $embedding)
            YIELD node, score
            OPTIONAL MATCH (node)-[:CREATED_BY]->(a:Author)
            OPTIONAL MATCH (node)-[:PUBLISHED_IN]->(m:Magazine)
            OPTIONAL MATCH (m)-[:PUBLISHED_BY]->(p:Publisher)
            OPTIONAL MATCH (s:Series)-[:CONTAINS]->(node)
            RETURN node.id as work_id, node.title as title, node.published_date as published_date,
                   node.first_published as first_published, node.last_published as last_published,
                   collect(DISTINCT a.name) as creators,
                   collect(DISTINCT p.name) as publishers,
                   collect(DISTINCT m.title) as magazines,
                   node.genre as genre, node.isbn as isbn, node.volume as volume,
                   s.id as series_id, s.name as series_name,
                   score
            ORDER BY score DESC
            """

            try:
                index_name = f"{label}_{property_name}_vector_index"
                result = session.run(query, index_name=index_name, limit=limit, embedding=embedding)
                # result = session.run(query, limit=limit, embedding=embedding)
                works = []
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
                        "similarity_score": record["score"],
                    }
                    works.append(work)

                logger.info(f"Found {len(works)} works by vector similarity")
                return works

            except Exception as e:
                logger.error(f"Vector search failed: {e}")
                return []

    def add_embedding_to_work(self, work_id: str, embedding: List[float]) -> bool:
        """Attach an embedding vector to a Work node."""
        session = self.driver.session()
        try:
            r = session.run(
                """
                    MATCH (w:Work {id: $work_id})
                    SET w.embedding = $embedding
                    RETURN w.id AS work_id
                    """,
                work_id=work_id,
                embedding=embedding,
            ).single()
            return r is not None
        except Exception as e:
            logger.error("Failed to add embedding to work %s: %s", work_id, e)
            return False

    def search_manga_works_with_vector(
        self,
        search_term: Optional[str] = None,
        embedding: Optional[List[float]] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Hybrid search combining text and vector results."""
        results: List[Dict[str, Any]] = []

        if search_term and embedding:
            half = max(1, int(limit) // 2)
            text_results = self.search_manga_works(search_term, half)
            vector_results = self.search_by_vector(embedding, limit=half)

            vec_by_id = {v.get("work_id"): v for v in vector_results}

            for t in text_results:
                rid = t.get("work_id")
                item = {**t}
                if rid in vec_by_id:
                    base = vec_by_id[rid].get("similarity_score") or 0.0
                    item["search_score"] = float(base) + 0.1
                else:
                    item["search_score"] = 0.6
                results.append(item)

            existing_ids = {r.get("work_id") for r in results}
            for v in vector_results:
                if v.get("work_id") not in existing_ids:
                    v = {**v}
                    v["search_score"] = v.get("similarity_score")
                    results.append(v)

            return results[:limit]

        if embedding and not search_term:
            vector_results = self.search_by_vector(embedding, limit=limit)
            for v in vector_results:
                v["search_score"] = v.get("similarity_score")
            return vector_results

        if search_term and not embedding:
            return self.search_manga_works(search_term, limit)

        return []

    def search_work_synopsis_by_vector(self, embedding: List[float], limit: int = 10) -> List[Dict[str, Any]]:
        """Vector search over Work.synopsis_embedding without using vector indexes."""
        session = self.driver.session()
        try:
            query = """
          MATCH (node:Work)
          WHERE node.synopsis_embedding IS NOT NULL AND size(node.synopsis_embedding) > 0
          WITH node, node.synopsis_embedding AS v1, $embedding AS v2,
              CASE WHEN size(node.synopsis_embedding) < size($embedding)
                  THEN size(node.synopsis_embedding) ELSE size($embedding) END AS n
          WITH node, v1, v2, n,
              reduce(dot = 0.0, i IN range(0, n-1) | dot + coalesce(v1[i], 0.0) * coalesce(v2[i], 0.0)) AS dot,
              sqrt(reduce(s1 = 0.0, i IN range(0, n-1) | s1 + coalesce(v1[i], 0.0)^2)) AS norm1,
              sqrt(reduce(s2 = 0.0, i IN range(0, n-1) | s2 + coalesce(v2[i], 0.0)^2)) AS norm2
          WITH node, CASE WHEN norm1 = 0 OR norm2 = 0 THEN 0.0 ELSE dot / (norm1 * norm2) END AS score
          ORDER BY score DESC
          LIMIT $limit
          RETURN node.id AS work_id,
                node.title AS title,
                node.synopsis AS synopsis,
                score AS score
            """
            result = session.run(query, embedding=embedding, limit=int(limit))
            out: List[Dict[str, Any]] = []
            for record in result:
                d = dict(record)
                d["similarity_score"] = d.pop("score", None)
                out.append(d)
            return out
        except Exception as e:
            logger.error("Synopsis vector search (scan) failed: %s", e)
            return []

    def search_work_titles_by_vector_minimal(
        self, embedding: List[float], limit: int = 10, similarity_threshold: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Vector search over Work.embedding returning only title and similarity.

        This uses a label scan and computes cosine similarity in pure Cypher
        without relying on vector indexes or procedures. It returns only the
        minimal fields to reduce latency.
        """
        with self.driver.session() as session:
            query = (
                "CALL db.index.vector.queryNodes($index_name, $limit, $embedding) "
                "YIELD node, score "
                "WITH node, score WHERE score >= $threshold "
                "RETURN node.title AS title, score AS score "
                "ORDER BY score DESC "
                "LIMIT $limit"
            )
            try:
                index_name = "Work_embedding_vector_index"
                result = session.run(
                    query,
                    index_name=index_name,
                    limit=int(limit),
                    embedding=embedding,
                    threshold=float(similarity_threshold),
                )
                out: List[Dict[str, Any]] = []
                for record in result:
                    out.append({"title": record["title"], "similarity_score": record.get("score")})
                return out
            except Exception as e:
                logger.error("Title vector index search failed: %s", e)
                return []
