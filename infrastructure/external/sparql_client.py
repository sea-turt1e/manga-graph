import logging
import time
from typing import Any, Dict, List, Optional

from requests.exceptions import RequestException, Timeout
from SPARQLWrapper import JSON, SPARQLWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MediaArtsSPARQLClient:
    """文化庁メディア芸術データベースSPARQLクライアント"""

    def __init__(self, endpoint_url: str = "https://mediaarts-db.artmuseums.go.jp/sparql"):
        self.endpoint_url = endpoint_url
        self.sparql = SPARQLWrapper(endpoint_url)
        self.sparql.setReturnFormat(JSON)
        self.rate_limit_delay = 1.0  # 秒間隔でレート制限

    def execute_query(self, query: str, timeout: int = 55) -> Optional[Dict[str, Any]]:
        """
        SPARQLクエリを実行

        Args:
            query: SPARQLクエリ文字列
            timeout: タイムアウト時間（秒）

        Returns:
            クエリ結果のJSON、失敗時はNone
        """
        try:
            self.sparql.setQuery(query)
            self.sparql.setTimeout(timeout)

            logger.info(f"Executing SPARQL query: {query[:100]}...")

            # レート制限のためのディレイ
            time.sleep(self.rate_limit_delay)

            results = self.sparql.queryAndConvert()
            logger.info(
                f"Query executed successfully, got {len(results.get('results', {}).get('bindings', []))} results"
            )

            return results

        except Timeout:
            logger.error(f"Query timeout after {timeout} seconds")
            return None
        except RequestException as e:
            logger.error(f"Request error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            return None

    def search_manga_works(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        漫画作品を検索

        Args:
            search_term: 検索語
            limit: 結果の上限

        Returns:
            作品データのリスト
        """
        query = f"""
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/data/class#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?work ?title ?creator ?creatorName ?genre ?publisher ?publishedDate
        WHERE {{
            ?work a madb:MangaBook .
            ?work schema:name ?title .

            FILTER(
                CONTAINS(LCASE(?title), LCASE("{search_term}"))
            )

            OPTIONAL {{
                ?work schema:creator ?creatorName .
            }}

            OPTIONAL {{
                ?work dcterms:creator ?creator .
            }}

            OPTIONAL {{
                ?work schema:genre ?genre .
            }}

            OPTIONAL {{
                ?work schema:publisher ?publisher .
            }}

            OPTIONAL {{
                ?work schema:datePublished ?publishedDate .
            }}
        }}
        ORDER BY ?title
        LIMIT {limit}
        """

        results = self.execute_query(query)
        if not results:
            return []

        works = []
        seen_works = set()

        for binding in results.get("results", {}).get("bindings", []):
            work_uri = binding.get("work", {}).get("value", "")

            # 重複する作品を除外
            if work_uri in seen_works:
                continue
            seen_works.add(work_uri)

            work_data = {
                "uri": work_uri,
                "title": binding.get("title", {}).get("value", ""),
                "creator_uri": binding.get("creator", {}).get("value", ""),
                "creator_name": binding.get("creatorName", {}).get("value", ""),
                "genre": binding.get("genre", {}).get("value", ""),
                "publisher": binding.get("publisher", {}).get("value", ""),
                "published_date": binding.get("publishedDate", {}).get("value", ""),
            }
            works.append(work_data)

        return works

    def get_manga_by_creator(self, creator_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        作者名で漫画作品を検索

        Args:
            creator_name: 作者名
            limit: 結果の上限

        Returns:
            作品データのリスト
        """
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?work ?title ?creator ?creatorName ?genre ?publisher ?publishedDate
        WHERE {{
            ?creator rdfs:label ?creatorName .
            FILTER(CONTAINS(LCASE(?creatorName), LCASE("{creator_name}")))

            ?work schema:creator ?creator ;
                  rdfs:label ?title ;
                  schema:genre ?genreUri .

            ?genreUri rdfs:label ?genre .
            FILTER(CONTAINS(LCASE(?genre), "漫画") || CONTAINS(LCASE(?genre), "マンガ") || CONTAINS(LCASE(?genre), "comic"))

            OPTIONAL {{
                ?work schema:publisher ?publisherUri .
                ?publisherUri rdfs:label ?publisher .
            }}

            OPTIONAL {{
                ?work schema:datePublished ?publishedDate .
            }}
        }}
        ORDER BY ?publishedDate ?title
        LIMIT {limit}
        """

        results = self.execute_query(query)
        if not results:
            return []

        works = []
        for binding in results.get("results", {}).get("bindings", []):
            work_data = {
                "uri": binding.get("work", {}).get("value", ""),
                "title": binding.get("title", {}).get("value", ""),
                "creator_uri": binding.get("creator", {}).get("value", ""),
                "creator_name": binding.get("creatorName", {}).get("value", ""),
                "genre": binding.get("genre", {}).get("value", ""),
                "publisher": binding.get("publisher", {}).get("value", ""),
                "published_date": binding.get("publishedDate", {}).get("value", ""),
            }
            works.append(work_data)

        return works

    def get_manga_magazines(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        漫画雑誌の情報を取得

        Args:
            limit: 結果の上限

        Returns:
            雑誌データのリスト
        """
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?magazine ?title ?publisher ?publisherName ?genre
        WHERE {{
            ?magazine a schema:Periodical ;
                      rdfs:label ?title ;
                      schema:genre ?genreUri .

            ?genreUri rdfs:label ?genre .
            FILTER(CONTAINS(LCASE(?genre), "漫画") || CONTAINS(LCASE(?genre), "マンガ") || CONTAINS(LCASE(?genre), "comic"))

            OPTIONAL {{
                ?magazine schema:publisher ?publisher .
                ?publisher rdfs:label ?publisherName .
            }}
        }}
        ORDER BY ?title
        LIMIT {limit}
        """

        results = self.execute_query(query)
        if not results:
            return []

        magazines = []
        for binding in results.get("results", {}).get("bindings", []):
            magazine_data = {
                "uri": binding.get("magazine", {}).get("value", ""),
                "title": binding.get("title", {}).get("value", ""),
                "publisher_uri": binding.get("publisher", {}).get("value", ""),
                "publisher_name": binding.get("publisherName", {}).get("value", ""),
                "genre": binding.get("genre", {}).get("value", ""),
            }
            magazines.append(magazine_data)

        return magazines

    def search_with_fulltext(
        self, search_term: str, search_type: str = "simple_query_string", limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        全文検索を使用した検索

        Args:
            search_term: 検索語
            search_type: 検索タイプ（simple_query_string, match, prefix, fuzzy, term, query_string）
            limit: 結果の上限

        Returns:
            検索結果のリスト
        """
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX neptune-fts: <http://aws.amazon.com/neptune/vocab/v01/QueryLanguage#>

        SELECT DISTINCT ?resource ?title ?type
        WHERE {{
            SERVICE neptune-fts:search {{
                neptune-fts:config neptune-fts:endpoint "{search_type}" .
                neptune-fts:config neptune-fts:queryType "{search_type}" .
                neptune-fts:config neptune-fts:query "{search_term}" .
                neptune-fts:config neptune-fts:limit {limit} .
                ?resource neptune-fts:score ?score .
            }}

            ?resource rdfs:label ?title .
            OPTIONAL {{ ?resource a ?type }}
        }}
        ORDER BY DESC(?score)
        """

        results = self.execute_query(query)
        if not results:
            return []

        resources = []
        for binding in results.get("results", {}).get("bindings", []):
            resource_data = {
                "uri": binding.get("resource", {}).get("value", ""),
                "title": binding.get("title", {}).get("value", ""),
                "type": binding.get("type", {}).get("value", ""),
            }
            resources.append(resource_data)

        return resources

    def get_related_works_by_overlap_period(self, reference_series: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        連載期間の重複が長い関連作品を取得

        Args:
            reference_series: 基準となる作品名
            limit: 結果の上限

        Returns:
            重複期間でソートされた関連作品のリスト
        """
        # まず基準作品の期間を推定
        ref_works = self.search_manga_works(reference_series, 20)
        if not ref_works:
            return []

        ref_years = []
        for work in ref_works:
            date_str = work.get("published_date", "")
            if date_str and len(date_str) >= 4 and date_str[:4].isdigit():
                ref_years.append(int(date_str[:4]))

        if not ref_years:
            return []

        ref_start = min(ref_years) - 1  # 連載開始を単行本より1年前と推定
        ref_end = max(ref_years)

        # 集英社の作品を取得するクエリ
        query = """
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/data/class#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?work ?title ?creator ?creatorName ?genre ?publisher ?publishedDate
        WHERE {{
            ?work a madb:MangaBook .
            ?work schema:name ?title .
            ?work schema:publisher ?publisher .
            ?work schema:datePublished ?publishedDate .

<<<<<<< HEAD
            FILTER(CONTAINS(LCASE(?publisher), "集英社"))

            OPTIONAL {{
                ?work schema:creator ?creatorName .
            }}

            OPTIONAL {{
                ?work dcterms:creator ?creator .
            }}

            OPTIONAL {{
                ?work schema:genre ?genre .
            }}
        }}
        ORDER BY ?title ?publishedDate
        LIMIT 100
        """

        results = self.execute_query(query)
        if not results:
            return []

        related_works = []
        for binding in results.get("results", {}).get("bindings", []):
            work_data = {
                "uri": binding.get("work", {}).get("value", ""),
                "title": binding.get("title", {}).get("value", ""),
                "creator_uri": binding.get("creator", {}).get("value", ""),
                "creator_name": binding.get("creatorName", {}).get("value", ""),
                "genre": binding.get("genre", {}).get("value", ""),
                "publisher": binding.get("publisher", {}).get("value", ""),
                "published_date": binding.get("publishedDate", {}).get("value", ""),
            }
            related_works.append(work_data)

        # タイトルごとに作品をグループ化し重複期間を計算
        series_data = {}
        for work in related_works:
            title = work.get("title", "").strip()
            date_str = work.get("published_date", "")

            # 基準作品を除外
            if reference_series.lower() in title.lower():
                continue

            base_title = self._extract_base_title(title)

            if base_title not in series_data:
                series_data[base_title] = {"work_data": work, "dates": []}

            if date_str and len(date_str) >= 4 and date_str[:4].isdigit():
                series_data[base_title]["dates"].append(int(date_str[:4]))

        # 重複期間を計算してソート
        overlap_scores = []
        for base_title, data in series_data.items():
            if not data["dates"]:
                continue

            start_year = min(data["dates"]) - 1
            end_year = max(data["dates"])

            # 重複期間を計算
            overlap_start = max(ref_start, start_year)
            overlap_end = min(ref_end, end_year)
            overlap_years = max(0, overlap_end - overlap_start + 1)

            if overlap_years > 0:
                overlap_scores.append(
                    {"work_data": data["work_data"], "overlap_years": overlap_years, "volume_count": len(data["dates"])}
                )

        # 重複期間の長さでソート（降順）
        overlap_scores.sort(key=lambda x: (-x["overlap_years"], -x["volume_count"]))

        return [item["work_data"] for item in overlap_scores[:limit]]

    def _extract_base_title(self, title: str) -> str:
        """
        タイトルからベースタイトルを抽出（巻数などを除去）
        """
        import re

        patterns = [
            r"\s*\d+$",  # 末尾の数字
            r"\s*第\d+巻?$",  # 第X巻
            r"\s*\(\d+\)$",  # (数字)
        ]

        base = title
        for pattern in patterns:
            base = re.sub(pattern, "", base, flags=re.IGNORECASE)

        return base.strip()

    def get_manga_works_by_magazine_period(self, magazine_name: str = None, year: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        同じ掲載誌・同じ時期の漫画作品を取得

        Args:
            magazine_name: 雑誌名（部分一致）
            year: 出版年
            limit: 結果の上限

        Returns:
            作品データのリスト
        """
        # 基本クエリ
        query_parts = [
            "PREFIX schema: <https://schema.org/>",
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
            "PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/data/class#>",
            "PREFIX dcterms: <http://purl.org/dc/terms/>",
            "",
            "SELECT DISTINCT ?work ?title ?creator ?creatorName ?magazine ?magazineName ?publishedDate ?publisher",
            "WHERE {"
        ]

        # 作品の基本情報
        query_parts.extend([
            "    ?work a madb:MangaBook ;",
            "          schema:name ?title .",
            "",
            "    OPTIONAL {",
            "        ?work schema:creator ?creatorName .",
            "    }",
            "",
            "    OPTIONAL {",
            "        ?work dcterms:creator ?creator .",
            "    }",
            "",
            "    OPTIONAL {",
            "        ?work schema:isPartOf ?magazine .",
            "        ?magazine rdfs:label ?magazineName .",
            "    }",
            "",
            "    OPTIONAL {",
            "        ?work schema:datePublished ?publishedDate .",
            "    }",
            "",
            "    OPTIONAL {",
            "        ?work schema:publisher ?publisher .",
            "    }"
        ])

        # フィルター条件
        filters = []

        if magazine_name:
            filters.append(f'CONTAINS(LCASE(?magazineName), LCASE("{magazine_name}"))')

        if year:
            filters.append(f'CONTAINS(?publishedDate, "{year}")')

        if filters:
            query_parts.extend([
                "",
                "    FILTER(" + " && ".join(filters) + ")"
            ])

        query_parts.extend([
            "}",
            "ORDER BY ?magazineName ?publishedDate ?title",
            f"LIMIT {limit}"
        ])

        query = "\n".join(query_parts)

        results = self.execute_query(query)
        if not results:
            return []

        works = []
        for binding in results.get('results', {}).get('bindings', []):
            work_data = {
                'uri': binding.get('work', {}).get('value', ''),
                'title': binding.get('title', {}).get('value', ''),
                'creator_uri': binding.get('creator', {}).get('value', ''),
                'creator_name': binding.get('creatorName', {}).get('value', ''),
                'magazine_uri': binding.get('magazine', {}).get('value', ''),
                'magazine_name': binding.get('magazineName', {}).get('value', ''),
                'published_date': binding.get('publishedDate', {}).get('value', ''),
                'publisher': binding.get('publisher', {}).get('value', '')
            }
            works.append(work_data)

        return works
