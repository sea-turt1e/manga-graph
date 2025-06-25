import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from domain.entities import Author, GraphEdge, GraphNode, Magazine, Work
from infrastructure.external import MediaArtsSPARQLClient

logger = logging.getLogger(__name__)


class MediaArtsDataService:
    """文化庁メディア芸術データベースからのデータ収集サービス"""

    def __init__(self, sparql_client: Optional[MediaArtsSPARQLClient] = None):
        self.sparql_client = sparql_client or MediaArtsSPARQLClient()

    def search_manga_data(self, search_term: str, limit: int = 20) -> Dict[str, List]:
        """
        漫画データを検索してグラフ形式で返す

        Args:
            search_term: 検索語
            limit: 結果の上限

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            # 作品データを取得
            works_data = self.sparql_client.search_manga_works(search_term, limit)

            nodes = []
            edges = []
            processed_uris = set()
            processed_titles = set()
            processed_works = {}  # タイトル → 作品データのマッピング

            for work_data in works_data:
                work_uri = work_data.get("uri", "")
                creator_uri = work_data.get("creator_uri", "")
                title = work_data.get("title", "").strip()

                # 同じタイトルの作品は最初の1つだけを記録（重複削除）
                if title and title not in processed_titles:
                    processed_titles.add(title)
                    processed_works[title] = work_data

                    work_node = {
                        "id": work_uri,
                        "label": title or "Unknown Work",
                        "type": "work",
                        "properties": {
                            "title": title,
                            "genre": work_data.get("genre", ""),
                            "publisher": work_data.get("publisher", ""),
                            "published_date": work_data.get("published_date", ""),
                            "source": "media_arts_db",
                        },
                    }
                    nodes.append(work_node)
                    processed_uris.add(work_uri)

                    # 作者ノードを追加
                    if creator_uri and creator_uri not in processed_uris:
                        creator_node = {
                            "id": creator_uri,
                            "label": work_data.get("creator_name", "Unknown Creator"),
                            "type": "author",
                            "properties": {"name": work_data.get("creator_name", ""), "source": "media_arts_db"},
                        }
                        nodes.append(creator_node)
                        processed_uris.add(creator_uri)

                    # 作者と作品の関係を追加
                    if work_uri and creator_uri:
                        edge = {
                            "id": f"{creator_uri}-created-{work_uri}",
                            "source": creator_uri,
                            "target": work_uri,
                            "type": "created",
                            "properties": {"source": "media_arts_db"},
                        }
                        edges.append(edge)

            # 掲載誌情報を取得して同じ掲載誌・同じ年の漫画同士を関連付け
            works_with_magazine_info = self.sparql_client.get_manga_works_by_magazine_period(limit=100)
            
            # 処理された作品のURIセット
            processed_work_uris = {work_data.get("uri", "") for work_data in works_data if work_data.get("uri", "")}
            
            # 掲載誌・年でグループ化
            magazine_year_groups = {}
            for work_info in works_with_magazine_info:
                work_uri = work_info.get("uri", "")
                magazine_name = work_info.get("magazine_name", "")
                published_date = work_info.get("published_date", "")
                
                # 検索結果に含まれる作品のみを対象とする
                if work_uri in processed_work_uris and magazine_name and published_date:
                    year = published_date[:4] if len(published_date) >= 4 else ""
                    if year:
                        group_key = f"{magazine_name}_{year}"
                        if group_key not in magazine_year_groups:
                            magazine_year_groups[group_key] = []
                        magazine_year_groups[group_key].append(work_info)

            # 同じ掲載誌・同じ年の作品同士を関連付け
            edge_ids = set()
            for group_key, group_works in magazine_year_groups.items():
                if len(group_works) > 1:
                    magazine_name, year = group_key.rsplit("_", 1)
                    for i, work1 in enumerate(group_works):
                        for work2 in group_works[i + 1:]:
                            work1_uri = work1.get("uri", "")
                            work2_uri = work2.get("uri", "")
                            
                            if work1_uri and work2_uri:
                                # 双方向エッジのIDを統一
                                edge_id1 = f"{work1_uri}-same_magazine_year-{work2_uri}"
                                edge_id2 = f"{work2_uri}-same_magazine_year-{work1_uri}"
                                canonical_edge_id = min(edge_id1, edge_id2)
                                
                                if canonical_edge_id not in edge_ids:
                                    edge = {
                                        "id": canonical_edge_id,
                                        "source": work1_uri,
                                        "target": work2_uri,
                                        "type": "same_magazine_year",
                                        "properties": {
                                            "magazine": magazine_name,
                                            "year": year,
                                            "source": "media_arts_db",
                                        },
                                    }
                                    edges.append(edge)
                                    edge_ids.add(canonical_edge_id)

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            logger.error(f"Error searching manga data: {e}")
            return {"nodes": [], "edges": []}

    def get_creator_works(self, creator_name: str, limit: int = 50) -> Dict[str, List]:
        """
        作者の作品リストを取得してグラフ形式で返す

        Args:
            creator_name: 作者名
            limit: 結果の上限

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            works_data = self.sparql_client.get_manga_by_creator(creator_name, limit)

            nodes = []
            edges = []
            processed_uris = set()
            creator_uri = None

            for work_data in works_data:
                work_uri = work_data.get("uri", "")
                current_creator_uri = work_data.get("creator_uri", "")

                # 作者ノードを追加（最初の一回のみ）
                if current_creator_uri and current_creator_uri not in processed_uris:
                    creator_uri = current_creator_uri
                    creator_node = {
                        "id": creator_uri,
                        "label": work_data.get("creator_name", "Unknown Creator"),
                        "type": "author",
                        "properties": {"name": work_data.get("creator_name", ""), "source": "media_arts_db"},
                    }
                    nodes.append(creator_node)
                    processed_uris.add(creator_uri)

                # 作品ノードを追加
                if work_uri and work_uri not in processed_uris:
                    work_node = {
                        "id": work_uri,
                        "label": work_data.get("title", "Unknown Work"),
                        "type": "work",
                        "properties": {
                            "title": work_data.get("title", ""),
                            "genre": work_data.get("genre", ""),
                            "publisher": work_data.get("publisher", ""),
                            "published_date": work_data.get("published_date", ""),
                            "source": "media_arts_db",
                        },
                    }
                    nodes.append(work_node)
                    processed_uris.add(work_uri)

                # 作者と作品の関係を追加
                if creator_uri and work_uri:
                    edge = {
                        "id": f"{creator_uri}-created-{work_uri}",
                        "source": creator_uri,
                        "target": work_uri,
                        "type": "created",
                        "properties": {"source": "media_arts_db"},
                    }
                    edges.append(edge)

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            logger.error(f"Error getting creator works: {e}")
            return {"nodes": [], "edges": []}

    def get_manga_magazines_graph(self, limit: int = 100) -> Dict[str, List]:
        """
        漫画雑誌データを取得してグラフ形式で返す

        Args:
            limit: 結果の上限

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            magazines_data = self.sparql_client.get_manga_magazines(limit)

            nodes = []
            edges = []
            processed_uris = set()

            for magazine_data in magazines_data:
                magazine_uri = magazine_data.get("uri", "")
                publisher_uri = magazine_data.get("publisher_uri", "")

                # 雑誌ノードを追加
                if magazine_uri and magazine_uri not in processed_uris:
                    magazine_node = {
                        "id": magazine_uri,
                        "label": magazine_data.get("title", "Unknown Magazine"),
                        "type": "magazine",
                        "properties": {
                            "title": magazine_data.get("title", ""),
                            "genre": magazine_data.get("genre", ""),
                            "source": "media_arts_db",
                        },
                    }
                    nodes.append(magazine_node)
                    processed_uris.add(magazine_uri)

                # 出版社ノードを追加
                if publisher_uri and publisher_uri not in processed_uris:
                    publisher_node = {
                        "id": publisher_uri,
                        "label": magazine_data.get("publisher_name", "Unknown Publisher"),
                        "type": "publisher",
                        "properties": {"name": magazine_data.get("publisher_name", ""), "source": "media_arts_db"},
                    }
                    nodes.append(publisher_node)
                    processed_uris.add(publisher_uri)

                # 出版社と雑誌の関係を追加
                if publisher_uri and magazine_uri:
                    edge = {
                        "id": f"{publisher_uri}-publishes-{magazine_uri}",
                        "source": publisher_uri,
                        "target": magazine_uri,
                        "type": "publishes",
                        "properties": {"source": "media_arts_db"},
                    }
                    edges.append(edge)

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            logger.error(f"Error getting magazine data: {e}")
            return {"nodes": [], "edges": []}

    def search_with_fulltext(
        self, search_term: str, search_type: str = "simple_query_string", limit: int = 20
    ) -> Dict[str, List]:
        """
        全文検索を使用してデータを検索

        Args:
            search_term: 検索語
            search_type: 検索タイプ
            limit: 結果の上限

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            results = self.sparql_client.search_with_fulltext(search_term, search_type, limit)

            nodes = []
            processed_uris = set()

            for result in results:
                uri = result.get("uri", "")
                if uri and uri not in processed_uris:
                    # URIから推測されるタイプを決定
                    node_type = "unknown"
                    if "creator" in uri.lower() or "author" in uri.lower():
                        node_type = "author"
                    elif "work" in uri.lower():
                        node_type = "work"
                    elif "magazine" in uri.lower() or "periodical" in uri.lower():
                        node_type = "magazine"

                    node = {
                        "id": uri,
                        "label": result.get("title", "Unknown"),
                        "type": node_type,
                        "properties": {
                            "title": result.get("title", ""),
                            "resource_type": result.get("type", ""),
                            "source": "media_arts_db",
                        },
                    }
                    nodes.append(node)
                    processed_uris.add(uri)

            return {"nodes": nodes, "edges": []}  # 全文検索では関係性情報が限定的

        except Exception as e:
            logger.error(f"Error in fulltext search: {e}")
            return {"nodes": [], "edges": []}

    def get_magazine_relationships(self, magazine_name: str = None, year: str = None, limit: int = 50) -> Dict[str, List]:
        """
        同じ掲載誌・同じ時期の漫画作品とその関係性を取得してグラフ形式で返す

        Args:
            magazine_name: 雑誌名（部分一致）
            year: 出版年
            limit: 結果の上限

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            works_data = self.sparql_client.get_manga_works_by_magazine_period(
                magazine_name=magazine_name, year=year, limit=limit
            )

            nodes = []
            edges = []
            processed_uris = set()
            magazine_groups = {}  # 雑誌名 → 作品リストのマッピング

            for work_data in works_data:
                work_uri = work_data.get("uri", "")
                creator_uri = work_data.get("creator_uri", "")
                magazine_uri = work_data.get("magazine_uri", "")
                magazine_name_data = work_data.get("magazine_name", "")

                # 雑誌ごとに作品をグループ化
                if magazine_name_data and work_uri:
                    if magazine_name_data not in magazine_groups:
                        magazine_groups[magazine_name_data] = []
                    magazine_groups[magazine_name_data].append(work_data)

                # 作品ノードを追加
                if work_uri and work_uri not in processed_uris:
                    work_node = {
                        "id": work_uri,
                        "label": work_data.get("title", "Unknown Work"),
                        "type": "work",
                        "properties": {
                            "title": work_data.get("title", ""),
                            "published_date": work_data.get("published_date", ""),
                            "magazine_name": magazine_name_data,
                            "source": "media_arts_db",
                        },
                    }
                    nodes.append(work_node)
                    processed_uris.add(work_uri)

                # 作者ノードを追加
                if creator_uri and creator_uri not in processed_uris:
                    creator_node = {
                        "id": creator_uri,
                        "label": work_data.get("creator_name", "Unknown Creator"),
                        "type": "author",
                        "properties": {"name": work_data.get("creator_name", ""), "source": "media_arts_db"},
                    }
                    nodes.append(creator_node)
                    processed_uris.add(creator_uri)


                # 作者と作品の関係を追加
                if creator_uri and work_uri:
                    edge = {
                        "id": f"{creator_uri}-created-{work_uri}",
                        "source": creator_uri,
                        "target": work_uri,
                        "type": "created",
                        "properties": {"source": "media_arts_db"},
                    }
                    edges.append(edge)


            # 同じ雑誌・同じ年の作品同士を関連付け
            edge_ids = set()
            for magazine_name_key, magazine_works in magazine_groups.items():
                if len(magazine_works) > 1:
                    # 年ごとにさらにグループ化
                    year_groups = {}
                    for work in magazine_works:
                        published_date = work.get("published_date", "")
                        year = published_date[:4] if len(published_date) >= 4 else ""
                        if year:
                            if year not in year_groups:
                                year_groups[year] = []
                            year_groups[year].append(work)
                    
                    # 同じ雑誌・同じ年の作品同士を接続
                    for year, year_works in year_groups.items():
                        if len(year_works) > 1:
                            for i, work1 in enumerate(year_works):
                                for work2 in year_works[i + 1:]:
                                    work1_uri = work1.get("uri", "")
                                    work2_uri = work2.get("uri", "")

                                    if work1_uri and work2_uri:
                                        # 双方向エッジのIDを統一
                                        edge_id1 = f"{work1_uri}-same_magazine_year-{work2_uri}"
                                        edge_id2 = f"{work2_uri}-same_magazine_year-{work1_uri}"
                                        canonical_edge_id = min(edge_id1, edge_id2)
                                        
                                        if canonical_edge_id not in edge_ids:
                                            edge = {
                                                "id": canonical_edge_id,
                                                "source": work1_uri,
                                                "target": work2_uri,
                                                "type": "same_magazine_year",
                                                "properties": {
                                                    "magazine": magazine_name_key,
                                                    "year": year,
                                                    "source": "media_arts_db",
                                                },
                                            }
                                            edges.append(edge)
                                            edge_ids.add(canonical_edge_id)

            return {"nodes": nodes, "edges": edges}

        except Exception as e:
            logger.error(f"Error getting magazine relationships: {e}")
            return {"nodes": [], "edges": []}
