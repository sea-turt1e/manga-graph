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

    def search_manga_data_with_related(self, search_term: str, limit: int = 20, include_related: bool = True) -> Dict[str, List]:
        """
        漫画データを検索して関連作品も含めてグラフ形式で返す

        Args:
            search_term: 検索語
            limit: 結果の上限
            include_related: 関連作品を含めるかどうか

        Returns:
            ノードとエッジのリストを含む辞書
        """
        try:
            # まず基本検索を実行
            base_result = self.search_manga_data(search_term, limit)
            
            if not include_related:
                return base_result
            
            nodes = base_result["nodes"]
            edges = base_result["edges"]
            processed_uris = set()
            processed_publishers = set()
            processed_years = set()
            
            # 既存のノードのURIを記録
            for node in nodes:
                processed_uris.add(node["id"])
                if node["type"] == "work":
                    publisher = node["properties"].get("publisher", "")
                    published_date = node["properties"].get("published_date", "")
                    
                    if publisher:
                        processed_publishers.add(publisher)
                    
                    if published_date:
                        year = published_date[:4] if len(published_date) >= 4 else None
                        if year and year.isdigit():
                            processed_years.add(year)
            
            # 重複期間が長い関連作品を優先的に取得
            try:
                overlap_works = self.sparql_client.get_related_works_by_overlap_period(search_term, 15)
                self._add_related_works_to_graph(overlap_works, nodes, edges, processed_uris, "overlap_period")
            except Exception as e:
                logger.warning(f"Error getting related works by overlap period: {e}")
            
            # 重複期間が検出できない場合は、基本の関連作品検索をフォールバック
            if len(nodes) <= 2:  # 基本検索結果のみの場合
                try:
                    # NARUTOなど他の有名作品を明示的に検索
                    famous_works = ['NARUTO', 'BLEACH', 'ハンター', 'デスノート', 'るろうに剣心']
                    for work_name in famous_works:
                        if work_name.lower() not in search_term.lower():
                            related_results = self.sparql_client.search_manga_works(work_name, 2)
                            self._add_related_works_to_graph(related_results, nodes, edges, processed_uris, "same_publisher")
                except Exception as e:
                    logger.warning(f"Error getting famous works: {e}")
            
            return {"nodes": nodes, "edges": edges}
            
        except Exception as e:
            logger.error(f"Error searching manga data with related: {e}")
            return {"nodes": [], "edges": []}
    
    def _add_related_works_to_graph(self, works_data: List[Dict], nodes: List, edges: List, processed_uris: set, relation_type: str):
        """
        関連作品をグラフに追加するヘルパーメソッド
        
        Args:
            works_data: 作品データのリスト
            nodes: ノードリスト（参照渡し）
            edges: エッジリスト（参照渡し）
            processed_uris: 処理済みURIのセット（参照渡し）
            relation_type: 関係タイプ
        """
        publisher_nodes = {}  # 出版社ノードを記録
        
        for work_data in works_data:
            work_uri = work_data.get("uri", "")
            creator_uri = work_data.get("creator_uri", "")
            title = work_data.get("title", "").strip()
            publisher = work_data.get("publisher", "").strip()
            
            # 作品ノードを追加（未処理の場合のみ）
            if work_uri and work_uri not in processed_uris:
                work_node = {
                    "id": work_uri,
                    "label": title or "Unknown Work",
                    "type": "work",
                    "properties": {
                        "title": title,
                        "genre": work_data.get("genre", ""),
                        "publisher": publisher,
                        "published_date": work_data.get("published_date", ""),
                        "source": "media_arts_db",
                    },
                }
                nodes.append(work_node)
                processed_uris.add(work_uri)
            
            # 作者ノードを追加（未処理の場合のみ）
            if creator_uri and creator_uri not in processed_uris:
                creator_node = {
                    "id": creator_uri,
                    "label": work_data.get("creator_name", "Unknown Creator"),
                    "type": "author",
                    "properties": {"name": work_data.get("creator_name", ""), "source": "media_arts_db"},
                }
                nodes.append(creator_node)
                processed_uris.add(creator_uri)
            
            # 出版社ノードを作成（まだない場合）
            if publisher and relation_type == "published_by":
                publisher_id = f"publisher_{hash(publisher)}"
                if publisher_id not in publisher_nodes:
                    publisher_node = {
                        "id": publisher_id,
                        "label": publisher,
                        "type": "publisher",
                        "properties": {"name": publisher, "source": "media_arts_db"},
                    }
                    nodes.append(publisher_node)
                    processed_uris.add(publisher_id)
                    publisher_nodes[publisher_id] = publisher_node
                
                # 出版社と作品の関係を追加
                if work_uri:
                    edge = {
                        "id": f"{publisher_id}-publishes-{work_uri}",
                        "source": publisher_id,
                        "target": work_uri,
                        "type": "publishes",
                        "properties": {"source": "media_arts_db"},
                    }
                    edges.append(edge)
            
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
