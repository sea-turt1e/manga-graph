from typing import List, Dict, Any, Optional
from infrastructure.external import MediaArtsSPARQLClient
from domain.entities import Work, Author, Magazine, GraphNode, GraphEdge
from datetime import datetime
import logging

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
            
            for work_data in works_data:
                work_uri = work_data.get('uri', '')
                creator_uri = work_data.get('creator_uri', '')
                
                # 作品ノードを追加
                if work_uri and work_uri not in processed_uris:
                    work_node = {
                        'id': work_uri,
                        'label': work_data.get('title', 'Unknown Work'),
                        'type': 'work',
                        'properties': {
                            'title': work_data.get('title', ''),
                            'genre': work_data.get('genre', ''),
                            'publisher': work_data.get('publisher', ''),
                            'published_date': work_data.get('published_date', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(work_node)
                    processed_uris.add(work_uri)
                
                # 作者ノードを追加
                if creator_uri and creator_uri not in processed_uris:
                    creator_node = {
                        'id': creator_uri,
                        'label': work_data.get('creator_name', 'Unknown Creator'),
                        'type': 'author',
                        'properties': {
                            'name': work_data.get('creator_name', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(creator_node)
                    processed_uris.add(creator_uri)
                
                # 作者と作品の関係を追加
                if work_uri and creator_uri:
                    edge = {
                        'id': f"{creator_uri}-created-{work_uri}",
                        'source': creator_uri,
                        'target': work_uri,
                        'type': 'created',
                        'properties': {
                            'source': 'media_arts_db'
                        }
                    }
                    edges.append(edge)
            
            return {
                'nodes': nodes,
                'edges': edges
            }
            
        except Exception as e:
            logger.error(f"Error searching manga data: {e}")
            return {'nodes': [], 'edges': []}
    
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
                work_uri = work_data.get('uri', '')
                current_creator_uri = work_data.get('creator_uri', '')
                
                # 作者ノードを追加（最初の一回のみ）
                if current_creator_uri and current_creator_uri not in processed_uris:
                    creator_uri = current_creator_uri
                    creator_node = {
                        'id': creator_uri,
                        'label': work_data.get('creator_name', 'Unknown Creator'),
                        'type': 'author',
                        'properties': {
                            'name': work_data.get('creator_name', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(creator_node)
                    processed_uris.add(creator_uri)
                
                # 作品ノードを追加
                if work_uri and work_uri not in processed_uris:
                    work_node = {
                        'id': work_uri,
                        'label': work_data.get('title', 'Unknown Work'),
                        'type': 'work',
                        'properties': {
                            'title': work_data.get('title', ''),
                            'genre': work_data.get('genre', ''),
                            'publisher': work_data.get('publisher', ''),
                            'published_date': work_data.get('published_date', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(work_node)
                    processed_uris.add(work_uri)
                
                # 作者と作品の関係を追加
                if creator_uri and work_uri:
                    edge = {
                        'id': f"{creator_uri}-created-{work_uri}",
                        'source': creator_uri,
                        'target': work_uri,
                        'type': 'created',
                        'properties': {
                            'source': 'media_arts_db'
                        }
                    }
                    edges.append(edge)
            
            return {
                'nodes': nodes,
                'edges': edges
            }
            
        except Exception as e:
            logger.error(f"Error getting creator works: {e}")
            return {'nodes': [], 'edges': []}
    
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
                magazine_uri = magazine_data.get('uri', '')
                publisher_uri = magazine_data.get('publisher_uri', '')
                
                # 雑誌ノードを追加
                if magazine_uri and magazine_uri not in processed_uris:
                    magazine_node = {
                        'id': magazine_uri,
                        'label': magazine_data.get('title', 'Unknown Magazine'),
                        'type': 'magazine',
                        'properties': {
                            'title': magazine_data.get('title', ''),
                            'genre': magazine_data.get('genre', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(magazine_node)
                    processed_uris.add(magazine_uri)
                
                # 出版社ノードを追加
                if publisher_uri and publisher_uri not in processed_uris:
                    publisher_node = {
                        'id': publisher_uri,
                        'label': magazine_data.get('publisher_name', 'Unknown Publisher'),
                        'type': 'publisher',
                        'properties': {
                            'name': magazine_data.get('publisher_name', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(publisher_node)
                    processed_uris.add(publisher_uri)
                
                # 出版社と雑誌の関係を追加
                if publisher_uri and magazine_uri:
                    edge = {
                        'id': f"{publisher_uri}-publishes-{magazine_uri}",
                        'source': publisher_uri,
                        'target': magazine_uri,
                        'type': 'publishes',
                        'properties': {
                            'source': 'media_arts_db'
                        }
                    }
                    edges.append(edge)
            
            return {
                'nodes': nodes,
                'edges': edges
            }
            
        except Exception as e:
            logger.error(f"Error getting magazine data: {e}")
            return {'nodes': [], 'edges': []}
    
    def search_with_fulltext(self, search_term: str, search_type: str = "simple_query_string", limit: int = 20) -> Dict[str, List]:
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
                uri = result.get('uri', '')
                if uri and uri not in processed_uris:
                    # URIから推測されるタイプを決定
                    node_type = 'unknown'
                    if 'creator' in uri.lower() or 'author' in uri.lower():
                        node_type = 'author'
                    elif 'work' in uri.lower():
                        node_type = 'work'
                    elif 'magazine' in uri.lower() or 'periodical' in uri.lower():
                        node_type = 'magazine'
                    
                    node = {
                        'id': uri,
                        'label': result.get('title', 'Unknown'),
                        'type': node_type,
                        'properties': {
                            'title': result.get('title', ''),
                            'resource_type': result.get('type', ''),
                            'source': 'media_arts_db'
                        }
                    }
                    nodes.append(node)
                    processed_uris.add(uri)
            
            return {
                'nodes': nodes,
                'edges': []  # 全文検索では関係性情報が限定的
            }
            
        except Exception as e:
            logger.error(f"Error in fulltext search: {e}")
            return {'nodes': [], 'edges': []}