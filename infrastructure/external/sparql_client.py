import logging
from typing import List, Dict, Any, Optional
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
from requests.exceptions import RequestException, Timeout
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MediaArtsSPARQLClient:
    """文化庁メディア芸術データベースSPARQLクライアント"""
    
    def __init__(self, endpoint_url: str = "https://sparql.cineii.jbf.ne.jp/sparql"):
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
            logger.info(f"Query executed successfully, got {len(results.get('results', {}).get('bindings', []))} results")
            
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
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX madb: <https://mediag.bunka.go.jp/madb_lab/lod/ns/>
        
        SELECT DISTINCT ?work ?title ?creator ?creatorName ?genre ?publisher ?publishedDate
        WHERE {{
            ?work a schema:CreativeWork ;
                  schema:genre ?genreUri ;
                  rdfs:label ?title .
            
            ?genreUri rdfs:label ?genre .
            FILTER(CONTAINS(LCASE(?genre), "漫画") || CONTAINS(LCASE(?genre), "マンガ") || CONTAINS(LCASE(?genre), "comic"))
            
            OPTIONAL {{
                ?work schema:creator ?creator .
                ?creator rdfs:label ?creatorName .
            }}
            
            OPTIONAL {{
                ?work schema:publisher ?publisherUri .
                ?publisherUri rdfs:label ?publisher .
            }}
            
            OPTIONAL {{
                ?work schema:datePublished ?publishedDate .
            }}
            
            FILTER(
                CONTAINS(LCASE(?title), LCASE("{search_term}")) ||
                CONTAINS(LCASE(str(?creatorName)), LCASE("{search_term}"))
            )
        }}
        ORDER BY ?title
        LIMIT {limit}
        """
        
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
                'genre': binding.get('genre', {}).get('value', ''),
                'publisher': binding.get('publisher', {}).get('value', ''),
                'published_date': binding.get('publishedDate', {}).get('value', '')
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
        for binding in results.get('results', {}).get('bindings', []):
            work_data = {
                'uri': binding.get('work', {}).get('value', ''),
                'title': binding.get('title', {}).get('value', ''),
                'creator_uri': binding.get('creator', {}).get('value', ''),
                'creator_name': binding.get('creatorName', {}).get('value', ''),
                'genre': binding.get('genre', {}).get('value', ''),
                'publisher': binding.get('publisher', {}).get('value', ''),
                'published_date': binding.get('publishedDate', {}).get('value', '')
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
        for binding in results.get('results', {}).get('bindings', []):
            magazine_data = {
                'uri': binding.get('magazine', {}).get('value', ''),
                'title': binding.get('title', {}).get('value', ''),
                'publisher_uri': binding.get('publisher', {}).get('value', ''),
                'publisher_name': binding.get('publisherName', {}).get('value', ''),
                'genre': binding.get('genre', {}).get('value', '')
            }
            magazines.append(magazine_data)
            
        return magazines
    
    def search_with_fulltext(self, search_term: str, search_type: str = "simple_query_string", limit: int = 20) -> List[Dict[str, Any]]:
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
        for binding in results.get('results', {}).get('bindings', []):
            resource_data = {
                'uri': binding.get('resource', {}).get('value', ''),
                'title': binding.get('title', {}).get('value', ''),
                'type': binding.get('type', {}).get('value', '')
            }
            resources.append(resource_data)
            
        return resources