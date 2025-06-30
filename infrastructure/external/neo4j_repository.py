"""
Neo4j database repository for manga graph data
"""
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
import logging

# Add scripts directory to path to import name_normalizer
scripts_dir = Path(__file__).parent.parent.parent / "scripts" / "data_import"
sys.path.append(str(scripts_dir))
from name_normalizer import normalize_creator_name, normalize_publisher_name, generate_normalized_id, normalize_and_split_creators

logger = logging.getLogger(__name__)


class Neo4jMangaRepository:
    """Neo4j-based manga data repository"""
    
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'password')
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
            # First, get all matching works
            query = """
            MATCH (w:Work)
            WHERE toLower(w.title) CONTAINS toLower($search_term)
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w)
            OPTIONAL MATCH (p:Publisher)-[:PUBLISHED]->(w)
            OPTIONAL MATCH (s:Series)-[:CONTAINS]->(w)
            RETURN w.id as work_id, w.title as title, w.published_date as published_date,
                   collect(DISTINCT a.name) as creators,
                   collect(DISTINCT p.name) as publishers,
                   w.genre as genre, w.isbn as isbn, w.volume as volume,
                   s.id as series_id, s.name as series_name
            ORDER BY w.title, w.published_date
            """
            
            logger.debug(f"Running query with search_term: {search_term}")
            result = session.run(query, search_term=search_term)
            all_works = []
            
            for record in result:
                work = {
                    'work_id': record['work_id'],
                    'title': record['title'],
                    'published_date': record['published_date'],
                    'creators': [c for c in record['creators'] if c],
                    'publishers': [p for p in record['publishers'] if p],
                    'genre': record['genre'],
                    'isbn': record['isbn'],
                    'volume': record['volume'],
                    'series_id': record['series_id'],
                    'series_name': record['series_name']
                }
                all_works.append(work)
            
            logger.info(f"Found {len(all_works)} works matching '{search_term}'")
            
            # Group works by series or base title
            series_groups = {}
            series_name_to_key = {}  # シリーズ名からキーへのマッピング
            standalone_works = []
            
            for work in all_works:
                if work['series_id']:
                    # シリーズ名を取得
                    series_name = work['series_name'] or self._extract_base_title(work['title'])
                    
                    # 既存のシリーズ名と一致するかチェック
                    if series_name in series_name_to_key:
                        # 既存のグループに追加
                        series_key = series_name_to_key[series_name]
                    else:
                        # 新しいシリーズキーとして登録
                        series_key = work['series_id']
                        series_name_to_key[series_name] = series_key
                        series_groups[series_key] = {
                            'series_id': work['series_id'],
                            'series_name': series_name,
                            'works': [],
                            'creators': set(),
                            'publishers': set(),
                            'earliest_date': work['published_date'],
                            'latest_date': work['published_date'],
                            'volumes': []
                        }
                    
                    series_groups[series_key]['works'].append(work)
                    series_groups[series_key]['creators'].update(work['creators'])
                    series_groups[series_key]['publishers'].update(work['publishers'])
                    if work['volume']:
                        series_groups[series_key]['volumes'].append(work['volume'])
                    # 最も古い日付と新しい日付を更新
                    if work['published_date'] and work['published_date'] < series_groups[series_key]['earliest_date']:
                        series_groups[series_key]['earliest_date'] = work['published_date']
                    if work['published_date'] and work['published_date'] > series_groups[series_key]['latest_date']:
                        series_groups[series_key]['latest_date'] = work['published_date']
                else:
                    # シリーズIDがない場合は、タイトルから基本タイトルを抽出してグループ化を試みる
                    base_title = self._extract_base_title(work['title'])
                    
                    # 既存のシリーズ名とマッチするかチェック
                    found_group = False
                    for existing_series_name, series_key in series_name_to_key.items():
                        if base_title == existing_series_name or base_title == self._extract_base_title(existing_series_name):
                            # 既存のグループに追加
                            series_groups[series_key]['works'].append(work)
                            series_groups[series_key]['creators'].update(work['creators'])
                            series_groups[series_key]['publishers'].update(work['publishers'])
                            if work['volume']:
                                series_groups[series_key]['volumes'].append(work['volume'])
                            # 日付を更新
                            if work['published_date'] and work['published_date'] < series_groups[series_key]['earliest_date']:
                                series_groups[series_key]['earliest_date'] = work['published_date']
                            if work['published_date'] and work['published_date'] > series_groups[series_key]['latest_date']:
                                series_groups[series_key]['latest_date'] = work['published_date']
                            found_group = True
                            break
                    
                    if not found_group:
                        # 新しいグループを作成
                        series_key = f"series_{abs(hash(base_title))}"
                        series_name_to_key[base_title] = series_key
                        series_groups[series_key] = {
                            'series_id': series_key,
                            'series_name': base_title,
                            'works': [work],
                            'creators': set(work['creators']),
                            'publishers': set(work['publishers']),
                            'earliest_date': work['published_date'],
                            'latest_date': work['published_date'],
                            'volumes': [work['volume']] if work['volume'] else []
                        }
            
            # Convert groups to single works representing the series
            consolidated_works = []
            for group_data in series_groups.values():
                # 複数の作品がある場合は最初の巻（最も古い出版日）のみを返す
                if len(group_data['works']) > 1:
                    # 出版日でソートして最初の作品を選択
                    sorted_works = sorted(group_data['works'], 
                                        key=lambda x: x['published_date'] if x['published_date'] else '9999-99-99')
                    first_work = sorted_works[0]
                    
                    # タイトルから巻数表記を除去
                    base_title = self._extract_base_title(first_work['title'])
                    
                    series_work = {
                        'work_id': first_work['work_id'],
                        'title': base_title,  # 巻数を除去したタイトル
                        'published_date': first_work['published_date'],
                        'creators': first_work['creators'],
                        'publishers': first_work['publishers'],
                        'genre': first_work['genre'],
                        'isbn': first_work['isbn'],
                        'volume': first_work['volume'],
                        'is_series': True,
                        'work_count': len(group_data['works']),
                        'series_volumes': f"{len(group_data['works'])}巻",  # シリーズ全体の巻数情報
                        'individual_works': group_data['works']  # 個別作品の情報を保持
                    }
                    consolidated_works.append(series_work)
                else:
                    # 単一作品の場合はそのまま使用
                    single_work = group_data['works'][0]
                    single_work['is_series'] = False
                    single_work['work_count'] = 1
                    consolidated_works.append(single_work)
            
            # Add standalone works
            consolidated_works.extend(standalone_works)
            
            # Sort by title and limit results
            consolidated_works.sort(key=lambda x: x['title'])
            return consolidated_works[:limit]
    
    def _extract_base_title(self, title: str) -> str:
        """Extract base title by removing volume numbers and other suffixes"""
        import re
        
        if not title:
            return title
            
        # パターンで巻数や番号を除去
        patterns = [
            r'\s*\d+$',  # 末尾の数字
            r'\s*第\d+巻?$',  # 第X巻
            r'\s*\(\d+\)$',  # (数字)
            r'\s*vol\.\s*\d+$',  # vol. X
            r'\s*VOLUME\s*\d+$',  # VOLUME X
            r'\s*巻\d+$',  # 巻X
            r'\s*その\d+$',  # そのX
        ]
        
        base = title
        for pattern in patterns:
            base = re.sub(pattern, '', base, flags=re.IGNORECASE)
        
        return base.strip()
    
    def get_related_works_by_author(self, work_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get related works by the same author"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work {id: $work_id})<-[:CREATED]-(a:Author)-[:CREATED]->(w2:Work)
            WHERE w1.id <> w2.id
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   a.name as author_name
            LIMIT $limit
            """
            
            result = session.run(query, work_id=work_id, limit=limit)
            return [dict(record) for record in result]
    
    def get_related_works_by_publisher(self, work_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get related works by the same publisher"""
        with self.driver.session() as session:
            query = """
            MATCH (w1:Work {id: $work_id})<-[:PUBLISHED]-(p:Publisher)-[:PUBLISHED]->(w2:Work)
            WHERE w1.id <> w2.id
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   p.name as publisher_name
            LIMIT $limit
            """
            
            result = session.run(query, work_id=work_id, limit=limit)
            return [dict(record) for record in result]
    
    def get_related_works_by_publication_period(self, work_id: str, year_range: int = 5, limit: int = 10) -> List[Dict[str, Any]]:
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
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w2)
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   collect(DISTINCT a.name) as creators,
                   abs(year1 - year2) as year_diff
            ORDER BY year_diff ASC
            LIMIT $limit
            """
            
            result = session.run(query, work_id=work_id, year_range=year_range, limit=limit)
            return [dict(record) for record in result]
    
    def get_related_works_by_magazine_and_period(self, work_id: str, year_range: int = 2, limit: int = 20) -> List[Dict[str, Any]]:
        """Get works published by the same publisher in the same period"""
        with self.driver.session() as session:
            # Since we don't have Magazine nodes, we'll use Publisher relationships
            query = """
            MATCH (w1:Work {id: $work_id})<-[:PUBLISHED]-(p:Publisher)
            WHERE w1.published_date IS NOT NULL AND w1.published_date <> ''
            WITH w1, p, toInteger(substring(w1.published_date, 0, 4)) as year1
            MATCH (w2:Work)<-[:PUBLISHED]-(p)
            WHERE w2.published_date IS NOT NULL AND w2.published_date <> ''
            AND w1.id <> w2.id
            WITH w1, w2, p, year1, toInteger(substring(w2.published_date, 0, 4)) as year2
            WHERE abs(year1 - year2) <= $year_range
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w2)
            RETURN w2.id as work_id, w2.title as title, w2.published_date as published_date,
                   collect(DISTINCT a.name) as creators,
                   p.name as publisher_name,
                   abs(year1 - year2) as year_diff
            ORDER BY year_diff ASC, w2.published_date ASC
            LIMIT $limit
            """
            
            result = session.run(query, work_id=work_id, year_range=year_range, limit=limit)
            return [dict(record) for record in result]
    
    def search_manga_data_with_related(self, search_term: str, limit: int = 20, include_related: bool = True) -> Dict[str, Any]:
        """Search manga data and include related works for graph visualization"""
        logger.info(f"search_manga_data_with_related called with term: '{search_term}', limit: {limit}, include_related: {include_related}")
        
        main_works = self.search_manga_works(search_term, limit)
        
        if not main_works:
            logger.warning(f"No works found for search term: '{search_term}'")
            return {'nodes': [], 'edges': []}
        
        nodes = []
        edges = []
        node_ids_seen = set()  # Track node IDs to prevent duplicates
        edge_ids_seen = set()  # Track edge IDs to prevent duplicates
        
        # Add main works as nodes
        for work in main_works:
            if work['work_id'] not in node_ids_seen:
                node = {
                    'id': work['work_id'],
                    'label': work['title'],
                    'type': 'work',
                    'data': work
                }
                nodes.append(node)
                node_ids_seen.add(work['work_id'])
            
            # Add authors as nodes and create edges
            for creator in work['creators']:
                if creator:
                    # Split multiple creators and normalize each one
                    normalized_creators = normalize_and_split_creators(creator)
                    for normalized_creator in normalized_creators:
                        if normalized_creator:
                            author_id = generate_normalized_id(normalized_creator, "author")
                            if author_id not in node_ids_seen:
                                author_node = {
                                    'id': author_id,
                                    'label': normalized_creator,
                                    'type': 'author'
                                }
                                nodes.append(author_node)
                                node_ids_seen.add(author_id)
                            
                            edge_id = f"{author_id}-created-{work['work_id']}"
                            if edge_id not in edge_ids_seen:
                                edge = {
                                    'from': author_id,
                                    'to': work['work_id'],
                                    'label': 'created',
                                    'type': 'created'
                                }
                                edges.append(edge)
                                edge_ids_seen.add(edge_id)
            
            # Add publishers as nodes and create edges
            for publisher in work['publishers']:
                if publisher:
                    normalized_publisher = normalize_publisher_name(publisher)
                    if normalized_publisher:
                        publisher_id = generate_normalized_id(normalized_publisher, "publisher")
                        if publisher_id not in node_ids_seen:
                            publisher_node = {
                                'id': publisher_id,
                                'label': normalized_publisher,
                                'type': 'publisher'
                            }
                            nodes.append(publisher_node)
                            node_ids_seen.add(publisher_id)
                    
                    edge_id = f"{publisher_id}-published-{work['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            'from': publisher_id,
                            'to': work['work_id'],
                            'label': 'published',
                            'type': 'published'
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)
        
        # Add related works if requested
        if include_related and main_works:
            main_work_id = main_works[0]['work_id']
            
            # Add works by same author
            author_related = self.get_related_works_by_author(main_work_id, 5)
            for related in author_related:
                if related['work_id'] not in node_ids_seen:
                    related_node = {
                        'id': related['work_id'],
                        'label': related['title'],
                        'type': 'work',
                        'data': related
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related['work_id'])
                
                # Create author relationship edge
                normalized_author = normalize_creator_name(related['author_name'])
                author_id = generate_normalized_id(normalized_author, "author")
                if author_id in node_ids_seen:
                    edge_id = f"{author_id}-created-{related['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            'from': author_id,
                            'to': related['work_id'],
                            'label': 'created',
                            'type': 'created'
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)
            
            # Add works from same magazine and period
            magazine_period_related = self.get_related_works_by_magazine_and_period(main_work_id, 2, 10)
            
            for related in magazine_period_related:
                if related['work_id'] not in node_ids_seen:
                    related_node = {
                        'id': related['work_id'],
                        'label': related['title'],
                        'type': 'work',
                        'data': related
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related['work_id'])
                    
                    # Add creators
                    for creator in related['creators']:
                        if creator:
                            # Split multiple creators and normalize each one
                            normalized_creators = normalize_and_split_creators(creator)
                            for normalized_creator in normalized_creators:
                                if normalized_creator:
                                    author_id = generate_normalized_id(normalized_creator, "author")
                                    author_node = {
                                        'id': author_id,
                                        'label': normalized_creator,
                                        'type': 'author'
                                    }
                                if author_id not in node_ids_seen:
                                    nodes.append(author_node)
                                    node_ids_seen.add(author_id)
                                
                                edge_id = f"{author_id}-created-{related['work_id']}"
                                if edge_id not in edge_ids_seen:
                                    edge = {
                                        'from': author_id,
                                        'to': related['work_id'],
                                        'label': 'created',
                                        'type': 'created'
                                    }
                                    edges.append(edge)
                                    edge_ids_seen.add(edge_id)
                    
                    # Add publishers
                    # Handle single publisher from query result
                    if related.get('publisher_name'):
                        normalized_publisher = normalize_publisher_name(related['publisher_name'])
                        if normalized_publisher:
                            publisher_id = generate_normalized_id(normalized_publisher, "publisher")
                            publisher_node = {
                                'id': publisher_id,
                                'label': normalized_publisher,
                                'type': 'publisher'
                            }
                        if publisher_id not in node_ids_seen:
                            nodes.append(publisher_node)
                            node_ids_seen.add(publisher_id)
                        
                        edge_id = f"{publisher_id}-published-{related['work_id']}"
                        if edge_id not in edge_ids_seen:
                            edge = {
                                'from': publisher_id,
                                'to': related['work_id'],
                                'label': 'published',
                                'type': 'published'
                            }
                            edges.append(edge)
                            edge_ids_seen.add(edge_id)
                
                # Create "same_publisher_period" edge between main work and related work
                if related.get('publisher_name'):
                    edge_id = f"{main_work_id}-same_publisher_period-{related['work_id']}"
                    if edge_id not in edge_ids_seen:
                        edge = {
                            'from': main_work_id,
                            'to': related['work_id'],
                            'label': f"同じ出版社({related['publisher_name']})・同時期",
                            'type': 'same_publisher_period'
                        }
                        edges.append(edge)
                        edge_ids_seen.add(edge_id)
            
            # Add works from same publication period (without magazine constraint)
            period_related = self.get_related_works_by_publication_period(main_work_id, 3, 5)
            for related in period_related:
                if related['work_id'] not in node_ids_seen:
                    related_node = {
                        'id': related['work_id'],
                        'label': related['title'],
                        'type': 'work',
                        'data': related
                    }
                    nodes.append(related_node)
                    node_ids_seen.add(related['work_id'])
                    
                    # Add creators of period-related works
                    for creator in related['creators']:
                        if creator:
                            # Split multiple creators and normalize each one
                            normalized_creators = normalize_and_split_creators(creator)
                            for normalized_creator in normalized_creators:
                                if normalized_creator:
                                    author_id = generate_normalized_id(normalized_creator, "author")
                                    author_node = {
                                        'id': author_id,
                                        'label': normalized_creator,
                                        'type': 'author'
                                    }
                                if author_id not in node_ids_seen:
                                    nodes.append(author_node)
                                    node_ids_seen.add(author_id)
                                
                                edge_id = f"{author_id}-created-{related['work_id']}"
                                if edge_id not in edge_ids_seen:
                                    edge = {
                                        'from': author_id,
                                        'to': related['work_id'],
                                        'label': 'created',
                                        'type': 'created'
                                    }
                                    edges.append(edge)
                                    edge_ids_seen.add(edge_id)
        
        # Final deduplication to ensure no duplicate nodes exist
        unique_nodes = []
        seen_work_titles = {}  # For work nodes, track by title to avoid duplicates
        unique_node_ids = set()  # Track by ID
        
        for node in nodes:
            if node['type'] == 'work':
                # For work nodes, prioritize by keeping the one with more complete data
                title = node['label']
                if title in seen_work_titles:
                    # Keep the node with more complete data (more properties)
                    existing_node = seen_work_titles[title]
                    existing_data_count = len(existing_node.get('data', {}))
                    current_data_count = len(node.get('data', {}))
                    
                    if current_data_count > existing_data_count:
                        # Replace with current node (has more data)
                        unique_nodes = [n for n in unique_nodes if n['label'] != title or n['type'] != 'work']
                        unique_nodes.append(node)
                        seen_work_titles[title] = node
                        unique_node_ids.add(node['id'])
                    # Otherwise keep the existing one
                else:
                    seen_work_titles[title] = node
                    unique_nodes.append(node)
                    unique_node_ids.add(node['id'])
            else:
                # For non-work nodes, use ID-based deduplication
                if node['id'] not in unique_node_ids:
                    unique_nodes.append(node)
                    unique_node_ids.add(node['id'])
        
        # Final deduplication for edges, ensuring they reference existing nodes
        valid_node_ids = {node['id'] for node in unique_nodes}
        work_title_to_id = {node['label']: node['id'] for node in unique_nodes if node['type'] == 'work'}
        
        unique_edges = []
        unique_edge_keys = set()
        
        for edge in edges:
            from_id = edge['from']
            to_id = edge['to']
            
            # If this edge references a work node that was deduplicated, update the reference
            # Check if the from/to IDs exist in our final node list
            if from_id not in valid_node_ids:
                # Try to find the correct node ID by matching with work titles
                found_replacement = False
                for node in unique_nodes:
                    if node['type'] == 'work' and node['id'] != from_id:
                        # Check if this might be the same work by looking at original edges
                        original_from_node = next((n for n in nodes if n['id'] == from_id), None)
                        if original_from_node and original_from_node['label'] == node['label']:
                            from_id = node['id']
                            found_replacement = True
                            break
                if not found_replacement:
                    continue  # Skip this edge if we can't find a valid from node
            
            if to_id not in valid_node_ids:
                # Try to find the correct node ID by matching with work titles
                found_replacement = False
                for node in unique_nodes:
                    if node['type'] == 'work' and node['id'] != to_id:
                        # Check if this might be the same work by looking at original edges
                        original_to_node = next((n for n in nodes if n['id'] == to_id), None)
                        if original_to_node and original_to_node['label'] == node['label']:
                            to_id = node['id']
                            found_replacement = True
                            break
                if not found_replacement:
                    continue  # Skip this edge if we can't find a valid to node
            
            # Only add edge if both nodes exist
            if from_id in valid_node_ids and to_id in valid_node_ids:
                edge_key = (from_id, to_id, edge['type'])
                if edge_key not in unique_edge_keys:
                    updated_edge = edge.copy()
                    updated_edge['from'] = from_id
                    updated_edge['to'] = to_id
                    unique_edges.append(updated_edge)
                    unique_edge_keys.add(edge_key)
        
        logger.info(f"After deduplication: {len(unique_nodes)} nodes and {len(unique_edges)} edges for search term: '{search_term}'")
        return {
            'nodes': unique_nodes,
            'edges': unique_edges
        }
    
    def get_database_statistics(self) -> Dict[str, int]:
        """Get database statistics"""
        try:
            with self.driver.session() as session:
                stats = {}
                
                # Count nodes
                for label in ['Work', 'Author', 'Publisher', 'Series']:
                    result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    stats[f'{label.lower()}_count'] = result.single()['count']
                
                # Count relationships
                for rel_type in ['CREATED', 'PUBLISHED', 'SAME_AUTHOR', 'SAME_PUBLISHER']:
                    result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                    stats[f'{rel_type.lower()}_relationships'] = result.single()['count']
                
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
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w)
            OPTIONAL MATCH (p:Publisher)-[:PUBLISHED]->(w)
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
                    "authors": record["authors"]
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
                works.append({
                    "id": record["id"],
                    "title": record["title"],
                    "isbn": record["isbn"]
                })
            
            return works