"""
Neo4j database repository for manga graph data
"""
import os
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
import logging

logger = logging.getLogger(__name__)


class Neo4jMangaRepository:
    """Neo4j-based manga data repository"""
    
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'password')
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        logger.info(f"Connected to Neo4j at {self.uri}")
    
    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()
    
    def search_manga_works(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for manga works by title, grouping by series"""
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
                # 複数の作品がある場合はシリーズとして統合
                if len(group_data['works']) > 1:
                    series_work = {
                        'work_id': group_data['series_id'],
                        'title': f"{group_data['series_name']} (シリーズ)",
                        'published_date': f"{group_data['earliest_date']} - {group_data['latest_date']}",
                        'creators': list(group_data['creators']),
                        'publishers': list(group_data['publishers']),
                        'genre': group_data['works'][0]['genre'],  # 最初の作品のジャンルを使用
                        'isbn': None,  # シリーズ全体のISBNはなし
                        'volume': f"{len(group_data['works'])}巻",
                        'is_series': True,
                        'work_count': len(group_data['works']),
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
        main_works = self.search_manga_works(search_term, limit)
        
        if not main_works:
            return {'nodes': [], 'edges': []}
        
        nodes = []
        edges = []
        
        # Add main works as nodes
        for work in main_works:
            node = {
                'id': work['work_id'],
                'label': work['title'],
                'type': 'work',
                'data': work
            }
            nodes.append(node)
            
            # Add authors as nodes and create edges
            for creator in work['creators']:
                if creator:
                    author_id = f"author_{abs(hash(creator))}"
                    author_node = {
                        'id': author_id,
                        'label': creator,
                        'type': 'author'
                    }
                    if author_node not in nodes:
                        nodes.append(author_node)
                    
                    edge = {
                        'from': author_id,
                        'to': work['work_id'],
                        'label': 'created',
                        'type': 'created'
                    }
                    edges.append(edge)
            
            # Add publishers as nodes and create edges
            for publisher in work['publishers']:
                if publisher:
                    publisher_id = f"publisher_{abs(hash(publisher))}"
                    publisher_node = {
                        'id': publisher_id,
                        'label': publisher,
                        'type': 'publisher'
                    }
                    if publisher_node not in nodes:
                        nodes.append(publisher_node)
                    
                    edge = {
                        'from': publisher_id,
                        'to': work['work_id'],
                        'label': 'published',
                        'type': 'published'
                    }
                    edges.append(edge)
        
        # Add related works if requested
        if include_related and main_works:
            main_work_id = main_works[0]['work_id']
            
            # Add works by same author
            author_related = self.get_related_works_by_author(main_work_id, 5)
            for related in author_related:
                related_node = {
                    'id': related['work_id'],
                    'label': related['title'],
                    'type': 'work',
                    'data': related
                }
                if related_node not in nodes:
                    nodes.append(related_node)
                
                # Create author relationship edge
                author_id = f"author_{abs(hash(related['author_name']))}"
                if any(n['id'] == author_id for n in nodes):
                    edge = {
                        'from': author_id,
                        'to': related['work_id'],
                        'label': 'created',
                        'type': 'created'
                    }
                    if edge not in edges:
                        edges.append(edge)
            
            # Add works from same magazine and period
            magazine_period_related = self.get_related_works_by_magazine_and_period(main_work_id, 2, 10)
            processed_work_ids = set(node['id'] for node in nodes if node['type'] == 'work')
            
            for related in magazine_period_related:
                if related['work_id'] not in processed_work_ids:
                    related_node = {
                        'id': related['work_id'],
                        'label': related['title'],
                        'type': 'work',
                        'data': related
                    }
                    nodes.append(related_node)
                    processed_work_ids.add(related['work_id'])
                    
                    # Add creators
                    for creator in related['creators']:
                        if creator:
                            author_id = f"author_{abs(hash(creator))}"
                            author_node = {
                                'id': author_id,
                                'label': creator,
                                'type': 'author'
                            }
                            if not any(n['id'] == author_id for n in nodes):
                                nodes.append(author_node)
                            
                            edge = {
                                'from': author_id,
                                'to': related['work_id'],
                                'label': 'created',
                                'type': 'created'
                            }
                            if edge not in edges:
                                edges.append(edge)
                    
                    # Add publishers
                    # Handle single publisher from query result
                    if related.get('publisher_name'):
                        publisher_id = f"publisher_{abs(hash(related['publisher_name']))}"
                        publisher_node = {
                            'id': publisher_id,
                            'label': related['publisher_name'],
                            'type': 'publisher'
                        }
                        if not any(n['id'] == publisher_id for n in nodes):
                            nodes.append(publisher_node)
                        
                        edge = {
                            'from': publisher_id,
                            'to': related['work_id'],
                            'label': 'published',
                            'type': 'published'
                        }
                        if edge not in edges:
                            edges.append(edge)
                
                # Create "same_publisher_period" edge between main work and related work
                if related.get('publisher_name'):
                    edge = {
                        'from': main_work_id,
                        'to': related['work_id'],
                        'label': f"同じ出版社({related['publisher_name']})・同時期",
                        'type': 'same_publisher_period'
                    }
                    if edge not in edges:
                        edges.append(edge)
            
            # Add works from same publication period (without magazine constraint)
            period_related = self.get_related_works_by_publication_period(main_work_id, 3, 5)
            for related in period_related:
                if related['work_id'] not in processed_work_ids:
                    related_node = {
                        'id': related['work_id'],
                        'label': related['title'],
                        'type': 'work',
                        'data': related
                    }
                    nodes.append(related_node)
                    processed_work_ids.add(related['work_id'])
                    
                    # Add creators of period-related works
                    for creator in related['creators']:
                        if creator:
                            author_id = f"author_{abs(hash(creator))}"
                            author_node = {
                                'id': author_id,
                                'label': creator,
                                'type': 'author'
                            }
                            if not any(n['id'] == author_id for n in nodes):
                                nodes.append(author_node)
                            
                            edge = {
                                'from': author_id,
                                'to': related['work_id'],
                                'label': 'created',
                                'type': 'created'
                            }
                            if edge not in edges:
                                edges.append(edge)
        
        return {
            'nodes': nodes,
            'edges': edges
        }
    
    def get_database_statistics(self) -> Dict[str, int]:
        """Get database statistics"""
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
            
            return stats