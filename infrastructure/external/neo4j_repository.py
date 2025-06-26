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
        """Search for manga works by title"""
        with self.driver.session() as session:
            query = """
            MATCH (w:Work)
            WHERE toLower(w.title) CONTAINS toLower($search_term)
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w)
            OPTIONAL MATCH (p:Publisher)-[:PUBLISHED]->(w)
            RETURN w.id as work_id, w.title as title, w.published_date as published_date,
                   collect(DISTINCT a.name) as creators,
                   collect(DISTINCT p.name) as publishers,
                   w.genre as genre, w.isbn as isbn, w.volume as volume
            LIMIT $limit
            """
            
            result = session.run(query, search_term=search_term, limit=limit)
            works = []
            
            for record in result:
                work = {
                    'work_id': record['work_id'],
                    'title': record['title'],
                    'published_date': record['published_date'],
                    'creators': [c for c in record['creators'] if c],
                    'publishers': [p for p in record['publishers'] if p],
                    'genre': record['genre'],
                    'isbn': record['isbn'],
                    'volume': record['volume']
                }
                works.append(work)
            
            return works
    
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
            
            # Add works from same publication period
            period_related = self.get_related_works_by_publication_period(main_work_id, 3, 5)
            for related in period_related:
                related_node = {
                    'id': related['work_id'],
                    'label': related['title'],
                    'type': 'work',
                    'data': related
                }
                if related_node not in nodes:
                    nodes.append(related_node)
                
                # Add creators of period-related works
                for creator in related['creators']:
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