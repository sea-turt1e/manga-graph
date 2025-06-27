"""
Mock Neo4j service for testing when Neo4j is not available
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class MockNeo4jService:
    """Mock service that returns sample data when Neo4j is not available"""
    
    def search_manga_data_with_related(
        self, search_term: str, limit: int = 20, include_related: bool = True
    ) -> Dict[str, List]:
        """Return mock manga data for testing"""
        logger.info(f"MockNeo4jService: Returning mock data for search term: '{search_term}'")
        
        # Mock data for "ONE" search
        if search_term.lower() == "one":
            nodes = [
                {
                    "id": "work_one_punch_man",
                    "label": "ワンパンマン (シリーズ)",
                    "type": "work",
                    "properties": {
                        "title": "ワンパンマン",
                        "published_date": "2012 - 2024",
                        "genre": "アクション",
                        "is_series": True,
                        "work_count": 30,
                        "source": "mock"
                    }
                },
                {
                    "id": "author_one",
                    "label": "ONE",
                    "type": "author",
                    "properties": {
                        "name": "ONE",
                        "source": "mock"
                    }
                },
                {
                    "id": "author_murata",
                    "label": "村田雄介",
                    "type": "author",
                    "properties": {
                        "name": "村田雄介",
                        "source": "mock"
                    }
                },
                {
                    "id": "publisher_shueisha",
                    "label": "集英社",
                    "type": "publisher",
                    "properties": {
                        "name": "集英社",
                        "source": "mock"
                    }
                }
            ]
            
            edges = [
                {
                    "id": "author_one-created-work_one_punch_man",
                    "source": "author_one",
                    "target": "work_one_punch_man",
                    "type": "created",
                    "properties": {"source": "mock"}
                },
                {
                    "id": "author_murata-created-work_one_punch_man",
                    "source": "author_murata",
                    "target": "work_one_punch_man",
                    "type": "created",
                    "properties": {"source": "mock"}
                },
                {
                    "id": "publisher_shueisha-published-work_one_punch_man",
                    "source": "publisher_shueisha",
                    "target": "work_one_punch_man",
                    "type": "published",
                    "properties": {"source": "mock"}
                }
            ]
            
            if include_related:
                # Add related work
                nodes.extend([
                    {
                        "id": "work_mob_psycho",
                        "label": "モブサイコ100 (シリーズ)",
                        "type": "work",
                        "properties": {
                            "title": "モブサイコ100",
                            "published_date": "2012 - 2017",
                            "genre": "アクション",
                            "is_series": True,
                            "work_count": 16,
                            "source": "mock"
                        }
                    },
                    {
                        "id": "publisher_shogakukan",
                        "label": "小学館",
                        "type": "publisher",
                        "properties": {
                            "name": "小学館",
                            "source": "mock"
                        }
                    }
                ])
                
                edges.extend([
                    {
                        "id": "author_one-created-work_mob_psycho",
                        "source": "author_one",
                        "target": "work_mob_psycho",
                        "type": "created",
                        "properties": {"source": "mock"}
                    },
                    {
                        "id": "publisher_shogakukan-published-work_mob_psycho",
                        "source": "publisher_shogakukan",
                        "target": "work_mob_psycho",
                        "type": "published",
                        "properties": {"source": "mock"}
                    }
                ])
            
            return {"nodes": nodes, "edges": edges}
        
        # Default empty response for other searches
        return {"nodes": [], "edges": []}
    
    def get_creator_works(self, creator_name: str, limit: int = 50) -> Dict[str, List]:
        """Return mock creator works"""
        return {"nodes": [], "edges": []}
    
    def get_database_statistics(self) -> Dict[str, int]:
        """Return mock database statistics"""
        return {
            "work_count": 0,
            "author_count": 0,
            "publisher_count": 0,
            "series_count": 0,
            "created_relationships": 0,
            "published_relationships": 0
        }
    
    def close(self):
        """Mock close method"""
        pass