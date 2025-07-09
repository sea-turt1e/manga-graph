"""
Mock Neo4j service for testing when Neo4j is not available
"""

import logging
from typing import Dict, List, Optional, Any

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

    def get_work_by_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """Return mock work data by ID"""
        logger.info(f"MockNeo4jService: Getting work by ID: {work_id}")

        # Mock data for specific works that were updated in bulk update
        mock_works = {
            "https://mediaarts-db.artmuseums.go.jp/id/M1032568": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032568",
                "title": "キジトラ猫の小梅さん",
                "isbn": "9784785977382",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=-HDJ0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "芳文社",
                "authors": ["ほしのなつみ"]
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032569": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032569",
                "title": "Dear Anemone",
                "isbn": "9784088841748",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=OCXh0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "集英社",
                "authors": ["松井琳"]
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032570": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032570",
                "title": "僕の心のヤバイやつ",
                "isbn": "9784253226707",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=t90tzwEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "秋田書店",
                "authors": ["桜井のりお"]
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032571": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032571",
                "title": "俺たち運命じゃないのに!",
                "isbn": "9784796416856",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=K5HG0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "芳文社",
                "authors": ["なまいきざかり。"]
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032572": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032572",
                "title": "幼稚園WARS",
                "isbn": "9784088841861",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=enjh0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "集英社",
                "authors": ["千葉侑生"]
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032573": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032573",
                "title": "ヒドゥラ",
                "isbn": "9784785977344",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=TczJ0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "芳文社",
                "authors": ["あさりよしとお"]
            }
        }

        return mock_works.get(work_id)

    def update_work_cover_image(self, work_id: str, cover_url: str) -> bool:
        """Mock update work cover image"""
        logger.info(f"MockNeo4jService: Updating cover for {work_id}: {cover_url}")
        # In real implementation, this would update the database
        # For mock, we just return True
        return True

    def get_works_needing_covers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return mock works that need covers"""
        logger.info(f"MockNeo4jService: Getting works needing covers, limit: {limit}")

        # Return some mock works without covers
        mock_works = [
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032568",
                "title": "キジトラ猫の小梅さん",
                "isbn": "9784785977382"
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032569",
                "title": "Dear Anemone",
                "isbn": "9784088841748"
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032570",
                "title": "僕の心のヤバイやつ",
                "isbn": "9784253226707"
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032571",
                "title": "俺たち運命じゃないのに!",
                "isbn": "9784796416856"
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032572",
                "title": "幼稚園WARS",
                "isbn": "9784088841861"
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032573",
                "title": "ヒドゥラ",
                "isbn": "9784785977344"
            }
        ]

        return mock_works[:limit]

    def close(self):
        """Mock close method"""
        pass
