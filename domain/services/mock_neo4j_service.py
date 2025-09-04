"""
Mock Neo4j service for testing when Neo4j is not available
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MockNeo4jService:
    """Mock service that returns sample data when Neo4j is not available"""

    def search_manga_data_with_related(
        self,
        search_term: str,
        limit: int = 20,
        include_related: bool = True,
        sort_total_volumes: Optional[str] = None,
    ) -> Dict[str, Any]:
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
                        "total_volumes": 30,
                        "source": "mock",
                    },
                },
                {"id": "author_one", "label": "ONE", "type": "author", "properties": {"name": "ONE", "source": "mock"}},
                {
                    "id": "author_murata",
                    "label": "村田雄介",
                    "type": "author",
                    "properties": {"name": "村田雄介", "source": "mock"},
                },
                {
                    "id": "publisher_shueisha",
                    "label": "集英社",
                    "type": "publisher",
                    "properties": {"name": "集英社", "source": "mock"},
                },
            ]

            edges = [
                {
                    "id": "author_one-created-work_one_punch_man",
                    "source": "author_one",
                    "target": "work_one_punch_man",
                    "type": "created",
                    "properties": {"source": "mock"},
                },
                {
                    "id": "author_murata-created-work_one_punch_man",
                    "source": "author_murata",
                    "target": "work_one_punch_man",
                    "type": "created",
                    "properties": {"source": "mock"},
                },
                {
                    "id": "publisher_shueisha-published-work_one_punch_man",
                    "source": "publisher_shueisha",
                    "target": "work_one_punch_man",
                    "type": "published",
                    "properties": {"source": "mock"},
                },
            ]

            if include_related:
                # Add related work
                nodes.extend(
                    [
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
                                "source": "mock",
                            },
                        },
                        {
                            "id": "publisher_shogakukan",
                            "label": "小学館",
                            "type": "publisher",
                            "properties": {"name": "小学館", "source": "mock"},
                        },
                    ]
                )

                edges.extend(
                    [
                        {
                            "id": "author_one-created-work_mob_psycho",
                            "source": "author_one",
                            "target": "work_mob_psycho",
                            "type": "created",
                            "properties": {"source": "mock"},
                        },
                        {
                            "id": "publisher_shogakukan-published-work_mob_psycho",
                            "source": "publisher_shogakukan",
                            "target": "work_mob_psycho",
                            "type": "published",
                            "properties": {"source": "mock"},
                        },
                    ]
                )

            # Apply sorting if requested
            if sort_total_volumes in ("asc", "desc"):
                reverse = sort_total_volumes == "desc"
                work_nodes = [n for n in nodes if n["type"] == "work"]
                other_nodes = [n for n in nodes if n["type"] != "work"]
                work_nodes.sort(
                    key=lambda n: n.get("properties", {}).get("total_volumes")
                    or n.get("properties", {}).get("work_count", 0),
                    reverse=reverse,
                )
                nodes = work_nodes + other_nodes

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
            "published_relationships": 0,
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
                "authors": ["ほしのなつみ"],
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032569": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032569",
                "title": "Dear Anemone",
                "isbn": "9784088841748",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=OCXh0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "集英社",
                "authors": ["松井琳"],
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032570": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032570",
                "title": "僕の心のヤバイやつ",
                "isbn": "9784253226707",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=t90tzwEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "秋田書店",
                "authors": ["桜井のりお"],
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032571": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032571",
                "title": "俺たち運命じゃないのに!",
                "isbn": "9784796416856",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=K5HG0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "芳文社",
                "authors": ["なまいきざかり。"],
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032572": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032572",
                "title": "幼稚園WARS",
                "isbn": "9784088841861",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=enjh0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "集英社",
                "authors": ["千葉侑生"],
            },
            "https://mediaarts-db.artmuseums.go.jp/id/M1032573": {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032573",
                "title": "ヒドゥラ",
                "isbn": "9784785977344",
                "genre": "マンガ",
                "published_date": "2023",
                "cover_image_url": "https://books.google.com/books/content?id=TczJ0AEACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api",
                "publisher": "芳文社",
                "authors": ["あさりよしとお"],
            },
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
                "isbn": "9784785977382",
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032569",
                "title": "Dear Anemone",
                "isbn": "9784088841748",
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032570",
                "title": "僕の心のヤバイやつ",
                "isbn": "9784253226707",
            },
            {
                "id": "https://mediaarts-db.artmuseums.go.jp/id/M1032571",
                "title": "俺たち運命じゃないのに!",
                "isbn": "9784796416856",
            },
            {"id": "https://mediaarts-db.artmuseums.go.jp/id/M1032572", "title": "幼稚園WARS", "isbn": "9784088841861"},
            {"id": "https://mediaarts-db.artmuseums.go.jp/id/M1032573", "title": "ヒドゥラ", "isbn": "9784785977344"},
        ]

        return mock_works[:limit]

    def create_vector_index(
        self, label: str, property_name: str = "embedding", dimension: int = 1536, similarity: str = "cosine"
    ):
        """Mock vector index creation"""
        logger.info(f"MockNeo4jService: Mock vector index created for {label}.{property_name}")

    def search_by_vector(
        self, embedding: List[float], label: str = "Work", property_name: str = "embedding", limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Mock vector search"""
        logger.info(f"MockNeo4jService: Mock vector search for {label} with limit {limit}")

        # Return mock results with similarity scores
        mock_results = [
            {
                "work_id": "mock_work_1",
                "title": "モックワンパンマン",
                "published_date": "2012-06-14",
                "first_published": "2012-06-14",
                "last_published": "2024-01-12",
                "creators": ["ONE", "村田雄介"],
                "publishers": ["小学館"],
                "magazines": ["裏サンデー"],
                "genre": "アクション",
                "isbn": "9784091234567",
                "volume": "1",
                "series_id": "mock_series_1",
                "series_name": "ワンパンマン",
                "similarity_score": 0.95,
            },
            {
                "work_id": "mock_work_2",
                "title": "モブサイコ100",
                "published_date": "2012-04-18",
                "first_published": "2012-04-18",
                "last_published": "2017-12-22",
                "creators": ["ONE"],
                "publishers": ["小学館"],
                "magazines": ["マンガワン"],
                "genre": "超能力",
                "isbn": "9784091234568",
                "volume": "1",
                "series_id": "mock_series_2",
                "series_name": "モブサイコ100",
                "similarity_score": 0.88,
            },
        ]

        return mock_results[:limit]

    def add_embedding_to_work(self, work_id: str, embedding: List[float]) -> bool:
        """Mock embedding addition"""
        logger.info(f"MockNeo4jService: Mock embedding added to work {work_id}")
        return True

    def search_manga_works_with_vector(
        self, search_term: str = None, embedding: List[float] = None, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Mock hybrid search combining text and vector"""
        logger.info(
            f"MockNeo4jService: Mock hybrid search for term '{search_term}' with vector: {embedding is not None}"
        )

        # Combine text search results with vector search results
        results = []

        if search_term:
            # Mock text search results
            text_results = [
                {
                    "work_id": "mock_text_1",
                    "title": f"テキスト検索結果: {search_term}",
                    "published_date": "2020-01-01",
                    "creators": ["テスト作者"],
                    "publishers": ["テスト出版社"],
                    "magazines": ["テスト雑誌"],
                    "genre": "テスト",
                    "search_score": 0.7,
                }
            ]
            results.extend(text_results)

        if embedding:
            # Mock vector search results
            vector_results = [
                {
                    "work_id": "mock_vector_1",
                    "title": "ベクトル検索結果",
                    "published_date": "2021-01-01",
                    "creators": ["ベクトル作者"],
                    "publishers": ["ベクトル出版社"],
                    "magazines": ["ベクトル雑誌"],
                    "genre": "AI",
                    "similarity_score": 0.92,
                    "search_score": 0.92,
                }
            ]
            results.extend(vector_results)

        return results[:limit]

    def search_manga_works(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Mock text search for manga works"""
        logger.info(f"MockNeo4jService: Mock text search for '{search_term}' with limit {limit}")

        mock_results = [
            {
                "work_id": "mock_search_1",
                "title": f"検索結果: {search_term}",
                "published_date": "2023-01-01",
                "first_published": "2023-01-01",
                "last_published": "2024-01-01",
                "creators": ["モック作者"],
                "publishers": ["モック出版社"],
                "magazines": ["モック雑誌"],
                "genre": "モック",
                "isbn": "9784000000000",
                "volume": "1",
                "series_id": "mock_series_search",
                "series_name": f"{search_term}シリーズ",
            }
        ]

        return mock_results[:limit]

    def close(self):
        """Mock close method"""
        pass
