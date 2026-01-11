"""Unit tests for MAL API client."""

from unittest.mock import MagicMock, patch

import pytest


class TestMalApiClient:
    """Test cases for MalApiClient."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock MAL API client."""
        with patch.dict("os.environ", {"MAL_CLIENT_ID": "test_client_id"}):
            from domain.services.mal_api_client import MalApiClient

            client = MalApiClient()
            yield client
            client.close()

    def test_client_initialization_without_client_id(self):
        """Test that initialization fails without client ID."""
        # Import first, then test with explicit None and cleared env
        # Temporarily clear the env var and pass explicit None
        import os

        from domain.services.mal_api_client import MalApiClient
        original_value = os.environ.pop("MAL_CLIENT_ID", None)
        try:
            with pytest.raises(ValueError) as exc_info:
                MalApiClient(client_id=None)
            assert "MAL_CLIENT_ID" in str(exc_info.value)
        finally:
            # Restore the original value
            if original_value is not None:
                os.environ["MAL_CLIENT_ID"] = original_value

    def test_client_initialization_with_client_id(self, mock_client):
        """Test client initializes correctly with client ID."""
        assert mock_client.client_id == "test_client_id"
        assert mock_client.rate_limit.requests_per_second == 1.0

    def test_rate_limit_config(self):
        """Test custom rate limit configuration."""
        from domain.services.mal_api_client import (MalApiClient,
                                                    RateLimitConfig)

        with patch.dict("os.environ", {"MAL_CLIENT_ID": "test_client_id"}):
            config = RateLimitConfig(requests_per_second=0.5, max_retries=3)
            client = MalApiClient(rate_limit=config)
            
            assert client.rate_limit.requests_per_second == 0.5
            assert client.rate_limit.max_retries == 3
            client.close()


class TestTransformFunctions:
    """Test cases for data transformation functions."""

    def test_transform_mal_manga_to_work_basic(self):
        """Test basic manga transformation."""
        from domain.services.mal_api_client import transform_mal_manga_to_work

        mal_data = {
            "id": 1,
            "title": "Test Manga",
            "alternative_titles": {
                "ja": "テストマンガ",
                "en": "Test Manga EN",
            },
            "synopsis": "A test manga description",
            "mean": 8.5,
            "rank": 100,
            "popularity": 50,
            "num_list_users": 10000,
            "num_scoring_users": 5000,
            "media_type": "manga",
            "status": "finished",
            "num_volumes": 10,
            "num_chapters": 100,
            "genres": [{"id": 1, "name": "Action"}, {"id": 2, "name": "Adventure"}],
            "authors": [{"node": {"first_name": "Test", "last_name": "Author"}, "role": "Story"}],
            "serialization": [{"node": {"name": "Test Magazine"}}],
        }

        result = transform_mal_manga_to_work(mal_data)

        assert result["id"] == "1"
        assert result["mal_id"] == 1
        assert result["title_name"] == "Test Manga"
        assert result["japanese_name"] == "テストマンガ"
        assert result["english_name"] == "Test Manga EN"
        assert result["description"] == "A test manga description"
        assert result["score"] == 8.5
        assert result["ranked"] == 100
        assert result["genres"] == ["Action", "Adventure"]
        assert result["authors"] == ["Test Author"]
        assert result["serialization"] == ["Test Magazine"]
        assert result["volumes"] == 10
        assert result["chapters"] == 100

    def test_transform_mal_manga_with_node_wrapper(self):
        """Test transformation when data is wrapped in 'node' key."""
        from domain.services.mal_api_client import transform_mal_manga_to_work

        mal_data = {
            "node": {
                "id": 2,
                "title": "Nested Manga",
                "media_type": "manga",
            },
            "ranking": {"rank": 5},
        }

        result = transform_mal_manga_to_work(mal_data)

        assert result["id"] == "2"
        assert result["title_name"] == "Nested Manga"

    def test_transform_mal_anime_to_work_basic(self):
        """Test basic anime transformation."""
        from domain.services.mal_api_client import transform_mal_anime_to_work

        mal_data = {
            "id": 100,
            "title": "Test Anime",
            "alternative_titles": {
                "ja": "テストアニメ",
                "en": "Test Anime EN",
            },
            "synopsis": "A test anime description",
            "mean": 9.0,
            "rank": 10,
            "num_episodes": 24,
            "media_type": "tv",
            "status": "finished_airing",
            "start_season": {"year": 2024, "season": "spring"},
            "genres": [{"id": 1, "name": "Action"}],
            "studios": [{"name": "Test Studio"}],
        }

        result = transform_mal_anime_to_work(mal_data)

        assert result["id"] == "anime_100"  # Prefixed with anime_
        assert result["mal_id"] == 100
        assert result["title_name"] == "Test Anime"
        assert result["japanese_name"] == "テストアニメ"
        assert result["episodes"] == 24
        assert result["genres"] == ["Action"]
        assert result["studios"] == ["Test Studio"]
        assert result["start_season"] == "spring 2024"

    def test_transform_handles_missing_fields(self):
        """Test transformation handles missing fields gracefully."""
        from domain.services.mal_api_client import transform_mal_manga_to_work

        mal_data = {
            "id": 3,
            "title": "Minimal Manga",
        }

        result = transform_mal_manga_to_work(mal_data)

        assert result["id"] == "3"
        assert result["title_name"] == "Minimal Manga"
        # Missing fields should not be in result
        assert "japanese_name" not in result
        assert "description" not in result
        assert "genres" not in result or result["genres"] == []


class TestMalRankingTypes:
    """Test cases for ranking type enums."""

    def test_manga_ranking_types(self):
        """Test manga ranking type values."""
        from domain.services.mal_api_client import MalRankingType

        assert MalRankingType.ALL.value == "all"
        assert MalRankingType.MANGA.value == "manga"
        assert MalRankingType.NOVELS.value == "novels"
        assert MalRankingType.BYPOPULARITY.value == "bypopularity"

    def test_media_type_values(self):
        """Test media type enum values."""
        from domain.services.mal_api_client import MalMediaType

        assert MalMediaType.MANGA.value == "manga"
        assert MalMediaType.ANIME.value == "all"
        assert MalMediaType.TV.value == "tv"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
