"""
Unit tests for embedding similarity search API functionality
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from presentation.schemas import (EmbeddingSimilaritySearchRequest,
                                  EmbeddingSimilaritySearchResponse,
                                  EmbeddingSimilaritySearchResultItem)


class TestEmbeddingSimilaritySearchRequest:
    """Test cases for EmbeddingSimilaritySearchRequest validation"""

    def test_default_values(self):
        """Test default values are set correctly"""
        request = EmbeddingSimilaritySearchRequest(query="test query")
        assert request.query == "test query"
        assert request.embedding_type == "title_ja"
        assert request.embedding_dims == 256
        assert request.limit == 5
        assert request.threshold == 0.5
        assert request.include_hentai is False

    def test_custom_values(self):
        """Test custom values are accepted"""
        request = EmbeddingSimilaritySearchRequest(
            query="custom query",
            embedding_type="description",
            embedding_dims=1024,
            limit=10,
            threshold=0.7,
            include_hentai=True,
        )
        assert request.query == "custom query"
        assert request.embedding_type == "description"
        assert request.embedding_dims == 1024
        assert request.limit == 10
        assert request.threshold == 0.7
        assert request.include_hentai is True


class TestEmbeddingSimilaritySearchResponse:
    """Test cases for EmbeddingSimilaritySearchResponse"""

    def test_response_creation(self):
        """Test response object creation"""
        items = [
            EmbeddingSimilaritySearchResultItem(
                work_id="12345",
                title_en="Attack on Titan",
                title_ja="進撃の巨人",
                description="Humanity fights titans",
                similarity_score=0.95,
                media_type="Manga",
                genres=["Action", "Fantasy"],
            )
        ]
        response = EmbeddingSimilaritySearchResponse(
            results=items,
            total=1,
            query="titan manga",
            embedding_type="title_ja",
            embedding_dims=256,
            threshold=0.5,
        )
        assert response.total == 1
        assert len(response.results) == 1
        assert response.results[0].work_id == "12345"
        assert response.query == "titan manga"


class TestMangaAnimeNeo4jServiceSimilaritySearch:
    """Test cases for MangaAnimeNeo4jService.search_similar_works"""

    @pytest.fixture
    def mock_service(self):
        """Create a mock service instance"""
        with patch("domain.services.manga_anime_neo4j_service.GraphDatabase") as mock_gdb:
            mock_driver = MagicMock()
            mock_gdb.driver.return_value = mock_driver
            
            from domain.services.manga_anime_neo4j_service import \
                MangaAnimeNeo4jService

            # Set environment variables for the service
            with patch.dict("os.environ", {
                "MANGA_ANIME_NEO4J_URI": "bolt://localhost:7687",
                "MANGA_ANIME_NEO4J_PASSWORD": "testpassword"
            }):
                service = MangaAnimeNeo4jService()
                yield service, mock_driver

    def test_search_similar_works_validates_property_name(self, mock_service):
        """Test that invalid property names are rejected"""
        service, _ = mock_service
        
        with pytest.raises(ValueError) as exc_info:
            service.search_similar_works(
                query_embedding=[0.1] * 256,
                property_name="invalid_property",
                limit=5,
                threshold=0.5,
            )
        assert "Unsupported embedding property" in str(exc_info.value)

    def test_search_similar_works_valid_property_names(self, mock_service):
        """Test that valid property names are accepted"""
        service, mock_driver = mock_service
        
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        mock_session.read_transaction.return_value = []
        
        for prop in ["embedding_title_ja", "embedding_title_en", "embedding_description"]:
            result = service.search_similar_works(
                query_embedding=[0.1] * 256,
                property_name=prop,
                limit=5,
                threshold=0.5,
            )
            assert isinstance(result, list)


class TestEmbeddingSimilarityAPIValidation:
    """Test cases for API validation logic"""

    def test_valid_embedding_types(self):
        """Test valid embedding type values"""
        valid_types = ["title_ja", "title_en", "description"]
        for t in valid_types:
            request = EmbeddingSimilaritySearchRequest(query="test", embedding_type=t)
            assert request.embedding_type == t

    def test_valid_embedding_dims(self):
        """Test valid Matryoshka dimension values"""
        valid_dims = [128, 256, 512, 1024, 2048]
        for d in valid_dims:
            request = EmbeddingSimilaritySearchRequest(query="test", embedding_dims=d)
            assert request.embedding_dims == d

    def test_threshold_range(self):
        """Test threshold value range"""
        # Valid thresholds
        for t in [0.0, 0.5, 1.0]:
            request = EmbeddingSimilaritySearchRequest(query="test", threshold=t)
            assert request.threshold == t

    def test_limit_range(self):
        """Test limit value range"""
        for limit in [1, 5, 50, 100]:
            request = EmbeddingSimilaritySearchRequest(query="test", limit=limit)
            assert request.limit == limit


class TestJinaEmbeddingClientTruncate:
    """Test cases for BaseEmbeddingClient.truncate"""

    @pytest.mark.skipif(True, reason="Requires torch/sentence-transformers")
    def test_truncate_valid_dims(self):
        """Test truncation with valid dimensions"""
        from domain.services.jina_embedding_client import BaseEmbeddingClient

        # Create a 2048-dim vector
        vector = np.random.randn(2048).astype(np.float32)
        
        for dims in [128, 256, 512, 1024, 2048]:
            result = BaseEmbeddingClient.truncate(vector, dims)
            assert len(result) == dims
            assert isinstance(result, list)

    @pytest.mark.skipif(True, reason="Requires torch/sentence-transformers")
    def test_truncate_invalid_dims(self):
        """Test truncation with invalid dimensions"""
        from domain.services.jina_embedding_client import BaseEmbeddingClient
        
        vector = np.random.randn(2048).astype(np.float32)
        
        # dims exceeds vector length
        with pytest.raises(ValueError) as exc_info:
            BaseEmbeddingClient.truncate(vector, 4096)
        assert "dims exceeds vector length" in str(exc_info.value)
        
        # dims is zero or negative
        with pytest.raises(ValueError) as exc_info:
            BaseEmbeddingClient.truncate(vector, 0)
        assert "dims must be positive" in str(exc_info.value)

    @pytest.mark.skipif(True, reason="Requires torch/sentence-transformers")
    def test_truncate_none_vector(self):
        """Test truncation with None vector"""
        from domain.services.jina_embedding_client import BaseEmbeddingClient
        
        result = BaseEmbeddingClient.truncate(None, 256)
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__])
