"""
Unit tests for vector search functionality
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from infrastructure.external.neo4j_repository import Neo4jMangaRepository


class TestVectorSearch:
    """Test cases for vector search functionality"""

    @pytest.fixture
    def mock_driver(self):
        """Create a mock Neo4j driver"""
        driver = MagicMock()
        session = MagicMock()
        # Return the session both directly and via context manager
        driver.session.return_value = session
        driver.session.return_value.__enter__.return_value = session
        driver.session.return_value.__exit__.return_value = None
        return driver, session

    @pytest.fixture
    def repository(self, mock_driver):
        """Create a repository instance with mock driver"""
        driver, session = mock_driver
        repo = Neo4jMangaRepository(driver=driver)
        return repo, session

    def test_create_vector_index_success(self, repository):
        """Test successful vector index creation"""
        repo, session = repository

        # Mock the check query to return 0 (no existing index)
        check_result = Mock()
        check_result.single.return_value = {"count": 0}

        # Mock the create query
        create_result = Mock()

        session.run.side_effect = [check_result, create_result]

        # Test index creation
        repo.create_vector_index("Work", "embedding", 1536, "cosine")

        # Verify queries were called
        assert session.run.call_count == 2

        # Check the create query was called with correct parameters
        create_call = session.run.call_args_list[1]
        assert "CALL db.index.vector.createNodeIndex" in create_call[0][0]

    def test_create_vector_index_already_exists(self, repository):
        """Test vector index creation when index already exists"""
        repo, session = repository

        # Mock the check query to return 1 (existing index)
        check_result = Mock()
        check_result.single.return_value = {"count": 1}

        session.run.return_value = check_result

        # Test index creation
        repo.create_vector_index("Work", "embedding", 1536, "cosine")

        # Verify only check query was called
        assert session.run.call_count == 1

    def test_search_by_vector_success(self, repository):
        """Test successful vector search"""
        repo, session = repository

        # Mock search results
        mock_records = [
            {
                "work_id": "work_1",
                "title": "Attack on Titan",
                "published_date": "2009-09-01",
                "first_published": "2009-09-01",
                "last_published": "2021-04-09",
                "creators": ["Hajime Isayama"],
                "publishers": ["Kodansha"],
                "magazines": ["Bessatsu Shonen Magazine"],
                "genre": "Action",
                "isbn": "123456789",
                "volume": "1",
                "series_id": "series_1",
                "series_name": "Attack on Titan",
                "score": 0.95,
            }
        ]

        # Mock session.run result
        result = Mock()
        result.__iter__ = Mock(return_value=iter(mock_records))
        session.run.return_value = result

        # Test vector search
        embedding = [0.1] * 1536
        results = repo.search_by_vector(embedding, "Work", "embedding", 10)

        # Verify results
        assert len(results) == 1
        assert results[0]["work_id"] == "work_1"
        assert results[0]["title"] == "Attack on Titan"
        assert results[0]["similarity_score"] == 0.95

    def test_search_by_vector_error(self, repository):
        """Test vector search with error"""
        repo, session = repository

        # Mock session.run to raise an exception
        session.run.side_effect = Exception("Vector index not found")

        # Test vector search
        embedding = [0.1] * 1536
        results = repo.search_by_vector(embedding, "Work", "embedding", 10)

        # Verify empty results on error
        assert results == []

    def test_add_embedding_to_work_success(self, repository):
        """Test successful embedding addition"""
        repo, session = repository

        # Mock successful result
        result = Mock()
        result.single.return_value = {"work_id": "work_1"}
        session.run.return_value = result

        # Test embedding addition
        embedding = [0.1] * 1536
        success = repo.add_embedding_to_work("work_1", embedding)

        # Verify success
        assert success is True

        # Verify query was called with correct parameters
        call_args = session.run.call_args
        assert "SET w.embedding = $embedding" in call_args[0][0]
        assert call_args[1]["work_id"] == "work_1"
        assert call_args[1]["embedding"] == embedding

    def test_add_embedding_to_work_not_found(self, repository):
        """Test embedding addition for non-existent work"""
        repo, session = repository

        # Mock no result (work not found)
        result = Mock()
        result.single.return_value = None
        session.run.return_value = result

        # Test embedding addition
        embedding = [0.1] * 1536
        success = repo.add_embedding_to_work("nonexistent_work", embedding)

        # Verify failure
        assert success is False

    def test_search_manga_works_with_vector_hybrid(self, repository):
        """Test hybrid search combining text and vector"""
        repo, session = repository

        # Mock the search_manga_works method
        with patch.object(repo, "search_manga_works") as mock_text_search:
            with patch.object(repo, "search_by_vector") as mock_vector_search:

                # Mock text search results
                mock_text_search.return_value = [
                    {"work_id": "work_1", "title": "Attack on Titan Vol 1"},
                    {"work_id": "work_2", "title": "Attack on Titan Vol 2"},
                ]

                # Mock vector search results
                mock_vector_search.return_value = [
                    {"work_id": "work_1", "title": "Attack on Titan Vol 1", "similarity_score": 0.9},
                    {"work_id": "work_3", "title": "Similar Manga", "similarity_score": 0.8},
                ]

                # Test hybrid search
                embedding = [0.1] * 1536
                results = repo.search_manga_works_with_vector(
                    search_term="Attack on Titan", embedding=embedding, limit=10
                )

                # Verify both searches were called
                mock_text_search.assert_called_once_with("Attack on Titan", 5)
                mock_vector_search.assert_called_once_with(embedding, limit=5)

                # Verify results are combined and deduplicated
                assert len(results) == 3

                # Verify work_1 has boosted score (found in both searches)
                work_1 = next(w for w in results if w["work_id"] == "work_1")
                assert work_1["search_score"] > 0.8  # Should be boosted

    def test_search_manga_works_with_vector_vector_only(self, repository):
        """Test vector-only search"""
        repo, session = repository

        # Mock the search_by_vector method
        with patch.object(repo, "search_by_vector") as mock_vector_search:
            mock_vector_search.return_value = [{"work_id": "work_1", "title": "Similar Manga", "similarity_score": 0.9}]

            # Test vector-only search
            embedding = [0.1] * 1536
            results = repo.search_manga_works_with_vector(search_term=None, embedding=embedding, limit=10)

            # Verify vector search was called
            mock_vector_search.assert_called_once_with(embedding, limit=10)
            assert len(results) == 1

    def test_search_manga_works_with_vector_text_only(self, repository):
        """Test text-only search via vector search method"""
        repo, session = repository

        # Mock the search_manga_works method
        with patch.object(repo, "search_manga_works") as mock_text_search:
            mock_text_search.return_value = [{"work_id": "work_1", "title": "Attack on Titan"}]

            # Test text-only search
            results = repo.search_manga_works_with_vector(search_term="Attack on Titan", embedding=None, limit=10)

            # Verify text search was called
            mock_text_search.assert_called_once_with("Attack on Titan", 10)
            assert len(results) == 1


if __name__ == "__main__":
    pytest.main([__file__])
