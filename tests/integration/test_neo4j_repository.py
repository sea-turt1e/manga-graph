from unittest.mock import MagicMock, Mock, patch

import pytest

from infrastructure.database import Neo4jMangaRepository


class TestNeo4jMangaRepository:
    def setup_method(self):
        self.repository = Neo4jMangaRepository(uri="bolt://localhost:7687", user="neo4j", password="password")

    @patch("infrastructure.database.neo4j_repository.GraphDatabase")
    def test_connect_success(self, mock_graph_database):
        # Arrange
        mock_driver = Mock()
        mock_graph_database.driver.return_value = mock_driver

        # Act
        self.repository.connect()

        # Assert
        assert self.repository.driver == mock_driver
        mock_graph_database.driver.assert_called_once_with("bolt://localhost:7687", auth=("neo4j", "password"))

    @patch("infrastructure.database.neo4j_repository.GraphDatabase")
    def test_connect_failure(self, mock_graph_database):
        # Arrange
        mock_graph_database.driver.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            self.repository.connect()

    def test_close_connection(self):
        # Arrange
        mock_driver = Mock()
        self.repository.driver = mock_driver

        # Act
        self.repository.close()

        # Assert
        mock_driver.close.assert_called_once()

    def test_serialize_properties(self):
        # Arrange
        properties = {"string_value": "test", "int_value": 42, "float_value": 3.14}

        # Act
        result = self.repository._serialize_properties(properties)

        # Assert
        assert result == properties

    @patch("infrastructure.database.neo4j_repository.GraphDatabase")
    def test_search_graph_empty_result(self, mock_graph_database):
        # Arrange
        mock_driver = Mock()
        # __enter__/__exit__ を持つ擬似コンテキストを用意
        mock_session_cm = MagicMock()
        mock_session = Mock()
        mock_session_cm.__enter__.return_value = mock_session
        mock_session_cm.__exit__.return_value = None
        mock_tx = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))

        mock_tx.run.return_value = mock_result
        mock_session.execute_read.return_value = {"nodes": [], "edges": []}
        mock_driver.session.return_value = mock_session_cm
        mock_graph_database.driver.return_value = mock_driver

        self.repository.connect()

        # Act
        result = self.repository.search_graph("nonexistent")

        # Assert
        assert result == {"nodes": [], "edges": []}
