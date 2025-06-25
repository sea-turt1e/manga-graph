import pytest
from unittest.mock import Mock
from domain.use_cases import SearchMangaUseCase
from domain.repositories import MangaRepository


class TestSearchMangaUseCase:
    def setup_method(self):
        self.mock_repository = Mock(spec=MangaRepository)
        self.use_case = SearchMangaUseCase(self.mock_repository)

    def test_execute_with_valid_query(self):
        # Arrange
        query = "ONE PIECE"
        expected_result = {
            "nodes": [
                {
                    "id": "1",
                    "label": "ONE PIECE",
                    "type": "work",
                    "properties": {"title": "ONE PIECE"}
                }
            ],
            "edges": []
        }
        self.mock_repository.search_graph.return_value = expected_result

        # Act
        result = self.use_case.execute(query)

        # Assert
        assert result == expected_result
        self.mock_repository.search_graph.assert_called_once_with(query, 2)

    def test_execute_with_empty_query(self):
        # Arrange
        query = ""

        # Act
        result = self.use_case.execute(query)

        # Assert
        assert result == {"nodes": [], "edges": []}
        self.mock_repository.search_graph.assert_not_called()

    def test_execute_with_node_type_filter(self):
        # Arrange
        query = "test"
        node_types = ["work"]
        mock_result = {
            "nodes": [
                {"id": "1", "type": "work", "label": "Work1", "properties": {}},
                {"id": "2", "type": "author", "label": "Author1", "properties": {}}
            ],
            "edges": []
        }
        expected_result = {
            "nodes": [
                {"id": "1", "type": "work", "label": "Work1", "properties": {}}
            ],
            "edges": []
        }
        self.mock_repository.search_graph.return_value = mock_result

        # Act
        result = self.use_case.execute(query, node_types=node_types)

        # Assert
        assert result == expected_result

    def test_execute_with_edge_type_filter(self):
        # Arrange
        query = "test"
        edge_types = ["created"]
        mock_result = {
            "nodes": [],
            "edges": [
                {"id": "1", "type": "created", "source": "1", "target": "2", "properties": {}},
                {"id": "2", "type": "published", "source": "2", "target": "3", "properties": {}}
            ]
        }
        expected_result = {
            "nodes": [],
            "edges": [
                {"id": "1", "type": "created", "source": "1", "target": "2", "properties": {}}
            ]
        }
        self.mock_repository.search_graph.return_value = mock_result

        # Act
        result = self.use_case.execute(query, edge_types=edge_types)

        # Assert
        assert result == expected_result