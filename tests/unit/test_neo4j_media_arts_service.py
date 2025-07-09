from unittest.mock import Mock, patch
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService
from domain.services.mock_neo4j_service import MockNeo4jService
from infrastructure.external.neo4j_repository import Neo4jMangaRepository


class TestNeo4jMediaArtsService:
    @pytest.fixture
    def mock_neo4j_repository(self):
        """Mock Neo4j repository"""
        mock = Mock(spec=Neo4jMangaRepository)
        mock.get_database_statistics.return_value = {"work_count": 100}
        return mock

    @pytest.fixture
    def service_with_mock_repo(self, mock_neo4j_repository):
        """Create service with mocked repository"""
        return Neo4jMediaArtsService(neo4j_repository=mock_neo4j_repository)

    def test_initialization_with_provided_repository(self, mock_neo4j_repository):
        service = Neo4jMediaArtsService(neo4j_repository=mock_neo4j_repository)
        assert service.neo4j_repository == mock_neo4j_repository
        assert service.use_mock is False

    @patch.dict('os.environ', {'USE_MOCK_NEO4J': 'true'})
    def test_initialization_with_mock_env(self):
        service = Neo4jMediaArtsService()
        assert isinstance(service.neo4j_repository, MockNeo4jService)
        assert service.use_mock is True

    @patch('infrastructure.external.neo4j_repository.Neo4jMangaRepository')
    def test_initialization_with_empty_database(self, mock_repo_class):
        mock_instance = Mock()
        mock_instance.get_database_statistics.return_value = {"work_count": 0}
        mock_repo_class.return_value = mock_instance

        service = Neo4jMediaArtsService()
        assert isinstance(service.neo4j_repository, MockNeo4jService)
        assert service.use_mock is True

    @patch('infrastructure.external.neo4j_repository.Neo4jMangaRepository')
    def test_initialization_with_connection_error(self, mock_repo_class):
        mock_repo_class.side_effect = Exception("Connection failed")

        service = Neo4jMediaArtsService()
        assert isinstance(service.neo4j_repository, MockNeo4jService)
        assert service.use_mock is True

    def test_search_manga_data_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_result = {
            "nodes": [
                {
                    "id": "work_1",
                    "label": "Test Manga",
                    "type": "work",
                    "properties": {
                        "title": "Test Manga",
                        "genre": "Manga",
                        "published_date": "2023"
                    }
                }
            ],
            "edges": []
        }
        mock_neo4j_repository.search_manga_data_with_related.return_value = mock_result

        result = service_with_mock_repo.search_manga_data("Test", limit=10)

        assert len(result["nodes"]) == 1
        assert result["nodes"][0]["id"] == "work_1"
        mock_neo4j_repository.search_manga_data_with_related.assert_called_once_with("Test", 10, include_related=False)

    def test_search_manga_data_error(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.search_manga_data_with_related.side_effect = Exception("Search error")

        result = service_with_mock_repo.search_manga_data("Test")

        assert result == {"nodes": [], "edges": []}

    def test_search_manga_data_with_related(self, service_with_mock_repo, mock_neo4j_repository):
        mock_result = {
            "nodes": [
                {
                    "id": "work_1",
                    "label": "Test Manga",
                    "type": "work",
                    "properties": {"title": "Test Manga"}
                },
                {
                    "id": "work_2",
                    "label": "Related Manga",
                    "type": "work",
                    "properties": {"title": "Related Manga"}
                }
            ],
            "edges": [
                {
                    "id": "edge_1",
                    "source": "work_1",
                    "target": "work_2",
                    "type": "related"
                }
            ]
        }
        mock_neo4j_repository.search_manga_data_with_related.return_value = mock_result

        result = service_with_mock_repo.search_manga_data_with_related("Test", include_related=True)

        assert len(result["nodes"]) == 2
        assert len(result["edges"]) == 1

    def test_get_creator_works_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_works = [
            {
                "work_id": "work_1",
                "title": "Manga 1",
                "published_date": "2020",
                "genre": "Shonen"
            },
            {
                "work_id": "work_2",
                "title": "Manga 2",
                "published_date": "2021",
                "genre": "Seinen"
            }
        ]
        mock_neo4j_repository.search_manga_works.return_value = mock_works

        result = service_with_mock_repo.get_creator_works("Test Author")

        assert len(result["nodes"]) == 3  # 1 author + 2 works
        assert len(result["edges"]) == 2  # 2 created relationships

        # Check author node
        author_node = next(n for n in result["nodes"] if n["type"] == "author")
        assert author_node["label"] == "Test Author"

        # Check work nodes
        work_nodes = [n for n in result["nodes"] if n["type"] == "work"]
        assert len(work_nodes) == 2

    def test_get_creator_works_empty(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.search_manga_works.return_value = []

        result = service_with_mock_repo.get_creator_works("Unknown Author")

        assert result == {"nodes": [], "edges": []}

    def test_get_database_statistics_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_stats = {"work_count": 100, "author_count": 50}
        mock_neo4j_repository.get_database_statistics.return_value = mock_stats

        result = service_with_mock_repo.get_database_statistics()

        assert result == mock_stats

    def test_get_database_statistics_error(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.get_database_statistics.side_effect = Exception("Stats error")

        result = service_with_mock_repo.get_database_statistics()

        assert result == {}

    def test_get_work_by_id_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_work = {
            "work_id": "work_1",
            "title": "Test Manga",
            "isbn": "123456789"
        }
        mock_neo4j_repository.get_work_by_id.return_value = mock_work

        result = service_with_mock_repo.get_work_by_id("work_1")

        assert result == mock_work

    def test_get_work_by_id_not_found(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.get_work_by_id.return_value = None

        result = service_with_mock_repo.get_work_by_id("nonexistent")

        assert result is None

    def test_update_work_cover_image_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.update_work_cover_image.return_value = True

        result = service_with_mock_repo.update_work_cover_image("work_1", "http://example.com/cover.jpg")

        assert result is True
        mock_neo4j_repository.update_work_cover_image.assert_called_once_with("work_1", "http://example.com/cover.jpg")

    def test_update_work_cover_image_error(self, service_with_mock_repo, mock_neo4j_repository):
        mock_neo4j_repository.update_work_cover_image.side_effect = Exception("Update error")

        result = service_with_mock_repo.update_work_cover_image("work_1", "http://example.com/cover.jpg")

        assert result is False

    def test_get_works_needing_covers_success(self, service_with_mock_repo, mock_neo4j_repository):
        mock_works = [
            {"work_id": "work_1", "title": "Manga 1", "isbn": "123"},
            {"work_id": "work_2", "title": "Manga 2", "isbn": "456"}
        ]
        mock_neo4j_repository.get_works_needing_covers.return_value = mock_works

        result = service_with_mock_repo.get_works_needing_covers(limit=50)

        assert len(result) == 2
        assert result == mock_works

    def test_convert_neo4j_to_graph_format_work_node(self, service_with_mock_repo):
        neo4j_result = {
            "nodes": [
                {
                    "id": "work_1",
                    "label": "Test Manga",
                    "type": "work",
                    "properties": {
                        "title": "Test Manga",
                        "published_date": "2023",
                        "genre": "Shonen",
                        "isbn": "123456789",
                        "volume": "5",
                        "is_series": False,
                        "publishers": ["Publisher A (ABC)", "Publisher B"],
                        "creators": ["Author 1", "Author 2"]
                    }
                }
            ],
            "edges": []
        }

        result = service_with_mock_repo._convert_neo4j_to_graph_format(neo4j_result)

        assert len(result["nodes"]) == 1
        node = result["nodes"][0]
        assert node["id"] == "work_1"
        assert node["properties"]["title"] == "Test Manga"
        assert node["properties"]["volume"] == "5"
        assert node["properties"]["is_series"] is False
        assert len(node["properties"]["publishers"]) == 2
        assert node["properties"]["source"] == "neo4j"

    def test_convert_neo4j_to_graph_format_series_node(self, service_with_mock_repo):
        neo4j_result = {
            "nodes": [
                {
                    "id": "work_1",
                    "label": "Test Series",
                    "type": "work",
                    "properties": {
                        "title": "Test Series",
                        "is_series": True,
                        "volume": "1-10",
                        "work_count": 10,
                        "published_date": "2020-2023"
                    }
                }
            ],
            "edges": []
        }

        result = service_with_mock_repo._convert_neo4j_to_graph_format(neo4j_result)

        node = result["nodes"][0]
        assert node["properties"]["volume"] == "1"  # Series always show volume 1
        assert node["properties"]["is_series"] is True
        assert node["properties"]["series_volumes"] == "1-10"
        assert node["properties"]["date_range"] == "2020-2023"

    def test_convert_neo4j_to_graph_format_edges(self, service_with_mock_repo):
        neo4j_result = {
            "nodes": [],
            "edges": [
                {
                    "id": "edge_1",
                    "source": "node_1",
                    "target": "node_2",
                    "type": "created",
                    "properties": {"since": "2020"}
                },
                {
                    "from": "node_3",  # Old format
                    "to": "node_4",
                    "type": "published"
                }
            ]
        }

        result = service_with_mock_repo._convert_neo4j_to_graph_format(neo4j_result)

        assert len(result["edges"]) == 2

        # Check first edge (new format)
        edge1 = result["edges"][0]
        assert edge1["id"] == "edge_1"
        assert edge1["source"] == "node_1"
        assert edge1["target"] == "node_2"
        assert edge1["properties"]["since"] == "2020"

        # Check second edge (old format)
        edge2 = result["edges"][1]
        assert edge2["source"] == "node_3"
        assert edge2["target"] == "node_4"
        assert edge2["type"] == "published"

    def test_close(self, service_with_mock_repo, mock_neo4j_repository):
        service_with_mock_repo.close()
        mock_neo4j_repository.close.assert_called_once()

    @patch.dict('os.environ', {'USE_MOCK_NEO4J': 'true'})
    def test_search_with_mock_service(self):
        service = Neo4jMediaArtsService()

        # Mock service should return data in correct format
        result = service.search_manga_data_with_related("Test")

        # Just verify structure, mock service content may vary
        assert "nodes" in result
        assert "edges" in result
        assert isinstance(result["nodes"], list)
        assert isinstance(result["edges"], list)
