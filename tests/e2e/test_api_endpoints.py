from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


class TestAPIEndpoints:
    def test_root_endpoint(self):
        # Act
        response = client.get("/")

        # Assert
        assert response.status_code == 200
        assert response.json() == {"message": "Manga Graph API"}

    def test_health_check_endpoint(self):
        # Act
        response = client.get("/health")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "database" in data

    def test_search_endpoint_with_valid_request(self):
        # Arrange
        search_request = {
            "query": "ONE PIECE",
            "depth": 2
        }

        # Act
        response = client.post("/api/v1/search", json=search_request)

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert "edges" in data
        assert "total_nodes" in data
        assert "total_edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)

    def test_search_endpoint_with_empty_query(self):
        # Arrange
        search_request = {
            "query": "",
            "depth": 2
        }

        # Act
        response = client.post("/api/v1/search", json=search_request)

        # Assert
        assert response.status_code == 200
        data = response.json()
        # Should still return valid response structure even for empty query
        assert "nodes" in data
        assert "edges" in data

    def test_authors_endpoint(self):
        # Act
        response = client.get("/api/v1/authors")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_works_endpoint(self):
        # Act
        response = client.get("/api/v1/works")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_magazines_endpoint(self):
        # Act
        response = client.get("/api/v1/magazines")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_search_endpoint_with_invalid_request(self):
        # Arrange - missing required 'query' field
        invalid_request = {
            "depth": 2
        }

        # Act
        response = client.post("/api/v1/search", json=invalid_request)

        # Assert
        assert response.status_code == 422  # Validation error
