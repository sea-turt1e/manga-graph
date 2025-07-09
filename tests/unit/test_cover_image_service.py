import pytest
from unittest.mock import Mock, patch
from domain.services.cover_image_service import CoverImageService, get_cover_service


class TestCoverImageService:
    @pytest.fixture
    def mock_cache_service(self):
        """Mock cache service"""
        mock = Mock()
        mock.get_cached_cover.return_value = None
        mock.cache_cover.return_value = None
        return mock

    @pytest.fixture
    def cover_service(self, mock_cache_service):
        """Create CoverImageService instance with mocked cache"""
        with patch('domain.services.cover_image_service.get_cache_service', return_value=mock_cache_service):
            service = CoverImageService()
            service.cache_service = mock_cache_service
            return service

    def test_initialization(self, cover_service):
        assert cover_service.google_books_base_url == "https://www.googleapis.com/books/v1/volumes"
        assert cover_service.openbd_base_url == "https://api.openbd.jp/v1/get"
        assert cover_service.request_delay == 0.1

    def test_get_cover_image_url_no_isbn(self, cover_service):
        result = cover_service.get_cover_image_url("")
        assert result is None

        result = cover_service.get_cover_image_url(None)
        assert result is None

    def test_get_cover_image_url_from_cache(self, cover_service, mock_cache_service):
        mock_cache_service.get_cached_cover.return_value = "http://cached.example.com/cover.jpg"

        result = cover_service.get_cover_image_url("isbn123", "Test Title")

        assert result == "http://cached.example.com/cover.jpg"
        mock_cache_service.get_cached_cover.assert_called_once_with("isbn123", "Test Title")
        mock_cache_service.cache_cover.assert_not_called()

    @patch('requests.get')
    def test_get_cover_from_openbd_success(self, mock_get, cover_service):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            "summary": {
                "cover": "http://example.com/openbd_cover.jpg"
            }
        }]
        mock_get.return_value = mock_response

        result = cover_service._get_cover_from_openbd("isbn123")

        assert result == "http://example.com/openbd_cover.jpg"
        mock_get.assert_called_once_with(
            "https://api.openbd.jp/v1/get?isbn=isbn123",
            timeout=10
        )

    @patch('requests.get')
    def test_get_cover_from_openbd_no_cover(self, mock_get, cover_service):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            "summary": {}
        }]
        mock_get.return_value = mock_response

        result = cover_service._get_cover_from_openbd("isbn123")

        assert result is None

    @patch('requests.get')
    def test_get_cover_from_openbd_empty_response(self, mock_get, cover_service):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = cover_service._get_cover_from_openbd("isbn123")

        assert result is None

    @patch('requests.get')
    def test_get_cover_from_openbd_error(self, mock_get, cover_service):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = cover_service._get_cover_from_openbd("isbn123")

        assert result is None

    @patch('requests.get')
    def test_get_cover_from_openbd_exception(self, mock_get, cover_service):
        mock_get.side_effect = Exception("Network error")

        result = cover_service._get_cover_from_openbd("isbn123")

        assert result is None

    @patch('requests.get')
    def test_get_cover_image_url_with_openbd(self, mock_get, cover_service, mock_cache_service):
        # Mock openBD response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            "summary": {
                "cover": "http://example.com/openbd_cover.jpg"
            }
        }]
        mock_get.return_value = mock_response

        result = cover_service.get_cover_image_url("isbn123", "Test Title")

        assert result == "http://example.com/openbd_cover.jpg"
        mock_cache_service.cache_cover.assert_called_once_with(
            "isbn123", "http://example.com/openbd_cover.jpg", "Test Title"
        )

    @patch('requests.get')
    def test_get_cover_image_url_no_cover_found(self, mock_get, cover_service, mock_cache_service):
        # Mock failed openBD response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = cover_service.get_cover_image_url("isbn123", "Test Title")

        assert result is None
        # Should cache negative result
        mock_cache_service.cache_cover.assert_called_once_with("isbn123", None, "Test Title")

    def test_get_placeholder_image_url_manga(self, cover_service):
        result = cover_service.get_placeholder_image_url(genre="少年マンガ")
        assert result == "/static/images/placeholders/manga-cover.png"

    def test_get_placeholder_image_url_shueisha(self, cover_service):
        result = cover_service.get_placeholder_image_url(publisher="集英社")
        assert result == "/static/images/placeholders/shueisha-cover.png"

    def test_get_placeholder_image_url_kodansha(self, cover_service):
        result = cover_service.get_placeholder_image_url(publisher="講談社")
        assert result == "/static/images/placeholders/kodansha-cover.png"

    def test_get_placeholder_image_url_shogakukan(self, cover_service):
        result = cover_service.get_placeholder_image_url(publisher="小学館")
        assert result == "/static/images/placeholders/shogakukan-cover.png"

    def test_get_placeholder_image_url_default(self, cover_service):
        result = cover_service.get_placeholder_image_url()
        assert result == "/static/images/placeholders/default-cover.png"

        result = cover_service.get_placeholder_image_url(genre="小説", publisher="Unknown")
        assert result == "/static/images/placeholders/default-cover.png"

    @patch('requests.head')
    def test_validate_cover_url_valid(self, mock_head, cover_service):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = cover_service.validate_cover_url("http://example.com/cover.jpg")

        assert result is True
        mock_head.assert_called_once_with("http://example.com/cover.jpg", timeout=5)

    @patch('requests.head')
    def test_validate_cover_url_invalid(self, mock_head, cover_service):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_head.return_value = mock_response

        result = cover_service.validate_cover_url("http://example.com/cover.jpg")

        assert result is False

    @patch('requests.head')
    def test_validate_cover_url_exception(self, mock_head, cover_service):
        mock_head.side_effect = Exception("Network error")

        result = cover_service.validate_cover_url("http://example.com/cover.jpg")

        assert result is False

    @patch('requests.get')
    @patch('requests.head')
    def test_get_cover_with_fallback_real_cover(self, mock_head, mock_get, cover_service):
        # Mock successful cover fetch
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            "summary": {
                "cover": "http://example.com/real_cover.jpg"
            }
        }]
        mock_get.return_value = mock_response

        # Mock successful validation
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head.return_value = mock_head_response

        result = cover_service.get_cover_with_fallback(
            "isbn123", "Test Title", "マンガ", "集英社"
        )

        assert result == {
            "cover_url": "http://example.com/real_cover.jpg",
            "source": "api",
            "has_real_cover": True
        }

    @patch('requests.get')
    def test_get_cover_with_fallback_placeholder(self, mock_get, cover_service):
        # Mock failed cover fetch
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = cover_service.get_cover_with_fallback(
            "isbn123", "Test Title", "少年マンガ", "集英社"
        )

        assert result == {
            "cover_url": "/static/images/placeholders/manga-cover.png",
            "source": "placeholder",
            "has_real_cover": False
        }

    def test_get_cover_service_singleton(self):
        # Reset singleton
        import domain.services.cover_image_service
        domain.services.cover_image_service._cover_service_instance = None

        service1 = get_cover_service()
        service2 = get_cover_service()

        assert service1 is service2
