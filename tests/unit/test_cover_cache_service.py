import pytest
import json
import time
from pathlib import Path
from unittest.mock import patch, mock_open
from domain.services.cover_cache_service import CoverCacheService, get_cache_service


class TestCoverCacheService:
    @pytest.fixture
    def temp_cache_dir(self, tmp_path):
        """Create temporary cache directory for testing"""
        cache_dir = tmp_path / "test_cache"
        cache_dir.mkdir(exist_ok=True)
        return cache_dir

    @pytest.fixture
    def cache_service(self, temp_cache_dir):
        """Create CoverCacheService instance with temporary directory"""
        return CoverCacheService(cache_dir=str(temp_cache_dir), cache_ttl=3600)

    def test_initialization(self, temp_cache_dir):
        service = CoverCacheService(cache_dir=str(temp_cache_dir), cache_ttl=3600)
        assert service.cache_dir == Path(temp_cache_dir)
        assert service.cache_ttl == 3600
        assert service.cache_file == temp_cache_dir / "cover_cache.json"
        assert isinstance(service._cache, dict)

    def test_cache_directory_creation(self, tmp_path):
        non_existent_dir = tmp_path / "new_cache_dir"
        CoverCacheService(cache_dir=str(non_existent_dir))
        assert non_existent_dir.exists()

    def test_load_existing_cache(self, temp_cache_dir):
        # Create existing cache file
        cache_data = {
            "isbn1": {"cover_url": "http://example.com/cover1.jpg", "timestamp": time.time()}
        }
        cache_file = temp_cache_dir / "cover_cache.json"
        with open(cache_file, "w") as f:
            json.dump(cache_data, f)

        service = CoverCacheService(cache_dir=str(temp_cache_dir))
        assert len(service._cache) == 1
        assert "isbn1" in service._cache

    def test_load_cache_with_invalid_json(self, temp_cache_dir):
        # Create invalid cache file
        cache_file = temp_cache_dir / "cover_cache.json"
        with open(cache_file, "w") as f:
            f.write("invalid json")

        service = CoverCacheService(cache_dir=str(temp_cache_dir))
        assert service._cache == {}

    def test_get_cache_key(self, cache_service):
        assert cache_service._get_cache_key("isbn123") == "isbn123"
        assert cache_service._get_cache_key("isbn123", "Title") == "isbn123:Title"

    def test_is_expired(self, cache_service):
        current_time = time.time()
        # Not expired
        assert not cache_service._is_expired(current_time - 1800)  # 30 minutes ago
        # Expired
        assert cache_service._is_expired(current_time - 7200)  # 2 hours ago

    def test_cache_cover_with_url(self, cache_service):
        cache_service.cache_cover("isbn123", "http://example.com/cover.jpg", "Test Title")

        assert "isbn123:Test Title" in cache_service._cache
        entry = cache_service._cache["isbn123:Test Title"]
        assert entry["cover_url"] == "http://example.com/cover.jpg"
        assert entry["isbn"] == "isbn123"
        assert entry["title"] == "Test Title"
        assert "timestamp" in entry

    def test_cache_cover_without_url(self, cache_service):
        # Test negative caching (no cover available)
        cache_service.cache_cover("isbn456", None)

        assert "isbn456" in cache_service._cache
        entry = cache_service._cache["isbn456"]
        assert entry["cover_url"] is None

    def test_cache_cover_without_isbn(self, cache_service):
        # Should not cache if ISBN is missing
        cache_service.cache_cover("", "http://example.com/cover.jpg")
        cache_service.cache_cover(None, "http://example.com/cover.jpg")

        assert len(cache_service._cache) == 0

    def test_get_cached_cover_hit(self, cache_service):
        # Cache a cover
        cache_service.cache_cover("isbn123", "http://example.com/cover.jpg")

        # Retrieve cached cover
        result = cache_service.get_cached_cover("isbn123")
        assert result == "http://example.com/cover.jpg"

    def test_get_cached_cover_miss(self, cache_service):
        result = cache_service.get_cached_cover("isbn_not_exists")
        assert result is None

    def test_get_cached_cover_expired(self, cache_service):
        # Cache a cover with old timestamp
        cache_service._cache["isbn123"] = {
            "cover_url": "http://example.com/cover.jpg",
            "timestamp": time.time() - 7200  # 2 hours ago
        }

        result = cache_service.get_cached_cover("isbn123")
        assert result is None
        assert "isbn123" not in cache_service._cache

    def test_get_cached_cover_negative_cache(self, cache_service):
        # Cache that no cover exists
        cache_service.cache_cover("isbn123", None)

        result = cache_service.get_cached_cover("isbn123")
        assert result is None

    def test_get_cached_cover_without_isbn(self, cache_service):
        result = cache_service.get_cached_cover("")
        assert result is None

        result = cache_service.get_cached_cover(None)
        assert result is None

    def test_invalidate_cache(self, cache_service):
        # Cache a cover
        cache_service.cache_cover("isbn123", "http://example.com/cover.jpg", "Title")
        assert "isbn123:Title" in cache_service._cache

        # Invalidate
        cache_service.invalidate_cache("isbn123", "Title")
        assert "isbn123:Title" not in cache_service._cache

    def test_invalidate_cache_non_existent(self, cache_service):
        # Should not raise error
        cache_service.invalidate_cache("isbn_not_exists")

    def test_clear_cache(self, cache_service):
        # Add multiple entries
        cache_service.cache_cover("isbn1", "http://example.com/1.jpg")
        cache_service.cache_cover("isbn2", "http://example.com/2.jpg")
        cache_service.cache_cover("isbn3", None)

        assert len(cache_service._cache) == 3

        cache_service.clear_cache()
        assert len(cache_service._cache) == 0

    def test_cleanup_expired(self, cache_service):
        current_time = time.time()

        # Add mixed entries
        cache_service._cache = {
            "isbn1": {"cover_url": "url1", "timestamp": current_time - 7200},  # Expired
            "isbn2": {"cover_url": "url2", "timestamp": current_time - 1800},  # Valid
            "isbn3": {"cover_url": None, "timestamp": current_time - 7200},   # Expired
            "isbn4": {"cover_url": "url4", "timestamp": current_time},        # Valid
        }

        cache_service.cleanup_expired()

        assert len(cache_service._cache) == 2
        assert "isbn1" not in cache_service._cache
        assert "isbn2" in cache_service._cache
        assert "isbn3" not in cache_service._cache
        assert "isbn4" in cache_service._cache

    def test_get_cache_stats(self, cache_service):
        current_time = time.time()

        # Add mixed entries
        cache_service._cache = {
            "isbn1": {"cover_url": "url1", "timestamp": current_time - 7200},  # Expired
            "isbn2": {"cover_url": "url2", "timestamp": current_time},        # Valid with cover
            "isbn3": {"cover_url": None, "timestamp": current_time},          # Valid without cover
            "isbn4": {"cover_url": "url4", "timestamp": current_time - 1800},  # Valid with cover
        }

        stats = cache_service.get_cache_stats()

        assert stats["total_entries"] == 4
        assert stats["valid_entries"] == 3
        assert stats["expired_entries"] == 1
        assert stats["entries_with_covers"] == 3
        assert stats["entries_without_covers"] == 1
        assert stats["cache_ttl_hours"] == 1.0

    @patch('domain.services.cover_cache_service.open', new_callable=mock_open)
    @patch('domain.services.cover_cache_service.Path.exists')
    def test_save_cache(self, mock_exists, mock_file, cache_service):
        mock_exists.return_value = True

        cache_service._cache = {"isbn1": {"cover_url": "url1", "timestamp": time.time()}}
        cache_service._save_cache()

        mock_file.assert_called_once()
        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        assert "isbn1" in written_content
        assert "url1" in written_content

    def test_save_cache_periodic(self, cache_service):
        # Test that cache is saved every 10 entries
        with patch.object(cache_service, '_save_cache') as mock_save:
            for i in range(15):
                cache_service.cache_cover(f"isbn{i}", f"url{i}")

            # Should be called at 10th entry (index 9)
            assert mock_save.call_count == 1

    def test_get_cache_service_singleton(self):
        # Reset singleton
        import domain.services.cover_cache_service
        domain.services.cover_cache_service._cache_service_instance = None

        service1 = get_cache_service()
        service2 = get_cache_service()

        assert service1 is service2
