#!/usr/bin/env python3
"""
Cover image cache service for storing and retrieving cached cover URLs
"""
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CoverCacheService:
    """Service for caching cover image URLs to reduce API calls"""

    def __init__(self, cache_dir: str = None, cache_ttl: int = 86400):
        """
        Initialize cache service

        Args:
            cache_dir: Directory to store cache files (default: ./cache/covers)
            cache_ttl: Cache time-to-live in seconds (default: 24 hours)
        """
        self.cache_dir = Path(cache_dir or "./cache/covers")
        self.cache_ttl = cache_ttl

        # Create cache directory if it doesn't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Cache file for ISBN -> URL mapping
        self.cache_file = self.cache_dir / "cover_cache.json"

        # Load existing cache
        self._cache = self._load_cache()

        logger.info(f"Cover cache initialized: {self.cache_dir}, TTL: {cache_ttl}s")

    def _load_cache(self) -> Dict[str, Any]:
        """Load cache from disk"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
                    logger.info(f"Loaded {len(cache_data)} items from cache")
                    return cache_data
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")

        return {}

    def _save_cache(self):
        """Save cache to disk"""
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

    def _is_expired(self, timestamp: float) -> bool:
        """Check if cache entry is expired"""
        return (time.time() - timestamp) > self.cache_ttl

    def _get_cache_key(self, isbn: str, title: str = None) -> str:
        """Generate cache key from ISBN and optional title"""
        if title:
            return f"{isbn}:{title}"
        return isbn

    def get_cached_cover(self, isbn: str, title: str = None) -> Optional[str]:
        """
        Get cached cover URL if available and not expired

        Args:
            isbn: ISBN of the work
            title: Optional title for enhanced caching

        Returns:
            Cached cover URL if available, None otherwise
        """
        if not isbn:
            return None

        cache_key = self._get_cache_key(isbn, title)

        if cache_key in self._cache:
            entry = self._cache[cache_key]
            timestamp = entry.get("timestamp", 0)

            if not self._is_expired(timestamp):
                cover_url = entry.get("cover_url")
                if cover_url:
                    logger.debug(f"Cache hit for {cache_key}: {cover_url}")
                    return cover_url
                else:
                    logger.debug(f"Cache hit for {cache_key}: no cover (negative cache)")
                    return None
            else:
                # Remove expired entry
                logger.debug(f"Cache expired for {cache_key}")
                del self._cache[cache_key]
                self._save_cache()

        logger.debug(f"Cache miss for {cache_key}")
        return None

    def cache_cover(self, isbn: str, cover_url: Optional[str], title: str = None):
        """
        Cache cover URL (or lack thereof)

        Args:
            isbn: ISBN of the work
            cover_url: Cover URL to cache (None for negative cache)
            title: Optional title for enhanced caching
        """
        if not isbn:
            return

        cache_key = self._get_cache_key(isbn, title)

        entry = {"cover_url": cover_url, "timestamp": time.time(), "isbn": isbn, "title": title}

        self._cache[cache_key] = entry

        logger.debug(f"Cached {'cover' if cover_url else 'no cover'} for {cache_key}")

        # Save cache periodically or when it grows large
        if len(self._cache) % 10 == 0:
            self._save_cache()

    def invalidate_cache(self, isbn: str, title: str = None):
        """
        Remove entry from cache

        Args:
            isbn: ISBN of the work
            title: Optional title
        """
        cache_key = self._get_cache_key(isbn, title)

        if cache_key in self._cache:
            del self._cache[cache_key]
            self._save_cache()
            logger.info(f"Invalidated cache for {cache_key}")

    def clear_cache(self):
        """Clear all cache entries"""
        self._cache.clear()
        self._save_cache()
        logger.info("Cache cleared")

    def cleanup_expired(self):
        """Remove all expired cache entries"""
        expired_keys = []

        for key, entry in self._cache.items():
            timestamp = entry.get("timestamp", 0)
            if self._is_expired(timestamp):
                expired_keys.append(key)

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            self._save_cache()
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self._cache)
        expired_count = 0
        with_covers = 0
        without_covers = 0

        for entry in self._cache.values():
            timestamp = entry.get("timestamp", 0)
            if self._is_expired(timestamp):
                expired_count += 1

            if entry.get("cover_url"):
                with_covers += 1
            else:
                without_covers += 1

        return {
            "total_entries": total_entries,
            "valid_entries": total_entries - expired_count,
            "expired_entries": expired_count,
            "entries_with_covers": with_covers,
            "entries_without_covers": without_covers,
            "cache_file": str(self.cache_file),
            "cache_ttl_hours": self.cache_ttl / 3600,
        }


# Singleton instance
_cache_service_instance = None


def get_cache_service() -> CoverCacheService:
    """Get singleton instance of CoverCacheService"""
    global _cache_service_instance
    if _cache_service_instance is None:
        _cache_service_instance = CoverCacheService()
    return _cache_service_instance
