#!/usr/bin/env python3
"""
Book cover image service for retrieving cover images from various sources
"""
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from .cover_cache_service import get_cache_service

logger = logging.getLogger(__name__)


class CoverImageService:
    """Service for retrieving book cover images from multiple sources"""

    def __init__(self):
        self.google_books_base_url = "https://www.googleapis.com/books/v1/volumes"
        self.openbd_base_url = "https://api.openbd.jp/v1/get"
        self.request_delay = 0.1  # Rate limiting delay
        self.cache_service = get_cache_service()

    def get_cover_image_url(self, isbn: str, title: str = None) -> Optional[str]:
        """
        Get cover image URL for a book by ISBN

        Args:
            isbn: ISBN of the book
            title: Optional title for fallback search

        Returns:
            Cover image URL if found, None otherwise
        """
        if not isbn:
            logger.warning("No ISBN provided for cover image lookup")
            return None

        # Check cache first
        cached_url = self.cache_service.get_cached_cover(isbn, title)
        if cached_url is not None:
            logger.debug(f"Using cached cover for ISBN {isbn}")
            return cached_url

        # Try Google Books API first (most reliable for covers)
        cover_url = self._get_cover_from_google_books(isbn, title)
        if cover_url:
            # Cache the successful result
            self.cache_service.cache_cover(isbn, cover_url, title)
            return cover_url

        # Try openBD as fallback
        cover_url = self._get_cover_from_openbd(isbn)
        if cover_url:
            # Cache the successful result
            self.cache_service.cache_cover(isbn, cover_url, title)
            return cover_url

        # Cache the negative result to avoid repeated API calls
        self.cache_service.cache_cover(isbn, None, title)
        logger.info(f"No cover image found for ISBN: {isbn}")
        return None

    def _get_cover_from_google_books(self, isbn: str, title: str = None) -> Optional[str]:
        """
        Get cover image from Google Books API

        Args:
            isbn: ISBN to search for
            title: Optional title for enhanced search

        Returns:
            Cover image URL if found
        """
        try:
            # First try ISBN search
            query = f"isbn:{isbn}"
            url = f"{self.google_books_base_url}?q={query}"

            logger.debug(f"Searching Google Books with ISBN: {isbn}")
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])

                if items:
                    # Get the first item's image links
                    volume_info = items[0].get("volumeInfo", {})
                    image_links = volume_info.get("imageLinks", {})

                    if image_links:
                        # Prefer larger images
                        for size in ["large", "medium", "thumbnail", "smallThumbnail"]:
                            if size in image_links:
                                cover_url = image_links[size]
                                # Convert to HTTPS if needed
                                if cover_url.startswith("http://"):
                                    cover_url = cover_url.replace("http://", "https://")

                                logger.info(f"Found cover via Google Books (ISBN): {cover_url}")
                                return cover_url

                # If ISBN search failed and we have a title, try title search
                if title and not items:
                    time.sleep(self.request_delay)
                    title_query = f"intitle:{title}"
                    title_url = f"{self.google_books_base_url}?q={title_query}"

                    logger.debug(f"Searching Google Books with title: {title}")
                    title_response = requests.get(title_url, timeout=10)

                    if title_response.status_code == 200:
                        title_data = title_response.json()
                        title_items = title_data.get("items", [])

                        if title_items:
                            # Look for best match (exact title)
                            for item in title_items[:3]:  # Check first 3 results
                                volume_info = item.get("volumeInfo", {})
                                item_title = volume_info.get("title", "").lower()

                                if title.lower() in item_title or item_title in title.lower():
                                    image_links = volume_info.get("imageLinks", {})
                                    if image_links:
                                        for size in ["large", "medium", "thumbnail", "smallThumbnail"]:
                                            if size in image_links:
                                                cover_url = image_links[size]
                                                if cover_url.startswith("http://"):
                                                    cover_url = cover_url.replace("http://", "https://")

                                                logger.info(f"Found cover via Google Books (title): {cover_url}")
                                                return cover_url
            else:
                logger.warning(f"Google Books API error: {response.status_code}")

        except Exception as e:
            logger.error(f"Error fetching cover from Google Books: {e}")

        return None

    def _get_cover_from_openbd(self, isbn: str) -> Optional[str]:
        """
        Get cover image from openBD API

        Args:
            isbn: ISBN to search for

        Returns:
            Cover image URL if found
        """
        try:
            url = f"{self.openbd_base_url}?isbn={isbn}"

            logger.debug(f"Searching openBD with ISBN: {isbn}")
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()

                if data and len(data) > 0 and data[0]:
                    book_data = data[0]
                    summary = book_data.get("summary", {})
                    cover_url = summary.get("cover", "")

                    if cover_url:
                        logger.info(f"Found cover via openBD: {cover_url}")
                        return cover_url
            else:
                logger.warning(f"openBD API error: {response.status_code}")

        except Exception as e:
            logger.error(f"Error fetching cover from openBD: {e}")

        return None

    def get_placeholder_image_url(self, genre: str = None, publisher: str = None) -> str:
        """
        Get placeholder image URL based on genre or publisher

        Args:
            genre: Genre of the work
            publisher: Publisher of the work

        Returns:
            Placeholder image URL
        """
        # Generate placeholder based on genre/publisher
        if genre and "マンガ" in genre:
            return "/static/images/placeholders/manga-cover.png"
        elif publisher:
            # Publisher-specific placeholder
            publisher_lower = publisher.lower()
            if "集英社" in publisher:
                return "/static/images/placeholders/shueisha-cover.png"
            elif "講談社" in publisher:
                return "/static/images/placeholders/kodansha-cover.png"
            elif "小学館" in publisher:
                return "/static/images/placeholders/shogakukan-cover.png"

        # Default placeholder
        return "/static/images/placeholders/default-cover.png"

    def validate_cover_url(self, url: str) -> bool:
        """
        Validate that a cover URL is accessible

        Args:
            url: URL to validate

        Returns:
            True if URL is accessible, False otherwise
        """
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    def get_cover_with_fallback(
        self, isbn: str, title: str = None, genre: str = None, publisher: str = None
    ) -> Dict[str, Any]:
        """
        Get cover image with fallback to placeholder

        Args:
            isbn: ISBN of the book
            title: Title of the book
            genre: Genre for placeholder selection
            publisher: Publisher for placeholder selection

        Returns:
            Dictionary with cover_url and source information
        """
        # Try to get actual cover
        cover_url = self.get_cover_image_url(isbn, title)

        if cover_url and self.validate_cover_url(cover_url):
            return {"cover_url": cover_url, "source": "api", "has_real_cover": True}

        # Fallback to placeholder
        placeholder_url = self.get_placeholder_image_url(genre, publisher)

        return {"cover_url": placeholder_url, "source": "placeholder", "has_real_cover": False}


# Singleton instance
_cover_service_instance = None


def get_cover_service() -> CoverImageService:
    """Get singleton instance of CoverImageService"""
    global _cover_service_instance
    if _cover_service_instance is None:
        _cover_service_instance = CoverImageService()
    return _cover_service_instance
