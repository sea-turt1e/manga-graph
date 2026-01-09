"""MyAnimeList API v2 Client.

Official API Documentation: https://myanimelist.net/apiconfig/references/api/v2

This client provides access to MAL's public API for fetching manga and anime data.
Authentication is done via Client ID (X-MAL-CLIENT-ID header).

Rate limiting: The official API doesn't specify exact limits, but we implement
conservative rate limiting (1 request/second) to avoid being blocked.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

import httpx

# Ensure .env is loaded
from config import env  # noqa: F401


class MalMediaType(str, Enum):
    """Media types for ranking endpoints."""
    # Manga types
    MANGA = "manga"
    NOVELS = "novels"
    LIGHTNOVELS = "lightnovels"
    ONESHOTS = "oneshots"
    DOUJIN = "doujin"
    MANHWA = "manhwa"
    MANHUA = "manhua"
    
    # Anime types
    ANIME = "all"
    AIRING = "airing"
    UPCOMING = "upcoming"
    TV = "tv"
    OVA = "ova"
    MOVIE = "movie"
    SPECIAL = "special"
    BYPOPULARITY = "bypopularity"
    FAVORITE = "favorite"


class MalRankingType(str, Enum):
    """Ranking types for the ranking endpoint."""
    ALL = "all"
    BYPOPULARITY = "bypopularity"
    FAVORITE = "favorite"
    # Manga-specific
    MANGA = "manga"
    NOVELS = "novels"
    LIGHTNOVELS = "lightnovels"
    ONESHOTS = "oneshots"
    DOUJIN = "doujin"
    MANHWA = "manhwa"
    MANHUA = "manhua"
    # Anime-specific
    AIRING = "airing"
    UPCOMING = "upcoming"
    TV = "tv"
    OVA = "ova"
    MOVIE = "movie"
    SPECIAL = "special"


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests_per_second: float = 1.0
    max_retries: int = 5
    base_backoff: float = 2.0
    max_backoff: float = 60.0


class MalApiClient:
    """Client for MyAnimeList API v2.
    
    Usage:
        client = MalApiClient()
        
        # Get a single manga
        manga = client.get_manga(1)
        
        # Iterate through all manga rankings
        for manga in client.iter_manga_ranking(ranking_type="all"):
            print(manga["node"]["title"])
    """
    
    BASE_URL = "https://api.myanimelist.net/v2"
    
    # Fields to request for manga
    MANGA_FIELDS = [
        "id",
        "title",
        "main_picture",
        "alternative_titles",
        "start_date",
        "end_date",
        "synopsis",
        "mean",
        "rank",
        "popularity",
        "num_list_users",
        "num_scoring_users",
        "nsfw",
        "created_at",
        "updated_at",
        "media_type",
        "status",
        "genres",
        "num_volumes",
        "num_chapters",
        "authors{first_name,last_name}",
        "pictures",
        "background",
        "related_anime",
        "related_manga",
        "recommendations",
        "serialization{name}",
    ]
    
    # Fields to request for anime
    ANIME_FIELDS = [
        "id",
        "title",
        "main_picture",
        "alternative_titles",
        "start_date",
        "end_date",
        "synopsis",
        "mean",
        "rank",
        "popularity",
        "num_list_users",
        "num_scoring_users",
        "nsfw",
        "created_at",
        "updated_at",
        "media_type",
        "status",
        "genres",
        "num_episodes",
        "start_season",
        "broadcast",
        "source",
        "average_episode_duration",
        "rating",
        "pictures",
        "background",
        "related_anime",
        "related_manga",
        "recommendations",
        "studios",
    ]
    
    _instance: Optional["MalApiClient"] = None
    _lock = threading.Lock()
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        rate_limit: Optional[RateLimitConfig] = None,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the MAL API client.
        
        Args:
            client_id: MAL API Client ID. If not provided, reads from MAL_CLIENT_ID env var.
            rate_limit: Rate limiting configuration.
            timeout: Request timeout in seconds.
        """
        self.client_id = client_id or os.getenv("MAL_CLIENT_ID")
        if not self.client_id:
            raise ValueError(
                "MAL_CLIENT_ID environment variable is required. "
                "Register your app at https://myanimelist.net/apiconfig"
            )
        
        self.rate_limit = rate_limit or RateLimitConfig()
        self.timeout = timeout
        
        self._last_request_time: float = 0.0
        self._rate_lock = threading.Lock()
        
        self._http_client = httpx.Client(
            base_url=self.BASE_URL,
            headers={
                "X-MAL-CLIENT-ID": self.client_id,
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        
        print(f"Initialized MalApiClient with rate limit: {self.rate_limit.requests_per_second} req/s")
    
    @classmethod
    def get_instance(cls) -> "MalApiClient":
        """Get singleton instance of the client."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def close(self) -> None:
        """Close the HTTP client."""
        self._http_client.close()
    
    def __enter__(self) -> "MalApiClient":
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.close()
    
    def _wait_for_rate_limit(self) -> None:
        """Wait to comply with rate limiting."""
        with self._rate_lock:
            elapsed = time.time() - self._last_request_time
            min_interval = 1.0 / self.rate_limit.requests_per_second
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request_time = time.time()
    
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make an API request with rate limiting and retry logic.
        
        Args:
            method: HTTP method.
            path: API path (e.g., "/manga/1").
            params: Query parameters.
            
        Returns:
            JSON response as a dictionary.
            
        Raises:
            httpx.HTTPStatusError: If the request fails after all retries.
        """
        retries = 0
        backoff = self.rate_limit.base_backoff
        
        while True:
            self._wait_for_rate_limit()
            
            try:
                response = self._http_client.request(method, path, params=params)
                response.raise_for_status()
                return response.json()
            
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Rate limited - back off
                    retries += 1
                    if retries > self.rate_limit.max_retries:
                        raise
                    
                    print(f"Rate limited. Backing off for {backoff}s (retry {retries}/{self.rate_limit.max_retries})")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, self.rate_limit.max_backoff)
                    continue
                
                elif e.response.status_code == 403:
                    # Possibly blocked - longer backoff
                    retries += 1
                    if retries > self.rate_limit.max_retries:
                        raise
                    
                    print(f"403 Forbidden. Backing off for {backoff * 2}s (retry {retries}/{self.rate_limit.max_retries})")
                    time.sleep(backoff * 2)
                    backoff = min(backoff * 2, self.rate_limit.max_backoff)
                    continue
                
                elif e.response.status_code == 404:
                    # Resource not found - return empty
                    return {}
                
                else:
                    raise
            
            except httpx.TimeoutException:
                retries += 1
                if retries > self.rate_limit.max_retries:
                    raise
                
                print(f"Timeout. Retrying in {backoff}s (retry {retries}/{self.rate_limit.max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.rate_limit.max_backoff)
                continue
    
    # =========================================================================
    # Manga Endpoints
    # =========================================================================
    
    def get_manga(
        self,
        manga_id: int,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get manga details by ID.
        
        Args:
            manga_id: MAL manga ID.
            fields: List of fields to include. If None, uses default MANGA_FIELDS.
            
        Returns:
            Manga data dictionary.
        """
        fields = fields or self.MANGA_FIELDS
        params = {"fields": ",".join(fields)}
        return self._request("GET", f"/manga/{manga_id}", params=params)
    
    def search_manga(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Search for manga by query string.
        
        Args:
            query: Search query.
            limit: Maximum number of results (max 100).
            offset: Offset for pagination.
            fields: List of fields to include.
            
        Returns:
            Search results with "data" and "paging" keys.
        """
        fields = fields or self.MANGA_FIELDS
        params = {
            "q": query,
            "limit": min(limit, 100),
            "offset": offset,
            "fields": ",".join(fields),
        }
        return self._request("GET", "/manga", params=params)
    
    def get_manga_ranking(
        self,
        ranking_type: str = "all",
        limit: int = 500,
        offset: int = 0,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get manga ranking.
        
        Args:
            ranking_type: Type of ranking. Options:
                - "all": All manga
                - "manga": Manga only
                - "novels": Novels
                - "lightnovels": Light novels
                - "oneshots": One-shots
                - "doujin": Doujinshi
                - "manhwa": Korean manga
                - "manhua": Chinese manga
                - "bypopularity": By popularity (most members)
                - "favorite": By favorites
            limit: Maximum number of results (max 500).
            offset: Offset for pagination.
            fields: List of fields to include.
            
        Returns:
            Ranking data with "data" and "paging" keys.
        """
        fields = fields or self.MANGA_FIELDS
        params = {
            "ranking_type": ranking_type,
            "limit": min(limit, 500),
            "offset": offset,
            "fields": ",".join(fields),
        }
        return self._request("GET", "/manga/ranking", params=params)
    
    def iter_manga_ranking(
        self,
        ranking_type: str = "all",
        fields: Optional[List[str]] = None,
        max_items: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate through all manga in a ranking.
        
        This generator handles pagination automatically.
        
        Args:
            ranking_type: Type of ranking (see get_manga_ranking for options).
            fields: List of fields to include.
            max_items: Maximum number of items to yield. None for all.
            
        Yields:
            Individual manga ranking entries with "node" and "ranking" keys.
        """
        offset = 0
        count = 0
        limit = 500  # Max per request
        
        while True:
            response = self.get_manga_ranking(
                ranking_type=ranking_type,
                limit=limit,
                offset=offset,
                fields=fields,
            )
            
            data = response.get("data", [])
            if not data:
                break
            
            for item in data:
                yield item
                count += 1
                if max_items and count >= max_items:
                    return
            
            # Check if there's a next page
            paging = response.get("paging", {})
            if "next" not in paging:
                break
            
            offset += len(data)
            print(f"  Fetched {offset} manga entries...")
    
    # =========================================================================
    # Anime Endpoints
    # =========================================================================
    
    def get_anime(
        self,
        anime_id: int,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get anime details by ID.
        
        Args:
            anime_id: MAL anime ID.
            fields: List of fields to include. If None, uses default ANIME_FIELDS.
            
        Returns:
            Anime data dictionary.
        """
        fields = fields or self.ANIME_FIELDS
        params = {"fields": ",".join(fields)}
        return self._request("GET", f"/anime/{anime_id}", params=params)
    
    def search_anime(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Search for anime by query string.
        
        Args:
            query: Search query.
            limit: Maximum number of results (max 100).
            offset: Offset for pagination.
            fields: List of fields to include.
            
        Returns:
            Search results with "data" and "paging" keys.
        """
        fields = fields or self.ANIME_FIELDS
        params = {
            "q": query,
            "limit": min(limit, 100),
            "offset": offset,
            "fields": ",".join(fields),
        }
        return self._request("GET", "/anime", params=params)
    
    def get_anime_ranking(
        self,
        ranking_type: str = "all",
        limit: int = 500,
        offset: int = 0,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get anime ranking.
        
        Args:
            ranking_type: Type of ranking. Options:
                - "all": All anime
                - "airing": Currently airing
                - "upcoming": Upcoming
                - "tv": TV series
                - "ova": OVA
                - "movie": Movies
                - "special": Specials
                - "bypopularity": By popularity
                - "favorite": By favorites
            limit: Maximum number of results (max 500).
            offset: Offset for pagination.
            fields: List of fields to include.
            
        Returns:
            Ranking data with "data" and "paging" keys.
        """
        fields = fields or self.ANIME_FIELDS
        params = {
            "ranking_type": ranking_type,
            "limit": min(limit, 500),
            "offset": offset,
            "fields": ",".join(fields),
        }
        return self._request("GET", "/anime/ranking", params=params)
    
    def get_anime_season(
        self,
        year: int,
        season: str,
        sort: str = "anime_score",
        limit: int = 500,
        offset: int = 0,
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get anime by season.
        
        Args:
            year: Year (e.g., 2024).
            season: Season ("winter", "spring", "summer", "fall").
            sort: Sort order ("anime_score" or "anime_num_list_users").
            limit: Maximum number of results (max 500).
            offset: Offset for pagination.
            fields: List of fields to include.
            
        Returns:
            Season anime data with "data" and "paging" keys.
        """
        fields = fields or self.ANIME_FIELDS
        params = {
            "sort": sort,
            "limit": min(limit, 500),
            "offset": offset,
            "fields": ",".join(fields),
        }
        return self._request("GET", f"/anime/season/{year}/{season}", params=params)
    
    def iter_anime_ranking(
        self,
        ranking_type: str = "all",
        fields: Optional[List[str]] = None,
        max_items: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate through all anime in a ranking.
        
        This generator handles pagination automatically.
        
        Args:
            ranking_type: Type of ranking (see get_anime_ranking for options).
            fields: List of fields to include.
            max_items: Maximum number of items to yield. None for all.
            
        Yields:
            Individual anime ranking entries with "node" and "ranking" keys.
        """
        offset = 0
        count = 0
        limit = 500  # Max per request
        
        while True:
            response = self.get_anime_ranking(
                ranking_type=ranking_type,
                limit=limit,
                offset=offset,
                fields=fields,
            )
            
            data = response.get("data", [])
            if not data:
                break
            
            for item in data:
                yield item
                count += 1
                if max_items and count >= max_items:
                    return
            
            # Check if there's a next page
            paging = response.get("paging", {})
            if "next" not in paging:
                break
            
            offset += len(data)
            print(f"  Fetched {offset} anime entries...")
    
    def iter_anime_seasons(
        self,
        start_year: int = 1950,
        end_year: Optional[int] = None,
        fields: Optional[List[str]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate through all anime across all seasons.
        
        Args:
            start_year: Starting year.
            end_year: Ending year. Defaults to current year + 1.
            fields: List of fields to include.
            
        Yields:
            Individual anime entries.
        """
        import datetime
        
        end_year = end_year or datetime.datetime.now().year + 1
        seasons = ["winter", "spring", "summer", "fall"]
        seen_ids: set[int] = set()
        
        for year in range(start_year, end_year + 1):
            for season in seasons:
                print(f"  Fetching {season} {year}...")
                offset = 0
                
                while True:
                    try:
                        response = self.get_anime_season(
                            year=year,
                            season=season,
                            limit=500,
                            offset=offset,
                            fields=fields,
                        )
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 404:
                            # Season doesn't exist yet
                            break
                        raise
                    
                    data = response.get("data", [])
                    if not data:
                        break
                    
                    for item in data:
                        anime_id = item.get("node", {}).get("id")
                        if anime_id and anime_id not in seen_ids:
                            seen_ids.add(anime_id)
                            yield item
                    
                    paging = response.get("paging", {})
                    if "next" not in paging:
                        break
                    
                    offset += len(data)


# Utility functions for data transformation
def transform_mal_manga_to_work(mal_data: Dict[str, Any], ranking_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Transform MAL API manga response to Work node format.
    
    Args:
        mal_data: Raw manga data from MAL API (the "node" part of response).
        ranking_info: Optional ranking information from ranking endpoint.
        
    Returns:
        Dictionary ready for Neo4j Work node creation.
    """
    node = mal_data if "id" in mal_data else mal_data.get("node", {})
    
    # Extract alternative titles
    alt_titles = node.get("alternative_titles", {})
    
    # Extract authors
    authors = []
    for author in node.get("authors", []):
        author_node = author.get("node", {})
        first_name = author_node.get("first_name", "")
        last_name = author_node.get("last_name", "")
        full_name = f"{first_name} {last_name}".strip()
        if full_name:
            role = author.get("role", "")
            authors.append({"name": full_name, "role": role})
    
    # Extract genres
    genres = [g.get("name") for g in node.get("genres", []) if g.get("name")]
    
    # Extract serialization (magazines)
    serialization = []
    for s in node.get("serialization", []):
        s_node = s.get("node", {})
        if s_node.get("name"):
            serialization.append(s_node.get("name"))
    
    # Extract related works
    related_manga = []
    for rm in node.get("related_manga", []):
        rm_node = rm.get("node", {})
        if rm_node.get("id"):
            related_manga.append({
                "id": rm_node.get("id"),
                "title": rm_node.get("title"),
                "relation_type": rm.get("relation_type_formatted", rm.get("relation_type")),
            })
    
    related_anime = []
    for ra in node.get("related_anime", []):
        ra_node = ra.get("node", {})
        if ra_node.get("id"):
            related_anime.append({
                "id": ra_node.get("id"),
                "title": ra_node.get("title"),
                "relation_type": ra.get("relation_type_formatted", ra.get("relation_type")),
            })
    
    # Build Work properties
    work = {
        "id": str(node.get("id")),
        "mal_id": node.get("id"),
        "title_name": node.get("title"),
        "japanese_name": alt_titles.get("ja"),
        "english_name": alt_titles.get("en"),
        "synonymns": alt_titles.get("synonyms", []),
        "item_type": node.get("media_type"),
        "status": node.get("status"),
        "score": node.get("mean"),
        "scored_by": node.get("num_scoring_users"),
        "ranked": node.get("rank"),
        "popularity": node.get("popularity"),
        "members": node.get("num_list_users"),
        "volumes": node.get("num_volumes"),
        "chapters": node.get("num_chapters"),
        "start_date": node.get("start_date"),
        "end_date": node.get("end_date"),
        "description": node.get("synopsis"),
        "background": node.get("background"),
        "genres": genres,
        "authors": [a["name"] for a in authors],
        "author_roles": authors,
        "serialization": serialization,
        "nsfw": node.get("nsfw"),
        "main_picture": node.get("main_picture", {}).get("large") or node.get("main_picture", {}).get("medium"),
        "pictures": [p.get("large") or p.get("medium") for p in node.get("pictures", []) if p],
        "related_manga": related_manga,
        "related_anime": related_anime,
        "recommendations": [
            {"id": r.get("node", {}).get("id"), "title": r.get("node", {}).get("title")}
            for r in node.get("recommendations", [])
            if r.get("node", {}).get("id")
        ],
        "created_at": node.get("created_at"),
        "updated_at": node.get("updated_at"),
        "source": "myanimelist_api",
        "source_link": f"https://myanimelist.net/manga/{node.get('id')}",
    }
    
    # Add ranking info if provided
    if ranking_info:
        work["ranking_position"] = ranking_info.get("rank")
    
    # Remove None values
    return {k: v for k, v in work.items() if v is not None}


def transform_mal_anime_to_work(mal_data: Dict[str, Any], ranking_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Transform MAL API anime response to Work node format.
    
    Args:
        mal_data: Raw anime data from MAL API (the "node" part of response).
        ranking_info: Optional ranking information from ranking endpoint.
        
    Returns:
        Dictionary ready for Neo4j Work node creation.
    """
    node = mal_data if "id" in mal_data else mal_data.get("node", {})
    
    # Extract alternative titles
    alt_titles = node.get("alternative_titles", {})
    
    # Extract genres
    genres = [g.get("name") for g in node.get("genres", []) if g.get("name")]
    
    # Extract studios
    studios = [s.get("name") for s in node.get("studios", []) if s.get("name")]
    
    # Extract related works
    related_manga = []
    for rm in node.get("related_manga", []):
        rm_node = rm.get("node", {})
        if rm_node.get("id"):
            related_manga.append({
                "id": rm_node.get("id"),
                "title": rm_node.get("title"),
                "relation_type": rm.get("relation_type_formatted", rm.get("relation_type")),
            })
    
    related_anime = []
    for ra in node.get("related_anime", []):
        ra_node = ra.get("node", {})
        if ra_node.get("id"):
            related_anime.append({
                "id": ra_node.get("id"),
                "title": ra_node.get("title"),
                "relation_type": ra.get("relation_type_formatted", ra.get("relation_type")),
            })
    
    # Extract season info
    start_season = node.get("start_season", {})
    season_str = None
    if start_season:
        season_str = f"{start_season.get('season', '')} {start_season.get('year', '')}".strip()
    
    # Build Work properties
    work = {
        "id": f"anime_{node.get('id')}",  # Prefix to distinguish from manga
        "mal_id": node.get("id"),
        "title_name": node.get("title"),
        "japanese_name": alt_titles.get("ja"),
        "english_name": alt_titles.get("en"),
        "synonymns": alt_titles.get("synonyms", []),
        "item_type": node.get("media_type"),
        "status": node.get("status"),
        "score": node.get("mean"),
        "scored_by": node.get("num_scoring_users"),
        "ranked": node.get("rank"),
        "popularity": node.get("popularity"),
        "members": node.get("num_list_users"),
        "episodes": node.get("num_episodes"),
        "start_date": node.get("start_date"),
        "end_date": node.get("end_date"),
        "start_season": season_str,
        "broadcast": node.get("broadcast", {}).get("day_of_the_week"),
        "source": node.get("source"),
        "duration": node.get("average_episode_duration"),
        "age_rating": node.get("rating"),
        "description": node.get("synopsis"),
        "background": node.get("background"),
        "genres": genres,
        "studios": studios,
        "nsfw": node.get("nsfw"),
        "main_picture": node.get("main_picture", {}).get("large") or node.get("main_picture", {}).get("medium"),
        "pictures": [p.get("large") or p.get("medium") for p in node.get("pictures", []) if p],
        "related_manga": related_manga,
        "related_anime": related_anime,
        "recommendations": [
            {"id": r.get("node", {}).get("id"), "title": r.get("node", {}).get("title")}
            for r in node.get("recommendations", [])
            if r.get("node", {}).get("id")
        ],
        "created_at": node.get("created_at"),
        "updated_at": node.get("updated_at"),
        "data_source": "myanimelist_api",
        "source_link": f"https://myanimelist.net/anime/{node.get('id')}",
    }
    
    # Add ranking info if provided
    if ranking_info:
        work["ranking_position"] = ranking_info.get("rank")
    
    # Remove None values
    return {k: v for k, v in work.items() if v is not None}
