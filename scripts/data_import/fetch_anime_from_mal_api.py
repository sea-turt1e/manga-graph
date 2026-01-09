#!/usr/bin/env python
"""Fetch all anime data from MyAnimeList API v2.

This script fetches anime data using the official MAL API and saves it to JSON files.
It supports multiple strategies for comprehensive data collection:
1. Ranking-based: Fetch from various ranking lists
2. Season-based: Fetch by year and season (more comprehensive)

Usage:
    # Fetch anime from ranking (faster but may miss some entries)
    python scripts/data_import/fetch_anime_from_mal_api.py --strategy ranking
    
    # Fetch anime by season (slower but more comprehensive)
    python scripts/data_import/fetch_anime_from_mal_api.py --strategy season
    
    # Fetch only top 1000 anime for testing
    python scripts/data_import/fetch_anime_from_mal_api.py --limit 1000
    
    # Resume from a previous run
    python scripts/data_import/fetch_anime_from_mal_api.py --resume
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm.auto import tqdm

# Ensure the project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import env  # noqa: F401
from domain.services.mal_api_client import (MalApiClient, RateLimitConfig,
                                            transform_mal_anime_to_work)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch anime data from MyAnimeList API v2",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/mal_api"),
        help="Directory to save output files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of anime to fetch (None for all)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="ranking",
        choices=["ranking", "season", "both"],
        help="Fetching strategy: ranking (fast), season (comprehensive), or both",
    )
    parser.add_argument(
        "--ranking-type",
        type=str,
        default="all",
        choices=["all", "airing", "upcoming", "tv", "ova", "movie", "special", "bypopularity", "favorite"],
        help="Type of ranking to fetch (when using ranking strategy)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1950,
        help="Start year for season-based fetching",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End year for season-based fetching (default: current year + 1)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from a previous run (skip already fetched IDs)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        help="Requests per second",
    )
    parser.add_argument(
        "--save-interval",
        type=int,
        default=500,
        help="Save progress every N items",
    )
    
    return parser.parse_args(argv)


def load_existing_data(output_path: Path) -> tuple[List[Dict[str, Any]], set[int]]:
    """Load existing data from output file."""
    if not output_path.exists():
        return [], set()
    
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "anime" in data:
                anime_list = data["anime"]
            else:
                anime_list = data
            
            existing_ids = {int(a.get("mal_id", 0)) for a in anime_list if a.get("mal_id")}
            return anime_list, existing_ids
    except (json.JSONDecodeError, KeyError):
        return [], set()


def save_data(
    output_path: Path,
    anime_list: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> None:
    """Save anime data with metadata."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    data = {
        "metadata": metadata,
        "anime": anime_list,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(anime_list)} anime entries to {output_path}")


def fetch_anime_ranking(
    client: MalApiClient,
    ranking_type: str,
    limit: Optional[int],
    existing_ids: set[int],
) -> List[Dict[str, Any]]:
    """Fetch anime from ranking endpoint."""
    anime_list: List[Dict[str, Any]] = []
    count = 0
    skipped = 0
    
    print(f"Fetching anime ranking (type: {ranking_type})...")
    
    for item in client.iter_anime_ranking(ranking_type=ranking_type, max_items=limit):
        node = item.get("node", {})
        mal_id = node.get("id")
        
        if mal_id in existing_ids:
            skipped += 1
            continue
        
        # Transform to Work format
        work = transform_mal_anime_to_work(node, item.get("ranking"))
        anime_list.append(work)
        existing_ids.add(mal_id)
        count += 1
        
        if limit and count >= limit:
            break
    
    print(f"Fetched {count} new anime entries from ranking (skipped {skipped} existing)")
    return anime_list


def fetch_anime_by_season(
    client: MalApiClient,
    start_year: int,
    end_year: Optional[int],
    existing_ids: set[int],
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch anime by iterating through all seasons."""
    anime_list: List[Dict[str, Any]] = []
    count = 0
    
    print(f"Fetching anime by season ({start_year} to {end_year or 'present'})...")
    
    for item in client.iter_anime_seasons(
        start_year=start_year,
        end_year=end_year,
    ):
        node = item.get("node", {})
        mal_id = node.get("id")
        
        if mal_id in existing_ids:
            continue
        
        # Transform to Work format
        work = transform_mal_anime_to_work(node)
        anime_list.append(work)
        existing_ids.add(mal_id)
        count += 1
        
        if limit and count >= limit:
            break
    
    print(f"Fetched {count} new anime entries from seasons")
    return anime_list


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    
    # Setup output paths
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    strategy_suffix = args.strategy if args.strategy != "ranking" else args.ranking_type
    output_file = output_dir / f"anime_{strategy_suffix}.json"
    
    # Load existing data if resuming
    existing_anime, existing_ids = [], set()
    if args.resume:
        existing_anime, existing_ids = load_existing_data(output_file)
        print(f"Resuming from {len(existing_anime)} existing anime entries")
    
    # Initialize client
    rate_config = RateLimitConfig(requests_per_second=args.rate_limit)
    client = MalApiClient(rate_limit=rate_config)
    
    try:
        new_anime: List[Dict[str, Any]] = []
        
        if args.strategy in ("ranking", "both"):
            # Fetch from ranking
            ranking_anime = fetch_anime_ranking(
                client=client,
                ranking_type=args.ranking_type,
                limit=args.limit,
                existing_ids=existing_ids,
            )
            new_anime.extend(ranking_anime)
        
        if args.strategy in ("season", "both"):
            # Fetch by season
            season_anime = fetch_anime_by_season(
                client=client,
                start_year=args.start_year,
                end_year=args.end_year,
                existing_ids=existing_ids,
                limit=args.limit if args.strategy == "season" else None,
            )
            new_anime.extend(season_anime)
        
        # Merge with existing data
        all_anime = existing_anime + new_anime
        
        # Prepare metadata
        metadata = {
            "source": "myanimelist_api_v2",
            "strategy": args.strategy,
            "ranking_type": args.ranking_type if args.strategy in ("ranking", "both") else None,
            "year_range": f"{args.start_year}-{args.end_year}" if args.strategy in ("season", "both") else None,
            "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "total_count": len(all_anime),
            "new_count": len(new_anime),
            "api_version": "v2",
        }
        
        # Save results
        save_data(output_file, all_anime, metadata)
        
        print(f"\nDone! Total anime: {len(all_anime)}")
        print(f"Output file: {output_file}")
        
    finally:
        client.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
