#!/usr/bin/env python
"""Fetch all manga data from MyAnimeList API v2.

This script fetches manga data using the official MAL API and saves it to JSON files.
It supports resuming from a previous run and can fetch incrementally.

Usage:
    # Fetch all manga (will take several hours)
    python scripts/data_import/fetch_manga_from_mal_api.py
    
    # Fetch only top 1000 manga for testing
    python scripts/data_import/fetch_manga_from_mal_api.py --limit 1000
    
    # Resume from a previous run
    python scripts/data_import/fetch_manga_from_mal_api.py --resume
    
    # Fetch with custom output directory
    python scripts/data_import/fetch_manga_from_mal_api.py --output-dir data/mal_api
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
                                            transform_mal_manga_to_work)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch manga data from MyAnimeList API v2",
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
        help="Maximum number of manga to fetch (None for all)",
    )
    parser.add_argument(
        "--ranking-type",
        type=str,
        default="all",
        choices=["all", "manga", "novels", "lightnovels", "oneshots", "doujin", "manhwa", "manhua", "bypopularity", "favorite"],
        help="Type of ranking to fetch",
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
    parser.add_argument(
        "--fetch-details",
        action="store_true",
        help="Fetch full details for each manga (slower but more complete data)",
    )
    
    return parser.parse_args(argv)


def load_existing_data(output_path: Path) -> tuple[List[Dict[str, Any]], set[int]]:
    """Load existing data from output file."""
    if not output_path.exists():
        return [], set()
    
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "manga" in data:
                manga_list = data["manga"]
            else:
                manga_list = data
            
            existing_ids = {int(m.get("mal_id", m.get("id", 0))) for m in manga_list}
            return manga_list, existing_ids
    except (json.JSONDecodeError, KeyError):
        return [], set()


def save_data(
    output_path: Path,
    manga_list: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> None:
    """Save manga data with metadata."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    data = {
        "metadata": metadata,
        "manga": manga_list,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(manga_list)} manga entries to {output_path}")


def fetch_manga_ranking(
    client: MalApiClient,
    ranking_type: str,
    limit: Optional[int],
    existing_ids: set[int],
    progress_callback: Optional[callable] = None,
) -> List[Dict[str, Any]]:
    """Fetch manga from ranking endpoint."""
    manga_list: List[Dict[str, Any]] = []
    count = 0
    skipped = 0
    
    print(f"Fetching manga ranking (type: {ranking_type})...")
    
    for item in client.iter_manga_ranking(ranking_type=ranking_type, max_items=limit):
        node = item.get("node", {})
        mal_id = node.get("id")
        
        if mal_id in existing_ids:
            skipped += 1
            continue
        
        # Transform to Work format
        work = transform_mal_manga_to_work(node, item.get("ranking"))
        manga_list.append(work)
        existing_ids.add(mal_id)
        count += 1
        
        if progress_callback and count % 100 == 0:
            progress_callback(count, skipped)
        
        if limit and count >= limit:
            break
    
    print(f"Fetched {count} new manga entries (skipped {skipped} existing)")
    return manga_list


def fetch_manga_details_batch(
    client: MalApiClient,
    manga_ids: List[int],
    existing_ids: set[int],
) -> List[Dict[str, Any]]:
    """Fetch full details for a batch of manga IDs."""
    manga_list: List[Dict[str, Any]] = []
    
    for mal_id in tqdm(manga_ids, desc="Fetching manga details"):
        if mal_id in existing_ids:
            continue
        
        try:
            data = client.get_manga(mal_id)
            if data:
                work = transform_mal_manga_to_work(data)
                manga_list.append(work)
                existing_ids.add(mal_id)
        except Exception as e:
            print(f"  Error fetching manga {mal_id}: {e}")
    
    return manga_list


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    
    # Setup output paths
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"manga_{args.ranking_type}.json"
    
    # Load existing data if resuming
    existing_manga, existing_ids = [], set()
    if args.resume:
        existing_manga, existing_ids = load_existing_data(output_file)
        print(f"Resuming from {len(existing_manga)} existing manga entries")
    
    # Initialize client
    rate_config = RateLimitConfig(requests_per_second=args.rate_limit)
    client = MalApiClient(rate_limit=rate_config)
    
    try:
        # Fetch manga from ranking
        new_manga = fetch_manga_ranking(
            client=client,
            ranking_type=args.ranking_type,
            limit=args.limit,
            existing_ids=existing_ids,
        )
        
        # Merge with existing data
        all_manga = existing_manga + new_manga
        
        # Prepare metadata
        metadata = {
            "source": "myanimelist_api_v2",
            "ranking_type": args.ranking_type,
            "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "total_count": len(all_manga),
            "new_count": len(new_manga),
            "api_version": "v2",
        }
        
        # Save results
        save_data(output_file, all_manga, metadata)
        
        print(f"\nDone! Total manga: {len(all_manga)}")
        print(f"Output file: {output_file}")
        
    finally:
        client.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
