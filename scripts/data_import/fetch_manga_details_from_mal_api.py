#!/usr/bin/env python
"""Fetch detailed manga data from MyAnimeList API v2.

This script fetches detailed information for each manga, including serialization
data that is not available from the ranking endpoint.

The script supports:
- Progress saving and resuming
- Rate limiting to avoid API blocks
- Incremental updates

Usage:
    # Fetch details for all manga in existing JSON
    python scripts/data_import/fetch_manga_details_from_mal_api.py

    # Fetch details for specific limit
    python scripts/data_import/fetch_manga_details_from_mal_api.py --limit 1000

    # Resume from previous run
    python scripts/data_import/fetch_manga_details_from_mal_api.py --resume

    # Update existing Neo4j database directly
    python scripts/data_import/fetch_manga_details_from_mal_api.py --update-neo4j
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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
        description="Fetch detailed manga data from MyAnimeList API v2",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=Path("data/mal_api/manga_all.json"),
        help="Input JSON file with manga list (from ranking endpoint)",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=Path("data/mal_api/manga_details.json"),
        help="Output JSON file for detailed manga data",
    )
    parser.add_argument(
        "--progress-file",
        type=Path,
        default=Path("data/mal_api/.manga_details_progress.json"),
        help="Progress file for resuming",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of manga to fetch details for",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous run",
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
        default=100,
        help="Save progress every N items",
    )
    parser.add_argument(
        "--update-neo4j",
        action="store_true",
        help="Update Neo4j database with serialization data",
    )
    parser.add_argument(
        "--neo4j-uri",
        type=str,
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j URI (for --update-neo4j)",
    )
    parser.add_argument(
        "--neo4j-user",
        type=str,
        default=os.getenv("NEO4J_USER", "neo4j"),
        help="Neo4j user",
    )
    parser.add_argument(
        "--neo4j-password",
        type=str,
        default=os.getenv("NEO4J_PASSWORD", "password"),
        help="Neo4j password",
    )

    return parser.parse_args(argv)


def load_manga_ids(input_file: Path) -> List[int]:
    """Load manga IDs from input JSON file."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    manga_list = data.get("manga", data) if isinstance(data, dict) else data
    
    ids = []
    for manga in manga_list:
        mal_id = manga.get("mal_id") or manga.get("id")
        if mal_id:
            # Remove anime_ prefix if present
            if isinstance(mal_id, str) and mal_id.startswith("anime_"):
                continue
            ids.append(int(str(mal_id).replace("anime_", "")))
    
    return ids


def load_progress(progress_file: Path) -> tuple[Set[int], List[Dict[str, Any]]]:
    """Load progress from previous run."""
    if not progress_file.exists():
        return set(), []

    with open(progress_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    completed_ids = set(data.get("completed_ids", []))
    results = data.get("results", [])
    
    return completed_ids, results


def save_progress(
    progress_file: Path,
    completed_ids: Set[int],
    results: List[Dict[str, Any]],
) -> None:
    """Save progress for resuming later."""
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    
    data = {
        "completed_ids": list(completed_ids),
        "results": results,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def save_results(
    output_file: Path,
    results: List[Dict[str, Any]],
) -> None:
    """Save final results to output file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    data = {
        "metadata": {
            "source": "myanimelist_api_v2_details",
            "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "total_count": len(results),
            "api_version": "v2",
        },
        "manga": results,
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(results)} manga entries to {output_file}")


def update_neo4j_serialization(
    results: List[Dict[str, Any]],
    uri: str,
    user: str,
    password: str,
) -> None:
    """Update Neo4j with serialization data."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        # Collect all unique magazines
        magazines: Dict[str, Set[str]] = {}  # magazine_name -> set of work_ids
        
        for manga in results:
            work_id = str(manga.get("id"))
            serialization = manga.get("serialization", [])
            
            for mag_name in serialization:
                if mag_name:
                    if mag_name not in magazines:
                        magazines[mag_name] = set()
                    magazines[mag_name].add(work_id)
        
        if not magazines:
            print("No serialization data found")
            return
        
        print(f"Creating {len(magazines)} Magazine nodes and PUBLISHED_IN relationships...")
        
        with driver.session() as session:
            # Create constraint if not exists
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.name IS UNIQUE")
            
            # Create magazines and relationships
            for mag_name, work_ids in tqdm(magazines.items(), desc="Creating PUBLISHED_IN"):
                session.run(
                    """
                    MERGE (m:Magazine {name: $mag_name})
                    WITH m
                    UNWIND $work_ids AS work_id
                    MATCH (w:Work {id: work_id})
                    MERGE (w)-[:PUBLISHED_IN]->(m)
                    """,
                    mag_name=mag_name,
                    work_ids=list(work_ids),
                )
        
        print(f"Created PUBLISHED_IN relationships for {sum(len(ids) for ids in magazines.values())} work-magazine pairs")
        
    finally:
        driver.close()


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    
    # Load manga IDs to fetch
    print(f"Loading manga IDs from {args.input_file}...")
    all_ids = load_manga_ids(args.input_file)
    print(f"Found {len(all_ids)} manga IDs")
    
    # Load progress if resuming
    completed_ids: Set[int] = set()
    results: List[Dict[str, Any]] = []
    
    if args.resume:
        completed_ids, results = load_progress(args.progress_file)
        print(f"Resuming from {len(completed_ids)} completed IDs")
    
    # Filter out already completed IDs
    pending_ids = [id for id in all_ids if id not in completed_ids]
    
    if args.limit:
        pending_ids = pending_ids[:args.limit]
    
    if not pending_ids:
        print("No pending manga to fetch")
        if results:
            save_results(args.output_file, results)
            if args.update_neo4j:
                update_neo4j_serialization(
                    results,
                    args.neo4j_uri,
                    args.neo4j_user,
                    args.neo4j_password,
                )
        return 0
    
    print(f"Fetching details for {len(pending_ids)} manga...")
    estimated_time = len(pending_ids) / args.rate_limit
    print(f"Estimated time: {estimated_time / 3600:.1f} hours ({estimated_time / 60:.1f} minutes)")
    
    # Initialize client
    rate_config = RateLimitConfig(requests_per_second=args.rate_limit)
    client = MalApiClient(rate_limit=rate_config)
    
    try:
        for i, mal_id in enumerate(tqdm(pending_ids, desc="Fetching manga details")):
            try:
                # Fetch detailed manga data
                data = client.get_manga(mal_id)
                
                if data:
                    work = transform_mal_manga_to_work(data)
                    results.append(work)
                    completed_ids.add(mal_id)
                
            except Exception as e:
                print(f"\nError fetching manga {mal_id}: {e}")
                # Continue with next manga
            
            # Save progress periodically
            if (i + 1) % args.save_interval == 0:
                save_progress(args.progress_file, completed_ids, results)
                print(f"\n  Progress saved: {len(completed_ids)} completed")
        
        # Final save
        save_progress(args.progress_file, completed_ids, results)
        save_results(args.output_file, results)
        
        # Update Neo4j if requested
        if args.update_neo4j:
            update_neo4j_serialization(
                results,
                args.neo4j_uri,
                args.neo4j_user,
                args.neo4j_password,
            )
        
        print(f"\nDone! Fetched details for {len(results)} manga")
        
    finally:
        client.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
