#!/usr/bin/env python
"""
Update Neo4j Work.total_volumes based on existing volume data.

Rules:
- Group works by normalized base title (handles aliases like "タイトル = ENGLISH TITLE").
- Side stories / spin-offs containing keywords (Remains, 外伝, 番外編, etc.) are treated as separate groups.
- Volume number is taken from `w.volume` when numeric, otherwise extracted from title's trailing number.
- Only update works whose current total_volumes is NULL, missing, 0, or 1. (>=2 is treated as already correct.)
- For each group, computed series_total = count of distinct numeric volumes (>=1).
- Every work in that group needing update gets total_volumes = series_total.

Usage:
  Dry run (default):
    python scripts/update_total_volumes.py --query "憂国のモリアーティ"
  Apply updates:
    python scripts/update_total_volumes.py --apply
  Filter to specific term (contains search on title):
    python scripts/update_total_volumes.py -q モリアーティ --apply

Environment variables (same as repository defaults):
  NEO4J_URI (default bolt://localhost:7687)
  NEO4J_USER (default neo4j)
  NEO4J_PASSWORD (default password)
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

logger = logging.getLogger("update_total_volumes")
load_dotenv()  # Load environment variables from .env file if present

SPIN_OFF_KEYWORDS = [
    "remains",  # The Remains
    "外伝",
    "番外編",
    "another",
    "side story",
    "spin-off",
    "スピンオフ",
]

# Patterns similar to repository _extract_base_title; reused for normalization
VOLUME_SUFFIX_PATTERNS = [
    r"\s*第\d+巻?$",  # 第X巻
    r"\s*\(\d+\)$",  # (数字)
    r"\s*vol\.\s*\d+$",  # vol. X
    r"\s*VOLUME\s*\d+$",  # VOLUME X
    r"\s*巻\d+$",  # 巻X
    r"\s*その\d+$",  # そのX
]

TRAILING_NUMBER_PATTERN = re.compile(r"(\d+)$")
ANY_NUMBER_PATTERN = re.compile(r"(\d+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update total_volumes for Work nodes in Neo4j")
    parser.add_argument("--apply", action="store_true", help="Actually perform updates (otherwise dry-run)")
    parser.add_argument("-q", "--query", help="Filter works whose title CONTAINS this term (case-insensitive)")
    parser.add_argument(
        "--min-existing-threshold",
        type=int,
        default=2,
        help="Skip updating works already having total_volumes >= this value (default: 2)",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for WRITE updates (default: 500)")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit on number of works fetched (0 = no limit)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Log level")
    return parser.parse_args()


def is_spin_off(title: str) -> bool:
    lower = title.lower()
    return any(k in lower for k in SPIN_OFF_KEYWORDS)


def normalize_title(title: str) -> str:
    if not title:
        return title
    # If alias form "A = B" take the left side (Japanese original part usually)
    if " = " in title:
        left = title.split(" = ", 1)[0]
    else:
        left = title
    # Remove trailing numbers and volume descriptors patterns iteratively
    base = left.strip()
    # Remove known suffix patterns (but keep spin-off markers like -The Remains-)
    changed = True
    while changed:
        changed = False
        for pat in VOLUME_SUFFIX_PATTERNS:
            new_base = re.sub(pat, "", base, flags=re.IGNORECASE)
            if new_base != base:
                base = new_base
                changed = True
    # Remove a simple trailing number at end
    base = re.sub(r"\s*\d+$", "", base)
    return base.strip()


def extract_volume_number(title: str, volume_field: Optional[str | int]) -> Optional[int]:
    # Prefer explicit volume field if numeric
    if volume_field is not None and str(volume_field).strip().isdigit():
        try:
            return int(str(volume_field).strip())
        except ValueError:
            pass
    # Try to get trailing number from title (before alias removal)
    m = TRAILING_NUMBER_PATTERN.search(title.strip())
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


@dataclass
class WorkRow:
    id: str
    title: str
    volume: Optional[str]
    total_volumes: Optional[int]


def fetch_works(driver: Driver, query_filter: Optional[str], limit: int) -> List[WorkRow]:
    cypher = [
        "MATCH (w:Work)",
    ]
    params: Dict[str, any] = {}
    if query_filter:
        cypher.append("WHERE toLower(w.title) CONTAINS toLower($filter)")
        params["filter"] = query_filter
    cypher.append("RETURN w.id as id, w.title as title, w.volume as volume, w.total_volumes as total_volumes")
    if limit and limit > 0:
        cypher.append("LIMIT $limit")
        params["limit"] = limit
    cypher_query = "\n".join(cypher)
    rows: List[WorkRow] = []
    with driver.session() as session:
        for rec in session.run(cypher_query, **params):
            rows.append(
                WorkRow(
                    id=rec["id"],
                    title=rec["title"],
                    volume=rec.get("volume"),
                    total_volumes=rec.get("total_volumes"),
                )
            )
    return rows


def group_and_compute(rows: List[WorkRow], min_existing_threshold: int) -> Tuple[List[Dict], Dict[str, any]]:
    groups: Dict[str, Dict[str, any]] = {}

    for row in rows:
        base = normalize_title(row.title)
        # spin-offs remain separated naturally because base retains '-The Remains-' etc
        key = base
        if key not in groups:
            groups[key] = {
                "key": key,
                "works": [],
                "volume_numbers": set(),
            }
        vol_num = extract_volume_number(row.title, row.volume)
        if vol_num and vol_num > 0:
            groups[key]["volume_numbers"].add(vol_num)
        groups[key]["works"].append(row)

    updates: List[Dict] = []
    stats = {
        "total_groups": len(groups),
        "candidate_groups": 0,
        "updates": 0,
    }

    for g in groups.values():
        volume_count = len(g["volume_numbers"]) if g["volume_numbers"] else len(g["works"])
        if volume_count < 2:
            # Single volume -> we will not attempt to set above 1; skip unless nodes have 0/None
            target_total = 1
        else:
            target_total = volume_count

        # Only generate updates for works whose total_volumes < min_existing_threshold
        group_update_candidates = [
            w for w in g["works"] if (w.total_volumes is None or w.total_volumes < min_existing_threshold)
        ]
        if not group_update_candidates:
            continue

        stats["candidate_groups"] += 1
        for w in group_update_candidates:
            updates.append({"id": w.id, "total_volumes": target_total})
        stats["updates"] += len(group_update_candidates)

    return updates, stats


def apply_updates(driver: Driver, updates: List[Dict], batch_size: int) -> int:
    if not updates:
        return 0
    total_updated = 0
    with driver.session() as session:
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            cypher = (
                "UNWIND $rows as row\n"
                "MATCH (w:Work {id: row.id})\n"
                "SET w.total_volumes = row.total_volumes\n"
                "RETURN count(w) as updated"
            )
            result = session.run(cypher, rows=batch)
            rec = result.single()
            if rec:
                total_updated += rec["updated"]
    return total_updated


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    logger.info("Connecting to Neo4j at %s as %s", uri, user)
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        rows = fetch_works(driver, args.query, args.limit)
        logger.info("Fetched %d works", len(rows))
        updates, stats = group_and_compute(rows, args.min_existing_threshold)
        logger.info(
            "Computed groups: total=%d candidate_groups=%d pending_updates=%d",
            stats["total_groups"],
            stats["candidate_groups"],
            stats["updates"],
        )

        # Show top 20 sample updates for visibility
        for sample in updates[:20]:
            logger.debug("UPDATE CANDIDATE: %s", sample)

        if not args.apply:
            logger.info("Dry-run mode. Use --apply to persist %d updates.", len(updates))
            return 0

        updated = apply_updates(driver, updates, args.batch_size)
        logger.info("Applied updates to %d Work nodes", updated)
        return 0
    finally:
        driver.close()


if __name__ == "main__":  # incorrect guard intentionally avoided; use standard below
    main()

if __name__ == "__main__":
    sys.exit(main())
