#!/usr/bin/env python
"""Create Publisher nodes and connect them to Magazine nodes using a mapping JSON.

Expected input:
    data/myanimelist/myanimelist-scraped-data-2025-July/publisher_magazine_mapping.json

This file should look like:
{
    "Kodansha": ["Afternoon", "Morning"],
    "Shueisha": ["Weekly Shonen Jump", "Ultra Jump"],
    ...
}

For each publisher entry, the script will:
1. MERGE a `Publisher` node keyed by its `name`.
2. MERGE `Magazine` nodes for each listed serialization name.
3. Connect them with `(m:Magazine)-[:PUBLISHED_BY]->(p:Publisher)` relationships.

It is safe to run multiple times thanks to MERGE semantics.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

from neo4j import Driver, GraphDatabase
from tqdm.auto import tqdm

# Ensure project root is importable so we can load config.env for env vars
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import env  # noqa: F401  # pylint: disable=unused-import

DEFAULT_MAPPING_PATH = Path(
    "data/myanimelist/myanimelist-scraped-data-2025-July/publisher_magazine_mapping.json"
)


@dataclass
class ImportConfig:
    mapping_path: Path
    uri: str
    user: str
    password: str
    batch_size: int = 200
    dry_run: bool = False
    reset: bool = False


def parse_args(argv: Optional[List[str]] = None) -> ImportConfig:
    parser = argparse.ArgumentParser(
        description="Create Publisher nodes and link them to Magazine nodes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mapping-path",
        type=Path,
        default=DEFAULT_MAPPING_PATH,
        help="Path to publisher_magazine_mapping.json",
    )
    parser.add_argument(
        "--uri",
        type=str,
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j bolt URI",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=os.getenv("NEO4J_USER", "neo4j"),
        help="Neo4j username",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.getenv("NEO4J_PASSWORD", "password"),
        help="Neo4j password",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of publishers per transaction",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the first few mapping entries without writing to Neo4j",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Remove existing Publisher nodes and PUBLISHED_BY relationships before importing",
    )

    args = parser.parse_args(argv)
    mapping_path = args.mapping_path.expanduser().resolve()

    return ImportConfig(
        mapping_path=mapping_path,
        uri=args.uri,
        user=args.user,
        password=args.password,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        reset=args.reset,
    )


def load_mapping(mapping_path: Path) -> Dict[str, List[str]]:
    if not mapping_path.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_path}")

    with mapping_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    normalized: Dict[str, List[str]] = {}
    for publisher, magazines in data.items():
        if not publisher:
            continue
        if not isinstance(magazines, list):
            continue
        cleaned_publisher = " ".join(publisher.strip().split())
        if not cleaned_publisher:
            continue
        cleaned_mags: List[str] = []
        seen = set()
        for mag in magazines:
            if not isinstance(mag, str):
                continue
            cleaned_mag = " ".join(mag.strip().split())
            if not cleaned_mag:
                continue
            if cleaned_mag in seen:
                continue
            seen.add(cleaned_mag)
            cleaned_mags.append(cleaned_mag)
        if cleaned_mags:
            normalized[cleaned_publisher] = cleaned_mags
    if not normalized:
        raise ValueError("Mapping JSON did not contain any usable publisher -> magazines entries.")
    return normalized


def chunked(items: Iterable, size: int) -> Iterator[List]:
    batch: List = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class PublisherImporter:
    def __init__(self, driver: Driver, batch_size: int = 200) -> None:
        self.driver = driver
        self.batch_size = batch_size

    def ensure_constraints(self) -> None:
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publisher) REQUIRE p.name IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.name IS UNIQUE")

    def clear_publishers(self) -> None:
        with self.driver.session() as session:
            session.run("MATCH (:Magazine)-[r:PUBLISHED_BY]->(:Publisher) DELETE r")
            session.run("MATCH (p:Publisher) DETACH DELETE p")

    def import_mapping(self, mapping: Dict[str, List[str]]) -> None:
        entries = list(mapping.items())
        total_batches = max(1, (len(entries) + self.batch_size - 1) // self.batch_size)
        for chunk in tqdm(chunked(entries, self.batch_size), total=total_batches, desc="Importing publishers"):
            self._write_chunk(chunk)

    def _write_chunk(self, chunk: List) -> None:
        payload = [
            {"publisher": publisher, "magazines": magazines}
            for publisher, magazines in chunk
        ]
        with self.driver.session() as session:
            session.execute_write(self._write_tx, payload)

    @staticmethod
    def _write_tx(tx, rows: List[Dict[str, List[str]]]) -> None:
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (p:Publisher {name: row.publisher})
            FOREACH (magazine IN row.magazines |
                MERGE (m:Magazine {name: magazine})
                MERGE (m)-[:PUBLISHED_BY]->(p)
            )
            """,
            rows=rows,
        )


def main(argv: Optional[List[str]] = None) -> int:
    config = parse_args(argv)
    mapping = load_mapping(config.mapping_path)

    print(f"Loaded {len(mapping)} publishers from {config.mapping_path}")

    if config.dry_run:
        preview = list(mapping.items())[:5]
        for publisher, magazines in preview:
            print(f"{publisher}: {magazines[:5]}" + ("..." if len(magazines) > 5 else ""))
        return 0

    driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
    importer = PublisherImporter(driver, batch_size=config.batch_size)
    try:
        importer.ensure_constraints()
        if config.reset:
            print("Reset flag detected. Removing existing Publisher nodes and relationships...")
            importer.clear_publishers()
        importer.import_mapping(mapping)
    finally:
        driver.close()

    print("Publisher import completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
