#!/usr/bin/env python
"""Import MyAnimeList CSV data into a Neo4j instance as Work nodes.

This script reads the manga_entries_head.csv file (or any compatible CSV file)
and creates/updates `Work` nodes in Neo4j where:

* The `id` column becomes the unique identifier for each node.
* Every other column is stored as a node property (when a value is present).
* Values that look like JSON arrays (e.g. `["Action", "Adventure"]`) are
  converted into real Python lists so they are stored as Neo4j lists.

Relationships are intentionally not created here so that they can be injected
later in a dedicated step, as requested.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import pandas as pd
from neo4j import Driver, GraphDatabase
from tqdm.auto import tqdm

# Ensure the project root is on sys.path so "config" and other packages resolve
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import env  # noqa: F401  # Ensure .env is loaded for NEO4J_* vars

DEFAULT_CSV_PATH = Path(
    "data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv"
)

MISSING_VALUE_TOKENS = {"", "unknown", "n/a", "na", "null", "none", "?"}


@dataclass
class ImportConfig:
    csv_path: Path
    uri: str
    user: str
    password: str
    batch_size: int = 500
    limit: Optional[int] = None
    dry_run: bool = False
    reset: bool = False


def parse_args(argv: Optional[List[str]] = None) -> ImportConfig:
    parser = argparse.ArgumentParser(
        description="Create Work nodes in Neo4j from MyAnimeList CSV data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="Path to the manga_entries.csv file",
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
        help="Neo4j user",
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
        default=500,
        help="Number of rows to send to Neo4j per transaction",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Import only the first N rows (useful for smoke tests)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the CSV and show the first few prepared nodes without writing to Neo4j",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing Work nodes before importing",
    )

    args = parser.parse_args(argv)
    csv_path = args.csv_path.expanduser().resolve()

    return ImportConfig(
        csv_path=csv_path,
        uri=args.uri,
        user=args.user,
        password=args.password,
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=args.dry_run,
        reset=args.reset,
    )


def chunked(iterable: Iterable[Any], size: int) -> Iterator[List[Any]]:
    chunk: List[Any] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def normalize_value(value: Any) -> Any:
    """Convert CSV cell values into Neo4j-friendly types."""

    if value is None:
        return None

    if isinstance(value, (int, float, bool, list, dict)):
        return value

    # Handle pandas-specific missing markers
    if value is pd.NA:  # type: ignore[attr-defined]
        return None

    if isinstance(value, str):
        text = value.strip()
        if text.lower() in MISSING_VALUE_TOKENS:
            return None

        parsed_collection = try_parse_collection(text)
        if parsed_collection is not None:
            return parsed_collection

        parsed_number = try_parse_number(text)
        if parsed_number is not None:
            return parsed_number

        return text

    return value


def try_parse_collection(text: str) -> Optional[Any]:
    if not text:
        return None

    if text[0] not in "[{":
        return None

    normalized = text.replace('""', '"')

    try:
        return json.loads(normalized)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(normalized)
        except (ValueError, SyntaxError):
            return None


def try_parse_number(text: str) -> Optional[Any]:
    if not text:
        return None

    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        try:
            return int(text)
        except ValueError:
            return None

    try:
        # Allow floats like 9.47 or scientific notation
        value = float(text)
        return value
    except ValueError:
        return None


def load_csv_records(csv_path: Path, limit: Optional[int]) -> List[Dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    if limit:
        df = df.head(limit)

    records = df.to_dict(orient="records")
    prepared: List[Dict[str, Any]] = []

    for row in records:
        work_id = str(row.get("id", "")).strip()
        if not work_id:
            continue

        properties: Dict[str, Any] = {}
        for column, value in row.items():
            if column == "id":
                continue
            parsed = normalize_value(value)
            if parsed is not None:
                properties[column] = parsed

        prepared.append({"id": work_id, "properties": properties})

    if not prepared:
        raise ValueError("No valid rows with an 'id' column were found in the CSV.")

    return prepared


class MangaEntriesImporter:
    def __init__(self, driver: Driver, batch_size: int = 500) -> None:
        self.driver = driver
        self.batch_size = batch_size

    def ensure_constraints(self) -> None:
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE")

    def clear_work_nodes(self) -> None:
        with self.driver.session() as session:
            session.run("MATCH (w:Work) DETACH DELETE w")

    def import_rows(self, rows: List[Dict[str, Any]]) -> None:
        total_batches = math.ceil(len(rows) / self.batch_size)

        for chunk in tqdm(
            chunked(rows, self.batch_size),
            total=total_batches,
            desc="Importing Work nodes",
        ):
            self._load_chunk(chunk)

    def _load_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        with self.driver.session() as session:
            session.execute_write(self._write_chunk, chunk)

    @staticmethod
    def _write_chunk(tx, chunk: List[Dict[str, Any]]) -> None:
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (w:Work {id: row.id})
            SET w += row.properties
            """,
            rows=chunk,
        )


def main(argv: Optional[List[str]] = None) -> int:
    config = parse_args(argv)

    print(f"Loading data from {config.csv_path} ...")
    rows = load_csv_records(config.csv_path, config.limit)
    print(f"Prepared {len(rows)} records with Work label.")

    if config.dry_run:
        preview = rows[:3]
        print("Dry-run preview (first 3 rows):")
        for idx, row in enumerate(preview, start=1):
            print(f"Row {idx}: id={row['id']}, properties={list(row['properties'].keys())}")
        return 0

    driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
    importer = MangaEntriesImporter(driver, batch_size=config.batch_size)

    try:
        importer.ensure_constraints()
        if config.reset:
            print("Removing existing Work nodes (reset mode enabled)...")
            importer.clear_work_nodes()

        importer.import_rows(rows)
    finally:
        driver.close()

    print("Import completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
