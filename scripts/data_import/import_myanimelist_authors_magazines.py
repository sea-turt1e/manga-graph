#!/usr/bin/env python
"""Import Author and Magazine nodes (and their Work relationships) from MyAnimeList CSV data.

This script reads `manga_entries.csv`, extracts the authors and serialization fields,
creates unique `Author` / `Magazine` nodes, and connects them to `Work` nodes via:

* `(w:Work)-[:CREATED_BY]->(a:Author)`
* `(w:Work)-[:PUBLISHED_IN]->(m:Magazine)`

`Work` nodes are merged by their `id` column (creating them on-the-fly when needed), so it is
safe to run this script even if the Work nodes were not imported beforehand.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import pandas as pd
from neo4j import Driver, GraphDatabase
from tqdm.auto import tqdm

# Ensure project root is importable so we can load config.env for environment variables
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import env  # noqa: F401  # pylint: disable=unused-import

DEFAULT_CSV_PATH = Path(
    "data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv"
)
MISSING_VALUE_TOKENS = {"", "none", "null", "n/a", "na", "unknown"}


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


@dataclass
class WorkRelationRecord:
    work_id: str
    title: Optional[str]
    authors: List[str]
    magazines: List[str]


def parse_args(argv: Optional[List[str]] = None) -> ImportConfig:
    parser = argparse.ArgumentParser(
        description="Create Author/Magazine nodes and relationships from MyAnimeList CSV data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="Path to manga_entries.csv",
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
        default=500,
        help="Number of rows to send per Neo4j transaction",
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
        help="Parse CSV and preview first few rows without writing to Neo4j",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete Author/Magazine nodes and their relationships before import",
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


def sanitize_entries(raw_list: Any) -> List[str]:
    if not raw_list:
        return []

    result: List[str] = []
    seen = set()

    if isinstance(raw_list, str):
        raw_list = [raw_list]

    if not isinstance(raw_list, list):
        return []

    for entry in raw_list:
        if not entry:
            continue
        if not isinstance(entry, str):
            continue
        cleaned = " ".join(entry.strip().split())
        if not cleaned or cleaned.lower() in MISSING_VALUE_TOKENS:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def parse_list_field(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parsed = try_parse_collection(value.strip())
    if parsed is None:
        return []
    return sanitize_entries(parsed)


def load_records(csv_path: Path, limit: Optional[int]) -> List[WorkRelationRecord]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    if limit:
        df = df.head(limit)

    records: List[WorkRelationRecord] = []
    for row in df.to_dict(orient="records"):
        work_id = str(row.get("id", "")).strip()
        if not work_id:
            continue

        authors = parse_list_field(row.get("authors"))
        magazines = parse_list_field(row.get("serialization"))
        title = (row.get("title_name") or row.get("english_name") or "").strip() or None

        if not authors and not magazines:
            continue

        records.append(
            WorkRelationRecord(
                work_id=work_id,
                title=title,
                authors=authors,
                magazines=magazines,
            )
        )

    if not records:
        raise ValueError("No rows with authors or serialization data were found in the CSV.")

    return records


class AuthorMagazineImporter:
    def __init__(self, driver: Driver, batch_size: int = 500) -> None:
        self.driver = driver
        self.batch_size = batch_size

    def ensure_constraints(self) -> None:
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.name IS UNIQUE",
        ]
        with self.driver.session() as session:
            for query in queries:
                session.run(query)

    def clear_author_magazine_graph(self) -> None:
        with self.driver.session() as session:
            session.run("MATCH (:Work)-[r:CREATED_BY|PUBLISHED_IN]->() DELETE r")
            session.run("MATCH (n) WHERE n:Author OR n:Magazine DETACH DELETE n")

    def import_records(self, records: List[WorkRelationRecord]) -> None:
        total_batches = max(1, (len(records) + self.batch_size - 1) // self.batch_size)
        for chunk in tqdm(
            chunked(records, self.batch_size),
            total=total_batches,
            desc="Importing authors/magazines",
        ):
            self._write_chunk(chunk)

    def _write_chunk(self, chunk: List[WorkRelationRecord]) -> None:
        payload = [
            {
                "work_id": record.work_id,
                "title": record.title,
                "authors": record.authors,
                "magazines": record.magazines,
            }
            for record in chunk
        ]
        with self.driver.session() as session:
            session.execute_write(self._write_tx, payload)

    @staticmethod
    def _write_tx(tx, rows: List[Dict[str, Any]]) -> None:
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (w:Work {id: row.work_id})
            ON CREATE SET w.title = row.title
            FOREACH (author IN row.authors |
                MERGE (a:Author {name: author})
                MERGE (w)-[:CREATED_BY]->(a)
            )
            FOREACH (magazine IN row.magazines |
                MERGE (m:Magazine {name: magazine})
                MERGE (w)-[:PUBLISHED_IN]->(m)
            )
            """,
            rows=rows,
        )


def main(argv: Optional[List[str]] = None) -> int:
    config = parse_args(argv)

    print(f"Loading data from {config.csv_path} ...")
    records = load_records(config.csv_path, config.limit)
    print(f"Prepared {len(records)} records containing author/magazine info.")

    if config.dry_run:
        preview = records[:3]
        for idx, record in enumerate(preview, start=1):
            print(
                f"Row {idx}: work_id={record.work_id}, title={record.title}, "
                f"authors={record.authors}, magazines={record.magazines}"
            )
        return 0

    driver = GraphDatabase.driver(config.uri, auth=(config.user, config.password))
    importer = AuthorMagazineImporter(driver, batch_size=config.batch_size)

    try:
        importer.ensure_constraints()
        if config.reset:
            print("Reset flag detected: removing author/magazine nodes and edges...")
            importer.clear_author_magazine_graph()
        importer.import_records(records)
    finally:
        driver.close()

    print("Import completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
