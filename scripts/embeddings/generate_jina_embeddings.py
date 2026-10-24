"""Populate Work node embeddings using jinaai/jina-embeddings-v4.

Usage:
  uv run python scripts/embeddings/generate_jina_embeddings.py --batch-size 128
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from neo4j import GraphDatabase
from tqdm import tqdm

# Ensure the project root is importable when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure env vars are loaded
from config import env  # noqa: F401
from domain.services.jina_embedding_client import get_jina_embedding_client

LOGGER = logging.getLogger("jina_embeddings")

DEFAULT_TITLE_DIMS = int(os.getenv("JINA_TITLE_EMBED_DIM", "128"))
DEFAULT_DESCRIPTION_DIMS = int(os.getenv("JINA_DESCRIPTION_EMBED_DIM", "1024"))
MAX_MODEL_DIMS = 2048
JINA_EMBEDDING_MODE = "local"  # or "api"


def fetch_batch(tx, last_id: Optional[str], limit: int, refresh_all: bool):
    cypher = """
    MATCH (w:Work)
    WHERE $refreshAll = true
       OR w.embedding_title_ja IS NULL
       OR w.embedding_title_en IS NULL
       OR w.embedding_description IS NULL
    WITH w
    WHERE $lastId IS NULL OR w.id > $lastId
    RETURN w.id AS id,
           w.japanese_name AS japanese_name,
           w.english_name AS english_name,
           w.description AS description
    ORDER BY w.id
    LIMIT $limit
    """
    result = tx.run(cypher, lastId=last_id, limit=limit, refreshAll=refresh_all)
    return list(result)


def update_embeddings(tx, rows: List[Dict[str, Any]]):
    cypher = """
    UNWIND $rows AS row
    MATCH (w:Work {id: row.id})
    SET w.embedding_title_ja = row.embedding_title_ja,
        w.embedding_title_en = row.embedding_title_en,
        w.embedding_description = row.embedding_description
    RETURN count(*)
    """
    tx.run(cypher, rows=rows)


def truncate(vector: np.ndarray, dims: int) -> List[float]:
    if dims > vector.shape[-1]:
        raise ValueError(f"Requested dims {dims} exceed vector size {vector.shape[-1]}")
    return vector[:dims].astype(np.float32).tolist()


def encode_field(rows: List[Dict[str, Any]], key: str, dims: int, client) -> List[Optional[List[float]]]:
    texts: List[str] = []
    indices: List[int] = []
    for idx, row in enumerate(rows):
        raw = row.get(key)
        if raw is None:
            continue

        # 数値など非文字列が来ても安全に処理できるように文字列化してから strip
        value = str(raw).strip()
        if not value:
            continue

        texts.append(value)
        indices.append(idx)

    if not texts:
        return [None] * len(rows)

    embeddings = client.encode_batch(texts)
    results: List[Optional[List[float]]] = [None] * len(rows)
    for idx, vec in zip(indices, embeddings):
        results[idx] = truncate(vec, dims)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate jina embeddings for Work nodes")
    parser.add_argument("--batch-size", type=int, default=128, help="Number of works to process per batch")
    parser.add_argument("--title-dims", type=int, default=DEFAULT_TITLE_DIMS, help="Dimension for title embeddings")
    parser.add_argument(
        "--description-dims",
        type=int,
        default=DEFAULT_DESCRIPTION_DIMS,
        help="Dimension for description embeddings",
    )
    parser.add_argument(
        "--refresh-all",
        action="store_true",
        help="Recompute embeddings even if they already exist",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()

    if args.title_dims > MAX_MODEL_DIMS or args.description_dims > MAX_MODEL_DIMS:
        raise ValueError("Requested dimensions exceed model output (2048)")

    uri = os.getenv("MANGA_ANIME_NEO4J_URI") or os.getenv("NEO4J_URI")
    user = os.getenv("MANGA_ANIME_NEO4J_USER") or os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("MANGA_ANIME_NEO4J_PASSWORD") or os.getenv("NEO4J_PASSWORD")
    if not uri or not password:
        raise ValueError("Neo4j connection variables are not set")

    client = get_jina_embedding_client(jina_embedding_mode=JINA_EMBEDDING_MODE)
    driver = GraphDatabase.driver(uri, auth=(user, password))

    last_id: Optional[str] = None
    processed = 0

    with driver.session() as session:
        with tqdm(desc="Embedding works", unit="work") as progress:
            while True:
                batch = session.read_transaction(fetch_batch, last_id, args.batch_size, args.refresh_all)
                if not batch:
                    break

                ja_embeddings = encode_field(batch, "japanese_name", args.title_dims, client)
                en_embeddings = encode_field(batch, "english_name", args.title_dims, client)
                desc_embeddings = encode_field(batch, "description", args.description_dims, client)

                payload = []
                for row, emb_ja, emb_en, emb_desc in zip(batch, ja_embeddings, en_embeddings, desc_embeddings):
                    # Skip rows where nothing could be encoded unless refresh-all requested
                    if not args.refresh_all and not any([emb_ja, emb_en, emb_desc]):
                        continue
                    payload.append(
                        {
                            "id": row["id"],
                            "embedding_title_ja": emb_ja,
                            "embedding_title_en": emb_en,
                            "embedding_description": emb_desc,
                        }
                    )

                if payload:
                    session.write_transaction(update_embeddings, payload)
                    processed += len(payload)
                    progress.update(len(payload))

                last_id = batch[-1]["id"]

    LOGGER.info("Updated embeddings for %s works", processed)
    driver.close()


if __name__ == "__main__":
    main()
