#!/usr/bin/env python3
"""
連続バッチ処理でプロパティを削除
"""

import logging

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_properties_in_batches():
    repository = Neo4jMangaRepository(uri="bolt://localhost:7687", user="neo4j", password="password")

    try:
        with repository.driver.session() as session:
            batch_size = 1000
            total_processed = 0

            while True:
                result = session.run(
                    """
                    CALL {
                      MATCH (w:Work)
                      WHERE w.synopsis IS NOT NULL OR w.synopsis_embedding IS NOT NULL OR w.embedding IS NOT NULL
                      WITH w LIMIT $batch_size
                      REMOVE w.synopsis, w.synopsis_embedding, w.embedding
                      RETURN count(w) as batch_count
                    }
                    RETURN batch_count
                """,
                    batch_size=batch_size,
                )

                batch_count = result.single()["batch_count"]
                total_processed += batch_count

                logger.info(f"Processed batch: {batch_count} nodes (Total: {total_processed})")

                if batch_count == 0:
                    break

            logger.info(f"Completed! Total processed: {total_processed} nodes")

    finally:
        repository.close()


if __name__ == "__main__":
    remove_properties_in_batches()
