#!/usr/bin/env python3
"""
Batch script to add vector embeddings to all works in the database
"""

import asyncio
import logging
import os
import sys
import time
from typing import Any, Dict, List

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from infrastructure.external.neo4j_repository import Neo4jMangaRepository  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def generate_embedding_from_text(text: str) -> List[float]:
    """
    Generate embedding from text.

    In production, replace this with:
    1. OpenAI API: openai.Embedding.create()
    2. Hugging Face transformers
    3. Other embedding models
    """
    import hashlib

    # Create a more sophisticated hash-based embedding
    # This is still a mock, but more realistic than random values
    hash_input = text.encode("utf-8")

    # Use multiple hash functions for better distribution
    hashes = []
    for i in range(6):  # Use 6 different hash functions
        hash_obj = hashlib.sha256(hash_input + str(i).encode())
        hash_hex = hash_obj.hexdigest()
        hashes.append(hash_hex)

    # Generate 1536-dimensional vector
    embedding = []
    combined_hash = "".join(hashes)

    for i in range(1536):
        # Use hash characters cyclically
        char_index = i % len(combined_hash)
        # Convert hex character to value and normalize
        if combined_hash[char_index].isdigit():
            value = int(combined_hash[char_index]) / 15.0 - 0.5
        else:
            value = (ord(combined_hash[char_index]) - ord("a") + 10) / 15.0 - 0.5

        # Add some variation based on position
        value += (i % 100) / 10000.0 - 0.005
        embedding.append(value)

    return embedding


def generate_openai_embedding(text: str, api_key: str) -> List[float]:
    """
    Generate embedding using OpenAI API (optional - requires API key)
    """
    try:
        import openai

        openai.api_key = api_key

        response = openai.Embedding.create(input=text, model="text-embedding-ada-002")
        return response["data"][0]["embedding"]
    except ImportError:
        logger.warning("OpenAI library not installed. Install with: pip install openai")
        return generate_embedding_from_text(text)
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return generate_embedding_from_text(text)


def generate_huggingface_embedding(text: str) -> List[float]:
    """
    Generate embedding using Hugging Face sentence-transformers (optional)
    """
    try:
        from sentence_transformers import SentenceTransformer

        # Load model (cached after first use)
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embedding = model.encode(text)

        # Pad or truncate to 1536 dimensions to match OpenAI format
        if len(embedding) < 1536:
            # Pad with zeros
            padded = [0.0] * 1536
            padded[: len(embedding)] = embedding.tolist()
            return padded
        else:
            # Truncate
            return embedding[:1536].tolist()

    except ImportError:
        logger.warning("sentence-transformers not installed. Install with: pip install sentence-transformers")
        return generate_embedding_from_text(text)
    except Exception as e:
        logger.error(f"Hugging Face model error: {e}")
        return generate_embedding_from_text(text)


class BatchEmbeddingProcessor:
    """Batch processor for adding embeddings to all works"""

    def __init__(self, embedding_method: str = "hash"):
        """
        Initialize processor

        Args:
            embedding_method: "hash", "openai", or "huggingface"
        """
        self.repository = Neo4jMangaRepository(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "password"),
        )
        self.embedding_method = embedding_method
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

        logger.info(f"Initialized with embedding method: {embedding_method}")

    def setup_vector_indexes(self):
        """Create vector indexes for all node types"""
        logger.info("Creating vector indexes...")

        # Work nodes (most important)
        self.repository.create_vector_index("Work", "embedding", 1536, "cosine")

        # Author nodes (for author similarity)
        self.repository.create_vector_index("Author", "embedding", 1536, "cosine")

        # Magazine nodes (for magazine similarity)
        self.repository.create_vector_index("Magazine", "embedding", 1536, "cosine")

        # Publisher nodes (for publisher similarity)
        self.repository.create_vector_index("Publisher", "embedding", 1536, "cosine")

        logger.info("Vector indexes created successfully")

    def get_all_works(self, limit: int = None) -> List[Dict[str, Any]]:
        """Get all works from the database"""
        logger.info("Fetching all works from database...")

        with self.repository.driver.session() as session:
            query = """
            MATCH (w:Work)
            WHERE w.title IS NOT NULL AND w.embedding IS NULL
            RETURN w.id as work_id, w.title as title, w.genre as genre,
                   w.published_date as published_date
            """
            if limit:
                query += f" LIMIT {limit}"

            result = session.run(query)
            works = []

            for record in result:
                works.append(
                    {
                        "work_id": record["work_id"],
                        "title": record["title"],
                        "genre": record["genre"],
                        "published_date": record["published_date"],
                    }
                )

            logger.info(f"Found {len(works)} works without embeddings")
            return works

    def get_all_authors(self, limit: int = None) -> List[Dict[str, Any]]:
        """Get all authors from the database"""
        logger.info("Fetching all authors from database...")

        with self.repository.driver.session() as session:
            query = """
            MATCH (a:Author)
            WHERE a.name IS NOT NULL AND a.embedding IS NULL
            RETURN a.id as author_id, a.name as name
            """
            if limit:
                query += f" LIMIT {limit}"

            result = session.run(query)
            authors = []

            for record in result:
                authors.append({"author_id": record["author_id"], "name": record["name"]})

            logger.info(f"Found {len(authors)} authors without embeddings")
            return authors

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding based on selected method"""
        if self.embedding_method == "openai" and self.openai_api_key:
            return generate_openai_embedding(text, self.openai_api_key)
        elif self.embedding_method == "huggingface":
            return generate_huggingface_embedding(text)
        else:
            return generate_embedding_from_text(text)

    def add_embeddings_to_works(self, works: List[Dict[str, Any]], batch_size: int = 100):
        """Add embeddings to works in batches"""
        logger.info(f"Adding embeddings to {len(works)} works...")

        success_count = 0
        failed_count = 0

        for i in range(0, len(works), batch_size):
            batch = works[i : i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: works {i+1}-{min(i+batch_size, len(works))}")

            for work in batch:
                try:
                    # Create text for embedding (combine title and genre)
                    text_parts = [work["title"]]
                    if work["genre"]:
                        text_parts.append(work["genre"])

                    text = " ".join(text_parts)

                    # Generate embedding
                    embedding = self.generate_embedding(text)

                    # Add to database
                    success = self.repository.add_embedding_to_work(work["work_id"], embedding)

                    if success:
                        success_count += 1
                        if success_count % 50 == 0:
                            logger.info(f"✓ Processed {success_count} works successfully")
                    else:
                        failed_count += 1
                        logger.warning(f"✗ Failed to add embedding to: {work['title']}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"✗ Error processing work {work['title']}: {e}")

                # Rate limiting to avoid overwhelming the database
                time.sleep(0.01)  # 10ms delay

            # Longer pause between batches
            time.sleep(1)
            logger.info(f"Batch completed. Success: {success_count}, Failed: {failed_count}")

        logger.info(f"Finished adding embeddings to works. Success: {success_count}, Failed: {failed_count}")

    def add_embeddings_to_authors(self, authors: List[Dict[str, Any]]):
        """Add embeddings to authors"""
        logger.info(f"Adding embeddings to {len(authors)} authors...")

        success_count = 0
        failed_count = 0

        with self.repository.driver.session() as session:
            for author in authors:
                try:
                    # Generate embedding from author name
                    embedding = self.generate_embedding(author["name"])

                    # Add to database
                    query = """
                    MATCH (a:Author {id: $author_id})
                    SET a.embedding = $embedding
                    RETURN a.id as author_id
                    """
                    result = session.run(query, author_id=author["author_id"], embedding=embedding)

                    if result.single():
                        success_count += 1
                        if success_count % 100 == 0:
                            logger.info(f"✓ Processed {success_count} authors successfully")
                    else:
                        failed_count += 1
                        logger.warning(f"✗ Failed to add embedding to author: {author['name']}")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"✗ Error processing author {author['name']}: {e}")

                # Rate limiting
                time.sleep(0.01)

        logger.info(f"Finished adding embeddings to authors. Success: {success_count}, Failed: {failed_count}")

    def get_progress_stats(self) -> Dict[str, Any]:
        """Get current progress statistics"""
        with self.repository.driver.session() as session:
            # Works with embeddings
            work_stats = session.run(
                """
                MATCH (w:Work)
                RETURN 
                    count(*) as total_works,
                    count(w.embedding) as works_with_embeddings
            """
            ).single()

            # Authors with embeddings
            author_stats = session.run(
                """
                MATCH (a:Author)
                RETURN 
                    count(*) as total_authors,
                    count(a.embedding) as authors_with_embeddings
            """
            ).single()

            return {
                "works": {
                    "total": work_stats["total_works"],
                    "with_embeddings": work_stats["works_with_embeddings"],
                    "percentage": (
                        (work_stats["works_with_embeddings"] / work_stats["total_works"] * 100)
                        if work_stats["total_works"] > 0
                        else 0
                    ),
                },
                "authors": {
                    "total": author_stats["total_authors"],
                    "with_embeddings": author_stats["authors_with_embeddings"],
                    "percentage": (
                        (author_stats["authors_with_embeddings"] / author_stats["total_authors"] * 100)
                        if author_stats["total_authors"] > 0
                        else 0
                    ),
                },
            }

    def close(self):
        """Close database connection"""
        self.repository.close()


async def main():
    """Main processing function"""
    import argparse

    parser = argparse.ArgumentParser(description="Add vector embeddings to all nodes in Neo4j database")
    parser.add_argument(
        "--method", choices=["hash", "openai", "huggingface"], default="hash", help="Embedding generation method"
    )
    parser.add_argument("--limit", type=int, help="Limit number of works to process (for testing)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--works-only", action="store_true", help="Only process works, skip authors")
    parser.add_argument("--authors-only", action="store_true", help="Only process authors, skip works")
    parser.add_argument("--progress", action="store_true", help="Show current progress and exit")

    args = parser.parse_args()

    processor = BatchEmbeddingProcessor(embedding_method=args.method)

    try:
        if args.progress:
            # Show progress stats
            stats = processor.get_progress_stats()
            logger.info("Current Progress:")
            logger.info(
                f"Works: {stats['works']['with_embeddings']}/{stats['works']['total']} ({stats['works']['percentage']:.1f}%)"
            )
            logger.info(
                f"Authors: {stats['authors']['with_embeddings']}/{stats['authors']['total']} ({stats['authors']['percentage']:.1f}%)"
            )
            return

        # Setup vector indexes
        processor.setup_vector_indexes()

        if not args.authors_only:
            # Process works
            works = processor.get_all_works(limit=args.limit)
            if works:
                processor.add_embeddings_to_works(works, batch_size=args.batch_size)
            else:
                logger.info("No works found without embeddings")

        if not args.works_only:
            # Process authors
            authors = processor.get_all_authors(limit=args.limit)
            if authors:
                processor.add_embeddings_to_authors(authors)
            else:
                logger.info("No authors found without embeddings")

        # Show final stats
        final_stats = processor.get_progress_stats()
        logger.info("\nFinal Progress:")
        logger.info(
            f"Works: {final_stats['works']['with_embeddings']}/{final_stats['works']['total']} ({final_stats['works']['percentage']:.1f}%)"
        )
        logger.info(
            f"Authors: {final_stats['authors']['with_embeddings']}/{final_stats['authors']['total']} ({final_stats['authors']['percentage']:.1f}%)"
        )

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        processor.close()


if __name__ == "__main__":
    asyncio.run(main())
