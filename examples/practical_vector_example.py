#!/usr/bin/env python3
"""
Practical example of adding embeddings to existing works and performing vector search
"""

import asyncio
import logging
import os
import sys
from typing import List

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import after path setup
from infrastructure.external.neo4j_repository import Neo4jMangaRepository  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_embedding_from_title(title: str) -> List[float]:
    """
    Generate a simple embedding from manga title using character frequencies.
    In production, use OpenAI API or Hugging Face models.
    """
    # Simple character-based embedding for demonstration
    char_counts = {}
    for char in title:
        char_counts[char] = char_counts.get(char, 0) + 1

    # Create a 1536-dimensional vector (matching OpenAI embedding size)
    embedding = []
    for i in range(1536):
        # Use title length and character frequency for pseudo-embedding
        if i < len(title):
            value = ord(title[i % len(title)]) / 1000.0
        else:
            value = sum(char_counts.values()) / (1000.0 * (i + 1))

        # Normalize to reasonable range
        embedding.append((value % 1.0) - 0.5)

    return embedding


class PracticalVectorExample:
    """Practical example using real data from the database"""

    def __init__(self):
        # Initialize Neo4j connection
        self.repository = Neo4jMangaRepository(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "password"),
        )

    def setup_vector_index(self):
        """Create vector index if it doesn't exist"""
        logger.info("Setting up vector index...")
        self.repository.create_vector_index(
            label="Work", property_name="embedding", dimension=1536, similarity="cosine"
        )

    def get_sample_works(self, limit: int = 5) -> List[dict]:
        """Get some sample works from the database"""
        logger.info(f"Getting {limit} sample works from database...")

        with self.repository.driver.session() as session:
            query = """
            MATCH (w:Work)
            WHERE w.title IS NOT NULL
            RETURN w.id as work_id, w.title as title
            LIMIT $limit
            """
            result = session.run(query, limit=limit)

            works = []
            for record in result:
                works.append({"work_id": record["work_id"], "title": record["title"]})

            logger.info(f"Found {len(works)} works")
            for work in works:
                logger.info(f"  - {work['title']} (ID: {work['work_id']})")

            return works

    def add_embeddings_to_works(self, works: List[dict]):
        """Add embeddings to the given works"""
        logger.info("Adding embeddings to works...")

        success_count = 0
        for work in works:
            title = work["title"]
            work_id = work["work_id"]

            # Generate embedding from title
            embedding = generate_embedding_from_title(title)

            # Add to database
            success = self.repository.add_embedding_to_work(work_id, embedding)
            if success:
                success_count += 1
                logger.info(f"✓ Added embedding to: {title}")
            else:
                logger.warning(f"✗ Failed to add embedding to: {title}")

        logger.info(f"Successfully added embeddings to {success_count}/{len(works)} works")

    def search_similar_works(self, query_title: str, limit: int = 5):
        """Search for works similar to the given title"""
        logger.info(f"Searching for works similar to: '{query_title}'")

        # Generate embedding for the query
        query_embedding = generate_embedding_from_title(query_title)

        # Perform vector search
        results = self.repository.search_by_vector(embedding=query_embedding, label="Work", limit=limit)

        logger.info(f"Found {len(results)} similar works:")
        for i, work in enumerate(results, 1):
            score = work.get("similarity_score", "N/A")
            logger.info(f"  {i}. {work['title']} (Score: {score:.4f})")

        return results

    def perform_hybrid_search(self, query: str, limit: int = 5):
        """Perform hybrid search"""
        logger.info(f"Performing hybrid search for: '{query}'")

        query_embedding = generate_embedding_from_title(query)

        results = self.repository.search_manga_works_with_vector(
            search_term=query, embedding=query_embedding, limit=limit
        )

        logger.info(f"Hybrid search found {len(results)} works:")
        for i, work in enumerate(results, 1):
            score = work.get("search_score", work.get("similarity_score", "N/A"))
            creators = ", ".join(work.get("creators", []))
            logger.info(f"  {i}. {work['title']} (Score: {score:.4f}) by {creators}")

        return results

    def close(self):
        """Close database connection"""
        self.repository.close()


async def main():
    """Main function"""
    logger.info("Practical Vector Search Example")
    logger.info("=" * 50)

    try:
        example = PracticalVectorExample()

        # Step 1: Setup vector index
        example.setup_vector_index()

        # Step 2: Get sample works from database
        works = example.get_sample_works(limit=5)

        if not works:
            logger.warning("No works found in database. Please import some data first.")
            return

        # Step 3: Add embeddings to sample works
        example.add_embeddings_to_works(works)

        # Step 4: Test vector search
        logger.info("\n" + "=" * 50)
        logger.info("Vector Search Test")
        logger.info("=" * 50)

        # Use first work's title as query
        query_title = works[0]["title"]
        example.search_similar_works(query_title, limit=3)

        # Step 5: Test hybrid search
        logger.info("\n" + "=" * 50)
        logger.info("Hybrid Search Test")
        logger.info("=" * 50)

        example.perform_hybrid_search("僕の心のやばいやつ", limit=5)

    except Exception as e:
        logger.error(f"Example failed: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        try:
            example.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
