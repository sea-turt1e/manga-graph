"""
Example usage of vector search functionality for manga data
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


class VectorSearchExample:
    """Example class demonstrating vector search functionality"""

    def __init__(self):
        # Initialize Neo4j connection
        self.repository = Neo4jMangaRepository(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "password"),
        )

    def create_vector_indexes(self):
        """Create vector indexes for different node types"""
        logger.info("Creating vector indexes...")

        # Create index for Work nodes
        self.repository.create_vector_index(
            label="Work", property_name="embedding", dimension=1536, similarity="cosine"
        )

        # Create index for Author nodes (if needed)
        self.repository.create_vector_index(
            label="Author", property_name="embedding", dimension=1536, similarity="cosine"
        )

        logger.info("Vector indexes created successfully")

    def add_sample_embeddings(self):
        """Add sample embeddings to work nodes"""
        logger.info("Adding sample embeddings...")

        # Sample embedding vector (1536 dimensions)
        # In practice, you would generate these using a model like OpenAI's text-embedding-ada-002
        sample_embedding = [0.1] * 1536  # Dummy embedding for demonstration

        # Add embedding to a work (you would need to replace with actual work IDs)
        work_id = "sample_work_id"
        success = self.repository.add_embedding_to_work(work_id, sample_embedding)

        if success:
            logger.info(f"Successfully added embedding to work: {work_id}")
        else:
            logger.warning(f"Failed to add embedding to work: {work_id}")

    def perform_vector_search(self, query_embedding: List[float], limit: int = 10):
        """Perform vector similarity search"""
        logger.info(f"Performing vector search with limit: {limit}")

        results = self.repository.search_by_vector(embedding=query_embedding, label="Work", limit=limit)

        logger.info(f"Found {len(results)} similar works")
        for i, work in enumerate(results, 1):
            logger.info(
                f"{i}. {work['title']} (Score: {work.get('similarity_score', 'N/A')}) "
                f"by {', '.join(work.get('creators', []))}"
            )

        return results

    def perform_hybrid_search(self, search_term: str, query_embedding: List[float], limit: int = 10):
        """Perform hybrid search combining text and vector similarity"""
        logger.info(f"Performing hybrid search for: '{search_term}' with limit: {limit}")

        results = self.repository.search_manga_works_with_vector(
            search_term=search_term, embedding=query_embedding, limit=limit
        )

        logger.info(f"Found {len(results)} works through hybrid search")
        for i, work in enumerate(results, 1):
            logger.info(
                f"{i}. {work['title']} (Search Score: {work.get('search_score', 'N/A')}) "
                f"by {', '.join(work.get('creators', []))}"
            )

        return results

    def close(self):
        """Close database connection"""
        self.repository.close()


def generate_dummy_embedding(text: str) -> List[float]:
    """
    Generate a dummy embedding for demonstration purposes.
    In practice, you would use a real embedding model like:

    import openai
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-ada-002"
    )
    return response['data'][0]['embedding']
    """
    # Simple hash-based dummy embedding for demonstration
    import hashlib

    hash_obj = hashlib.md5(text.encode())
    hash_hex = hash_obj.hexdigest()

    # Convert to pseudo-embedding (1536 dimensions)
    embedding = []
    for i in range(1536):
        # Use hash characters cyclically to generate float values
        char_index = i % len(hash_hex)
        value = ord(hash_hex[char_index]) / 255.0 - 0.5  # Normalize to [-0.5, 0.5]
        embedding.append(value)

    return embedding


async def main():
    """Main example function"""
    try:
        example = VectorSearchExample()
    except Exception as e:
        logger.error(f"Failed to initialize VectorSearchExample: {e}")
        logger.error("Make sure Neo4j is running and connection details are correct")
        return

    try:
        # Step 1: Create vector indexes
        logger.info("Step 1: Creating vector indexes...")
        example.create_vector_indexes()

        # Step 2: Add sample embeddings (in practice, you'd do this for all works)
        logger.info("Step 2: Adding sample embeddings...")
        example.add_sample_embeddings()

        # Step 3: Perform vector search
        query_text = "進撃の巨人"
        query_embedding = generate_dummy_embedding(query_text)

        logger.info("=" * 50)
        logger.info("Vector Search Example")
        logger.info("=" * 50)

        example.perform_vector_search(query_embedding, limit=5)

        # Step 4: Perform hybrid search
        logger.info("=" * 50)
        logger.info("Hybrid Search Example")
        logger.info("=" * 50)

        example.perform_hybrid_search(query_text, query_embedding, limit=5)

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
