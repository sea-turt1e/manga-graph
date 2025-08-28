#!/usr/bin/env python3
"""
Batch embedding processor for generating and managing embeddings
"""

import logging
import os
import time
from typing import Any, Dict, List

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

logger = logging.getLogger(__name__)


def generate_embedding_from_text(text: str, dimension: int = 1536) -> List[float]:
    """
    Generate embedding from text using hash-based approach.

    This is a fallback method when other embedding methods are not available.
    """
    import hashlib

    # Create a more sophisticated hash-based embedding
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

    for i in range(dimension):
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


class BatchEmbeddingProcessor:
    """Batch processor for adding embeddings to all works"""

    def __init__(
        self,
        embedding_method: str = "huggingface",
        sentence_transformer_model: str = "",
        neo4j_uri: str = None,
        neo4j_user: str = None,
        neo4j_password: str = None,
    ):
        """
        Initialize processor

        Args:
            embedding_method: "hash", "openai", or "huggingface"
            sentence_transformer_model: Model name for sentence-transformers
            neo4j_uri: Neo4j URI (defaults to environment variable)
            neo4j_user: Neo4j user (defaults to environment variable)
            neo4j_password: Neo4j password (defaults to environment variable)
        """
        self.repository = Neo4jMangaRepository(
            uri=neo4j_uri or os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=neo4j_user or os.getenv("NEO4J_USER", "neo4j"),
            password=neo4j_password or os.getenv("NEO4J_PASSWORD", "password"),
        )
        self.embedding_method = embedding_method
        self.sentence_transformer_model = sentence_transformer_model
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self._huggingface_model = None  # キャッシュ用

        logger.info(f"Initialized with embedding method: {embedding_method}")
        if embedding_method == "huggingface":
            logger.info(f"Using sentence-transformers model: {sentence_transformer_model}")

    def _get_huggingface_model(self):
        """Get or initialize the Hugging Face model (cached)"""
        if self._huggingface_model is None:
            try:
                import torch
                from sentence_transformers import SentenceTransformer

                logger.info(
                    f"Loading Hugging Face model '{self.sentence_transformer_model}' (this may take a while for first time)..."
                )
                device = "mps" if torch.backends.mps.is_available() else "cpu"
                self._huggingface_model = SentenceTransformer(self.sentence_transformer_model, device=device)
                logger.info(f"Hugging Face model loaded on {device}")
            except Exception as e:
                logger.error(f"Failed to load Hugging Face model: {e}")
                self._huggingface_model = None

        return self._huggingface_model

    def setup_vector_indexes(self, dimension: int = 768):
        """Create vector indexes for all node types"""
        logger.info("Creating vector indexes...")

        # Work nodes (most important)
        self.repository.create_vector_index("Work", "embedding", dimension, "cosine")

        # Author nodes (for author similarity)
        self.repository.create_vector_index("Author", "embedding", dimension, "cosine")

        # Magazine nodes (for magazine similarity)
        self.repository.create_vector_index("Magazine", "embedding", dimension, "cosine")

        # Publisher nodes (for publisher similarity)
        self.repository.create_vector_index("Publisher", "embedding", dimension, "cosine")

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

    def generate_embedding(self, text: str, dimension: int = 768) -> List[float]:
        """Generate embedding based on selected method"""
        if self.embedding_method == "openai" and self.openai_api_key:
            return generate_openai_embedding(text, self.openai_api_key)
        elif self.embedding_method == "huggingface":
            return self._generate_huggingface_embedding(text, dimension)
        else:
            return generate_embedding_from_text(text)

    def _generate_huggingface_embedding(self, text: str, dimension: int = 768) -> List[float]:
        """Generate embedding using cached Hugging Face model"""
        try:
            model = self._get_huggingface_model()
            if model is None:
                raise ValueError("Hugging Face model not available")

            # Generate embedding
            embedding = model.encode(text, convert_to_tensor=True)
            embedding_list = embedding.tolist()

            # Pad or truncate to dimensions to match Neo4j index
            if len(embedding_list) < dimension:
                # Pad with zeros
                padded = [0.0] * dimension
                padded[: len(embedding_list)] = embedding_list
                return padded
            else:
                # Truncate if longer than dimension
                return embedding_list[:dimension]

        except Exception as e:
            logger.error(f"Hugging Face embedding generation error: {e}")
            return generate_embedding_from_text(text)

    def add_embeddings_to_works(self, works: List[Dict[str, Any]], batch_size: int = 100):
        """Add embeddings to works in batches"""
        logger.info(f"Adding embeddings to {len(works)} works...")

        success_count = 0
        failed_count = 0

        for i in range(0, len(works), batch_size):
            batch = works[i : i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}: works {i + 1}-{min(i + batch_size, len(works))}")

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

    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self.repository, "close"):
            self.repository.close()

        # Clear the sentence transformer model to free memory
        if hasattr(self, "_huggingface_model") and self._huggingface_model:
            del self._huggingface_model
            self._huggingface_model = None

    def close(self):
        """Close database connection (deprecated, use cleanup instead)"""
        self.cleanup()
