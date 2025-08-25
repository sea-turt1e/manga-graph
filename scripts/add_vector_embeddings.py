#!/usr/bin/env python3
"""
Batch script to add vector embeddings to all works in the database
"""

import logging
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from domain.services.batch_embedding_processor import BatchEmbeddingProcessor  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """Main function to add vector embeddings to all works"""
    logger.info("Starting vector embeddings addition process...")

    # You can specify the model here
    processor = BatchEmbeddingProcessor(sentence_transformer_model="cl-nagoya/ruri-v3-310m")

    try:
        # Setup vector indexes
        logger.info("Setting up vector indexes...")
        processor.setup_vector_indexes()

        # Process works
        logger.info("Getting all works from database...")
        works = processor.get_all_works()
        logger.info(f"Found {len(works)} works to process")

        if works:
            logger.info("Adding embeddings to works...")
            processor.add_embeddings_to_works(works, batch_size=50)

        # Process authors
        logger.info("Getting all authors from database...")
        authors = processor.get_all_authors()
        logger.info(f"Found {len(authors)} authors to process")

        if authors:
            logger.info("Adding embeddings to authors...")
            processor.add_embeddings_to_authors(authors)

        logger.info("Vector embeddings addition process completed successfully!")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        # Cleanup
        processor.cleanup()


if __name__ == "__main__":
    main()
