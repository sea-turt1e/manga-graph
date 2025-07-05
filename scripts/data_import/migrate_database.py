#!/usr/bin/env python3
"""
Database migration script to apply name normalization
"""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from import_to_neo4j import Neo4jImporter

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts"


def migrate_database():
    """Migrate database to use normalized names"""
    logger.info("Starting database migration to apply name normalization")

    # Neo4j接続情報
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    try:
        importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)

        # Get current database stats before migration
        logger.info("Getting database statistics before migration...")
        old_stats = importer.get_statistics()
        logger.info("Current statistics:")
        for key, value in old_stats.items():
            logger.info(f"  {key}: {value:,}")

        # Ask for confirmation before clearing
        response = input(
            "\n⚠️  This will CLEAR ALL DATA and re-import with normalized names.\nAre you sure? Type 'YES' to continue: "
        )
        if response != "YES":
            logger.info("Migration cancelled")
            return

        # Clear database
        logger.info("Clearing database...")
        importer.clear_database()

        # Recreate constraints
        logger.info("Creating constraints...")
        importer.create_constraints()

        # Re-import data with normalization
        logger.info("Re-importing data with name normalization...")

        # Import manga books
        book_file = DATA_DIR / "metadata101.json"
        if book_file.exists():
            logger.info(f"Importing manga books from {book_file}")
            importer.import_manga_books(book_file)
        else:
            logger.warning(f"Book file not found: {book_file}")

        # Import manga series
        series_file = DATA_DIR / "metadata104.json"
        if series_file.exists():
            logger.info(f"Importing manga series from {series_file}")
            importer.import_manga_series(series_file)
        else:
            logger.warning(f"Series file not found: {series_file}")

        # Create additional relationships
        logger.info("Creating additional relationships...")
        importer.create_additional_relationships()

        # Get new statistics
        logger.info("Getting database statistics after migration...")
        new_stats = importer.get_statistics()

        # Display comparison
        logger.info("\n=== Migration Results ===")
        logger.info("Before -> After:")
        for key in old_stats:
            old_val = old_stats.get(key, 0)
            new_val = new_stats.get(key, 0)
            change = new_val - old_val
            logger.info(f"  {key}: {old_val:,} -> {new_val:,} ({change:+,})")

        logger.info("\n✅ Migration completed successfully!")
        logger.info("The database now uses normalized creator and publisher names.")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        if "importer" in locals():
            importer.close()


def check_normalization_impact():
    """Check how many duplicates would be resolved by normalization"""
    logger.info("Analyzing normalization impact...")

    # This would analyze the JSON files to see how many duplicates exist
    # For now, we'll just log a message
    logger.info("Use this function to analyze the impact before migration")

    # TODO: Implement analysis of JSON files to count duplicates


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        check_normalization_impact()
    else:
        migrate_database()
