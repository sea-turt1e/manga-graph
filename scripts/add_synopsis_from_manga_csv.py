#!/usr/bin/env python3
"""
Script to add synopsis data from manga.csv to Neo4j database and create embeddings
"""

import csv
import logging
import os
import sys
from pathlib import Path
from typing import List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def clean_title(title: str) -> str:
    """Clean title for better matching"""
    if not title:
        return ""

    # Remove extra whitespace and normalize
    title = title.strip()

    # Remove common punctuation and symbols that might cause mismatches
    import re

    # Remove ～, 〜, -, !, ?, spaces, and other common punctuation
    title = re.sub(r"[～〜\-!?！？\s]+", "", title)

    # Remove parentheses and their contents (often contain extra info)
    title = re.sub(r"[\(（].*?[\)）]", "", title)

    return title.strip()


def load_manga_csv(csv_path: str) -> List[dict]:
    """Load manga data from CSV file"""
    manga_data = []

    logger.info(f"Loading manga data from: {csv_path}")

    try:
        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Only process rows that have both title_japanese and synopsis
                if row.get("title_japanese") and row.get("synopsis"):
                    manga_data.append(
                        {
                            "manga_id": row.get("manga_id", ""),
                            "title_japanese": clean_title(row.get("title_japanese", "")),
                            "title_english": row.get("title_english", ""),
                            "synopsis": row.get("synopsis", "").strip(),
                            "genres": row.get("genres", ""),
                            "themes": row.get("themes", ""),
                            "demographics": row.get("demographics", ""),
                            "score": row.get("score", ""),
                            "scored_by": row.get("scored_by", ""),
                        }
                    )

        logger.info(f"Loaded {len(manga_data)} manga entries with synopsis data")
        return manga_data

    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return []


def find_matching_works(
    repository: Neo4jMangaRepository, manga_data: List[dict], is_preview: bool = False
) -> List[dict]:
    """Find matching works in Neo4j database"""
    matches = []

    logger.info("Finding matching works in Neo4j database...")

    with repository.driver.session() as session:
        from tqdm import tqdm

        if is_preview:
            manga_data = manga_data[:100]  # Limit to first 20 entries for preview
        for manga in tqdm(manga_data):
            title_japanese = manga["title_japanese"]
            cleaned_title = clean_title(title_japanese)

            # Try exact match first (with both original and cleaned titles)
            query = """
            MATCH (w:Work)
            WHERE w.title = $title OR w.title = $cleaned_title
            RETURN w.id as work_id, w.title as title
            LIMIT 1
            """

            result = session.run(query, title=title_japanese, cleaned_title=cleaned_title)
            record = result.single()

            if record:
                matches.append({"work_id": record["work_id"], "neo4j_title": record["title"], "manga_data": manga})
                logger.debug(f"Exact match found: {title_japanese} -> {record['work_id']}")
            else:
                # Try flexible matching with multiple approaches
                query = """
                MATCH (w:Work)
                WHERE (
                    // Approach 1: Cleaned vs cleaned comparison (remove spaces from both)
                    (replace(toLower(w.title), ' ', '') = replace(toLower($title), ' ', ''))
                    OR
                    // Approach 2: Original contains cleaned (for "ONE PIECE" contains "ONEPIECE")
                    (toLower(w.title) CONTAINS toLower($cleaned_title) AND size($cleaned_title) >= 5)
                    OR
                    // Approach 3: Cleaned contains original (for "ONEPIECE" contains "ONE PIECE")
                    (toLower($cleaned_title) CONTAINS toLower(w.title) AND size(w.title) >= 5)
                )
                // Ensure reasonable similarity
                AND (
                    abs(size(replace(w.title, ' ', '')) - size(replace($title, ' ', ''))) <= 3
                )
                RETURN w.id as work_id, w.title as title,
                       size(replace(w.title, ' ', '')) as neo4j_length_clean,
                       size(replace($title, ' ', '')) as csv_length_clean
                ORDER BY abs(size(replace(w.title, ' ', '')) - size(replace($title, ' ', ''))) ASC
                LIMIT 1
                """

                result = session.run(query, title=title_japanese, cleaned_title=cleaned_title)
                record = result.single()

                if record:
                    # Additional validation: check if the match makes sense
                    neo4j_title = record["title"]
                    neo4j_length_clean = record["neo4j_length_clean"]
                    csv_length_clean = record["csv_length_clean"]

                    # Allow reasonable length differences
                    if neo4j_length_clean > 0 and csv_length_clean > 0:
                        length_ratio = max(neo4j_length_clean, csv_length_clean) / min(
                            neo4j_length_clean, csv_length_clean
                        )

                        if length_ratio <= 1.8:  # More lenient for titles like "ONE PIECE"
                            matches.append(
                                {"work_id": record["work_id"], "neo4j_title": neo4j_title, "manga_data": manga}
                            )
                            logger.debug(
                                f"Flexible match found: {title_japanese} -> {neo4j_title} ({record['work_id']})"
                            )
                        else:
                            logger.debug(
                                f"Rejected match (length ratio {length_ratio:.1f}): {title_japanese} -> {neo4j_title}"
                            )

    logger.info(f"Found {len(matches)} matching works")
    return matches


def update_works_with_synopsis(
    repository: Neo4jMangaRepository,
    matches: List[dict],
    create_embeddings: bool = True,
    embedding_method: str = "huggingface",
):
    """Update Neo4j works with synopsis data and embeddings"""
    logger.info(f"Updating {len(matches)} works with synopsis data...")

    # Initialize embedding processor if embeddings are requested
    embedding_processor = None
    if create_embeddings:
        from domain.services.batch_embedding_processor import BatchEmbeddingProcessor

        # Create processor with specified model
        if embedding_method == "huggingface":
            embedding_processor = BatchEmbeddingProcessor(
                sentence_transformer_model="sentence-transformers/all-mpnet-base-v2"
            )
        else:
            embedding_processor = BatchEmbeddingProcessor()

    success_count = 0
    error_count = 0

    with repository.driver.session() as session:
        for match in matches:
            try:
                work_id = match["work_id"]
                manga_data = match["manga_data"]
                synopsis = manga_data["synopsis"]

                # Generate embedding if requested
                synopsis_embedding = None
                if create_embeddings and embedding_processor:
                    synopsis_embedding = embedding_processor.generate_embedding(
                        synopsis,
                        dimension=768 if embedding_method == "huggingface" else 1536,
                    )  # Update work with synopsis and optional embedding
                if synopsis_embedding:
                    query = """
                    MATCH (w:Work {id: $work_id})
                    SET w.synopsis = $synopsis,
                        w.synopsis_embedding = $synopsis_embedding,
                        w.genres_csv = $genres,
                        w.themes_csv = $themes,
                        w.demographics_csv = $demographics,
                        w.manga_csv_id = $manga_id,
                        w.score = $score,
                        w.scored_by = $scored_by,
                        w.updated_with_synopsis = timestamp()
                    RETURN w.id
                    """
                    result = session.run(
                        query,
                        work_id=work_id,
                        synopsis=synopsis,
                        synopsis_embedding=synopsis_embedding,
                        genres=manga_data["genres"],
                        themes=manga_data["themes"],
                        demographics=manga_data["demographics"],
                        manga_id=manga_data["manga_id"],
                        score=manga_data["score"],
                        scored_by=manga_data["scored_by"],
                    )
                else:
                    query = """
                    MATCH (w:Work {id: $work_id})
                    SET w.synopsis = $synopsis,
                        w.genres_csv = $genres,
                        w.themes_csv = $themes,
                        w.demographics_csv = $demographics,
                        w.manga_csv_id = $manga_id,
                        w.score = $score,
                        w.scored_by = $scored_by,
                        w.updated_with_synopsis = timestamp()
                    RETURN w.id
                    """
                    result = session.run(
                        query,
                        work_id=work_id,
                        synopsis=synopsis,
                        genres=manga_data["genres"],
                        themes=manga_data["themes"],
                        demographics=manga_data["demographics"],
                        manga_id=manga_data["manga_id"],
                        score=manga_data["score"],
                        scored_by=manga_data["scored_by"],
                    )

                if result.single():
                    success_count += 1
                    if success_count % 10 == 0:
                        logger.info(f"Updated {success_count} works so far...")
                else:
                    error_count += 1
                    logger.warning(f"Failed to update work {work_id}")

            except Exception as e:
                error_count += 1
                logger.error(f"Error updating work {match['work_id']}: {e}")

    logger.info(f"Update completed: {success_count} successful, {error_count} errors")

    # Clean up embedding processor
    if embedding_processor:
        embedding_processor.cleanup()


def create_synopsis_vector_index(repository: Neo4jMangaRepository):
    """Create vector index for synopsis embeddings"""
    logger.info("Creating vector index for synopsis embeddings...")

    try:
        repository.create_vector_index(
            label="Work", property_name="synopsis_embedding", dimension=768, similarity="cosine"
        )
        logger.info("Synopsis vector index created successfully")
    except Exception as e:
        logger.error(f"Failed to create synopsis vector index: {e}")


def get_statistics(repository: Neo4jMangaRepository):
    """Get statistics about synopsis data in the database"""
    logger.info("Getting synopsis statistics...")

    with repository.driver.session() as session:
        # Count works with synopsis
        query = """
        MATCH (w:Work)
        RETURN
            count(*) as total_works,
            count(w.synopsis) as works_with_synopsis,
            count(w.synopsis_embedding) as works_with_synopsis_embedding
        """
        result = session.run(query)
        stats = result.single()

        logger.info(f"Total works: {stats['total_works']}")
        logger.info(f"Works with synopsis: {stats['works_with_synopsis']}")
        logger.info(f"Works with synopsis embedding: {stats['works_with_synopsis_embedding']}")


def main():
    """Main function"""
    import argparse

    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="Add synopsis data from manga.csv to Neo4j")
    parser.add_argument("csv_path", nargs="?", help="Path to manga.csv file")
    parser.add_argument("--no-embeddings", action="store_true", help="Skip creating embeddings")
    parser.add_argument(
        "--method", choices=["hash", "openai", "huggingface"], default="huggingface", help="Embedding generation method"
    )
    parser.add_argument("--create-index", action="store_true", help="Create vector index for synopsis embeddings")
    parser.add_argument("--preview", action="store_true", help="Preview matches without updating")
    parser.add_argument("--stats", action="store_true", help="Show statistics about synopsis data")

    args = parser.parse_args()

    # Initialize Neo4j repository
    repository = Neo4jMangaRepository(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password"),
    )

    try:
        # Show statistics if requested
        if args.stats:
            get_statistics(repository)
            return

        # Create vector index if requested
        if args.create_index:
            create_synopsis_vector_index(repository)
            return

        # CSV path is required for other operations
        if not args.csv_path:
            parser.error("csv_path is required unless using --stats or --create-index")

        # Check if CSV file exists
        if not os.path.exists(args.csv_path):
            logger.error(f"CSV file not found: {args.csv_path}")
            return

        # Load manga data from CSV
        manga_data = load_manga_csv(args.csv_path)
        if not manga_data:
            logger.error("No manga data loaded")
            return

        # Find matching works
        matches = find_matching_works(repository, manga_data, is_preview=args.preview)
        if not matches:
            logger.warning("No matching works found")
            return

        # Preview matches
        logger.info("\n=== Preview of matches ===")
        for i, match in enumerate(matches[:100]):  # Show first 100
            manga_data = match["manga_data"]
            logger.info(f"{i + 1}. {manga_data['title_japanese']} -> {match['neo4j_title']}")
            logger.info(f"   Work ID: {match['work_id']}")
            logger.info(f"   Synopsis: {manga_data['synopsis'][:100]}...")
            logger.info("")

        if len(matches) > 10:
            logger.info(f"   ... and {len(matches) - 10} more matches")

        if args.preview:
            logger.info("Preview mode - no updates performed")
            return

        # Update works with synopsis data
        create_embeddings = not args.no_embeddings
        update_works_with_synopsis(repository, matches, create_embeddings, args.method)

        # Show final statistics
        logger.info("\n=== Final Statistics ===")
        get_statistics(repository)

        logger.info("Synopsis update completed successfully!")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback

        logger.error(traceback.format_exc())
    finally:
        repository.close()


if __name__ == "__main__":
    main()
