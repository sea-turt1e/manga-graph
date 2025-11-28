#!/usr/bin/env python3
"""Create recommended Neo4j indexes for optimal query performance.

Usage:
    uv run python scripts/create_neo4j_indexes.py
    uv run python scripts/create_neo4j_indexes.py --show-only  # Show existing indexes only
    uv run python scripts/create_neo4j_indexes.py --vector-dim-title 256 --vector-dim-desc 1024
"""

import argparse
import logging
import os
import sys

from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def show_existing_indexes(driver) -> list[dict]:
    """Show all existing indexes and return them as a list."""
    with driver.session() as session:
        result = session.run("SHOW INDEXES")
        indexes = []
        logger.info("\n" + "=" * 60)
        logger.info("EXISTING INDEXES")
        logger.info("=" * 60)
        for record in result:
            index_info = {
                "name": record["name"],
                "type": record["type"],
                "state": record["state"],
                "entity_type": record["entityType"],
                "labels_or_types": record["labelsOrTypes"],
                "properties": record["properties"],
            }
            indexes.append(index_info)
            labels = index_info["labels_or_types"] or "N/A"
            props = index_info["properties"] or "N/A"
            logger.info(
                f"  [{index_info['type']:10}] {index_info['name']:35} "
                f"| {index_info['state']:7} | {labels} -> {props}"
            )
        logger.info("=" * 60 + "\n")
        return indexes


def get_existing_index_info(indexes: list[dict]) -> dict:
    """Extract existing index information for comparison."""
    existing = {
        "property_indexes": set(),  # (label, property) tuples
        "fulltext_indexes": set(),  # index names
        "vector_indexes": set(),  # index names
    }

    for idx in indexes:
        idx_type = idx["type"]
        labels = idx["labels_or_types"] or []
        props = idx["properties"] or []

        if idx_type == "RANGE" and labels and props:
            for label in labels:
                for prop in props:
                    existing["property_indexes"].add((label, prop))
        elif idx_type == "FULLTEXT":
            existing["fulltext_indexes"].add(idx["name"])
        elif idx_type == "VECTOR":
            existing["vector_indexes"].add(idx["name"])

    return existing


def create_property_indexes(session, existing: dict) -> int:
    """Create property indexes for common query patterns."""
    property_indexes = [
        # Work node indexes
        ("Work", "id", "work_id_index"),
        ("Work", "title_name", "work_title_name_index"),
        ("Work", "japanese_name", "work_japanese_name_index"),
        ("Work", "english_name", "work_english_name_index"),
        ("Work", "members", "work_members_index"),
        ("Work", "media_type", "work_media_type_index"),
        # Author node indexes
        ("Author", "id", "author_id_index"),
        ("Author", "name", "author_name_index"),
        # Magazine node indexes
        ("Magazine", "id", "magazine_id_index"),
        ("Magazine", "name", "magazine_name_index"),
        # Publisher node indexes
        ("Publisher", "id", "publisher_id_index"),
        ("Publisher", "name", "publisher_name_index"),
    ]

    created_count = 0
    for label, prop, index_name in property_indexes:
        if (label, prop) in existing["property_indexes"]:
            logger.info(f"  [SKIP] Property index on {label}.{prop} already exists")
            continue

        try:
            query = f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{label}) ON (n.{prop})"
            session.run(query)
            logger.info(f"  [CREATE] {index_name}: {label}.{prop}")
            created_count += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"  [SKIP] {index_name} already exists")
            else:
                logger.warning(f"  [ERROR] Failed to create {index_name}: {e}")

    return created_count


def create_fulltext_index(session, existing: dict) -> int:
    """Create fulltext index for text search."""
    index_name = "work_titles_fulltext"

    if index_name in existing["fulltext_indexes"]:
        logger.info(f"  [SKIP] Fulltext index '{index_name}' already exists")
        return 0

    try:
        # Note: CREATE ... IF NOT EXISTS does not work for fulltext indexes
        # We need to check existence first
        query = """
        CALL db.index.fulltext.createNodeIndex(
            'work_titles_fulltext',
            ['Work'],
            ['title_name', 'english_name', 'japanese_name', 'title'],
            {analyzer: 'standard-folding'}
        )
        """
        session.run(query)
        logger.info(f"  [CREATE] {index_name}: Work[title_name, english_name, japanese_name, title]")
        return 1
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"  [SKIP] {index_name} already exists")
        else:
            logger.warning(f"  [ERROR] Failed to create fulltext index: {e}")
        return 0


def create_vector_indexes(session, existing: dict, title_dim: int, desc_dim: int) -> int:
    """Create vector indexes for embedding similarity search."""
    vector_indexes = [
        ("work_embedding_title_ja", "embedding_title_ja", title_dim),
        ("work_embedding_title_en", "embedding_title_en", title_dim),
        ("work_embedding_description", "embedding_description", desc_dim),
    ]

    created_count = 0
    for index_name, property_name, dimension in vector_indexes:
        if index_name in existing["vector_indexes"]:
            logger.info(f"  [SKIP] Vector index '{index_name}' already exists")
            continue

        try:
            # Neo4j 5.x vector index creation syntax
            query = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (w:Work) ON (w.{property_name})
            OPTIONS {{
                indexConfig: {{
                    `vector.dimensions`: {dimension},
                    `vector.similarity_function`: 'cosine'
                }}
            }}
            """
            session.run(query)
            logger.info(f"  [CREATE] {index_name}: Work.{property_name} (dim={dimension}, cosine)")
            created_count += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"  [SKIP] {index_name} already exists")
            else:
                # Try alternative syntax for older Neo4j versions
                try:
                    alt_query = f"""
                    CALL db.index.vector.createNodeIndex(
                        '{index_name}',
                        'Work',
                        '{property_name}',
                        {dimension},
                        'cosine'
                    )
                    """
                    session.run(alt_query)
                    logger.info(f"  [CREATE] {index_name}: Work.{property_name} (dim={dimension}, cosine)")
                    created_count += 1
                except Exception as e2:
                    if "already exists" in str(e2).lower():
                        logger.info(f"  [SKIP] {index_name} already exists")
                    else:
                        logger.warning(f"  [ERROR] Failed to create vector index {index_name}: {e2}")

    return created_count


def create_composite_indexes(session, existing: dict) -> int:
    """Create composite indexes for complex queries."""
    # Composite index for sorting by members
    composite_indexes = [
        # For queries that filter by label and sort by members
        ("Work", ["media_type", "members"], "work_media_type_members_index"),
    ]

    created_count = 0
    for label, props, index_name in composite_indexes:
        # Check if composite already exists
        props_tuple = tuple(props)
        key = (label, props_tuple)

        try:
            props_str = ", ".join([f"n.{p}" for p in props])
            query = f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{label}) ON ({props_str})"
            session.run(query)
            logger.info(f"  [CREATE] {index_name}: {label}.{props}")
            created_count += 1
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"  [SKIP] {index_name} already exists")
            else:
                logger.warning(f"  [ERROR] Failed to create composite index {index_name}: {e}")

    return created_count


def main():
    parser = argparse.ArgumentParser(description="Create Neo4j indexes for manga-graph")
    parser.add_argument("--show-only", action="store_true", help="Show existing indexes only")
    parser.add_argument("--vector-dim-title", type=int, default=256, help="Vector dimension for title embeddings")
    parser.add_argument("--vector-dim-desc", type=int, default=1024, help="Vector dimension for description embeddings")
    parser.add_argument("--uri", type=str, help="Neo4j URI (default: from env)")
    parser.add_argument("--user", type=str, help="Neo4j user (default: from env)")
    parser.add_argument("--password", type=str, help="Neo4j password (default: from env)")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    # Get connection details
    uri = args.uri or os.getenv("NEO4J_URI")
    user = args.user or os.getenv("NEO4J_USER", "neo4j")
    password = args.password or os.getenv("NEO4J_PASSWORD")

    if not uri:
        logger.error("Neo4j URI not provided. Set NEO4J_URI environment variable.")
        sys.exit(1)
    if not password:
        logger.error("Neo4j password not provided. Set NEO4J_PASSWORD environment variable.")
        sys.exit(1)

    logger.info(f"Connecting to Neo4j at {uri}")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        # Verify connection
        driver.verify_connectivity()
        logger.info("Connected successfully!")

        # Show existing indexes
        existing_indexes = show_existing_indexes(driver)
        existing_info = get_existing_index_info(existing_indexes)

        if args.show_only:
            logger.info("Show-only mode. Exiting without creating indexes.")
            return

        # Create indexes
        logger.info("Creating indexes...")
        total_created = 0

        with driver.session() as session:
            logger.info("\n--- Property Indexes ---")
            total_created += create_property_indexes(session, existing_info)

            logger.info("\n--- Fulltext Indexes ---")
            total_created += create_fulltext_index(session, existing_info)

            logger.info("\n--- Vector Indexes ---")
            total_created += create_vector_indexes(
                session, existing_info, args.vector_dim_title, args.vector_dim_desc
            )

            logger.info("\n--- Composite Indexes ---")
            total_created += create_composite_indexes(session, existing_info)

        logger.info(f"\nCreated {total_created} new index(es)")

        # Show updated indexes
        logger.info("\nVerifying indexes...")
        show_existing_indexes(driver)

        logger.info("Done!")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
