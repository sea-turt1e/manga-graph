#!/usr/bin/env python3
"""
Test script to verify vector search dependencies and setup
"""

import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)


def test_imports():
    """Test if all required modules can be imported"""
    print("Testing imports...")

    try:
        from infrastructure.external.neo4j_repository import Neo4jMangaRepository

        # Touch symbol to avoid unused warnings
        _ = Neo4jMangaRepository
        print("✓ Neo4jMangaRepository imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import Neo4jMangaRepository: {e}")
        return False

    try:
        from neo4j import GraphDatabase

        _ = GraphDatabase
        print("✓ Neo4j driver imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import Neo4j driver: {e}")
        print("  Please install: pip install neo4j")
        return False

    return True


def test_neo4j_connection():
    """Test Neo4j connection"""
    print("\nTesting Neo4j connection...")

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    print(f"Connecting to: {uri}")
    print(f"User: {user}")

    try:
        from infrastructure.external.neo4j_repository import Neo4jMangaRepository

        # Uses environment variables loaded externally
        repo = Neo4jMangaRepository()
        print("✓ Neo4j connection successful")
        repo.close()
        return True
    except Exception as e:
        print(f"✗ Neo4j connection failed: {e}")
        print("  Make sure Neo4j is running and credentials are correct")
        return False


def main():
    """Run all tests"""
    print("Vector Search Setup Test")
    print("=" * 40)

    # Test imports
    if not test_imports():
        print("\n✗ Import test failed")
        return 1

    # Test Neo4j connection
    if not test_neo4j_connection():
        print("\n✗ Connection test failed")
        return 1

    print("\n✓ All tests passed! Vector search is ready to use.")
    print("\nYou can now run:")
    print("  python examples/vector_search_example.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
