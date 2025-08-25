#!/usr/bin/env python3
"""
Test the vector search API endpoints
"""

import asyncio
import json
import logging
import os
import sys

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"


def generate_dummy_embedding() -> list:
    """Generate a dummy embedding for testing"""
    return [0.1] * 1536


async def test_create_vector_index():
    """Test vector index creation endpoint"""
    logger.info("Testing vector index creation...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/neo4j/vector/create-index",
                json={"label": "Work", "property_name": "embedding", "dimension": 1536, "similarity": "cosine"},
            )

            if response.status_code == 200:
                logger.info("✓ Vector index creation successful")
                logger.info(f"Response: {response.json()}")
                return True
            else:
                logger.error(f"✗ Vector index creation failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"✗ Request failed: {e}")
            return False


async def test_vector_search():
    """Test vector search endpoint"""
    logger.info("Testing vector search...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/neo4j/vector/search",
                json={"embedding": generate_dummy_embedding(), "limit": 5, "use_hybrid": False},
            )

            if response.status_code == 200:
                result = response.json()
                logger.info("✓ Vector search successful")
                logger.info(f"Found {result['total_nodes']} nodes")

                for i, node in enumerate(result["nodes"][:3], 1):
                    score = node["properties"].get("similarity_score", "N/A")
                    logger.info(f"  {i}. {node['label']} (Score: {score})")

                return True
            else:
                logger.error(f"✗ Vector search failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"✗ Request failed: {e}")
            return False


async def test_hybrid_search():
    """Test hybrid search endpoint"""
    logger.info("Testing hybrid search...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/neo4j/vector/search",
                json={"query": "進撃", "embedding": generate_dummy_embedding(), "limit": 5, "use_hybrid": True},
            )

            if response.status_code == 200:
                result = response.json()
                logger.info("✓ Hybrid search successful")
                logger.info(f"Found {result['total_nodes']} nodes")

                for i, node in enumerate(result["nodes"][:3], 1):
                    score = node["properties"].get("search_score", node["properties"].get("similarity_score", "N/A"))
                    logger.info(f"  {i}. {node['label']} (Score: {score})")

                return True
            else:
                logger.error(f"✗ Hybrid search failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"✗ Request failed: {e}")
            return False


async def test_add_embedding():
    """Test add embedding endpoint"""
    logger.info("Testing add embedding...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/neo4j/vector/add-embedding",
                json={
                    "work_id": "https://mediaarts-db.artmuseums.go.jp/id/M1032568",
                    "embedding": generate_dummy_embedding(),
                },
            )

            if response.status_code == 200:
                logger.info("✓ Add embedding successful")
                logger.info(f"Response: {response.json()}")
                return True
            else:
                logger.error(f"✗ Add embedding failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"✗ Request failed: {e}")
            return False


async def check_server_running():
    """Check if the server is running"""
    logger.info("Checking if server is running...")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/docs")
            if response.status_code == 200:
                logger.info("✓ Server is running")
                return True
            else:
                logger.error("✗ Server returned unexpected status")
                return False
        except Exception as e:
            logger.error(f"✗ Server is not running: {e}")
            logger.error("Please start the server with: uvicorn main:app --reload")
            return False


async def main():
    """Run all API tests"""
    logger.info("Vector Search API Test")
    logger.info("=" * 40)

    # Check if server is running
    if not await check_server_running():
        return 1

    success_count = 0
    total_tests = 4

    # Test vector index creation
    if await test_create_vector_index():
        success_count += 1

    # Test vector search
    if await test_vector_search():
        success_count += 1

    # Test hybrid search
    if await test_hybrid_search():
        success_count += 1

    # Test add embedding
    if await test_add_embedding():
        success_count += 1

    logger.info("\n" + "=" * 40)
    logger.info(f"Tests passed: {success_count}/{total_tests}")

    if success_count == total_tests:
        logger.info("✓ All tests passed!")
        return 0
    else:
        logger.error("✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
