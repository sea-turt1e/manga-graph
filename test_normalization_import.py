#!/usr/bin/env python3
"""
Test the normalization implementation with sample data
"""
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

# Add scripts directory to path
scripts_dir = Path(__file__).parent / "scripts" / "data_import"
sys.path.append(str(scripts_dir))

from import_to_neo4j import Neo4jImporter

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_normalization_with_sample_data():
    """Test normalization with the sample data"""
    logger.info("Testing normalization with sample data")
    
    # Neo4j connection info
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j') 
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')
    
    try:
        importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)
        
        # Load test data
        test_file = Path(__file__).parent / "test_data_sample.json"
        with open(test_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        items = data.get('@graph', [])
        logger.info(f"Loaded {len(items)} test items")
        
        # Create a test namespace in database
        logger.info("Importing test data...")
        
        # Import the test batch
        importer._import_book_batch(items)
        
        # Query the results to verify normalization
        logger.info("Verifying normalization results...")
        
        with importer.driver.session() as session:
            # Check authors
            author_query = """
            MATCH (a:Author)
            WHERE a.name CONTAINS "尾田"
            RETURN a.id, a.name, a.original_name
            ORDER BY a.name
            """
            
            logger.info("Authors found:")
            author_result = session.run(author_query)
            author_count = 0
            for record in author_result:
                author_count += 1
                logger.info(f"  ID: {record['a.id']}")
                logger.info(f"  Normalized: {record['a.name']}")
                logger.info(f"  Original: {record['a.original_name']}")
                logger.info("  ---")
            
            # Check publishers
            publisher_query = """
            MATCH (p:Publisher)
            WHERE p.name CONTAINS "集英社"
            RETURN p.id, p.name, p.original_name
            ORDER BY p.name
            """
            
            logger.info("Publishers found:")
            publisher_result = session.run(publisher_query)
            publisher_count = 0
            for record in publisher_result:
                publisher_count += 1
                logger.info(f"  ID: {record['p.id']}")
                logger.info(f"  Normalized: {record['p.name']}")
                logger.info(f"  Original: {record['p.original_name']}")
                logger.info("  ---")
            
            # Check works
            work_query = """
            MATCH (w:Work)
            WHERE w.title CONTAINS "ONE PIECE"
            OPTIONAL MATCH (a:Author)-[:CREATED]->(w)
            OPTIONAL MATCH (p:Publisher)-[:PUBLISHED]->(w)
            RETURN w.title, collect(a.name) as authors, collect(p.name) as publishers
            ORDER BY w.title
            """
            
            logger.info("Works found:")
            work_result = session.run(work_query)
            work_count = 0
            for record in work_result:
                work_count += 1
                logger.info(f"  Title: {record['w.title']}")
                logger.info(f"  Authors: {record['authors']}")
                logger.info(f"  Publishers: {record['publishers']}")
                logger.info("  ---")
        
        # Evaluation
        logger.info("\n=== Test Results ===")
        logger.info(f"Unique authors found: {author_count}")
        logger.info(f"Unique publishers found: {publisher_count}")
        logger.info(f"Works found: {work_count}")
        
        success = author_count == 1 and publisher_count == 1 and work_count == 3
        
        if success:
            logger.info("🎉 NORMALIZATION TEST PASSED!")
            logger.info("All variants of '尾田栄一郎' were normalized to a single author")
            logger.info("All variants of '集英社' were normalized to a single publisher")
        else:
            logger.error("❌ NORMALIZATION TEST FAILED!")
            logger.error("Expected 1 author, 1 publisher, 3 works")
        
        return success
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        if 'importer' in locals():
            importer.close()


if __name__ == "__main__":
    success = test_normalization_with_sample_data()
    sys.exit(0 if success else 1)