#!/usr/bin/env python3
"""
Neo4j service test script
"""
import json
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService


def test_neo4j_service():
    """Test Neo4j service functionality"""
    service = Neo4jMediaArtsService()
    
    try:
        print("=== Testing Neo4j Service ===")
        
        # Test database statistics
        print("\n1. Database Statistics:")
        stats = service.get_database_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Test search functionality
        print("\n2. Testing Search (using test data):")
        
        # Since our test data might not have ONE PIECE, let's search for any work
        # First, let's see what works are actually in the database
        from infrastructure.external.neo4j_repository import Neo4jMangaRepository
        repo = Neo4jMangaRepository()
        
        # Get some sample works
        with repo.driver.session() as session:
            result = session.run("MATCH (w:Work) RETURN w.title as title LIMIT 5")
            sample_titles = [record['title'] for record in result if record['title']]
        
        if sample_titles:
            search_term = sample_titles[0][:5]  # Use first few characters of first title
            print(f"Searching for: '{search_term}'")
            
            search_result = service.search_manga_data_with_related(search_term, limit=10)
            
            print(f"Found {len(search_result['nodes'])} nodes and {len(search_result['edges'])} edges")
            
            print("\nNodes:")
            for node in search_result['nodes'][:3]:  # Show first 3 nodes
                print(f"  - {node['type']}: {node['label']}")
            
            print("\nEdges:")
            for edge in search_result['edges'][:3]:  # Show first 3 edges
                print(f"  - {edge['source']} --{edge['type']}--> {edge['target']}")
        else:
            print("No works found in database")
        
        # Test performance
        print("\n3. Performance Test:")
        import time
        
        if sample_titles:
            start_time = time.time()
            result = service.search_manga_data_with_related(search_term, limit=20)
            end_time = time.time()
            
            print(f"Search completed in {end_time - start_time:.2f} seconds")
            print(f"Retrieved {len(result['nodes'])} nodes and {len(result['edges'])} edges")
        
    except Exception as e:
        print(f"Error testing Neo4j service: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        service.close()


if __name__ == "__main__":
    test_neo4j_service()