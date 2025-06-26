#!/usr/bin/env python3
"""
Performance test script for Neo4j vs SPARQL comparison
"""
import asyncio
import time
import requests
import json
from typing import Dict, Any

# API endpoints
BASE_URL = "http://localhost:8000"
NEO4J_SEARCH_URL = f"{BASE_URL}/api/v1/neo4j/search"
MEDIA_ARTS_SEARCH_URL = f"{BASE_URL}/api/v1/media-arts/search-with-related"
NEO4J_STATS_URL = f"{BASE_URL}/api/v1/neo4j/stats"

def test_neo4j_search_performance(search_term: str = "キジトラ猫", iterations: int = 5) -> Dict[str, Any]:
    """Test Neo4j search performance"""
    print(f"\n=== Testing Neo4j Search Performance ===")
    print(f"Search term: '{search_term}'")
    print(f"Iterations: {iterations}")
    
    times = []
    results = []
    
    for i in range(iterations):
        print(f"Iteration {i+1}/{iterations}...")
        
        start_time = time.time()
        
        try:
            response = requests.get(NEO4J_SEARCH_URL, params={
                'q': search_term,
                'limit': 20,
                'include_related': True
            })
            
            end_time = time.time()
            elapsed = end_time - start_time
            times.append(elapsed)
            
            if response.status_code == 200:
                data = response.json()
                result = {
                    'nodes': len(data.get('nodes', [])),
                    'edges': len(data.get('edges', [])),
                    'time': elapsed
                }
                results.append(result)
                print(f"  Success: {result['nodes']} nodes, {result['edges']} edges in {elapsed:.3f}s")
            else:
                print(f"  Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"  Exception: {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\nNeo4j Performance Results:")
        print(f"  Average time: {avg_time:.3f}s")
        print(f"  Min time: {min_time:.3f}s")
        print(f"  Max time: {max_time:.3f}s")
        
        if results:
            avg_nodes = sum(r['nodes'] for r in results) / len(results)
            avg_edges = sum(r['edges'] for r in results) / len(results)
            print(f"  Average nodes: {avg_nodes:.1f}")
            print(f"  Average edges: {avg_edges:.1f}")
        
        return {
            'type': 'neo4j',
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'results': results
        }
    
    return {'type': 'neo4j', 'error': 'No successful requests'}

def test_media_arts_search_performance(search_term: str = "ONE PIECE", iterations: int = 3) -> Dict[str, Any]:
    """Test Media Arts SPARQL search performance"""
    print(f"\n=== Testing Media Arts SPARQL Performance ===")
    print(f"Search term: '{search_term}'")
    print(f"Iterations: {iterations}")
    print("Note: This uses SPARQL and may take 30-50 seconds per request")
    
    times = []
    results = []
    
    for i in range(iterations):
        print(f"Iteration {i+1}/{iterations}...")
        
        start_time = time.time()
        
        try:
            response = requests.get(MEDIA_ARTS_SEARCH_URL, params={
                'q': search_term,
                'limit': 20,
                'include_related': True
            }, timeout=120)  # 2 minute timeout
            
            end_time = time.time()
            elapsed = end_time - start_time
            times.append(elapsed)
            
            if response.status_code == 200:
                data = response.json()
                result = {
                    'nodes': len(data.get('nodes', [])),
                    'edges': len(data.get('edges', [])),
                    'time': elapsed
                }
                results.append(result)
                print(f"  Success: {result['nodes']} nodes, {result['edges']} edges in {elapsed:.3f}s")
            else:
                print(f"  Error: {response.status_code} - {response.text}")
                
        except requests.Timeout:
            print(f"  Timeout after 120 seconds")
        except Exception as e:
            print(f"  Exception: {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\nMedia Arts SPARQL Performance Results:")
        print(f"  Average time: {avg_time:.3f}s")
        print(f"  Min time: {min_time:.3f}s")
        print(f"  Max time: {max_time:.3f}s")
        
        if results:
            avg_nodes = sum(r['nodes'] for r in results) / len(results)
            avg_edges = sum(r['edges'] for r in results) / len(results)
            print(f"  Average nodes: {avg_nodes:.1f}")
            print(f"  Average edges: {avg_edges:.1f}")
        
        return {
            'type': 'sparql',
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'results': results
        }
    
    return {'type': 'sparql', 'error': 'No successful requests'}

def get_neo4j_stats():
    """Get Neo4j database statistics"""
    print(f"\n=== Neo4j Database Statistics ===")
    
    try:
        response = requests.get(NEO4J_STATS_URL)
        
        if response.status_code == 200:
            data = response.json()
            stats = data.get('data', {})
            
            print(f"Database Contents:")
            for key, value in stats.items():
                print(f"  {key}: {value:,}")
            
            return stats
        else:
            print(f"Error getting stats: {response.status_code}")
            
    except Exception as e:
        print(f"Exception getting stats: {e}")
    
    return None

def main():
    """Main performance test"""
    print("Manga Graph API Performance Test")
    print("=" * 50)
    
    # Check if API is running
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code != 200:
            print("Error: API is not responding")
            return
    except Exception:
        print("Error: Cannot connect to API. Make sure the server is running at http://localhost:8000")
        return
    
    # Get database statistics
    stats = get_neo4j_stats()
    
    if not stats or stats.get('work_count', 0) == 0:
        print("\nWarning: Neo4j database appears to be empty.")
        print("Please run the data import script first.")
        return
    
    # Test Neo4j performance (fast, local database)
    neo4j_results = test_neo4j_search_performance("キジトラ猫", 5)
    
    # Optionally test SPARQL performance (slow, external API)
    # Only run if user confirms, as it takes a long time
    test_sparql = input("\nDo you want to test SPARQL performance? (takes 30-50s per request) [y/N]: ")
    
    sparql_results = None
    if test_sparql.lower() == 'y':
        sparql_results = test_media_arts_search_performance("ONE PIECE", 2)
    
    # Summary comparison
    print(f"\n" + "=" * 50)
    print("PERFORMANCE COMPARISON SUMMARY")
    print("=" * 50)
    
    if 'avg_time' in neo4j_results:
        print(f"Neo4j (Local):    {neo4j_results['avg_time']:.3f}s average")
    
    if sparql_results and 'avg_time' in sparql_results:
        print(f"SPARQL (Remote):   {sparql_results['avg_time']:.3f}s average")
        speedup = sparql_results['avg_time'] / neo4j_results['avg_time']
        print(f"Speedup:           {speedup:.1f}x faster with Neo4j")
    else:
        print("SPARQL test skipped or failed")
    
    print(f"\nDatabase size:     {stats.get('work_count', 0):,} works")
    print(f"                   {stats.get('author_count', 0):,} authors")
    print(f"                   {stats.get('publisher_count', 0):,} publishers")

if __name__ == "__main__":
    main()