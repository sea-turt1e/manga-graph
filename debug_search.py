#!/usr/bin/env python3
"""
Debug script to check Neo4j search issues
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository


def debug_search():
    """Debug search functionality"""
    repo = Neo4jMangaRepository()
    
    try:
        print("=== Debug Neo4j Search Issues ===")
        
        # Check all works in database
        with repo.driver.session() as session:
            result = session.run("MATCH (w:Work) RETURN w.title as title LIMIT 10")
            titles = [record['title'] for record in result if record['title']]
        
        print(f"\nFirst 10 work titles in database:")
        for i, title in enumerate(titles, 1):
            print(f"  {i}. '{title}'")
        
        # Check for works containing "ONE" or "PIECE"
        with repo.driver.session() as session:
            result = session.run("""
                MATCH (w:Work) 
                WHERE toLower(w.title) CONTAINS 'one' OR toLower(w.title) CONTAINS 'piece'
                RETURN w.title as title
            """)
            one_piece_titles = [record['title'] for record in result]
        
        print(f"\nWorks containing 'one' or 'piece': {len(one_piece_titles)}")
        for title in one_piece_titles:
            print(f"  - '{title}'")
        
        # Test various search patterns
        search_terms = ["ONE PIECE", "one piece", "ONE", "PIECE", "キジトラ猫", "猫"]
        
        print(f"\nTesting search patterns:")
        for term in search_terms:
            works = repo.search_manga_works(term, limit=5)
            print(f"  '{term}': {len(works)} results")
            for work in works[:2]:  # Show first 2 results
                print(f"    - '{work['title']}'")
        
        # Test direct Cypher query with debug
        print(f"\nDirect Cypher query test:")
        with repo.driver.session() as session:
            # Test case-insensitive search
            result = session.run("""
                MATCH (w:Work)
                WHERE toLower(w.title) CONTAINS toLower($search_term)
                RETURN w.title as title, w.id as id
                LIMIT 5
            """, search_term="ONE PIECE")
            
            direct_results = list(result)
            print(f"  Direct query for 'ONE PIECE': {len(direct_results)} results")
            for record in direct_results:
                print(f"    - '{record['title']}'")
        
        # Check for exact matches
        with repo.driver.session() as session:
            result = session.run("""
                MATCH (w:Work)
                WHERE w.title = $search_term
                RETURN w.title as title
            """, search_term="ONE PIECE")
            
            exact_results = list(result)
            print(f"  Exact match for 'ONE PIECE': {len(exact_results)} results")
        
        # Check if there are any works with spaces in titles
        with repo.driver.session() as session:
            result = session.run("""
                MATCH (w:Work)
                WHERE w.title CONTAINS ' '
                RETURN w.title as title
                LIMIT 10
            """)
            
            space_titles = [record['title'] for record in result]
            print(f"\nWorks with spaces in titles: {len(space_titles)}")
            for title in space_titles[:5]:
                print(f"  - '{title}'")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        repo.close()


if __name__ == "__main__":
    debug_search()