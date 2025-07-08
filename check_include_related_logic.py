#!/usr/bin/env python3
"""
include_relatedロジックを直接確認
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

def check_logic():
    """include_relatedロジックを直接確認"""
    repo = Neo4jMangaRepository()
    
    with repo.driver.session() as session:
        # include_relatedロジックを再現
        search_term = "ONE PIECE"
        limit = 5
        include_related = True
        
        # main_worksを取得
        main_works = repo.search_manga_works(search_term, limit)
        print(f"main_works count: {len(main_works)}")
        print(f"main_works is truthy: {bool(main_works)}")
        print(f"include_related: {include_related}")
        print(f"Condition (include_related and main_works): {include_related and main_works}")
        
        if main_works:
            print(f"\nFirst work ID: {main_works[0]['work_id']}")
            
            # get_related_works_by_magazine_and_periodを直接呼ぶ
            magazine_related = repo.get_related_works_by_magazine_and_period(main_works[0]['work_id'], 2, 10)
            print(f"\nMagazine related works: {len(magazine_related)}")
            for work in magazine_related[:5]:
                print(f"  - {work['title']}")
    
    repo.close()

if __name__ == "__main__":
    check_logic()