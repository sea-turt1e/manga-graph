#!/usr/bin/env python3
"""
include_related=trueの問題を調査
"""

import sys
from pathlib import Path
import logging

# パスを追加
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService

# ロギング設定
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_include_related():
    """include_related=trueのテスト"""
    print("=== Testing include_related behavior ===\n")
    
    try:
        repo = Neo4jMangaRepository()
        
        # 1. include_related=falseでテスト
        print("1. Testing with include_related=False:")
        result_false = repo.search_manga_data_with_related("ONE PIECE", limit=5, include_related=False)
        print(f"Nodes: {len(result_false['nodes'])}")
        print(f"Edges: {len(result_false['edges'])}")
        
        # 2. include_related=trueでテスト
        print("\n2. Testing with include_related=True:")
        result_true = repo.search_manga_data_with_related("ONE PIECE", limit=5, include_related=True)
        print(f"Nodes: {len(result_true['nodes'])}")
        print(f"Edges: {len(result_true['edges'])}")
        
        # 3. ONE PIECEの詳細情報を確認
        print("\n3. Checking ONE PIECE work details:")
        with repo.driver.session() as session:
            query = """
            MATCH (w:Work {title: 'ONE PIECE'})
            RETURN w.id AS work_id, w.first_published AS first_published, 
                   w.last_published AS last_published, w.total_volumes AS total_volumes
            """
            result = session.run(query)
            for record in result:
                print(f"Work ID: {record['work_id']}")
                print(f"First published: {record['first_published']}")
                print(f"Last published: {record['last_published']}")
                print(f"Total volumes: {record['total_volumes']}")
        
        # 4. 関連作品検索メソッドを個別にテスト
        print("\n4. Testing individual related works methods:")
        one_piece_id = "https://mediaarts-db.artmuseums.go.jp/id/M1037106"
        
        # 同じ著者の作品
        print("\n4.1. get_related_works_by_author:")
        author_related = repo.get_related_works_by_author(one_piece_id, 5)
        print(f"Found {len(author_related)} related works by author")
        for work in author_related:
            print(f"  - {work['title']} by {work['author_name']}")
        
        # 同じ雑誌・同じ時期の作品
        print("\n4.2. get_related_works_by_magazine_and_period:")
        magazine_related = repo.get_related_works_by_magazine_and_period(one_piece_id, 2, 10)
        print(f"Found {len(magazine_related)} related works by magazine and period")
        for work in magazine_related:
            print(f"  - {work['title']} in {work.get('magazine_name', 'Unknown magazine')}")
        
        repo.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_include_related()