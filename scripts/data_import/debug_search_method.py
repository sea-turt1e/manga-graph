#!/usr/bin/env python3
"""
search_manga_data_with_relatedメソッドのデバッグ
"""

import os
import sys
from pathlib import Path
import logging

# パスを追加
sys.path.append(str(Path(__file__).parent.parent.parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService

# ロギング設定
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_repository_directly():
    """リポジトリを直接テスト"""
    print("=== Testing Neo4jMangaRepository directly ===\n")
    
    try:
        repo = Neo4jMangaRepository()
        
        # 1. search_manga_worksのテスト
        print("1. Testing search_manga_works('ONE PIECE'):")
        works = repo.search_manga_works("ONE PIECE", limit=5)
        print(f"Found {len(works)} works")
        for work in works:
            print(f"  - {work['title']} (ID: {work['work_id']})")
        
        # 2. search_manga_data_with_relatedのテスト
        print("\n2. Testing search_manga_data_with_related('ONE PIECE'):")
        result = repo.search_manga_data_with_related("ONE PIECE", limit=5, include_related=False)
        print(f"Nodes: {len(result['nodes'])}")
        print(f"Edges: {len(result['edges'])}")
        
        if result['nodes']:
            print("\nFirst few nodes:")
            for node in result['nodes'][:3]:
                print(f"  - {node['type']}: {node['label']} (ID: {node['id']})")
        
        if result['edges']:
            print("\nFirst few edges:")
            for edge in result['edges'][:3]:
                print(f"  - {edge['type']}: {edge['source']} -> {edge['target']}")
        
        repo.close()
        
    except Exception as e:
        print(f"Error in repository test: {e}")
        import traceback
        traceback.print_exc()

def test_service_directly():
    """サービスを直接テスト"""
    print("\n\n=== Testing Neo4jMediaArtsService directly ===\n")
    
    try:
        service = Neo4jMediaArtsService()
        
        print("1. Testing search_manga_data_with_related('ONE PIECE'):")
        result = service.search_manga_data_with_related("ONE PIECE", limit=5, include_related=False)
        print(f"Nodes: {len(result['nodes'])}")
        print(f"Edges: {len(result['edges'])}")
        
        if result['nodes']:
            print("\nFirst few nodes:")
            for node in result['nodes'][:3]:
                print(f"  - {node['type']}: {node['label']} (ID: {node['id']})")
        
        service.close()
        
    except Exception as e:
        print(f"Error in service test: {e}")
        import traceback
        traceback.print_exc()

def test_api_flow():
    """API呼び出しフローを模擬"""
    print("\n\n=== Simulating API call flow ===\n")
    
    from presentation.api.manga_api import get_neo4j_media_arts_service
    
    try:
        # 依存性注入を通じてサービスを取得
        service = get_neo4j_media_arts_service()
        print(f"Service type: {type(service)}")
        print(f"Repository type: {type(service.neo4j_repository)}")
        print(f"Use mock: {service.use_mock}")
        
        # 検索実行
        result = service.search_manga_data_with_related("ONE PIECE", limit=5, include_related=False)
        print(f"\nSearch result:")
        print(f"Nodes: {len(result['nodes'])}")
        print(f"Edges: {len(result['edges'])}")
        
    except Exception as e:
        print(f"Error in API flow test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_repository_directly()
    test_service_directly()
    test_api_flow()