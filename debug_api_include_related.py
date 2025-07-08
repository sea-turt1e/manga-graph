#!/usr/bin/env python3
"""
APIのinclude_related=true問題を詳細調査
"""

import sys
from pathlib import Path
import logging
import json

# パスを追加
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository
from domain.services.neo4j_media_arts_service import Neo4jMediaArtsService

# ロギング設定
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def trace_api_flow():
    """API呼び出しフローを詳細にトレース"""
    print("=== Tracing API flow with include_related=true ===\n")
    
    try:
        # 1. リポジトリレベルでテスト
        print("1. Testing at Repository level:")
        repo = Neo4jMangaRepository()
        
        # search_manga_worksの結果を確認
        print("\n1.1. Checking search_manga_works result:")
        main_works = repo.search_manga_works("ONE PIECE", limit=5)
        print(f"main_works count: {len(main_works)}")
        for work in main_works[:3]:
            print(f"  - {work['title']} (ID: {work['work_id']})")
        
        repo_result = repo.search_manga_data_with_related("ONE PIECE", limit=5, include_related=True)
        
        print(f"Repository nodes: {len(repo_result['nodes'])}")
        print(f"Repository edges: {len(repo_result['edges'])}")
        
        # ノードのタイプと作品名を表示
        work_nodes = [n for n in repo_result['nodes'] if n['type'] == 'work']
        print(f"\nWork nodes from repository ({len(work_nodes)}):")
        for node in work_nodes[:10]:  # 最初の10個
            print(f"  - {node['label']}")
        
        # 関連作品を確認
        one_piece_id = "https://mediaarts-db.artmuseums.go.jp/id/M1037106"
        related_ids = []
        for node in work_nodes:
            if node['id'] != one_piece_id and 'ONE PIECE' not in node['label']:
                related_ids.append(node['id'])
        
        print(f"\nRelated works (non-ONE PIECE): {len(related_ids)}")
        for i, node_id in enumerate(related_ids[:5]):
            node = next(n for n in work_nodes if n['id'] == node_id)
            print(f"  - {node['label']}")
        
        repo.close()
        
        # 2. サービスレベルでテスト
        print("\n\n2. Testing at Service level:")
        service = Neo4jMediaArtsService()
        service_result = service.search_manga_data_with_related("ONE PIECE", limit=5, include_related=True)
        
        print(f"Service nodes: {len(service_result['nodes'])}")
        print(f"Service edges: {len(service_result['edges'])}")
        
        work_nodes_service = [n for n in service_result['nodes'] if n['type'] == 'work']
        print(f"\nWork nodes from service ({len(work_nodes_service)}):")
        for node in work_nodes_service[:10]:
            print(f"  - {node['label']}")
        
        # 3. _convert_neo4j_to_graph_formatの影響を確認
        print("\n\n3. Checking _convert_neo4j_to_graph_format:")
        if not service.use_mock:
            # リポジトリの結果を変換
            converted = service._convert_neo4j_to_graph_format(repo_result)
            print(f"Converted nodes: {len(converted['nodes'])}")
            print(f"Converted edges: {len(converted['edges'])}")
            
            work_nodes_converted = [n for n in converted['nodes'] if n['type'] == 'work']
            print(f"\nWork nodes after conversion ({len(work_nodes_converted)}):")
            for node in work_nodes_converted[:10]:
                print(f"  - {node['label']}")
        
        service.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    trace_api_flow()