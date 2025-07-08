#!/usr/bin/env python3
"""
作品→出版社の直接エッジがないことを確認するテスト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

def test_no_direct_work_publisher_edges():
    """作品→出版社の直接エッジがないことを確認"""
    print("=== Test: No Direct Work -> Publisher Edges ===\n")
    
    try:
        repo = Neo4jMangaRepository()
        
        # NARUTOを検索
        result = repo.search_manga_data_with_related("NARUTO", limit=5, include_related=True)
        
        print(f"Total nodes: {len(result['nodes'])}")
        print(f"Total edges: {len(result['edges'])}")
        
        # エッジの詳細分析
        work_to_publisher_edges = []
        magazine_to_publisher_edges = []
        work_to_magazine_edges = []
        author_to_work_edges = []
        
        for edge in result['edges']:
            source_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
            target_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
            
            source_type = source_node['type'] if source_node else 'unknown'
            target_type = target_node['type'] if target_node else 'unknown'
            
            if source_type == 'publisher' and target_type == 'work':
                work_to_publisher_edges.append(edge)
            elif source_type == 'magazine' and target_type == 'publisher':
                magazine_to_publisher_edges.append(edge)
            elif source_type == 'magazine' and target_type == 'work':
                work_to_magazine_edges.append(edge)
            elif source_type == 'author' and target_type == 'work':
                author_to_work_edges.append(edge)
        
        print(f"\n=== Edge Analysis ===")
        print(f"Publisher -> Work (SHOULD BE 0): {len(work_to_publisher_edges)}")
        print(f"Magazine -> Publisher (SHOULD BE >0): {len(magazine_to_publisher_edges)}")
        print(f"Magazine -> Work (SHOULD BE >0): {len(work_to_magazine_edges)}")
        print(f"Author -> Work (SHOULD BE >0): {len(author_to_work_edges)}")
        
        # 具体的なエッジを表示
        if work_to_publisher_edges:
            print(f"\n❌ PROBLEM: Found {len(work_to_publisher_edges)} direct work->publisher edges:")
            for edge in work_to_publisher_edges:
                source_node = next(n for n in result['nodes'] if n['id'] == edge['source'])
                target_node = next(n for n in result['nodes'] if n['id'] == edge['target'])
                print(f"  {source_node['label']} --{edge['type']}--> {target_node['label']}")
        else:
            print(f"\n✅ GOOD: No direct work->publisher edges found")
        
        if magazine_to_publisher_edges:
            print(f"\n✅ GOOD: Found {len(magazine_to_publisher_edges)} magazine->publisher edges:")
            for edge in magazine_to_publisher_edges:
                source_node = next(n for n in result['nodes'] if n['id'] == edge['source'])
                target_node = next(n for n in result['nodes'] if n['id'] == edge['target'])
                print(f"  {source_node['label']} --{edge['type']}--> {target_node['label']}")
        else:
            print(f"\n⚠️  WARNING: No magazine->publisher edges found")
        
        if work_to_magazine_edges:
            print(f"\n✅ GOOD: Found {len(work_to_magazine_edges)} work->magazine edges")
        else:
            print(f"\n⚠️  WARNING: No work->magazine edges found")
        
        # 全エッジの詳細表示
        print(f"\n=== All Edges Detail ===")
        for edge in result['edges']:
            source_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
            target_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
            
            source_label = source_node['label'] if source_node else edge['source']
            target_label = target_node['label'] if target_node else edge['target']
            source_type = source_node['type'] if source_node else 'unknown'
            target_type = target_node['type'] if target_node else 'unknown'
            
            print(f"  {source_label} ({source_type}) --{edge['type']}--> {target_label} ({target_type})")
        
        # 成功判定：作品→出版社の直接エッジがないこと
        success = len(work_to_publisher_edges) == 0
        
        if success:
            print(f"\n🎯 SUCCESS: No unwanted direct work->publisher edges!")
        else:
            print(f"\n❌ FAILED: Found {len(work_to_publisher_edges)} unwanted direct work->publisher edges")
        
        repo.close()
        return success
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_no_direct_work_publisher_edges()
    sys.exit(0 if success else 1)