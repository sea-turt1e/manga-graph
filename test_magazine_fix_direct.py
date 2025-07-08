#!/usr/bin/env python3
"""
雑誌エッジ修正の直接テスト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

def test_magazine_edges_direct():
    """雑誌エッジが修正されているかリポジトリ直接テスト"""
    print("=== Direct Repository Test for Magazine Edges ===\n")
    
    try:
        repo = Neo4jMangaRepository()
        
        # include_related=trueでテスト
        result = repo.search_manga_data_with_related("ONE PIECE", limit=5, include_related=True)
        
        print(f"Total nodes: {len(result['nodes'])}")
        print(f"Total edges: {len(result['edges'])}")
        
        # ノードの分析
        nodes_by_type = {}
        for node in result['nodes']:
            node_type = node.get('type', 'unknown')
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node)
        
        print(f"\nNodes by type:")
        for node_type, nodes in nodes_by_type.items():
            print(f"  {node_type}: {len(nodes)}")
            if node_type == 'work':
                print("    Works:")
                for node in nodes[:10]:
                    print(f"      - {node['label']} (ID: {node['id']})")
            elif node_type == 'magazine':
                print("    Magazines:")
                for node in nodes:
                    print(f"      - {node['label']} (ID: {node['id']})")
        
        # エッジの分析
        edges_by_type = {}
        for edge in result['edges']:
            edge_type = edge.get('type', 'unknown')
            if edge_type not in edges_by_type:
                edges_by_type[edge_type] = []
            edges_by_type[edge_type].append(edge)
        
        print(f"\nEdges by type:")
        for edge_type, edges in edges_by_type.items():
            print(f"  {edge_type}: {len(edges)}")
            
        # Check for magazine -> publisher edges
        print(f"\n=== Checking magazine -> publisher edges ===")
        mag_pub_edges = edges_by_type.get('published_by', [])
        print(f"Found {len(mag_pub_edges)} magazine -> publisher edges:")
        for edge in mag_pub_edges:
            magazine_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
            publisher_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
            
            mag_label = magazine_node['label'] if magazine_node else edge['source']
            pub_label = publisher_node['label'] if publisher_node else edge['target']
            
            print(f"  {mag_label} -> {pub_label}")
        
        # NARUTOと週刊少年ジャンプのエッジをチェック
        print(f"\n=== Checking for NARUTO magazine edge ===")
        
        # NARUTOノードを探す
        naruto_node = None
        for node in result['nodes']:
            if node.get('type') == 'work' and 'NARUTO' in node.get('label', ''):
                naruto_node = node
                print(f"Found NARUTO node: {node['label']} (ID: {node['id']})")
                break
        
        if naruto_node:
            # NARUTOに関連するエッジを探す
            naruto_edges = []
            for edge in result['edges']:
                if edge.get('target') == naruto_node['id'] or edge.get('source') == naruto_node['id']:
                    naruto_edges.append(edge)
            
            print(f"NARUTO edges: {len(naruto_edges)}")
            for edge in naruto_edges:
                source_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
                target_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
                
                source_label = source_node['label'] if source_node else edge['source']
                target_label = target_node['label'] if target_node else edge['target']
                source_type = source_node['type'] if source_node else 'unknown'
                
                print(f"  {source_label} ({source_type}) --{edge['type']}--> {target_label}")
            
            # 雑誌エッジがあるかチェック
            magazine_edges = [e for e in naruto_edges if any(n for n in result['nodes'] if n['id'] == e['source'] and n['type'] == 'magazine')]
            print(f"NARUTO magazine edges: {len(magazine_edges)}")
            
            if magazine_edges:
                print("✅ SUCCESS: NARUTO has magazine edges!")
                for edge in magazine_edges:
                    magazine_node = next(n for n in result['nodes'] if n['id'] == edge['source'])
                    print(f"  NARUTO connected to magazine: {magazine_node['label']}")
                return True
            else:
                print("❌ FAILED: NARUTO still missing magazine edges")
                return False
        else:
            print("⚠️  WARNING: NARUTO node not found in results")
            
            # 他の作品の雑誌エッジをチェック
            print("\n=== Checking magazine edges for other works ===")
            work_nodes = [n for n in result['nodes'] if n['type'] == 'work']
            magazine_edges_found = 0
            
            for work_node in work_nodes[:5]:  # 最初の5作品をチェック
                work_edges = [e for e in result['edges'] if e.get('target') == work_node['id']]
                work_magazine_edges = [e for e in work_edges if any(n for n in result['nodes'] if n['id'] == e['source'] and n['type'] == 'magazine')]
                
                if work_magazine_edges:
                    magazine_edges_found += len(work_magazine_edges)
                    print(f"  {work_node['label']}: {len(work_magazine_edges)} magazine edges")
                    for edge in work_magazine_edges[:2]:  # 最初の2つ表示
                        magazine_node = next(n for n in result['nodes'] if n['id'] == edge['source'])
                        print(f"    -> {magazine_node['label']}")
            
            print(f"Total magazine edges found: {magazine_edges_found}")
            return magazine_edges_found > 0
            
        repo.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_magazine_edges_direct()
    sys.exit(0 if success else 1)