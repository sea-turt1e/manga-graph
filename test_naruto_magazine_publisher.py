#!/usr/bin/env python3
"""
NARUTO検索でmagazine -> publisherエッジをテスト
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository

def test_naruto_magazine_publisher():
    """NARUTOでmagazine -> publisherエッジをテスト"""
    print("=== NARUTO Magazine -> Publisher Edge Test ===\n")
    
    try:
        repo = Neo4jMangaRepository()
        
        # NARUTOを検索
        result = repo.search_manga_data_with_related("NARUTO", limit=5, include_related=True)
        
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
            if node_type == 'magazine':
                print("    Magazines:")
                for node in nodes:
                    print(f"      - {node['label']} (ID: {node['id']})")
            elif node_type == 'publisher':
                print("    Publishers:")
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
        
        # magazine -> publisher エッジをチェック
        print(f"\n=== Magazine -> Publisher Edges ===")
        mag_pub_edges = edges_by_type.get('published_by', [])
        print(f"Found {len(mag_pub_edges)} magazine -> publisher edges:")
        
        for edge in mag_pub_edges:
            magazine_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
            publisher_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
            
            mag_label = magazine_node['label'] if magazine_node else edge['source']
            pub_label = publisher_node['label'] if publisher_node else edge['target']
            
            print(f"  ✅ {mag_label} -> {pub_label}")
            
            # 期待される関係をチェック
            if mag_label == "週刊少年ジャンプ" and pub_label == "集英社":
                print(f"    🎯 Expected relationship found!")
        
        # NARUTOノードとその全エッジを表示
        print(f"\n=== NARUTO Node Analysis ===")
        naruto_node = None
        for node in result['nodes']:
            if node.get('type') == 'work' and 'NARUTO' in node.get('label', ''):
                naruto_node = node
                print(f"Found NARUTO: {node['label']} (ID: {node['id']})")
                break
        
        if naruto_node:
            # NARUTOに関連するすべてのエッジ
            naruto_edges = []
            for edge in result['edges']:
                if edge.get('target') == naruto_node['id'] or edge.get('source') == naruto_node['id']:
                    naruto_edges.append(edge)
            
            print(f"NARUTO total edges: {len(naruto_edges)}")
            for edge in naruto_edges:
                source_node = next((n for n in result['nodes'] if n['id'] == edge['source']), None)
                target_node = next((n for n in result['nodes'] if n['id'] == edge['target']), None)
                
                source_label = source_node['label'] if source_node else edge['source']
                target_label = target_node['label'] if target_node else edge['target']
                source_type = source_node['type'] if source_node else 'unknown'
                target_type = target_node['type'] if target_node else 'unknown'
                
                print(f"  {source_label} ({source_type}) --{edge['type']}--> {target_label} ({target_type})")
        
        success = len(mag_pub_edges) > 0
        if success:
            print(f"\n✅ SUCCESS: Magazine -> Publisher edges are working!")
        else:
            print(f"\n❌ FAILED: No magazine -> publisher edges found")
            
        repo.close()
        return success
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_naruto_magazine_publisher()
    sys.exit(0 if success else 1)