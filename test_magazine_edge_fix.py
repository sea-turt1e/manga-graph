#!/usr/bin/env python3
"""
雑誌エッジ修正のテスト
"""

import requests
import json
import sys
from pathlib import Path

def test_magazine_edges():
    """雑誌エッジが修正されているかテスト"""
    print("=== Testing magazine edges fix ===\n")
    
    # APIリクエスト
    url = "http://localhost:8000/api/v1/neo4j/search"
    params = {
        "query": "ONE PIECE",
        "limit": 5,
        "include_related": "true"
    }
    
    print(f"Request: GET {url}")
    print(f"Params: {params}\n")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"Response status: {response.status_code}")
        print(f"Total nodes: {len(data.get('nodes', []))}")
        print(f"Total edges: {len(data.get('edges', []))}")
        
        # ノードの分析
        nodes_by_type = {}
        for node in data.get('nodes', []):
            node_type = node.get('type', 'unknown')
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node)
        
        print(f"\nNodes by type:")
        for node_type, nodes in nodes_by_type.items():
            print(f"  {node_type}: {len(nodes)}")
            if node_type == 'work':
                print("    Works:")
                for node in nodes[:10]:  # 最初の10個
                    print(f"      - {node['label']} (ID: {node['id']})")
            elif node_type == 'magazine':
                print("    Magazines:")
                for node in nodes:
                    print(f"      - {node['label']} (ID: {node['id']})")
        
        # エッジの分析
        edges_by_type = {}
        for edge in data.get('edges', []):
            edge_type = edge.get('type', 'unknown')
            if edge_type not in edges_by_type:
                edges_by_type[edge_type] = []
            edges_by_type[edge_type].append(edge)
        
        print(f"\nEdges by type:")
        for edge_type, edges in edges_by_type.items():
            print(f"  {edge_type}: {len(edges)}")
        
        # 特定のケースをチェック：NARUTOと週刊少年ジャンプのエッジ
        print(f"\n=== Checking NARUTO magazine edge ===")
        
        # NARUTOノードを探す
        naruto_node = None
        for node in data.get('nodes', []):
            if node.get('type') == 'work' and 'NARUTO' in node.get('label', ''):
                naruto_node = node
                print(f"Found NARUTO node: {node['label']} (ID: {node['id']})")
                break
        
        if naruto_node:
            # NARUTOに関連するエッジを探す
            naruto_edges = []
            for edge in data.get('edges', []):
                if edge.get('target') == naruto_node['id'] or edge.get('source') == naruto_node['id']:
                    naruto_edges.append(edge)
            
            print(f"NARUTO edges: {len(naruto_edges)}")
            for edge in naruto_edges:
                source_node = next((n for n in data['nodes'] if n['id'] == edge['source']), None)
                target_node = next((n for n in data['nodes'] if n['id'] == edge['target']), None)
                
                source_label = source_node['label'] if source_node else edge['source']
                target_label = target_node['label'] if target_node else edge['target']
                source_type = source_node['type'] if source_node else 'unknown'
                
                print(f"  {source_label} ({source_type}) --{edge['type']}--> {target_label}")
            
            # 雑誌エッジがあるかチェック
            magazine_edges = [e for e in naruto_edges if any(n for n in data['nodes'] if n['id'] == e['source'] and n['type'] == 'magazine')]
            print(f"NARUTO magazine edges: {len(magazine_edges)}")
            
            if magazine_edges:
                print("✅ SUCCESS: NARUTO has magazine edges!")
                for edge in magazine_edges:
                    magazine_node = next(n for n in data['nodes'] if n['id'] == edge['source'])
                    print(f"  NARUTO connected to magazine: {magazine_node['label']}")
            else:
                print("❌ FAILED: NARUTO still missing magazine edges")
        else:
            print("⚠️  WARNING: NARUTO node not found in results")
        
        return len(magazine_edges) > 0 if naruto_node else False
            
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_magazine_edges()
    sys.exit(0 if success else 1)