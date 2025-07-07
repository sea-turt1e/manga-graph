#!/usr/bin/env python3
"""
API検索のテスト
"""

import requests
import json

# APIエンドポイント
base_url = "http://127.0.0.1:8000"

def test_neo4j_search():
    """Neo4j検索エンドポイントのテスト"""
    print("=== Testing Neo4j Search API ===")
    
    # 1. ONE PIECEで検索
    print("\n1. Searching for 'ONE PIECE':")
    response = requests.get(
        f"{base_url}/api/v1/neo4j/search",
        params={"q": "ONE PIECE", "limit": 20, "include_related": True}
    )
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    # 2. ワンピースで検索
    print("\n2. Searching for 'ワンピース':")
    response = requests.get(
        f"{base_url}/api/v1/neo4j/search",
        params={"q": "ワンピース", "limit": 20, "include_related": True}
    )
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    # 3. NARUTO検索
    print("\n3. Searching for 'NARUTO':")
    response = requests.get(
        f"{base_url}/api/v1/neo4j/search",
        params={"q": "NARUTO", "limit": 5, "include_related": False}
    )
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2, ensure_ascii=False)}")

def test_media_arts_search():
    """Media Arts検索エンドポイントのテスト（比較用）"""
    print("\n=== Testing Media Arts Search API ===")
    
    print("\nSearching for 'ONE PIECE' in Media Arts:")
    response = requests.get(
        f"{base_url}/api/v1/media-arts/search",
        params={"q": "ONE PIECE", "limit": 5}
    )
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Found {data['total_nodes']} nodes and {data['total_edges']} edges")
    
    # 最初の数ノードを表示
    if data['nodes']:
        print("\nFirst few nodes:")
        for node in data['nodes'][:3]:
            print(f"  - {node['type']}: {node['label']}")

if __name__ == "__main__":
    test_neo4j_search()
    test_media_arts_search()