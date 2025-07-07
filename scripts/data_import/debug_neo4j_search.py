#!/usr/bin/env python3
"""
Neo4j検索のデバッグスクリプト
"""

import os
from neo4j import GraphDatabase

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def test_connection():
    """接続テスト"""
    print("=== Neo4j Connection Test ===")
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            record = result.single()
            print(f"✓ Connection successful: {record['test']}")
            return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def check_node_counts():
    """ノード数の確認"""
    print("\n=== Node Counts ===")
    with driver.session() as session:
        for label in ["Work", "Author", "Publisher", "Magazine"]:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
            count = result.single()["count"]
            print(f"{label}: {count:,}")

def check_relationship_counts():
    """リレーションシップ数の確認"""
    print("\n=== Relationship Counts ===")
    with driver.session() as session:
        for rel_type in ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]:
            result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count")
            count = result.single()["count"]
            print(f"{rel_type}: {count:,}")

def search_one_piece():
    """ONE PIECEの検索テスト"""
    print("\n=== Searching for 'ONE PIECE' ===")
    
    search_term = "ONE PIECE"
    
    with driver.session() as session:
        # 1. 基本的な検索
        print("\n1. Basic search (exact match):")
        query = """
        MATCH (w:Work)
        WHERE w.title = $search_term
        RETURN w.id, w.title
        LIMIT 5
        """
        result = session.run(query, search_term=search_term)
        for record in result:
            print(f"  - ID: {record['w.id']}, Title: {record['w.title']}")
        
        # 2. 大文字小文字を無視した部分一致検索
        print("\n2. Case-insensitive contains search:")
        query = """
        MATCH (w:Work)
        WHERE toLower(w.title) CONTAINS toLower($search_term)
        RETURN w.id, w.title
        LIMIT 5
        """
        result = session.run(query, search_term=search_term)
        count = 0
        for record in result:
            print(f"  - ID: {record['w.id']}, Title: {record['w.title']}")
            count += 1
        print(f"  Total found: {count}")
        
        # 3. ワンピースのカタカナ検索
        print("\n3. Searching for 'ワンピース':")
        query = """
        MATCH (w:Work)
        WHERE toLower(w.title) CONTAINS toLower($search_term)
        RETURN w.id, w.title
        LIMIT 5
        """
        result = session.run(query, search_term="ワンピース")
        count = 0
        for record in result:
            print(f"  - ID: {record['w.id']}, Title: {record['w.title']}")
            count += 1
        print(f"  Total found: {count}")
        
        # 4. リレーションシップ付きの検索
        print("\n4. Search with relationships:")
        query = """
        MATCH (w:Work)
        WHERE toLower(w.title) CONTAINS toLower($search_term)
        OPTIONAL MATCH (w)-[:CREATED_BY]->(a:Author)
        OPTIONAL MATCH (w)-[:PUBLISHED_IN]->(m:Magazine)
        RETURN w.id, w.title, collect(DISTINCT a.name) AS authors, collect(DISTINCT m.title) AS magazines
        LIMIT 5
        """
        result = session.run(query, search_term="ワンピース")
        for record in result:
            print(f"  - ID: {record['w.id']}")
            print(f"    Title: {record['w.title']}")
            print(f"    Authors: {record['authors']}")
            print(f"    Magazines: {record['magazines']}")

def check_sample_works():
    """サンプル作品の確認"""
    print("\n=== Sample Works ===")
    with driver.session() as session:
        query = """
        MATCH (w:Work)
        RETURN w.id, w.title
        ORDER BY w.title
        LIMIT 10
        """
        result = session.run(query)
        for record in result:
            print(f"  - ID: {record['w.id']}, Title: {record['w.title']}")

def check_work_schema():
    """Workノードのスキーマ確認"""
    print("\n=== Work Node Schema ===")
    with driver.session() as session:
        query = """
        MATCH (w:Work)
        WITH w LIMIT 1
        RETURN keys(w) AS properties
        """
        result = session.run(query)
        record = result.single()
        if record:
            print(f"Properties: {record['properties']}")
        else:
            print("No Work nodes found!")

# メイン実行
if __name__ == "__main__":
    if test_connection():
        check_node_counts()
        check_relationship_counts()
        check_work_schema()
        check_sample_works()
        search_one_piece()
    
    driver.close()