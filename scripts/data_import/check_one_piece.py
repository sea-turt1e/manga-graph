#!/usr/bin/env python3
"""
ONE PIECEのデータを詳細に確認
"""

import os

from neo4j import GraphDatabase

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))


def check_one_piece_details():
    """ONE PIECEの詳細確認"""
    print("=== ONE PIECE Details ===")

    with driver.session() as session:
        # 1. IDで直接検索
        print("\n1. Direct search by ID:")
        query = """
        MATCH (w:Work {id: 'https://mediaarts-db.artmuseums.go.jp/id/M1037106'})
        RETURN w
        """
        result = session.run(query)
        record = result.single()
        if record:
            work = record["w"]
            print(f"Found: {work}")
            for key, value in work.items():
                print(f"  {key}: {value}")
        else:
            print("Not found by ID")

        # 2. タイトルで検索（完全一致）
        print("\n2. Search by exact title:")
        query = """
        MATCH (w:Work {title: 'ONE PIECE'})
        RETURN w
        """
        result = session.run(query)
        for record in result:
            work = record["w"]
            print(f"Found: ID={work['id']}, Title={work['title']}")

        # 3. リレーションシップの確認
        print("\n3. Check relationships for ONE PIECE:")
        query = """
        MATCH (w:Work)
        WHERE w.title = 'ONE PIECE'
        OPTIONAL MATCH (w)-[r1:CREATED_BY]->(a:Author)
        OPTIONAL MATCH (w)-[r2:PUBLISHED_IN]->(m:Magazine)
        RETURN w.id AS work_id, w.title AS title, 
               collect(DISTINCT a) AS authors,
               collect(DISTINCT m) AS magazines,
               count(r1) AS author_count,
               count(r2) AS magazine_count
        """
        result = session.run(query)
        for record in result:
            print(f"Work ID: {record['work_id']}")
            print(f"Title: {record['title']}")
            print(f"Author relationships: {record['author_count']}")
            print(f"Magazine relationships: {record['magazine_count']}")
            print(f"Authors: {[a['name'] if 'name' in a else a['id'] for a in record['authors']]}")
            print(f"Magazines: {[m['title'] if 'title' in m else m['id'] for m in record['magazines']]}")

        # 4. データベース内のONE PIECE関連作品を全て検索
        print("\n4. All ONE PIECE related works:")
        query = """
        MATCH (w:Work)
        WHERE toLower(w.title) CONTAINS 'one piece' OR toLower(w.title) CONTAINS 'ワンピース'
        RETURN w.id, w.title, w.is_series, w.total_volumes
        ORDER BY w.title
        LIMIT 20
        """
        result = session.run(query)
        count = 0
        for record in result:
            print(
                f"  - {record['w.title']} (ID: {record['w.id']}, Series: {record['w.is_series']}, Volumes: {record['w.total_volumes']})"
            )
            count += 1
        print(f"Total found: {count}")


def check_author_format():
    """著者名のフォーマット確認"""
    print("\n\n=== Author Name Format Check ===")

    with driver.session() as session:
        query = """
        MATCH (a:Author)
        WHERE a.name CONTAINS '[著]'
        RETURN a.id, a.name
        LIMIT 10
        """
        result = session.run(query)
        for record in result:
            print(f"  ID: {record['a.id']}, Name: {record['a.name']}")


if __name__ == "__main__":
    check_one_piece_details()
    check_author_format()
    driver.close()
