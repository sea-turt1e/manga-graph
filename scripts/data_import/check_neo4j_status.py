import os

from neo4j import GraphDatabase

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

print(f"Connecting to Neo4j at {neo4j_uri}...")


class MangaGraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_status(self):
        """データベースの状態を確認"""
        with self.driver.session() as session:
            # ノード数をカウント
            print("\n=== Node Counts ===")
            for label in ["Work", "Author", "Publisher", "Magazine"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                count = result.single()["count"]
                print(f"{label}: {count:,}")

            # リレーションシップ数をカウント
            print("\n=== Relationship Counts ===")
            relationships = ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]
            for rel in relationships:
                result = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS count")
                count = result.single()["count"]
                print(f"{rel}: {count:,}")

            # サンプルデータを表示
            print("\n=== Sample Data ===")

            # 作品のサンプル
            print("\nSample Works (first 5):")
            result = session.run("MATCH (w:Work) RETURN w.title AS title LIMIT 5")
            for record in result:
                print(f"  - {record['title']}")

            # 著者のサンプル
            print("\nSample Authors (first 5):")
            result = session.run("MATCH (a:Author) RETURN a.id AS id LIMIT 5")
            for record in result:
                print(f"  - {record['id']}")

            # 出版社のサンプル
            print("\nSample Publishers (first 5):")
            result = session.run("MATCH (p:Publisher) RETURN p.name AS name LIMIT 5")
            for record in result:
                print(f"  - {record['name']}")

            # 制約の確認
            print("\n=== Constraints ===")
            result = session.run("SHOW CONSTRAINTS")
            for record in result:
                print(f"  - {record['name']}: {record['labelsOrTypes']} {record['properties']}")


try:
    db = MangaGraphDB(neo4j_uri, neo4j_user, neo4j_password)
    db.get_status()
    db.close()

except Exception as e:
    print(f"Error: {e}")
    print("\nMake sure Neo4j is running and the credentials are correct.")
    print("You can set environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")
