import json
import os
import sys
from pathlib import Path

from neo4j import GraphDatabase


# データ読み込み関数
def load_json_ld(filename):
    """JSON-LD形式のファイルを読み込む"""
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("@graph", [])


# 1つのJSONファイルから限定的なデータを読み込む
script_dir = Path(__file__).parent
data_dir = script_dir.parent.parent / "data" / "mediaarts"
json_files = list(data_dir.glob("*.json"))

print(f"Looking for JSON files in: {data_dir}")
print(f"Found {len(json_files)} JSON files")

# 最初のファイルから最大1000件のみ読み込む
all_data = []
if json_files:
    test_file = json_files[0]
    print(f"\nLoading test data from {test_file.name}...")
    data = load_json_ld(str(test_file))
    all_data = data[:1000]  # 最大1000件に制限
    print(f"Loaded {len(all_data)} items for testing")

# データの分類
manga_books = []
magazines = []
authors = set()
publishers = set()

for item in all_data:
    item_type = item.get("@type", "")
    genre = item.get("schema:genre", "")
    
    if item_type == "class:MangaBook" and genre == "マンガ単行本":
        manga_books.append(item)
        
        creator = item.get("dcterms:creator", {})
        if isinstance(creator, dict) and "@id" in creator:
            authors.add(creator["@id"])
        
        publisher = item.get("schema:publisher", "")
        if publisher:
            if isinstance(publisher, list):
                for pub in publisher:
                    if isinstance(pub, str):
                        publishers.add(pub)
            elif isinstance(publisher, str):
                publishers.add(publisher)

print(f"\nTest data summary:")
print(f"- Manga books: {len(manga_books)}")
print(f"- Authors: {len(authors)}")
print(f"- Publishers: {len(publishers)}")

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

print(f"\nConnecting to Neo4j at {neo4j_uri}...")

class MangaGraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def test_connection(self):
        """接続テスト"""
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS num")
            return result.single()["num"] == 1

    def count_nodes(self):
        """ノード数をカウント"""
        with self.driver.session() as session:
            counts = {}
            for label in ["Work", "Author", "Publisher", "Magazine"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                counts[label] = result.single()["count"]
            return counts

    def create_sample_publisher(self, name):
        """サンプル出版社を作成"""
        with self.driver.session() as session:
            query = """
            MERGE (p:Publisher {id: $publisher_id})
            SET p.name = $name
            """
            session.run(query, {"publisher_id": name, "name": name})

try:
    db = MangaGraphDB(neo4j_uri, neo4j_user, neo4j_password)
    
    # 接続テスト
    if db.test_connection():
        print("Successfully connected to Neo4j!")
    
    # 現在のノード数を確認
    print("\nCurrent node counts:")
    counts = db.count_nodes()
    for label, count in counts.items():
        print(f"  {label}: {count}")
    
    # テストデータをインポート
    if publishers:
        print(f"\nImporting {len(publishers)} test publishers...")
        for i, publisher in enumerate(list(publishers)[:10]):  # 最大10件
            db.create_sample_publisher(publisher)
            print(f"  Imported {i+1}/{min(10, len(publishers))} publishers")
    
    # インポート後のノード数を確認
    print("\nNode counts after test import:")
    counts = db.count_nodes()
    for label, count in counts.items():
        print(f"  {label}: {count}")
    
    db.close()
    print("\nTest completed successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)