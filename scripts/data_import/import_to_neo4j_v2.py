import glob
import json
import os
import sys
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase


# データ読み込み関数
def load_json_ld(filename):
    """JSON-LD形式のファイルを読み込む"""
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("@graph", [])


# 全てのJSONファイルからデータを読み込む
all_data = []

# スクリプトの場所から相対パスを解決
script_dir = Path(__file__).parent
data_dir = script_dir.parent.parent / "data" / "mediaarts"
json_files = list(data_dir.glob("*.json"))

print(f"Looking for JSON files in: {data_dir}")
print(f"Found {len(json_files)} JSON files")

for file_path in json_files:
    print(f"Loading {file_path.name}...")
    try:
        data = load_json_ld(str(file_path))
        all_data.extend(data)
        print(f"  Loaded {len(data)} items from {file_path.name}")
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

print(f"\nTotal items loaded: {len(all_data)}")

# データの分類
manga_books = []  # マンガ単行本
manga_works = []  # マンガ作品（シリーズ）
magazines = []  # 雑誌
authors = set()  # 著者
publishers = set()  # 出版社

# データを分類
for item in all_data:
    item_type = item.get("@type", "")
    genre = item.get("schema:genre", "")

    if item_type == "class:MangaBook" and genre == "マンガ単行本":
        manga_books.append(item)

        # 著者情報を収集
        creator = item.get("dcterms:creator", {})
        if isinstance(creator, dict) and "@id" in creator:
            authors.add(creator["@id"])

        # 出版社情報を収集
        publisher = item.get("schema:publisher", "")
        if publisher:
            if isinstance(publisher, list):
                for pub in publisher:
                    if isinstance(pub, str):
                        publishers.add(pub)
            elif isinstance(publisher, str):
                publishers.add(publisher)

    elif genre in ["マンガ雑誌", "雑誌", "雑誌全号まとめ"] or "Magazine" in item_type:
        magazines.append(item)

        # 出版社情報を収集
        publisher = item.get("schema:publisher", "")
        if publisher:
            if isinstance(publisher, list):
                for pub in publisher:
                    if isinstance(pub, str):
                        publishers.add(pub)
            elif isinstance(publisher, str):
                publishers.add(publisher)

# マンガ作品（シリーズ）の抽出 - 単行本から作品名でグループ化
work_dict = {}
for book in manga_books:
    # 作品名を取得（巻数を除いたタイトル）
    full_name = book.get("schema:name", "")

    # nameフィールドの処理 - リストや辞書の場合を考慮
    if isinstance(full_name, list):
        # リストの場合、最初の要素を取得（文字列のみ）
        for name in full_name:
            if isinstance(name, str):
                full_name = name
                break
            elif isinstance(name, dict) and "@value" in name:
                full_name = name["@value"]
                break
        else:
            continue  # 有効な名前が見つからない場合はスキップ
    elif isinstance(full_name, dict):
        # 辞書の場合、@valueフィールドを確認
        if "@value" in full_name:
            full_name = full_name["@value"]
        else:
            continue  # 有効な名前が見つからない場合はスキップ
    elif not isinstance(full_name, str):
        continue  # 文字列でない場合はスキップ

    # 巻数を除いた作品名を取得
    work_name = full_name
    volume_num = book.get("schema:volumeNumber", "")
    if volume_num and isinstance(work_name, str):
        # 巻数部分を削除
        work_name = work_name.replace(f" {volume_num}", "").strip()

    if work_name and isinstance(work_name, str):
        if work_name not in work_dict:
            work_dict[work_name] = {"name": work_name, "books": [], "creators": set(), "publishers": set()}

        work_dict[work_name]["books"].append(book)

        # 著者情報を追加
        creator = book.get("dcterms:creator", {})
        if isinstance(creator, dict) and "@id" in creator:
            work_dict[work_name]["creators"].add(creator["@id"])

        # 出版社情報を追加
        publisher = book.get("schema:publisher", "")
        if publisher:
            if isinstance(publisher, list):
                for pub in publisher:
                    if isinstance(pub, str):
                        work_dict[work_name]["publishers"].add(pub)
            elif isinstance(publisher, str):
                work_dict[work_name]["publishers"].add(publisher)

print(f"Found {len(manga_books)} manga books")
print(f"Found {len(work_dict)} manga works")
print(f"Found {len(magazines)} magazines")
print(f"Found {len(authors)} authors")
print(f"Found {len(publishers)} publishers")


class MangaGraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_constraints(self):
        with self.driver.session() as session:
            # ユニーク制約の作成
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publisher) REQUIRE p.id IS UNIQUE")

    def create_work_node(self, work_name, work_data):
        """マンガ作品（シリーズ）ノードを作成"""
        with self.driver.session() as session:
            query = """
            MERGE (w:Work {id: $work_id})
            SET w.title = $title,
                w.total_volumes = $total_volumes,
                w.first_published = $first_published,
                w.last_published = $last_published
            """

            # 総巻数と最初・最後の出版日を計算
            total_volumes = len(work_data["books"])
            dates = [
                book.get("schema:datePublished", "") for book in work_data["books"] if book.get("schema:datePublished")
            ]
            first_published = min(dates) if dates else ""
            last_published = max(dates) if dates else ""

            session.run(
                query,
                {
                    "work_id": work_name,  # 作品名をIDとして使用
                    "title": work_name,
                    "total_volumes": total_volumes,
                    "first_published": first_published,
                    "last_published": last_published,
                },
            )

    def create_author_node(self, author_id):
        """著者ノードを作成（現時点ではIDのみ）"""
        with self.driver.session() as session:
            query = """
            MERGE (a:Author {id: $author_id})
            """
            session.run(query, {"author_id": author_id})

    def create_magazine_node(self, magazine):
        """雑誌ノードを作成"""
        with self.driver.session() as session:
            query = """
            MERGE (m:Magazine {id: $magazine_id})
            SET m.title = $title,
                m.publisher = $publisher,
                m.genre = $genre
            """

            # タイトルの取得
            title = magazine.get("schema:name", "")
            if isinstance(title, list):
                title = title[0] if title else ""

            session.run(
                query,
                {
                    "magazine_id": magazine.get("@id", ""),
                    "title": title,
                    "publisher": magazine.get("schema:publisher", ""),
                    "genre": magazine.get("schema:genre", ""),
                },
            )

    def create_publisher_node(self, publisher_name):
        """出版社ノードを作成"""
        with self.driver.session() as session:
            query = """
            MERGE (p:Publisher {id: $publisher_id})
            SET p.name = $name
            """
            session.run(query, {"publisher_id": publisher_name, "name": publisher_name})

    def create_work_author_relationship(self, work_name, author_id):
        """作品と著者の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            MATCH (a:Author {id: $author_id})
            MERGE (w)-[:CREATED_BY]->(a)
            """
            session.run(query, {"work_id": work_name, "author_id": author_id})

    def create_work_magazine_relationship(self, work_name, magazine_id):
        """作品と雑誌の関係を作成（TODO: 掲載情報が必要）"""
        pass

    def create_magazine_publisher_relationship(self, magazine_id, publisher_name):
        """雑誌と出版社の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (m:Magazine {id: $magazine_id})
            MATCH (p:Publisher {id: $publisher_id})
            MERGE (m)-[:PUBLISHED_BY]->(p)
            """
            session.run(query, {"magazine_id": magazine_id, "publisher_id": publisher_name})


# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

print(f"\nConnecting to Neo4j at {neo4j_uri}...")

try:
    db = MangaGraphDB(neo4j_uri, neo4j_user, neo4j_password)
except Exception as e:
    print(f"Failed to connect to Neo4j: {e}")
    print("Please check your Neo4j connection settings.")
    print("You can set environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")
    sys.exit(1)

# 制約の作成
try:
    print("Creating constraints...")
    db.create_constraints()
except Exception as e:
    print(f"Error creating constraints: {e}")

# データのサマリーを表示
print(f"\nData summary:")
print(f"- Publishers: {len(publishers)}")
print(f"- Authors: {len(authors)}")
print(f"- Magazines: {len(magazines)}")
print(f"- Works: {len(work_dict)}")

# バッチ処理のサイズ
BATCH_SIZE = 100

# 1. 出版社ノードの作成
print("\nCreating publisher nodes...")
publisher_list = list(publishers)
for i in range(0, len(publisher_list), BATCH_SIZE):
    batch = publisher_list[i : i + BATCH_SIZE]
    try:
        for publisher in batch:
            db.create_publisher_node(publisher)
        print(f"  Processed {min(i + BATCH_SIZE, len(publisher_list))}/{len(publisher_list)} publishers")
    except Exception as e:
        print(f"  Error processing publishers batch {i//BATCH_SIZE + 1}: {e}")

# 2. 著者ノードの作成
print("\nCreating author nodes...")
author_list = list(authors)
for i in range(0, len(author_list), BATCH_SIZE):
    batch = author_list[i : i + BATCH_SIZE]
    try:
        for author_id in batch:
            db.create_author_node(author_id)
        print(f"  Processed {min(i + BATCH_SIZE, len(author_list))}/{len(author_list)} authors")
    except Exception as e:
        print(f"  Error processing authors batch {i//BATCH_SIZE + 1}: {e}")

# 3. 雑誌ノードの作成
print("\nCreating magazine nodes...")
for i in range(0, len(magazines), BATCH_SIZE):
    batch = magazines[i : i + BATCH_SIZE]
    try:
        for magazine in batch:
            db.create_magazine_node(magazine)

            # 雑誌と出版社の関係を作成
            publisher = magazine.get("schema:publisher", "")
            if publisher:
                if isinstance(publisher, list):
                    publisher = publisher[0] if publisher else ""
                if publisher:
                    db.create_magazine_publisher_relationship(magazine.get("@id", ""), publisher)
        print(f"  Processed {min(i + BATCH_SIZE, len(magazines))}/{len(magazines)} magazines")
    except Exception as e:
        print(f"  Error processing magazines batch {i//BATCH_SIZE + 1}: {e}")

# 4. 作品ノードの作成と関係の構築
print("\nCreating work nodes and relationships...")
work_items = list(work_dict.items())
for i in range(0, len(work_items), BATCH_SIZE):
    batch = work_items[i : i + BATCH_SIZE]
    try:
        for work_name, work_data in batch:
            # 作品ノードを作成
            db.create_work_node(work_name, work_data)

            # 作品と著者の関係を作成
            for creator_id in work_data["creators"]:
                db.create_work_author_relationship(work_name, creator_id)
        print(f"  Processed {min(i + BATCH_SIZE, len(work_items))}/{len(work_items)} works")
    except Exception as e:
        print(f"  Error processing works batch {i//BATCH_SIZE + 1}: {e}")

print("\nImport completed!")

# 接続を閉じる
db.close()
