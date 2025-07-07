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


# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")


class MangaGraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_current_status(self):
        """現在のインポート状況を確認"""
        with self.driver.session() as session:
            status = {}
            for label in ["Work", "Author", "Publisher", "Magazine"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                status[label] = result.single()["count"]
            return status

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
                    "work_id": work_name,
                    "title": work_name,
                    "total_volumes": total_volumes,
                    "first_published": first_published,
                    "last_published": last_published,
                },
            )

    def create_work_author_relationship(self, work_name, author_id):
        """作品と著者の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            MATCH (a:Author {id: $author_id})
            MERGE (w)-[:CREATED_BY]->(a)
            """
            session.run(query, {"work_id": work_name, "author_id": author_id})

    def create_magazine_node(self, magazine):
        """雑誌ノードを作成（継続用）"""
        with self.driver.session() as session:
            # 既に存在するかチェック
            check_query = "MATCH (m:Magazine {id: $magazine_id}) RETURN m"
            result = session.run(check_query, {"magazine_id": magazine.get("@id", "")})
            if result.single() is not None:
                return False  # 既に存在

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
            return True

    def create_magazine_publisher_relationship(self, magazine_id, publisher_name):
        """雑誌と出版社の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (m:Magazine {id: $magazine_id})
            MATCH (p:Publisher {id: $publisher_id})
            MERGE (m)-[:PUBLISHED_BY]->(p)
            """
            session.run(query, {"magazine_id": magazine_id, "publisher_id": publisher_name})


# メイン処理
print("=== Continuing Neo4j Import ===")

try:
    db = MangaGraphDB(neo4j_uri, neo4j_user, neo4j_password)

    # 現在の状況を確認
    print("\nCurrent status:")
    status = db.get_current_status()
    for label, count in status.items():
        print(f"  {label}: {count:,}")

    # データを読み込む
    print("\nLoading data...")
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent / "data" / "mediaarts"
    json_files = list(data_dir.glob("*.json"))

    all_data = []
    for file_path in json_files:
        print(f"Loading {file_path.name}...")
        try:
            data = load_json_ld(str(file_path))
            all_data.extend(data)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")

    print(f"\nTotal items loaded: {len(all_data)}")

    # データの分類
    manga_books = []
    magazines = []

    for item in all_data:
        item_type = item.get("@type", "")
        genre = item.get("schema:genre", "")

        if item_type == "class:MangaBook" and genre == "マンガ単行本":
            manga_books.append(item)
        elif genre in ["マンガ雑誌", "雑誌", "雑誌全号まとめ"] or "Magazine" in item_type:
            magazines.append(item)

    # 残りの雑誌をインポート
    if status["Magazine"] < len(magazines):
        print(f"\nContinuing magazine import from {status['Magazine']:,}/{len(magazines):,}...")

        processed = 0
        skipped = 0
        for i, magazine in enumerate(magazines):
            if i % 1000 == 0:
                print(f"  Progress: {i:,}/{len(magazines):,} (processed: {processed}, skipped: {skipped})")

            try:
                if db.create_magazine_node(magazine):
                    processed += 1

                    # 雑誌と出版社の関係を作成
                    publisher = magazine.get("schema:publisher", "")
                    if publisher:
                        if isinstance(publisher, list):
                            publisher = publisher[0] if publisher else ""
                        if publisher:
                            db.create_magazine_publisher_relationship(magazine.get("@id", ""), publisher)
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Error processing magazine {i}: {e}")

        print(f"  Magazine import completed. Processed: {processed}, Skipped: {skipped}")

    # 作品データの作成
    if status["Work"] == 0:
        print("\nCreating work nodes...")

        # マンガ作品（シリーズ）の抽出
        work_dict = {}
        for book in manga_books:
            # 作品名を取得（巻数を除いたタイトル）
            full_name = book.get("schema:name", "")

            # nameフィールドの処理
            if isinstance(full_name, list):
                for name in full_name:
                    if isinstance(name, str):
                        full_name = name
                        break
                    elif isinstance(name, dict) and "@value" in name:
                        full_name = name["@value"]
                        break
                else:
                    continue
            elif isinstance(full_name, dict):
                if "@value" in full_name:
                    full_name = full_name["@value"]
                else:
                    continue
            elif not isinstance(full_name, str):
                continue

            # 巻数を除いた作品名を取得
            work_name = full_name
            volume_num = book.get("schema:volumeNumber", "")
            if volume_num and isinstance(work_name, str):
                work_name = work_name.replace(f" {volume_num}", "").strip()

            if work_name and isinstance(work_name, str):
                if work_name not in work_dict:
                    work_dict[work_name] = {"name": work_name, "books": [], "creators": set()}

                work_dict[work_name]["books"].append(book)

                # 著者情報を追加
                creator = book.get("dcterms:creator", {})
                if isinstance(creator, dict) and "@id" in creator:
                    work_dict[work_name]["creators"].add(creator["@id"])

        print(f"Found {len(work_dict)} works to import")

        # バッチ処理
        BATCH_SIZE = 100
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

    # 最終状況を確認
    print("\nFinal status:")
    status = db.get_current_status()
    for label, count in status.items():
        print(f"  {label}: {count:,}")

    db.close()
    print("\nImport continuation completed!")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
