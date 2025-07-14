import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from kanjiconv import KanjiConv
from neo4j import GraphDatabase
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Import manga data to Neo4j")
parser.add_argument("--clear", action="store_true", help="Clear database before import")
parser.add_argument("--test", action="store_true", help="Test mode - show mapping only")
parser.add_argument("--production", action="store_true", help="Production mode - use production database settings")
parser.add_argument("--sample", action="store_true", help="Use sample data for testing")
args = parser.parse_args()

load_dotenv()


# データ読み込み関数
def load_json_ld(filename):
    """JSON-LD形式のファイルを読み込む"""
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("@graph", [])


if args.production:
    # Neo4j接続設定
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
else:
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"


class NameNormalizer:
    """名前の正規化と統一を行うクラス"""

    def __init__(self):
        self.kanjiconv = KanjiConv()
        self.name_map = {}  # 正規化名 -> 表示名のマッピング
        self.reverse_map = {}  # 入力名 -> 正規化名のマッピング

    def is_editorial_author(self, name):
        """編集部・出版社関連の作者かどうかを判定"""
        if not name or not isinstance(name, str):
            return False

        # 編集部・出版社関連のキーワード
        editorial_keywords = [
            "編集部",
            "出版編集部",
            "コミック出版編集部",
            "コミックス出版編集部",
            "企画・編集",
            "企画編集",
            "編集",
            "構成・編集",
            "構成編集",
            "ジャンプ編集部",
            "Vジャンプ編集部",
            "週刊少年ジャンプ編集部",
            "ジャンプ・コミック出版編集部",
            "ジャンプコミック出版編集部",
            "ジャンプコミックス出版編集部",
        ]

        for keyword in editorial_keywords:
            if keyword in name:
                return True
        return False

    def extract_author_name(self, name):
        """役割プレフィックスを除去して実際の作者名を取得"""
        if not name or not isinstance(name, str):
            return name

        # 役割プレフィックスのパターン
        import re

        # [著]、[原作]、[漫画]、[作]、[作画]、[脚本]等を除去
        pattern = r"^\[.*?\]"
        clean_name = re.sub(pattern, "", name).strip()

        # 追加の清掃: 「・かんしゅう」などの付加情報を除去
        clean_name = clean_name.split("・")[0].strip()

        return clean_name if clean_name else name

    def normalize(self, name):
        """名前を正規化（ひらがなに変換）"""
        if not name or not isinstance(name, str):
            return None
        # カッコ内の内容を削除
        name_without_paren = name.split("(")[0].split("（")[0].strip()
        # ひらがなに変換
        normalized = self.kanjiconv.to_hiragana(name_without_paren)
        return normalized.lower()

    def register_name(self, name):
        """名前を登録して正規化IDを返す"""
        if not name or not isinstance(name, str):
            return None

        normalized = self.normalize(name)
        if not normalized:
            return None

        # 既に登録されている場合
        if name in self.reverse_map:
            return self.reverse_map[name]

        # 正規化名が既に存在する場合
        if normalized in self.name_map:
            # 優先順位に基づいて表示名を更新
            existing_name = self.name_map[normalized]
            if self._should_update_display_name(existing_name, name):
                self.name_map[normalized] = name
        else:
            # 新規登録
            self.name_map[normalized] = name

        self.reverse_map[name] = normalized
        return normalized

    def get_display_name(self, normalized_name):
        """正規化名から表示名を取得"""
        return self.name_map.get(normalized_name, normalized_name)

    def _should_update_display_name(self, existing, new):
        """表示名を更新すべきかどうかを判定"""

        # 優先順位: 漢字 > ローマ字 > カタカナ > ひらがな
        def get_priority(name):
            if any("\u4e00" <= char <= "\u9fff" for char in name):  # 漢字
                return 4
            elif any("A" <= char <= "Z" or "a" <= char <= "z" for char in name):  # ローマ字
                return 3
            elif any("\u30a0" <= char <= "\u30ff" for char in name):  # カタカナ
                return 2
            else:  # ひらがな
                return 1

        return get_priority(new) > get_priority(existing)


def extract_magazines_from_description(description):
    """schema:descriptionフィールドから雑誌名を抽出する"""
    if not description or not isinstance(description, str):
        return []

    magazines = []
    # 「初出：」で始まる部分を探す
    if "初出：" in description:
        # 初出：以降の部分を取得
        initial_part = description.split("初出：", 1)[1]

        # 「」で囲まれた雑誌名を抽出
        magazine_matches = re.findall(r"「([^」]+)」", initial_part)

        for magazine in magazine_matches:
            # 雑誌名をクリーンアップ（余分な文字を除去）
            clean_magazine = magazine.strip()
            if clean_magazine and len(clean_magazine) > 1:  # 空でなく、1文字以上
                magazines.append(clean_magazine)

    return magazines


def extract_magazines_from_brand(brand_field):
    """schema:brandフィールドから雑誌名を抽出する"""
    if not brand_field:
        return []

    magazines = []

    # リストの場合
    if isinstance(brand_field, list):
        for item in brand_field:
            if isinstance(item, str):
                # "=" で区切られている場合は最初の部分を使用
                magazine_name = item.split("=")[0].strip()
                if magazine_name:
                    # ジャンプ・コミックス -> 週刊少年ジャンプに変換
                    normalized_name = normalize_brand_to_magazine(magazine_name)
                    magazines.append(normalized_name)
            elif isinstance(item, dict) and "@value" in item:
                # 辞書形式の場合
                magazine_name = item["@value"].split("=")[0].strip()
                if magazine_name:
                    normalized_name = normalize_brand_to_magazine(magazine_name)
                    magazines.append(normalized_name)
    # 文字列の場合
    elif isinstance(brand_field, str):
        magazine_name = brand_field.split("=")[0].strip()
        if magazine_name:
            normalized_name = normalize_brand_to_magazine(magazine_name)
            magazines.append(normalized_name)

    return magazines


# ブランド名と雑誌名のマッピングを読み込む関数
def load_brand_to_magazine_mapping():
    """brand_to_magazine.jsonからマッピングを読み込む"""
    mapping = {}
    json_path = Path(__file__).parent / "brand_to_magazine.json"

    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # カテゴリごとのマッピングを1つの辞書に統合
                for category, items in data.items():
                    mapping.update(items)
        except Exception as e:
            print(f"Warning: Failed to load brand_to_magazine.json: {e}")
            # フォールバック用の基本的なマッピング
            mapping = {
                "ジャンプ・コミックス": "週刊少年ジャンプ",
                "ジャンプ コミックス": "週刊少年ジャンプ",
                "ジャンプコミックス": "週刊少年ジャンプ",
                "少年サンデーコミックス": "週刊少年サンデー",
                "少年マガジンコミックス": "週刊少年マガジン",
                "少年チャンピオン・コミックス": "週刊少年チャンピオン",
            }
    else:
        print(f"Warning: brand_to_magazine.json not found at {json_path}")
        # フォールバック用の基本的なマッピング
        mapping = {
            "ジャンプ・コミックス": "週刊少年ジャンプ",
            "ジャンプ コミックス": "週刊少年ジャンプ",
            "ジャンプコミックス": "週刊少年ジャンプ",
            "少年サンデーコミックス": "週刊少年サンデー",
            "少年マガジンコミックス": "週刊少年マガジン",
            "少年チャンピオン・コミックス": "週刊少年チャンピオン",
        }

    return mapping


def normalize_brand_to_magazine(brand_name):
    """ブランド名を雑誌名に正規化"""
    # グローバル変数として保存し、毎回読み込まないようにする
    if not hasattr(normalize_brand_to_magazine, "brand_to_magazine"):
        normalize_brand_to_magazine.brand_to_magazine = load_brand_to_magazine_mapping()

    return normalize_brand_to_magazine.brand_to_magazine.get(brand_name, brand_name)


class MangaGraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.author_normalizer = NameNormalizer()
        self.publisher_normalizer = NameNormalizer()
        self.magazine_normalizer = NameNormalizer()

    def close(self):
        self.driver.close()

    def clear_database(self):
        """データベースをクリア"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.")

    def create_constraints(self):
        """制約を作成"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publisher) REQUIRE p.id IS UNIQUE",
            ]
            for constraint in constraints:
                session.run(constraint)

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
        # 最初の本から@idを取得
        first_book = work_data["books"][0] if work_data["books"] else {}
        work_id = first_book.get("@id", "")

        # @idがない場合は作品名から生成
        if not work_id:
            work_id = work_name.upper()

        with self.driver.session() as session:
            query = """
            MERGE (w:Work {id: $work_id})
            SET w.title = $title,
                w.total_volumes = $total_volumes,
                w.first_published = $first_published,
                w.last_published = $last_published,
                w.genre = $genre,
                w.is_series = $is_series
            """

            # 総巻数と最初・最後の出版日を計算
            total_volumes = len(work_data["books"])
            dates = [
                book.get("schema:datePublished", "") for book in work_data["books"] if book.get("schema:datePublished")
            ]
            first_published = min(dates) if dates else ""
            last_published = max(dates) if dates else ""

            # ジャンルを取得
            genre = first_book.get("schema:genre", "")

            # シリーズかどうか（複数巻あるか）
            is_series = total_volumes > 1

            session.run(
                query,
                {
                    "work_id": work_id,
                    "title": work_name,  # 表示名は元のまま
                    "total_volumes": total_volumes,
                    "first_published": first_published,
                    "last_published": last_published,
                    "genre": genre,
                    "is_series": is_series,
                },
            )

        return work_id  # 作成したwork_idを返す

    def create_author_node(self, author_id, author_name=None):
        """著者ノードを作成"""
        with self.driver.session() as session:
            # author_nameが提供されている場合は正規化
            if author_name:
                normalized_id = self.author_normalizer.register_name(author_name)
                if normalized_id:
                    display_name = self.author_normalizer.get_display_name(normalized_id)
                    query = """
                    MERGE (a:Author {id: $author_id})
                    SET a.name = $name,
                        a.normalized_id = $normalized_id
                    """
                    session.run(
                        query, {"author_id": normalized_id, "name": display_name, "normalized_id": normalized_id}
                    )
                    return normalized_id

            # IDのみの場合
            query = """
            MERGE (a:Author {id: $author_id})
            """
            session.run(query, {"author_id": author_id})
            return author_id

    def create_publisher_node(self, publisher_name):
        """出版社ノードを作成"""
        normalized_id = self.publisher_normalizer.register_name(publisher_name)
        if not normalized_id:
            return None

        display_name = self.publisher_normalizer.get_display_name(normalized_id)

        with self.driver.session() as session:
            query = """
            MERGE (p:Publisher {id: $publisher_id})
            SET p.name = $name
            """
            session.run(query, {"publisher_id": normalized_id, "name": display_name})

        return normalized_id

    def create_magazine_node(self, magazine):
        """雑誌ノードを作成"""
        # タイトルの取得
        title = magazine.get("schema:name", "")
        if isinstance(title, list):
            title = title[0] if title else ""

        if not title:
            return None

        normalized_id = self.magazine_normalizer.register_name(title)
        if not normalized_id:
            return None

        display_name = self.magazine_normalizer.get_display_name(normalized_id)

        with self.driver.session() as session:
            query = """
            MERGE (m:Magazine {id: $magazine_id})
            SET m.title = $title,
                m.genre = $genre,
                m.original_id = $original_id
            """

            session.run(
                query,
                {
                    "magazine_id": normalized_id,
                    "title": display_name,
                    "genre": magazine.get("schema:genre", ""),
                    "original_id": magazine.get("@id", ""),
                },
            )

        return normalized_id

    def create_work_author_relationship(self, work_id, author_id):
        """作品と著者の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            MATCH (a:Author {id: $author_id})
            MERGE (w)-[:CREATED_BY]->(a)
            """
            session.run(query, {"work_id": work_id, "author_id": author_id})

    def create_work_magazine_relationship(self, work_id, magazine_id):
        """作品と雑誌の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (w:Work {id: $work_id})
            MATCH (m:Magazine {id: $magazine_id})
            MERGE (w)-[:PUBLISHED_IN]->(m)
            """
            session.run(query, {"work_id": work_id, "magazine_id": magazine_id})

    def create_magazine_publisher_relationship(self, magazine_id, publisher_id):
        """雑誌と出版社の関係を作成"""
        with self.driver.session() as session:
            query = """
            MATCH (m:Magazine {id: $magazine_id})
            MATCH (p:Publisher {id: $publisher_id})
            MERGE (m)-[:PUBLISHED_BY]->(p)
            """
            session.run(query, {"magazine_id": magazine_id, "publisher_id": publisher_id})


# メイン処理
if __name__ == "__main__":
    print("=== Neo4j Import v3 ===")

    # テストモードの場合はマッピングを表示
    if args.test:
        print("\n=== Testing brand to magazine mapping ===")
        mapping = load_brand_to_magazine_mapping()
        print(f"Total mappings loaded: {len(mapping)}")
        print("\nSample mappings:")
        for i, (brand, magazine) in enumerate(mapping.items()):
            if i < 20:
                print(f"  {brand} -> {magazine}")
            else:
                print(f"  ... and {len(mapping) - 20} more mappings")
                break
        print("\n=== Test completed ===")
        sys.exit(0)

    # データベースクリアの確認
    clear_db = args.clear

    # グローバルなNameNormalizerインスタンスを作成
    author_normalizer = NameNormalizer()

    try:
        db = MangaGraphDB(neo4j_uri, neo4j_user, neo4j_password)

        if clear_db:
            db.clear_database()

        # 制約を作成
        print("\nCreating constraints...")
        db.create_constraints()

        # 現在の状況を確認
        print("\nCurrent status:")
        status = db.get_current_status()
        for label, count in status.items():
            print(f"  {label}: {count:,}")

        # データを読み込む
        print("\nLoading data...")
        script_dir = Path(__file__).parent
        data_dir = script_dir.parent.parent / "data" / "mediaarts"
        print(f"Looking for data in: {data_dir}")
        print(f"Data directory exists: {data_dir.exists()}")
        json_files = list(data_dir.glob("*.json"))
        print(f"Found {len(json_files)} JSON files")

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
        manga_series = []  # MangaBookSeriesを収集
        magazines = []
        authors_data = {}  # author_id -> author_data
        publishers = set()
        series_magazine_map = {}  # series_id -> set of magazines

        print("\nClassifying data...")
        error_count = 0

        for i, item in enumerate(tqdm(all_data)):
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break
            try:
                item_type = item.get("@type", "")
                genre = item.get("schema:genre", "")

                # デバッグ: 最初の10件の詳細を表示
                if i < 10:
                    print(f"    Item {i}: type='{item_type}', genre='{genre}'")

                if item_type == "class:MangaBook" and genre == "マンガ単行本":
                    manga_books.append(item)

                    # 著者情報を収集
                    creator = item.get("dcterms:creator", {})
                    if isinstance(creator, dict) and "@id" in creator:
                        author_id = creator["@id"]
                        if author_id not in authors_data:
                            authors_data[author_id] = {"id": author_id, "names": set()}

                        # 著者名を収集（schema:creatorから）
                        creator_names = item.get("schema:creator", [])
                        if isinstance(creator_names, str):
                            creator_names = [creator_names]
                        elif not isinstance(creator_names, list):
                            creator_names = []

                        for name in creator_names:
                            if isinstance(name, str) and name:
                                # 編集部・出版社関連の作者を除外
                                if not author_normalizer.is_editorial_author(name):
                                    authors_data[author_id]["names"].add(name)
                            elif isinstance(name, dict):
                                name_value = name.get("@value", name.get("name", ""))
                                if name_value and isinstance(name_value, str):
                                    # 編集部・出版社関連の作者を除外
                                    if not author_normalizer.is_editorial_author(name_value):
                                        authors_data[author_id]["names"].add(name_value)

                    # 出版社情報を収集
                    publisher = item.get("schema:publisher", "")
                    if publisher:
                        if isinstance(publisher, list):
                            for pub in publisher:
                                if isinstance(pub, str):
                                    publishers.add(pub)
                                elif isinstance(pub, dict):
                                    # 辞書の場合、@valueや他のフィールドを確認
                                    pub_name = pub.get("@value", pub.get("name", ""))
                                    if pub_name and isinstance(pub_name, str):
                                        publishers.add(pub_name)
                        elif isinstance(publisher, str):
                            publishers.add(publisher)
                        elif isinstance(publisher, dict):
                            # 辞書の場合、@valueや他のフィールドを確認
                            pub_name = publisher.get("@value", publisher.get("name", ""))
                            if pub_name and isinstance(pub_name, str):
                                publishers.add(pub_name)

                elif item_type == "class:MangaBookSeries" and genre == "マンガ単行本シリーズ":
                    manga_series.append(item)

                    # シリーズのschema:brandから雑誌情報を収集
                    series_id = item.get("@id", "")
                    if series_id:
                        brand = item.get("schema:brand", [])
                        if brand:
                            magazines_from_brand = extract_magazines_from_brand(brand)
                            if magazines_from_brand:
                                series_magazine_map[series_id] = set(magazines_from_brand)

                    # シリーズの著者情報も収集
                    creator = item.get("dcterms:creator", {})
                    if isinstance(creator, list):
                        for cr in creator:
                            if isinstance(cr, dict) and "@id" in cr:
                                author_id = cr["@id"]
                                if author_id not in authors_data:
                                    authors_data[author_id] = {"id": author_id, "names": set()}
                    elif isinstance(creator, dict) and "@id" in creator:
                        author_id = creator["@id"]
                        if author_id not in authors_data:
                            authors_data[author_id] = {"id": author_id, "names": set()}

                    # schema:creatorからも著者名を収集
                    creator_names = item.get("schema:creator", [])
                    if isinstance(creator_names, str):
                        creator_names = [creator_names]
                    elif not isinstance(creator_names, list):
                        creator_names = []

                    # 最初の著者IDに名前を関連付け
                    if creator_names and series_id:
                        first_creator = creator
                        if isinstance(creator, list) and creator:
                            first_creator = creator[0]

                        if isinstance(first_creator, dict) and "@id" in first_creator:
                            author_id = first_creator["@id"]
                            for name in creator_names:
                                if isinstance(name, str) and name:
                                    # 編集部・出版社関連の作者を除外
                                    if not author_normalizer.is_editorial_author(name):
                                        authors_data[author_id]["names"].add(name)

                elif genre in ["マンガ雑誌", "雑誌", "雑誌全号まとめ"] or "Magazine" in item_type:
                    magazines.append(item)

                    # 出版社情報を収集
                    publisher = item.get("schema:publisher", "")
                    if publisher:
                        if isinstance(publisher, list):
                            for pub in publisher:
                                if isinstance(pub, str):
                                    publishers.add(pub)
                                elif isinstance(pub, dict):
                                    # 辞書の場合、@valueや他のフィールドを確認
                                    pub_name = pub.get("@value", pub.get("name", ""))
                                    if pub_name and isinstance(pub_name, str):
                                        publishers.add(pub_name)
                        elif isinstance(publisher, str):
                            publishers.add(publisher)
                        elif isinstance(publisher, dict):
                            # 辞書の場合、@valueや他のフィールドを確認
                            pub_name = publisher.get("@value", publisher.get("name", ""))
                            if pub_name and isinstance(pub_name, str):
                                publishers.add(pub_name)
            except Exception as e:
                error_count += 1
                if error_count <= 10:
                    print(f"  Error processing item {i}: {e}")
                    print(f"    Item type: {item.get('@type', 'Unknown')}")
                    print(f"    Genre: {item.get('schema:genre', 'Unknown')}")

        print(f"\nClassification completed. Errors: {error_count}")

        # マンガ作品（シリーズ）の抽出 - 単行本から作品名でグループ化
        work_dict = {}
        work_magazine_map = defaultdict(set)  # work -> set of magazines

        print("\nGrouping manga books into works...")

        for book in tqdm(manga_books, desc="Grouping manga books"):
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
                # 作品名を大文字に統一してキーとする
                work_key = work_name.upper()

                if work_key not in work_dict:
                    work_dict[work_key] = {
                        "name": work_name,  # 最初に見つかった表記を保持
                        "books": [],
                        "creators": set(),
                        "publishers": set(),
                    }

                work_dict[work_key]["books"].append(book)

                # 著者情報を追加
                creator = book.get("dcterms:creator", {})
                if isinstance(creator, dict) and "@id" in creator:
                    creator_id = creator["@id"]
                    if isinstance(creator_id, str):
                        work_dict[work_key]["creators"].add(creator_id)

                # 出版社情報を追加
                publisher = book.get("schema:publisher", "")
                if publisher:
                    if isinstance(publisher, list):
                        for pub in publisher:
                            if isinstance(pub, str):
                                work_dict[work_key]["publishers"].add(pub)
                            elif isinstance(pub, dict):
                                pub_name = pub.get("@value", pub.get("name", ""))
                                if pub_name and isinstance(pub_name, str):
                                    work_dict[work_key]["publishers"].add(pub_name)
                    elif isinstance(publisher, str):
                        work_dict[work_key]["publishers"].add(publisher)
                    elif isinstance(publisher, dict):
                        pub_name = publisher.get("@value", publisher.get("name", ""))
                        if pub_name and isinstance(pub_name, str):
                            work_dict[work_key]["publishers"].add(pub_name)

                # 掲載雑誌情報を収集（schema:descriptionから初出情報を抽出）
                description = book.get("schema:description", "")
                if description:
                    extracted_magazines = extract_magazines_from_description(description)
                    for magazine_name in extracted_magazines:
                        work_magazine_map[work_key].add(magazine_name)

        # シリーズから得た雑誌情報を作品に適用
        print("\nApplying series magazine information to works...")
        series_matches = 0
        for i, (series_id, series_magazines) in tqdm(
            enumerate(series_magazine_map.items()), total=len(series_magazine_map)
        ):
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break
            # シリーズ名を取得
            series_name = None
            for series in manga_series:
                if series.get("@id") == series_id:
                    series_name = series.get("schema:name", "")
                    if isinstance(series_name, list):
                        series_name = series_name[0] if series_name else ""
                    elif isinstance(series_name, dict):
                        series_name = series_name.get("@value", "")
                    break

            if series_name and isinstance(series_name, str):
                # 作品名を正規化してマッチング
                series_key = series_name.upper()

                # 完全一致を試す
                if series_key in work_dict:
                    for magazine_name in series_magazines:
                        work_magazine_map[series_key].add(magazine_name)
                    series_matches += 1
                    # print(f"  Matched series: {series_name} -> {list(series_magazines)}")
                else:
                    # 部分一致を試す（例：「ドラゴンボール」シリーズと「ドラゴンボール」単行本）
                    for work_key in work_dict.keys():
                        # シリーズ名が作品名に含まれる、または作品名がシリーズ名に含まれる
                        if (series_key in work_key or work_key in series_key) and abs(
                            len(series_key) - len(work_key)
                        ) <= 10:
                            for magazine_name in series_magazines:
                                work_magazine_map[work_key].add(magazine_name)
                            series_matches += 1
                            if series_matches <= 10:
                                # 最初の10件のみ表示
                                print(
                                    f"  Partial match: {series_name} -> {work_dict[work_key]['name']} -> {list(series_magazines)}"
                                )
                            break

        print(f"Found {len(manga_books)} manga books")
        print(f"Found {len(manga_series)} manga series")
        print(f"Found {len(work_dict)} unique works")
        print(f"Found {len(magazines)} magazines")
        print(f"Found {len(authors_data)} authors")
        print(f"Applied {series_matches} series magazine mappings")
        print(f"Found {len(publishers)} publishers")

        # デバッグ情報を追加
        total_magazine_relations = sum(len(mags) for mags in work_magazine_map.values())
        print(f"Found {total_magazine_relations} work-magazine relationships")
        print(f"Found {len(work_magazine_map)} works with magazine info")

        # バッチ処理のサイズ
        BATCH_SIZE = 100

        # 1. 出版社ノードの作成
        print("\nCreating publisher nodes...")
        publisher_map = {}  # original -> normalized_id

        for i, publisher in enumerate(tqdm(publishers), total=len(publishers), desc="Processing publishers"):
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break

            publisher_id = db.create_publisher_node(publisher)
            if publisher_id:
                publisher_map[publisher] = publisher_id

        # 2. 著者ノードの作成
        print("\nCreating author nodes...")
        author_map = {}  # original_id -> normalized_id

        for i, (author_id, author_data) in enumerate(
            tqdm(authors_data.items(), total=len(authors_data), desc="Processing authors")
        ):
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break

            # 最も優先度の高い名前を選択
            if author_data["names"]:
                # 名前のリストから最も優先度の高いものを選択
                best_name = None
                best_priority = 0

                for name in author_data["names"]:
                    # 編集部・出版社関連の作者を除外
                    if author_normalizer.is_editorial_author(name):
                        continue

                    # 実際の作者名を抽出
                    clean_name = author_normalizer.extract_author_name(name)

                    priority = 0
                    # 実際の作者名（役割プレフィックス除去後）に基づいて優先度を決定
                    if any("\u4e00" <= char <= "\u9fff" for char in clean_name):  # 漢字
                        priority = 4
                    elif any("A" <= char <= "Z" or "a" <= char <= "z" for char in clean_name):  # ローマ字
                        priority = 3
                    elif any("\u30a0" <= char <= "\u30ff" for char in clean_name):  # カタカナ
                        priority = 2
                    else:  # ひらがな
                        priority = 1

                    # 役割プレフィックスがある場合は優先度を上げる
                    if name.startswith("[") and "]" in name:
                        role = name.split("]")[0] + "]"
                        if role in ["[著]", "[原作]", "[漫画]", "[作]", "[作画]"]:
                            priority += 5  # 実際の作者役割を優先

                    if priority > best_priority:
                        best_priority = priority
                        best_name = name

                normalized_id = db.create_author_node(author_id, best_name)
            else:
                normalized_id = db.create_author_node(author_id)

            author_map[author_id] = normalized_id

        # 3. 雑誌ノードの作成
        print("\nCreating magazine nodes...")
        magazine_map = {}  # magazine_name -> magazine_id
        magazine_id_to_name = {}  # magazine_id -> magazine_name

        for i, magazine in enumerate(tqdm(magazines), total=len(magazines), desc="Processing magazines"):
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break

            normalized_id = db.create_magazine_node(magazine)
            if normalized_id:
                # タイトルの取得
                title = magazine.get("schema:name", "")
                if isinstance(title, list):
                    title = title[0] if title else ""

                # 雑誌名をキーとして保存
                if title:
                    magazine_map[title] = normalized_id
                    magazine_id_to_name[normalized_id] = title

                # 雑誌と出版社の関係を作成
                publisher = magazine.get("schema:publisher", "")
                if publisher:
                    if isinstance(publisher, list):
                        # リストの最初の要素を取得
                        for pub in publisher:
                            if isinstance(pub, str):
                                publisher = pub
                                break
                            elif isinstance(pub, dict):
                                pub_name = pub.get("@value", pub.get("name", ""))
                                if pub_name and isinstance(pub_name, str):
                                    publisher = pub_name
                                    break
                        else:
                            publisher = ""
                    elif isinstance(publisher, dict):
                        publisher = publisher.get("@value", publisher.get("name", ""))

                    if publisher and publisher in publisher_map:
                        db.create_magazine_publisher_relationship(normalized_id, publisher_map[publisher])

        # 4. 作品ノードの作成と関係の構築
        print("\nCreating work nodes and relationships...")
        work_items = list(work_dict.items())

        # デバッグ: 雑誌マップのサンプルを表示
        print("\nMagazine map sample (first 10):")
        for i, (name, mag_id) in enumerate(list(magazine_map.items())[:10]):
            print(f"  {name} -> {mag_id}")

        for i in tqdm(range(0, len(work_items), BATCH_SIZE), desc="Processing works"):
            batch = work_items[i : i + BATCH_SIZE]
            if args.sample and i >= 100:
                print("Sample limit reached, stopping early.")
                break
            try:
                for work_key, work_data in batch:
                    # 作品ノードを作成
                    work_id = db.create_work_node(work_data["name"], work_data)

                    # work_idがNoneの場合はスキップ
                    if not work_id:
                        print(f"  Warning: Failed to create work node for {work_data['name']}")
                        continue

                    # 作品と著者の関係を作成
                    for creator_id in work_data["creators"]:
                        if creator_id in author_map:
                            db.create_work_author_relationship(work_id, author_map[creator_id])

                    # 作品と雑誌の関係を作成（雑誌名から検索）
                    magazine_names = work_magazine_map.get(work_key, [])
                    if magazine_names and i == 0 and work_key == work_items[0][0]:  # 最初の作品のみデバッグ
                        print("\nDebug - First work magazine mapping:")
                        print(f"  Work: {work_data['name']}")
                        print(f"  Magazines from work: {magazine_names}")
                        print(f"  Magazines found in map: {[m for m in magazine_names if m in magazine_map]}")

                    for magazine_name in magazine_names:
                        # 雑誌名で直接検索
                        if magazine_name in magazine_map:
                            db.create_work_magazine_relationship(work_id, magazine_map[magazine_name])

                print(f"Processed {min(i + BATCH_SIZE, len(work_items))}/{len(work_items)} works")
            except Exception as e:
                print(f"Error processing works batch {i // BATCH_SIZE + 1}: {e}")

        # 最終状況を確認
        print("\nFinal status:")
        status = db.get_current_status()
        for label, count in status.items():
            print(f"  {label}: {count:,}")

        # リレーションシップのカウント
        print("\nRelationship counts:")
        with db.driver.session() as session:
            for rel_type in ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count")
                count = result.single()["count"]
                print(f"  {rel_type}: {count:,}")

        db.close()
        print("\nImport completed!")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
