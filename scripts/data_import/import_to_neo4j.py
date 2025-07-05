#!/usr/bin/env python3
"""
メディア芸術データベースからNeo4jへデータをインポート
"""
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.data_import.name_normalizer import (
    generate_normalized_id,
    normalize_and_split_creators,
    normalize_creator_name,
    normalize_publisher_name,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts"


class Neo4jImporter:
    """Neo4jへのデータインポートクラス"""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"Connected to Neo4j at {uri}")

    def close(self):
        """接続を閉じる"""
        self.driver.close()

    def clear_database(self):
        """データベースをクリア（開発用）"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")

    def create_constraints(self):
        """インデックスと制約を作成"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Work) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Publisher) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Magazine) REQUIRE m.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (mi:MagazineIssue) REQUIRE mi.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Series) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (pub:Publication) REQUIRE pub.id IS UNIQUE",
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    logger.warning(f"Constraint might already exist: {e}")

    def import_manga_books(self, filepath: Path, batch_size: int = 1000):
        """マンガ単行本データをインポート"""
        logger.info(f"Importing manga books from {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("@graph", [])
        logger.info(f"Found {len(items)} manga book items")

        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing manga books"):
            batch = items[i : i + batch_size]
            self._import_book_batch(batch)

    def _import_book_batch(self, items: List[Dict]):
        """マンガ単行本のバッチをインポート"""
        with self.driver.session() as session:
            for item in items:
                try:
                    # 作品ノードを作成
                    work_id = item.get("@id", "")
                    title = self._extract_value(item.get("schema:name", ""))

                    if not title:
                        continue

                    work_props = {
                        "id": work_id,
                        "title": title,
                        "published_date": item.get("schema:datePublished", ""),
                        "publisher": item.get("schema:publisher", ""),
                        "isbn": item.get("schema:isbn", ""),
                        "volume": item.get("schema:volumeNumber", ""),
                        "genre": item.get("schema:genre", ""),
                        "pages": item.get("schema:numberOfPages", ""),
                        "price": item.get("schema:price", ""),
                        "source": "media_arts_db",
                    }

                    # 作品ノードを作成
                    session.run("MERGE (w:Work {id: $id}) SET w += $props", id=work_id, props=work_props)

                    # 作者を処理
                    creators = item.get("schema:creator", [])
                    if isinstance(creators, str):
                        creators = [creators]
                    elif not isinstance(creators, list):
                        creators = []

                    for creator in creators:
                        creator_name = self._extract_value(creator)
                        if creator_name and isinstance(creator_name, str):
                            # 複数著者を分割して正規化
                            normalized_creator_names = normalize_and_split_creators(creator_name)
                            for normalized_creator_name in normalized_creator_names:
                                if normalized_creator_name:  # 正規化後に空でない場合のみ処理
                                    author_id = generate_normalized_id(normalized_creator_name, "author")
                                    session.run(
                                        """
                                        MERGE (a:Author {id: $id})
                                        SET a.name = $normalized_name, a.original_name = $original_name, a.source = 'media_arts_db'
                                        WITH a
                                        MATCH (w:Work {id: $work_id})
                                        MERGE (a)-[:CREATED]->(w)
                                        """,
                                        id=author_id,
                                        normalized_name=normalized_creator_name,
                                        original_name=creator_name,
                                        work_id=work_id,
                                    )

                    # 出版社を処理
                    publisher = item.get("schema:publisher", "")
                    publisher_name = self._extract_value(publisher)
                    if publisher_name and isinstance(publisher_name, str):
                        # 出版社名を正規化
                        normalized_publisher_name = normalize_publisher_name(publisher_name)
                        if normalized_publisher_name:  # 正規化後に空でない場合のみ処理
                            publisher_id = generate_normalized_id(normalized_publisher_name, "publisher")
                            session.run(
                                """
                                MERGE (p:Publisher {id: $id})
                                SET p.name = $normalized_name, p.original_name = $original_name, p.source = 'media_arts_db'
                                WITH p
                                MATCH (w:Work {id: $work_id})
                                MERGE (p)-[:PUBLISHED]->(w)
                                """,
                                id=publisher_id,
                                normalized_name=normalized_publisher_name,
                                original_name=publisher_name,
                                work_id=work_id,
                            )

                except Exception as e:
                    logger.error(f"Error importing item {item.get('@id', 'unknown')}: {e}")

    def import_manga_magazines(self, filepath: Path, batch_size: int = 1000):
        """マンガ雑誌シリーズデータをインポート (metadata105)"""
        logger.info(f"Importing manga magazines from {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("@graph", [])
        logger.info(f"Found {len(items)} manga magazine items")

        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing manga magazines"):
            batch = items[i : i + batch_size]
            self._import_magazine_batch(batch)

    def import_magazine_issues(self, filepath: Path, batch_size: int = 1000):
        """マンガ雑誌各号データをインポート (metadata102)"""
        logger.info(f"Importing magazine issues from {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("@graph", [])
        logger.info(f"Found {len(items)} magazine issue items")

        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing magazine issues"):
            batch = items[i : i + batch_size]
            self._import_magazine_issue_batch(batch)

    def import_magazine_publications(self, filepath: Path, batch_size: int = 1000):
        """マンガ雑誌掲載履歴データをインポート"""
        logger.info(f"Importing magazine publications from {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("@graph", [])
        logger.info(f"Found {len(items)} magazine publication items")

        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing magazine publications"):
            batch = items[i : i + batch_size]
            self._import_publication_batch(batch)

    def import_manga_series(self, filepath: Path, batch_size: int = 1000):
        """マンガシリーズデータをインポート"""
        logger.info(f"Importing manga series from {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        items = data.get("@graph", [])
        logger.info(f"Found {len(items)} manga series items")

        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing manga series"):
            batch = items[i : i + batch_size]
            self._import_series_batch(batch)

    def _import_series_batch(self, items: List[Dict]):
        """マンガシリーズのバッチをインポート"""
        with self.driver.session() as session:
            for item in items:
                try:
                    series_id = item.get("@id", "")
                    series_name = self._extract_value(item.get("schema:name", ""))

                    if not series_name:
                        continue

                    series_props = {
                        "id": series_id,
                        "name": series_name,
                        "volume_count": item.get("schema:numberOfItems", 0),
                        "start_date": item.get("schema:datePublished", ""),
                        "publisher": item.get("schema:publisher", ""),
                        "source": "media_arts_db",
                    }

                    # シリーズノードを作成
                    session.run("MERGE (s:Series {id: $id}) SET s += $props", id=series_id, props=series_props)

                    # シリーズと作品の関連を作成（タイトルマッチング）
                    if series_name:
                        # シリーズ名から数字を除去してベースタイトルを取得
                        base_title = series_name.strip()

                        # 短すぎるシリーズ名（3文字以下）は部分一致を避ける
                        if len(base_title) <= 3:
                            # 完全一致または先頭一致のみ
                            session.run(
                                """
                                MATCH (s:Series {id: $series_id})
                                MATCH (w:Work)
                                WHERE w.title = $base_title 
                                   OR w.title STARTS WITH ($base_title + ' ')
                                   OR w.title STARTS WITH ($base_title + '　')
                                MERGE (s)-[:CONTAINS]->(w)
                                """,
                                series_id=series_id,
                                base_title=base_title,
                            )
                        else:
                            # 通常のシリーズ名は、より厳密なマッチング
                            session.run(
                                """
                                MATCH (s:Series {id: $series_id})
                                MATCH (w:Work)
                                WHERE w.title CONTAINS $base_title
                                   AND (
                                       w.title = $base_title
                                       OR w.title STARTS WITH ($base_title + ' ')
                                       OR w.title STARTS WITH ($base_title + '　')
                                       OR w.title =~ $pattern
                                   )
                                MERGE (s)-[:CONTAINS]->(w)
                                """,
                                series_id=series_id,
                                base_title=base_title,
                                pattern=f".*{base_title}\\s*(\\d+|第\\d+巻|\\(\\d+\\)|vol\\.\\s*\\d+|VOLUME\\s*\\d+).*",
                            )

                except Exception as e:
                    logger.error(f"Error importing series {item.get('@id', 'unknown')}: {e}")

    def _import_magazine_batch(self, items: List[Dict]):
        """マンガ雑誌シリーズのバッチをインポート (metadata105)"""
        with self.driver.session() as session:
            for item in items:
                try:
                    magazine_id = item.get("@id", "")
                    magazine_name = self._extract_value(item.get("schema:name", ""))

                    if not magazine_name:
                        continue

                    magazine_props = {
                        "id": magazine_id,
                        "name": magazine_name,
                        "publisher": item.get("schema:publisher", ""),
                        "publication_periodicity": item.get("ma:publicationPeriodicity", ""),
                        "start_date": item.get("schema:datePublished", ""),
                        "end_date": item.get("ma:dayPublishedFinal", ""),
                        "genre": item.get("schema:genre", ""),
                        "location": item.get("schema:location", ""),
                        "source": "media_arts_db",
                    }

                    # 雑誌シリーズノードを作成
                    session.run("MERGE (m:Magazine {id: $id}) SET m += $props", id=magazine_id, props=magazine_props)

                except Exception as e:
                    logger.error(f"Error importing magazine {item.get('@id', 'unknown')}: {e}")

    def _import_magazine_issue_batch(self, items: List[Dict]):
        """マンガ雑誌各号のバッチをインポート (metadata102)"""
        with self.driver.session() as session:
            for item in items:
                try:
                    issue_id = item.get("@id", "")
                    issue_name = self._extract_value(item.get("schema:name", ""))

                    if not issue_name:
                        continue

                    issue_props = {
                        "id": issue_id,
                        "name": issue_name,
                        "issue_number": item.get("schema:issueNumber", ""),
                        "volume_number": item.get("schema:volumeNumber", ""),
                        "publication_date": item.get("schema:datePublished", ""),
                        "publisher": item.get("schema:publisher", ""),
                        "pages": item.get("schema:numberOfPages", ""),
                        "genre": item.get("schema:genre", ""),
                        "source": "media_arts_db",
                    }

                    # 雑誌号ノードを作成
                    session.run("MERGE (mi:MagazineIssue {id: $id}) SET mi += $props", id=issue_id, props=issue_props)

                    # 雑誌シリーズとの関連を作成
                    parent_magazine_id = item.get("schema:isPartOf", {}).get("@id", "")
                    if parent_magazine_id:
                        session.run(
                            """
                            MATCH (mi:MagazineIssue {id: $issue_id})
                            MATCH (m:Magazine {id: $magazine_id})
                            MERGE (mi)-[:ISSUE_OF]->(m)
                        """,
                            issue_id=issue_id,
                            magazine_id=parent_magazine_id,
                        )

                except Exception as e:
                    logger.error(f"Error importing magazine issue {item.get('@id', 'unknown')}: {e}")

    def _import_publication_batch(self, items: List[Dict]):
        """マンガ雑誌掲載履歴のバッチをインポート"""
        with self.driver.session() as session:
            for item in items:
                try:
                    publication_id = item.get("@id", "")
                    work_title = self._extract_value(item.get("schema:name", ""))

                    if not work_title:
                        continue

                    # 掲載履歴ノードを作成
                    publication_props = {
                        "id": publication_id,
                        "title": work_title,
                        "publication_date": item.get("schema:datePublished", ""),
                        "genre": item.get("schema:genre", ""),
                        "source": "media_arts_db",
                    }

                    # 掲載履歴ノードを作成
                    session.run(
                        "MERGE (p:Publication {id: $id}) SET p += $props", id=publication_id, props=publication_props
                    )

                    # 作者を処理
                    creators = item.get("schema:creator", [])
                    if isinstance(creators, str):
                        creators = [creators]
                    elif not isinstance(creators, list):
                        creators = []

                    for creator in creators:
                        creator_name = self._extract_value(creator)
                        if creator_name and isinstance(creator_name, str):
                            normalized_creator_names = normalize_and_split_creators(creator_name)
                            for normalized_creator_name in normalized_creator_names:
                                if normalized_creator_name:
                                    author_id = generate_normalized_id(normalized_creator_name, "author")
                                    session.run(
                                        """
                                        MERGE (a:Author {id: $id})
                                        SET a.name = $normalized_name, a.original_name = $original_name, a.source = 'media_arts_db'
                                        WITH a
                                        MATCH (p:Publication {id: $publication_id})
                                        MERGE (a)-[:CREATED_PUBLICATION]->(p)
                                        """,
                                        id=author_id,
                                        normalized_name=normalized_creator_name,
                                        original_name=creator_name,
                                        publication_id=publication_id,
                                    )

                except Exception as e:
                    logger.error(f"Error importing publication {item.get('@id', 'unknown')}: {e}")

    def _extract_magazine_info(self, magazine_name: str) -> tuple:
        """雑誌名から基本名と号数情報を抽出"""
        import re

        # 基本的なパターンマッチング
        patterns = [
            r"(.+?)\s+(\d+月号)",  # "月刊 楽書館 4月号"
            r"(.+?)\s+(\d+号)",  # "雑誌名 123号"
            r"(.+?)\s+(NO[.,]\s*\d+)",  # "雑誌名 NO.123"
            r"(.+?)\s+(\d+)",  # "雑誌名 123"
        ]

        for pattern in patterns:
            match = re.search(pattern, magazine_name)
            if match:
                return match.group(1).strip(), match.group(2).strip()

        # パターンにマッチしない場合は全体を基本名として扱う
        return magazine_name, ""

    def _extract_value(self, value: Any) -> str:
        """JSON-LDの値を文字列に変換"""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return value.get("@value", "")
        elif isinstance(value, list) and value:
            return self._extract_value(value[0])
        return ""

    def create_magazine_relationships(self):
        """雑誌ベースの関係性を作成（改善版）"""
        with self.driver.session() as session:
            logger.info("Creating magazine relationships with improved matching logic")

            # まず、掲載履歴の総数を確認
            result = session.run("MATCH (p:Publication) RETURN count(p) as count")
            total_publications = result.single()["count"]
            logger.info(f"Total publications to process: {total_publications}")

            # Step 1: 日付による精密なマッチング（バッチ処理）
            logger.info("Step 1: Linking publications to magazine issues by exact date match")
            processed = 0
            batch_size = 1000

            while processed < total_publications:
                result = session.run(
                    """
                    MATCH (p:Publication)
                    WHERE p.publication_date IS NOT NULL
                    WITH p SKIP $skip LIMIT $batch_size
                    MATCH (mi:MagazineIssue)
                    WHERE mi.publication_date IS NOT NULL
                    AND substring(p.publication_date, 0, 10) = substring(mi.publication_date, 0, 10)
                    AND (
                        (p.title CONTAINS 'ONE PIECE' AND (mi.name CONTAINS 'ジャンプ' OR mi.name CONTAINS 'Jump'))
                        OR
                        (p.title CONTAINS 'DRAGON BALL' AND (mi.name CONTAINS 'ジャンプ' OR mi.name CONTAINS 'Jump'))
                        OR
                        substring(p.publication_date, 0, 7) = substring(mi.publication_date, 0, 7)
                    )
                    WITH p, mi
                    MERGE (p)-[:PUBLISHED_IN]->(mi)
                    RETURN count(*) as created
                """,
                    skip=processed,
                    batch_size=batch_size,
                )

                created = result.single()["created"]
                processed += batch_size
                logger.info(
                    f"  Processed {min(processed, total_publications)}/{total_publications} publications, created {created} relationships"
                )

            # Step 2: タイトルと雑誌名の一致による関連付け
            logger.info("Step 2: Linking by title and magazine name patterns")
            session.run(
                """
                MATCH (p:Publication)
                WHERE p.title CONTAINS 'ONE PIECE'
                MATCH (mi:MagazineIssue)
                WHERE mi.name CONTAINS '週刊少年ジャンプ'
                AND substring(p.publication_date, 0, 7) = substring(mi.publication_date, 0, 7)
                MERGE (p)-[:PUBLISHED_IN]->(mi)
            """
            )

            # Step 3: 雑誌シリーズとの関連付け
            logger.info("Step 3: Linking publications to magazines via magazine issues")
            session.run(
                """
                MATCH (p:Publication)-[:PUBLISHED_IN]->(mi:MagazineIssue)-[:ISSUE_OF]->(m:Magazine)
                WITH p, m
                MERGE (p)-[:PUBLISHED_IN_MAGAZINE]->(m)
            """
            )

            # Step 4: 同じ雑誌号の作品間のリレーション
            logger.info("Step 4: Creating relationships between publications in same magazine issue")
            session.run(
                """
                MATCH (p1:Publication)-[:PUBLISHED_IN]->(mi:MagazineIssue)<-[:PUBLISHED_IN]-(p2:Publication)
                WHERE id(p1) < id(p2)
                WITH p1, p2
                MERGE (p1)-[:SAME_MAGAZINE_ISSUE]->(p2)
            """
            )

            logger.info("Created magazine-based relationships")

    def create_additional_relationships(self):
        """追加の関係性を作成"""
        with self.driver.session() as session:
            logger.info("Creating additional relationships with memory optimization")

            # 雑誌ベースのリレーション数を確認
            magazine_relation_count = session.run(
                """
                MATCH ()-[r:SAME_MAGAZINE_PERIOD]->()
                RETURN count(r) as count
            """
            ).single()["count"]

            logger.info(f"Found {magazine_relation_count} magazine-based relationships")

            # 閾値を設定（例：100個未満の場合は出版社ベースも使用）
            min_magazine_relations = 100

            if magazine_relation_count < min_magazine_relations:
                logger.info("Magazine-based relationships are insufficient, creating publisher-based relationships")

                # 同じ出版社の作品間にリレーションを作成（バッチ処理）
                logger.info("Creating same publisher relationships")
                session.run(
                    """
                    MATCH (w1:Work)<-[:PUBLISHED]-(p:Publisher)-[:PUBLISHED]->(w2:Work)
                    WHERE id(w1) < id(w2)
                    WITH w1, w2 LIMIT 1000
                    MERGE (w1)-[:SAME_PUBLISHER]->(w2)
                """
                )

                # 掲載履歴と単行本の関連付け（タイトルベース）
                logger.info("Creating publication-work relationships")
                session.run(
                    """
                    MATCH (p:Publication), (w:Work)
                    WHERE p.title = w.title
                       OR w.title CONTAINS p.title
                       OR p.title CONTAINS w.title
                    WITH p, w LIMIT 1000
                    MERGE (p)-[:RELATED_TO]->(w)
                """
                )

                # 掲載履歴経由で単行本間のリレーションを作成
                logger.info("Creating work relationships via publications")
                session.run(
                    """
                    MATCH (w1:Work)<-[:RELATED_TO]-(p1:Publication)-[:SAME_MAGAZINE_PERIOD]->(p2:Publication)-[:RELATED_TO]->(w2:Work)
                    WHERE id(w1) < id(w2)
                    WITH w1, w2 LIMIT 1000
                    MERGE (w1)-[:SAME_MAGAZINE_PERIOD]->(w2)
                """
                )

            # 同じ作者の作品間にリレーションを作成（バッチ処理）
            logger.info("Creating same author relationships")
            session.run(
                """
                MATCH (w1:Work)<-[:CREATED]-(a:Author)-[:CREATED]->(w2:Work)
                WHERE id(w1) < id(w2)
                WITH w1, w2 LIMIT 1000
                MERGE (w1)-[:SAME_AUTHOR]->(w2)
            """
            )

            logger.info("Created additional relationships")

    def get_statistics(self) -> Dict[str, int]:
        """データベースの統計情報を取得"""
        with self.driver.session() as session:
            stats = {}

            # ノード数をカウント
            for label in ["Work", "Author", "Publisher", "Magazine", "MagazineIssue", "Series", "Publication"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                stats[label] = result.single()["count"]

            # リレーション数をカウント
            for rel_type in [
                "CREATED",
                "PUBLISHED",
                "CONTAINS",
                "SAME_PUBLISHER",
                "SAME_AUTHOR",
                "PUBLISHED_IN",
                "PUBLISHED_IN_MAGAZINE",
                "SAME_MAGAZINE_ISSUE",
                "SAME_MAGAZINE_PERIOD",
                "CREATED_PUBLICATION",
                "RELATED_TO",
                "ISSUE_OF",
            ]:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                stats[f"rel_{rel_type}"] = result.single()["count"]

            return stats


def main():
    """メイン処理"""
    # Neo4j接続情報
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)

    try:
        # データベースをクリア（開発時のみ）
        if input("Clear database? (y/N): ").lower() == "y":
            importer.clear_database()

        # 制約を作成
        importer.create_constraints()

        # マンガ単行本をインポート
        book_file = DATA_DIR / "metadata101.json"
        if book_file.exists():
            importer.import_manga_books(book_file)

        # マンガシリーズをインポート
        series_file = DATA_DIR / "metadata104.json"
        if series_file.exists():
            importer.import_manga_series(series_file)

        # マンガ雑誌シリーズをインポート (metadata105)
        magazine_file = DATA_DIR / "metadata105.json"
        if magazine_file.exists():
            importer.import_manga_magazines(magazine_file)

        # マンガ雑誌各号をインポート (metadata102)
        magazine_issue_file = DATA_DIR / "metadata102.json"
        if magazine_issue_file.exists():
            importer.import_magazine_issues(magazine_issue_file)

        # マンガ雑誌掲載履歴をインポート (metadata106)
        publication_file = DATA_DIR / "metadata106.json"
        if publication_file.exists():
            importer.import_magazine_publications(publication_file)

        # 雑誌ベースの関係性を作成
        importer.create_magazine_relationships()

        # 追加の関係性を作成
        importer.create_additional_relationships()

        # 統計情報を表示
        stats = importer.get_statistics()
        logger.info("\n=== Import Statistics ===")
        for key, value in stats.items():
            logger.info(f"{key}: {value:,}")

    finally:
        importer.close()


if __name__ == "__main__":
    main()
