#!/usr/bin/env python3
"""
メディア芸術データベースからNeo4jへデータをインポート
"""
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Series) REQUIRE s.id IS UNIQUE",
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
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        items = data.get('@graph', [])
        logger.info(f"Found {len(items)} manga book items")
        
        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing manga books"):
            batch = items[i:i + batch_size]
            self._import_book_batch(batch)
    
    def _import_book_batch(self, items: List[Dict]):
        """マンガ単行本のバッチをインポート"""
        with self.driver.session() as session:
            for item in items:
                try:
                    # 作品ノードを作成
                    work_id = item.get('@id', '')
                    title = self._extract_value(item.get('schema:name', ''))
                    
                    if not title:
                        continue
                    
                    work_props = {
                        'id': work_id,
                        'title': title,
                        'published_date': item.get('schema:datePublished', ''),
                        'publisher': item.get('schema:publisher', ''),
                        'isbn': item.get('schema:isbn', ''),
                        'volume': item.get('schema:volumeNumber', ''),
                        'genre': item.get('schema:genre', ''),
                        'pages': item.get('schema:numberOfPages', ''),
                        'price': item.get('schema:price', ''),
                        'source': 'media_arts_db'
                    }
                    
                    # 作品ノードを作成
                    session.run(
                        "MERGE (w:Work {id: $id}) SET w += $props",
                        id=work_id, props=work_props
                    )
                    
                    # 作者を処理
                    creators = item.get('schema:creator', [])
                    if isinstance(creators, str):
                        creators = [creators]
                    elif not isinstance(creators, list):
                        creators = []
                    
                    for creator in creators:
                        creator_name = self._extract_value(creator)
                        if creator_name and isinstance(creator_name, str):
                            # 作者ノードを作成
                            author_id = f"author_{abs(hash(creator_name))}"
                            session.run(
                                """
                                MERGE (a:Author {id: $id})
                                SET a.name = $name, a.source = 'media_arts_db'
                                WITH a
                                MATCH (w:Work {id: $work_id})
                                MERGE (a)-[:CREATED]->(w)
                                """,
                                id=author_id, name=creator_name, work_id=work_id
                            )
                    
                    # 出版社を処理
                    publisher = item.get('schema:publisher', '')
                    publisher_name = self._extract_value(publisher)
                    if publisher_name and isinstance(publisher_name, str):
                        publisher_id = f"publisher_{abs(hash(publisher_name))}"
                        session.run(
                            """
                            MERGE (p:Publisher {id: $id})
                            SET p.name = $name, p.source = 'media_arts_db'
                            WITH p
                            MATCH (w:Work {id: $work_id})
                            MERGE (p)-[:PUBLISHED]->(w)
                            """,
                            id=publisher_id, name=publisher_name, work_id=work_id
                        )
                    
                except Exception as e:
                    logger.error(f"Error importing item {item.get('@id', 'unknown')}: {e}")
    
    def import_manga_series(self, filepath: Path, batch_size: int = 1000):
        """マンガシリーズデータをインポート"""
        logger.info(f"Importing manga series from {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        items = data.get('@graph', [])
        logger.info(f"Found {len(items)} manga series items")
        
        # バッチ処理でインポート
        for i in tqdm(range(0, len(items), batch_size), desc="Importing manga series"):
            batch = items[i:i + batch_size]
            self._import_series_batch(batch)
    
    def _import_series_batch(self, items: List[Dict]):
        """マンガシリーズのバッチをインポート"""
        with self.driver.session() as session:
            for item in items:
                try:
                    series_id = item.get('@id', '')
                    series_name = self._extract_value(item.get('schema:name', ''))
                    
                    if not series_name:
                        continue
                    
                    series_props = {
                        'id': series_id,
                        'name': series_name,
                        'volume_count': item.get('schema:numberOfItems', 0),
                        'start_date': item.get('schema:datePublished', ''),
                        'publisher': item.get('schema:publisher', ''),
                        'source': 'media_arts_db'
                    }
                    
                    # シリーズノードを作成
                    session.run(
                        "MERGE (s:Series {id: $id}) SET s += $props",
                        id=series_id, props=series_props
                    )
                    
                    # シリーズと作品の関連を作成（タイトルマッチング）
                    if series_name:
                        # シリーズ名から数字を除去してベースタイトルを取得
                        base_title = series_name.strip()
                        session.run(
                            """
                            MATCH (s:Series {id: $series_id})
                            MATCH (w:Work)
                            WHERE w.title CONTAINS $base_title
                            MERGE (s)-[:CONTAINS]->(w)
                            """,
                            series_id=series_id, base_title=base_title
                        )
                    
                except Exception as e:
                    logger.error(f"Error importing series {item.get('@id', 'unknown')}: {e}")
    
    def _extract_value(self, value: Any) -> str:
        """JSON-LDの値を文字列に変換"""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return value.get('@value', '')
        elif isinstance(value, list) and value:
            return self._extract_value(value[0])
        return ''
    
    def create_additional_relationships(self):
        """追加の関係性を作成"""
        with self.driver.session() as session:
            # 同じ出版社の作品間にリレーションを作成
            session.run("""
                MATCH (w1:Work)<-[:PUBLISHED]-(p:Publisher)-[:PUBLISHED]->(w2:Work)
                WHERE id(w1) < id(w2)
                MERGE (w1)-[:SAME_PUBLISHER]->(w2)
            """)
            
            # 同じ作者の作品間にリレーションを作成
            session.run("""
                MATCH (w1:Work)<-[:CREATED]-(a:Author)-[:CREATED]->(w2:Work)
                WHERE id(w1) < id(w2)
                MERGE (w1)-[:SAME_AUTHOR]->(w2)
            """)
            
            logger.info("Created additional relationships")
    
    def get_statistics(self) -> Dict[str, int]:
        """データベースの統計情報を取得"""
        with self.driver.session() as session:
            stats = {}
            
            # ノード数をカウント
            for label in ['Work', 'Author', 'Publisher', 'Magazine', 'Series']:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                stats[label] = result.single()['count']
            
            # リレーション数をカウント
            for rel_type in ['CREATED', 'PUBLISHED', 'CONTAINS', 'SAME_PUBLISHER', 'SAME_AUTHOR']:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
                stats[f'rel_{rel_type}'] = result.single()['count']
            
            return stats


def main():
    """メイン処理"""
    # Neo4j接続情報
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')
    
    importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        # データベースをクリア（開発時のみ）
        if input("Clear database? (y/N): ").lower() == 'y':
            importer.clear_database()
        
        # 制約を作成
        importer.create_constraints()
        
        # マンガ単行本をインポート
        book_file = DATA_DIR / "metadata101_json" / "metadata101.json"
        if book_file.exists():
            importer.import_manga_books(book_file)
        
        # マンガシリーズをインポート
        series_file = DATA_DIR / "metadata104_json" / "metadata104.json"
        if series_file.exists():
            importer.import_manga_series(series_file)
        
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