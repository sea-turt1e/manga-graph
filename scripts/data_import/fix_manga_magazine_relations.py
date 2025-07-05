#!/usr/bin/env python3
"""
漫画作品と雑誌の直接的な関係を修正するスクリプト
特にONE PIECEと週刊少年ジャンプの関係を修正
"""
import logging
import os
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase

# ロギング設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()


class MangaMagazineRelationFixer:
    def __init__(self):
        """Neo4jへの接続を初期化"""
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        username = os.getenv("NEO4J_USERNAME", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")

        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        logger.info(f"Connected to Neo4j at {uri}")

    def close(self):
        """接続を閉じる"""
        self.driver.close()

    def analyze_current_relations(self):
        """現在の関係性を分析"""
        with self.driver.session() as session:
            # ONE PIECEに関連するノードとリレーションを調査
            result = session.run(
                """
                MATCH (w:Work)
                WHERE w.name CONTAINS 'ONE PIECE'
                OPTIONAL MATCH (w)-[r1]-(p:Publication)
                OPTIONAL MATCH (p)-[r2]-(m:Magazine)
                RETURN w.name as work_name, 
                       type(r1) as relation1, 
                       p.name as publication_name,
                       type(r2) as relation2,
                       m.name as magazine_name
                LIMIT 10
            """
            )

            logger.info("現在のONE PIECE関連のリレーション:")
            for record in result:
                logger.info(f"  作品: {record['work_name']}")
                logger.info(f"  関係1: {record['relation1']}")
                logger.info(f"  掲載履歴: {record['publication_name']}")
                logger.info(f"  関係2: {record['relation2']}")
                logger.info(f"  雑誌: {record['magazine_name']}")
                logger.info("-" * 50)

    def get_series_magazine_mappings(self) -> List[Tuple[str, str]]:
        """シリーズと雑誌のマッピングを定義"""
        return [
            ("ONE PIECE", "週刊少年ジャンプ"),
            ("NARUTO", "週刊少年ジャンプ"),
            ("BLEACH", "週刊少年ジャンプ"),
            ("DRAGON BALL", "週刊少年ジャンプ"),
            ("鬼滅の刃", "週刊少年ジャンプ"),
            ("僕のヒーローアカデミア", "週刊少年ジャンプ"),
            ("呪術廻戦", "週刊少年ジャンプ"),
            ("進撃の巨人", "別冊少年マガジン"),
            ("名探偵コナン", "週刊少年サンデー"),
            ("ドラえもん", "コロコロコミック"),
        ]

    def create_direct_series_magazine_relations(self):
        """シリーズと雑誌の直接的な関係を作成"""
        mappings = self.get_series_magazine_mappings()

        with self.driver.session() as session:
            for series_name, magazine_name in mappings:
                # シリーズノードと雑誌ノードを見つけて関係を作成
                # 正確な雑誌名でマッチング
                result = session.run(
                    """
                    MATCH (s:Series)
                    WHERE s.name CONTAINS $series_name
                    MATCH (m:Magazine)
                    WHERE m.name = $magazine_name
                    MERGE (s)-[r:SERIALIZED_IN]->(m)
                    SET r.created_at = datetime()
                    RETURN s.name as series, m.name as magazine, type(r) as relation
                """,
                    series_name=series_name,
                    magazine_name=magazine_name,
                )

                for record in result:
                    logger.info(f"Created relation: {record['series']} -[{record['relation']}]-> {record['magazine']}")

    def create_work_magazine_relations(self):
        """個別の作品（巻）と雑誌の関係を作成"""
        mappings = self.get_series_magazine_mappings()

        with self.driver.session() as session:
            for series_name, magazine_name in mappings:
                # 各作品の巻と雑誌を関連付け
                # 正確な雑誌名でマッチング
                result = session.run(
                    """
                    MATCH (w:Work)
                    WHERE w.name CONTAINS $series_name
                    MATCH (m:Magazine)
                    WHERE m.name = $magazine_name
                    MERGE (w)-[r:ORIGINALLY_PUBLISHED_IN]->(m)
                    SET r.created_at = datetime()
                    RETURN count(r) as count, m.name as magazine
                """,
                    series_name=series_name,
                    magazine_name=magazine_name,
                )

                for record in result:
                    logger.info(f"Created {record['count']} relations for {series_name} -> {record['magazine']}")

    def fix_one_piece_specific_relations(self):
        """ONE PIECE特有の関係を修正"""
        with self.driver.session() as session:
            # ONE PIECEの全巻を週刊少年ジャンプに関連付け
            result = session.run(
                """
                MATCH (w:Work)
                WHERE w.name CONTAINS 'ONE PIECE'
                AND NOT w.name CONTAINS 'FILM'
                AND NOT w.name CONTAINS 'PARTY'
                MATCH (m:Magazine {name: '週刊少年ジャンプ'})
                MERGE (w)-[r:ORIGINALLY_PUBLISHED_IN]->(m)
                SET r.created_at = datetime(),
                    r.note = 'Direct relation from Work to Magazine'
                RETURN count(r) as count
            """
            )

            count = result.single()["count"]
            logger.info(f"Created {count} direct relations from ONE PIECE works to 週刊少年ジャンプ")

            # ONE PIECEシリーズと週刊少年ジャンプの関係
            result = session.run(
                """
                MATCH (s:Series)
                WHERE s.name = 'ONE PIECE'
                MATCH (m:Magazine {name: '週刊少年ジャンプ'})
                MERGE (s)-[r:SERIALIZED_IN]->(m)
                SET r.created_at = datetime(),
                    r.start_year = 1997,
                    r.note = 'ONE PIECE series published in Weekly Shonen Jump since 1997'
                RETURN s.name as series, m.name as magazine
            """
            )

            for record in result:
                logger.info(f"Created series relation: {record['series']} -> {record['magazine']}")

    def remove_incorrect_relations(self):
        """間違った関係を削除"""
        with self.driver.session() as session:
            # 増刊号への関係を削除
            result = session.run("""
                MATCH (s:Series)-[r:SERIALIZED_IN]->(m:Magazine)
                WHERE m.name CONTAINS '増刊'
                DELETE r
                RETURN count(r) as deleted_count
            """)
            
            count = result.single()["deleted_count"]
            logger.info(f"削除した増刊号への関係: {count}件")
    
    def verify_relations(self):
        """作成された関係を確認"""
        with self.driver.session() as session:
            # ONE PIECEの直接的な雑誌関係を確認
            result = session.run(
                """
                MATCH (w:Work)-[r:ORIGINALLY_PUBLISHED_IN]->(m:Magazine)
                WHERE w.name CONTAINS 'ONE PIECE'
                RETURN w.name as work, type(r) as relation, m.name as magazine
                LIMIT 5
            """
            )

            logger.info("\n作成されたONE PIECEの直接的な雑誌関係:")
            for record in result:
                logger.info(f"  {record['work']} -[{record['relation']}]-> {record['magazine']}")

            # シリーズレベルの関係を確認
            result = session.run(
                """
                MATCH (s:Series)-[r:SERIALIZED_IN]->(m:Magazine)
                WHERE s.name = 'ONE PIECE'
                RETURN s.name as series, type(r) as relation, m.name as magazine
            """
            )

            logger.info("\nシリーズレベルの関係:")
            for record in result:
                logger.info(f"  {record['series']} -[{record['relation']}]-> {record['magazine']}")

    def run_fix(self):
        """修正処理を実行"""
        try:
            logger.info("漫画と雑誌の関係修正を開始します...")

            # 既存の間違った関係を削除
            logger.info("\n=== 既存の間違った関係を削除 ===")
            self.remove_incorrect_relations()
            
            # 現在の状態を分析
            logger.info("\n=== 現在の関係性を分析 ===")
            self.analyze_current_relations()

            # シリーズと雑誌の関係を作成
            logger.info("\n=== シリーズと雑誌の関係を作成 ===")
            self.create_direct_series_magazine_relations()

            # 個別作品と雑誌の関係を作成
            logger.info("\n=== 個別作品と雑誌の関係を作成 ===")
            self.create_work_magazine_relations()

            # ONE PIECE特有の修正
            logger.info("\n=== ONE PIECE特有の関係を修正 ===")
            self.fix_one_piece_specific_relations()

            # 結果を確認
            logger.info("\n=== 修正結果を確認 ===")
            self.verify_relations()

            logger.info("\n修正が完了しました！")

        except Exception as e:
            logger.error(f"エラーが発生しました: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    fixer = MangaMagazineRelationFixer()
    fixer.run_fix()
