#!/usr/bin/env python3
"""
漫画作品と雑誌の関係性を分析するスクリプト
直接的な関係が欠けている作品を特定する
"""
import logging
import os
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()


class MangaMagazineRelationAnalyzer:
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
    
    def analyze_all_works_without_direct_magazine_relations(self):
        """雑誌との直接的な関係がない作品を分析"""
        with self.driver.session() as session:
            # 作品が雑誌と直接関係を持っていないケースを検出
            result = session.run("""
                MATCH (w:Work)
                WHERE NOT EXISTS((w)-[:ORIGINALLY_PUBLISHED_IN]->(:Magazine))
                OPTIONAL MATCH (w)-[:RELATED_TO]-(p:Publication)-[:PUBLISHED_IN_MAGAZINE]-(m:Magazine)
                WITH w, COLLECT(DISTINCT m.name) as magazines
                WHERE SIZE(magazines) > 0
                RETURN w.name as work_name, magazines
                ORDER BY w.name
                LIMIT 100
            """)
            
            works_without_direct_relation = []
            for record in result:
                works_without_direct_relation.append({
                    'work': record['work_name'],
                    'indirect_magazines': record['magazines']
                })
            
            logger.info(f"\n直接的な雑誌関係がない作品数: {len(works_without_direct_relation)}")
            return works_without_direct_relation
    
    def analyze_series_without_magazine_relations(self):
        """雑誌との関係がないシリーズを分析"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Series)
                WHERE NOT EXISTS((s)-[:SERIALIZED_IN]->(:Magazine))
                OPTIONAL MATCH (s)<-[:PART_OF_SERIES]-(w:Work)-[:RELATED_TO]-(p:Publication)-[:PUBLISHED_IN_MAGAZINE]-(m:Magazine)
                WITH s, COLLECT(DISTINCT m.name) as magazines
                WHERE SIZE(magazines) > 0
                RETURN s.name as series_name, magazines
                ORDER BY s.name
                LIMIT 50
            """)
            
            series_without_relation = []
            for record in result:
                series_without_relation.append({
                    'series': record['series_name'],
                    'indirect_magazines': record['magazines']
                })
            
            logger.info(f"\n直接的な雑誌関係がないシリーズ数: {len(series_without_relation)}")
            return series_without_relation
    
    def analyze_magazine_distribution(self):
        """雑誌ごとの作品分布を分析"""
        with self.driver.session() as session:
            # 直接関係を持つ作品数
            direct_result = session.run("""
                MATCH (m:Magazine)<-[:ORIGINALLY_PUBLISHED_IN]-(w:Work)
                RETURN m.name as magazine, COUNT(DISTINCT w) as direct_count
                ORDER BY direct_count DESC
            """)
            
            direct_counts = {}
            for record in direct_result:
                direct_counts[record['magazine']] = record['direct_count']
            
            # 間接関係を持つ作品数
            indirect_result = session.run("""
                MATCH (m:Magazine)<-[:PUBLISHED_IN_MAGAZINE]-(p:Publication)-[:RELATED_TO]-(w:Work)
                WHERE NOT EXISTS((w)-[:ORIGINALLY_PUBLISHED_IN]->(m))
                RETURN m.name as magazine, COUNT(DISTINCT w) as indirect_count
                ORDER BY indirect_count DESC
            """)
            
            indirect_counts = {}
            for record in indirect_result:
                indirect_counts[record['magazine']] = record['indirect_count']
            
            return direct_counts, indirect_counts
    
    def identify_popular_series_without_direct_relations(self):
        """人気シリーズで直接関係がないものを特定"""
        with self.driver.session() as session:
            # 複数の巻がある人気シリーズを検出
            result = session.run("""
                MATCH (s:Series)<-[:PART_OF_SERIES]-(w:Work)
                WITH s, COUNT(w) as work_count
                WHERE work_count >= 5
                AND NOT EXISTS((s)-[:SERIALIZED_IN]->(:Magazine))
                OPTIONAL MATCH (s)<-[:PART_OF_SERIES]-(w2:Work)-[:RELATED_TO]-(p:Publication)-[:PUBLISHED_IN_MAGAZINE]-(m:Magazine)
                WITH s, work_count, COLLECT(DISTINCT m.name) as magazines
                WHERE SIZE(magazines) > 0
                RETURN s.name as series_name, work_count, magazines[0] as main_magazine
                ORDER BY work_count DESC
                LIMIT 30
            """)
            
            popular_series = []
            for record in result:
                popular_series.append({
                    'series': record['series_name'],
                    'volume_count': record['work_count'],
                    'magazine': record['main_magazine']
                })
            
            return popular_series
    
    def generate_comprehensive_mapping(self):
        """包括的なシリーズと雑誌のマッピングを生成"""
        with self.driver.session() as session:
            # Publicationを経由して実際の関係を検出
            result = session.run("""
                MATCH (s:Series)<-[:PART_OF_SERIES]-(w:Work)-[:RELATED_TO]-(p:Publication)-[:PUBLISHED_IN_MAGAZINE]-(m:Magazine)
                WITH s.name as series_name, m.name as magazine_name, COUNT(DISTINCT w) as relation_count
                WHERE relation_count >= 3
                RETURN series_name, magazine_name, relation_count
                ORDER BY relation_count DESC
            """)
            
            mappings = []
            for record in result:
                mappings.append({
                    'series': record['series_name'],
                    'magazine': record['magazine_name'],
                    'count': record['relation_count']
                })
            
            return mappings
    
    def run_analysis(self):
        """分析を実行"""
        try:
            logger.info("=" * 80)
            logger.info("漫画と雑誌の関係性分析を開始します...")
            logger.info("=" * 80)
            
            # 1. 直接関係がない作品を分析
            logger.info("\n【1. 直接的な雑誌関係がない作品の分析】")
            works_without_direct = self.analyze_all_works_without_direct_magazine_relations()
            if works_without_direct:
                logger.info("サンプル（最初の10件）:")
                for i, work_info in enumerate(works_without_direct[:10]):
                    logger.info(f"  {i+1}. {work_info['work']} → {', '.join(work_info['indirect_magazines'])}")
            
            # 2. 直接関係がないシリーズを分析
            logger.info("\n【2. 直接的な雑誌関係がないシリーズの分析】")
            series_without_direct = self.analyze_series_without_magazine_relations()
            if series_without_direct:
                logger.info("サンプル（最初の10件）:")
                for i, series_info in enumerate(series_without_direct[:10]):
                    logger.info(f"  {i+1}. {series_info['series']} → {', '.join(series_info['indirect_magazines'])}")
            
            # 3. 雑誌ごとの分布を分析
            logger.info("\n【3. 雑誌ごとの作品分布】")
            direct_counts, indirect_counts = self.analyze_magazine_distribution()
            logger.info("\n直接関係を持つ作品数:")
            for magazine, count in list(direct_counts.items())[:10]:
                indirect = indirect_counts.get(magazine, 0)
                logger.info(f"  {magazine}: 直接={count}, 間接のみ={indirect}")
            
            # 4. 人気シリーズで直接関係がないもの
            logger.info("\n【4. 人気シリーズ（5巻以上）で直接関係がないもの】")
            popular_series = self.identify_popular_series_without_direct_relations()
            for i, series_info in enumerate(popular_series[:20]):
                logger.info(f"  {i+1}. {series_info['series']} ({series_info['volume_count']}巻) → {series_info['magazine']}")
            
            # 5. 包括的なマッピングを生成
            logger.info("\n【5. 実際のシリーズと雑誌の関係（3巻以上）】")
            mappings = self.generate_comprehensive_mapping()
            logger.info(f"検出されたマッピング数: {len(mappings)}")
            logger.info("\nトップ30のマッピング:")
            for i, mapping in enumerate(mappings[:30]):
                logger.info(f"  {i+1}. {mapping['series']} → {mapping['magazine']} ({mapping['count']}巻)")
            
            # マッピングをファイルに保存
            import json
            output_file = "detected_manga_magazine_mappings.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(mappings, f, ensure_ascii=False, indent=2)
            logger.info(f"\nマッピングを {output_file} に保存しました")
            
            logger.info("\n" + "=" * 80)
            logger.info("分析が完了しました！")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"エラーが発生しました: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    analyzer = MangaMagazineRelationAnalyzer()
    analyzer.run_analysis()