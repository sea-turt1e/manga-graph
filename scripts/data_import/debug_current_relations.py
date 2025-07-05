#!/usr/bin/env python3
"""
現在のグラフ構造をデバッグするスクリプト
ONE PIECEとNARUTOの関係を詳しく調査
"""
import logging
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()


class GraphDebugger:
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
    
    def debug_work_relations(self, work_name: str):
        """特定の作品の全関係を調査"""
        with self.driver.session() as session:
            logger.info(f"\n=== {work_name} の関係を調査 ===")
            
            # 1. Workノードを確認
            result = session.run("""
                MATCH (w:Work)
                WHERE w.name CONTAINS $work_name
                RETURN w.name as name, labels(w) as labels
                LIMIT 5
            """, work_name=work_name)
            
            logger.info(f"\n{work_name}関連のWorkノード:")
            for record in result:
                logger.info(f"  - {record['name']} (ラベル: {record['labels']})")
            
            # 2. 全ての関係を確認
            result = session.run("""
                MATCH (w:Work)-[r]-(n)
                WHERE w.name CONTAINS $work_name
                RETURN DISTINCT type(r) as relation_type, labels(n) as target_labels, count(*) as count
                ORDER BY count DESC
            """, work_name=work_name)
            
            logger.info(f"\n{work_name}の関係タイプ:")
            for record in result:
                logger.info(f"  - {record['relation_type']} → {record['target_labels']} ({record['count']}件)")
            
            # 3. 雑誌との関係を詳しく調査
            result = session.run("""
                MATCH (w:Work)
                WHERE w.name CONTAINS $work_name
                OPTIONAL MATCH (w)-[r1:ORIGINALLY_PUBLISHED_IN]->(m1:Magazine)
                OPTIONAL MATCH (w)-[r2:RELATED_TO]->(p:Publication)-[r3:PUBLISHED_IN_MAGAZINE]->(m2:Magazine)
                RETURN 
                    w.name as work_name,
                    m1.name as direct_magazine,
                    p.name as publication_name,
                    m2.name as indirect_magazine
                LIMIT 5
            """, work_name=work_name)
            
            logger.info(f"\n{work_name}の雑誌関係詳細:")
            for record in result:
                logger.info(f"  作品: {record['work_name']}")
                logger.info(f"    直接雑誌: {record['direct_magazine']}")
                logger.info(f"    掲載履歴: {record['publication_name']}")
                logger.info(f"    間接雑誌: {record['indirect_magazine']}")
                logger.info("")
    
    def debug_series_relations(self, series_name: str):
        """シリーズの関係を調査"""
        with self.driver.session() as session:
            logger.info(f"\n=== {series_name}シリーズ の関係を調査 ===")
            
            # シリーズノードを確認
            result = session.run("""
                MATCH (s:Series)
                WHERE s.name CONTAINS $series_name
                OPTIONAL MATCH (s)-[r:SERIALIZED_IN]->(m:Magazine)
                RETURN s.name as series_name, m.name as magazine_name
            """, series_name=series_name)
            
            logger.info(f"\n{series_name}シリーズの雑誌関係:")
            for record in result:
                logger.info(f"  {record['series_name']} → {record['magazine_name']}")
    
    def check_magazine_nodes(self):
        """雑誌ノードの存在を確認"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (m:Magazine)
                RETURN m.name as name
                ORDER BY m.name
                LIMIT 20
            """)
            
            logger.info("\n存在する雑誌ノード:")
            for record in result:
                logger.info(f"  - {record['name']}")
    
    def analyze_graph_visualization_pattern(self):
        """画像で見えるパターンを再現するクエリ"""
        with self.driver.session() as session:
            # ONE PIECEから雑誌への経路を調査
            logger.info("\n=== 画像のパターンを調査 ===")
            
            result = session.run("""
                MATCH path = (w:Work)-[*1..3]-(m:Magazine)
                WHERE w.name CONTAINS 'ONE PIECE' 
                AND m.name CONTAINS 'ジャンプ'
                RETURN 
                    w.name as start_node,
                    [n in nodes(path) | labels(n)[0] + ':' + coalesce(n.name, n.title, 'unnamed')] as path_nodes,
                    m.name as end_node,
                    length(path) as path_length
                LIMIT 10
            """)
            
            logger.info("\nONE PIECEから週刊少年ジャンプへの経路:")
            for record in result:
                logger.info(f"  経路長: {record['path_length']}")
                logger.info(f"  ノード: {' -> '.join(record['path_nodes'])}")
                logger.info("")
            
            # NARUTOでも同様に
            result = session.run("""
                MATCH path = (w:Work)-[*1..3]-(m:Magazine)
                WHERE w.name CONTAINS 'NARUTO' 
                AND NOT w.name CONTAINS 'BORUTO'
                AND m.name CONTAINS 'ジャンプ'
                RETURN 
                    w.name as start_node,
                    [n in nodes(path) | labels(n)[0] + ':' + coalesce(n.name, n.title, 'unnamed')] as path_nodes,
                    m.name as end_node,
                    length(path) as path_length
                LIMIT 10
            """)
            
            logger.info("\nNARUTOから週刊少年ジャンプへの経路:")
            for record in result:
                logger.info(f"  経路長: {record['path_length']}")
                logger.info(f"  ノード: {' -> '.join(record['path_nodes'])}")
                logger.info("")
    
    def run_debug(self):
        """デバッグを実行"""
        try:
            # 雑誌ノードの確認
            self.check_magazine_nodes()
            
            # ONE PIECEの関係を調査
            self.debug_work_relations("ONE PIECE")
            self.debug_series_relations("ONE PIECE")
            
            # NARUTOの関係を調査
            self.debug_work_relations("NARUTO")
            self.debug_series_relations("NARUTO")
            
            # 画像のパターンを分析
            self.analyze_graph_visualization_pattern()
            
        except Exception as e:
            logger.error(f"エラーが発生しました: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    debugger = GraphDebugger()
    debugger.run_debug()