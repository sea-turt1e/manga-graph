#!/usr/bin/env python3
"""
雑誌ノードの正確な名前を確認するスクリプト
"""
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(uri, auth=(username, password))

with driver.session() as session:
    # 週刊少年ジャンプ関連の雑誌を検索
    result = session.run("""
        MATCH (m:Magazine)
        WHERE m.name CONTAINS 'ジャンプ' AND m.name CONTAINS '週刊'
        RETURN m.name as name
        ORDER BY m.name
        LIMIT 20
    """)
    
    logger.info("週刊少年ジャンプ関連の雑誌:")
    for record in result:
        logger.info(f"  - {record['name']}")
    
    # 正確な週刊少年ジャンプを検索
    result = session.run("""
        MATCH (m:Magazine)
        WHERE m.name = '週刊少年ジャンプ'
        RETURN m.name as name, m as node
    """)
    
    logger.info("\n'週刊少年ジャンプ'の正確な名前で検索:")
    count = 0
    for record in result:
        logger.info(f"  Found: {record['name']}")
        count += 1
    if count == 0:
        logger.info("  見つかりませんでした")
    
    # ジャンプで始まる雑誌を検索
    result = session.run("""
        MATCH (m:Magazine)
        WHERE m.name STARTS WITH '週刊少年ジャンプ'
        RETURN m.name as name
        ORDER BY m.name
        LIMIT 10
    """)
    
    logger.info("\n'週刊少年ジャンプ'で始まる雑誌:")
    for record in result:
        logger.info(f"  - {record['name']}")

driver.close()