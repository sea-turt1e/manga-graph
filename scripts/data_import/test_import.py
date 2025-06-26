#!/usr/bin/env python3
"""
テストデータでのNeo4jインポート
"""
import os
import sys
from pathlib import Path

# インポートスクリプトを使用
sys.path.append(str(Path(__file__).parent))
from import_to_neo4j import Neo4jImporter

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts" / "test"

def main():
    """テストインポート"""
    # Neo4j接続情報
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')
    
    importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)
    
    try:
        print("Clearing database...")
        importer.clear_database()
        
        # 制約を作成
        print("Creating constraints...")
        importer.create_constraints()
        
        # テスト用マンガ単行本をインポート
        book_file = DATA_DIR / "test_metadata101.json"
        if book_file.exists():
            print(f"Importing manga books from {book_file}...")
            importer.import_manga_books(book_file)
        
        # テスト用マンガシリーズをインポート
        series_file = DATA_DIR / "test_metadata104.json"
        if series_file.exists():
            print(f"Importing manga series from {series_file}...")
            importer.import_manga_series(series_file)
        
        # 追加の関係性を作成
        print("Creating additional relationships...")
        importer.create_additional_relationships()
        
        # 統計情報を表示
        stats = importer.get_statistics()
        print("\n=== Test Import Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value:,}")
        
    finally:
        importer.close()

if __name__ == "__main__":
    main()