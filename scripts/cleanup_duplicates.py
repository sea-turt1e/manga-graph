"""Cleanup duplicate nodes in Neo4j Aura.

Usage:
    # 環境変数を設定（.envのAURA_URI, AURA_PASSWORDを有効にする）
    
    # 重複を確認（削除しない）
    uv run python scripts/cleanup_duplicates.py --check
    
    # 重複を削除
    uv run python scripts/cleanup_duplicates.py --delete
    
    # 全データをクリア（やり直す場合）
    uv run python scripts/cleanup_duplicates.py --clear-all
"""

import argparse
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

# Aura接続設定（環境変数またはデフォルト値）
AURA_URI = os.getenv("AURA_URI")
AURA_USER = os.getenv("AURA_USER")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")


def check_status(session):
    """Check current database status."""
    print("=" * 60)
    print("Database Status")
    print("=" * 60)
    
    # 総ノード数
    total = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
    print(f"\nTotal nodes: {total}")
    
    # 総リレーションシップ数
    total_rels = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
    print(f"Total relationships: {total_rels}")
    
    # ラベル別のノード数
    result = session.run("""
        MATCH (n)
        UNWIND labels(n) AS label
        RETURN label, count(*) AS count
        ORDER BY count DESC
    """)
    print("\nNodes by label:")
    for record in result:
        print(f"  {record['label']}: {record['count']}")


def check_duplicates(session):
    """Check for duplicate nodes."""
    print("\n" + "=" * 60)
    print("Checking Duplicates")
    print("=" * 60)
    
    # idプロパティでの重複チェック
    result = session.run("""
        MATCH (n)
        WHERE n.id IS NOT NULL
        WITH n.id AS id, labels(n) AS labels, count(*) AS cnt
        WHERE cnt > 1
        RETURN labels[0] AS label, id, cnt
        ORDER BY cnt DESC
        LIMIT 20
    """)
    
    duplicates = list(result)
    if duplicates:
        print("\nDuplicates found by 'id' property:")
        total_dups = 0
        for record in duplicates:
            print(f"  {record['label']} id={record['id']}: {record['cnt']} copies")
            total_dups += record['cnt'] - 1  # 1つは残す
        print(f"\n  Total duplicate nodes to remove: ~{total_dups}+")
    else:
        print("\nNo duplicates found by 'id' property")
    
    # nameプロパティでの重複チェック
    result = session.run("""
        MATCH (n)
        WHERE n.name IS NOT NULL AND n.id IS NULL
        WITH n.name AS name, labels(n) AS labels, count(*) AS cnt
        WHERE cnt > 1
        RETURN labels[0] AS label, name, cnt
        ORDER BY cnt DESC
        LIMIT 20
    """)
    
    duplicates_name = list(result)
    if duplicates_name:
        print("\nDuplicates found by 'name' property (nodes without id):")
        for record in duplicates_name:
            print(f"  {record['label']} name='{record['name'][:50]}...': {record['cnt']} copies")
    
    # _migration_idでの重複チェック
    result = session.run("""
        MATCH (n)
        WHERE n._migration_id IS NOT NULL
        RETURN count(n) AS count
    """)
    migration_count = result.single()["count"]
    print(f"\nNodes with _migration_id: {migration_count}")
    
    return len(duplicates) > 0 or len(duplicates_name) > 0


def delete_duplicates(session):
    """Delete duplicate nodes, keeping the first one. Process in batches."""
    print("\n" + "=" * 60)
    print("Deleting Duplicates")
    print("=" * 60)
    
    total_deleted = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        print(f"\nBatch {batch_num}...")
        
        # idプロパティでの重複を削除（バッチで処理）
        result = session.run("""
            MATCH (n)
            WHERE n.id IS NOT NULL
            WITH n.id AS id, labels(n) AS labels, collect(n) AS nodes
            WHERE size(nodes) > 1
            WITH nodes
            LIMIT 100
            UNWIND tail(nodes) AS duplicate
            DETACH DELETE duplicate
            RETURN count(*) AS deleted
        """)
        deleted = result.single()["deleted"]
        
        if deleted == 0:
            break
            
        total_deleted += deleted
        print(f"  Deleted {deleted} nodes (total: {total_deleted})")
    
    # nameプロパティでの重複を削除（idがないノード、バッチで処理）
    batch_num = 0
    while True:
        batch_num += 1
        result = session.run("""
            MATCH (n)
            WHERE n.name IS NOT NULL AND n.id IS NULL
            WITH n.name AS name, labels(n) AS labels, collect(n) AS nodes
            WHERE size(nodes) > 1
            WITH nodes
            LIMIT 100
            UNWIND tail(nodes) AS duplicate
            DETACH DELETE duplicate
            RETURN count(*) AS deleted
        """)
        deleted = result.single()["deleted"]
        
        if deleted == 0:
            break
            
        total_deleted += deleted
        print(f"  Deleted by name: {deleted} nodes (total: {total_deleted})")
    
    print(f"\nTotal deleted: {total_deleted} nodes")


def cleanup_migration_ids(session):
    """Remove _migration_id properties."""
    print("\nCleaning up _migration_id properties...")
    result = session.run("""
        MATCH (n)
        WHERE n._migration_id IS NOT NULL
        REMOVE n._migration_id
        RETURN count(*) AS cleaned
    """)
    cleaned = result.single()["cleaned"]
    print(f"Cleaned _migration_id from {cleaned} nodes")


def clear_all(session):
    """Clear all data from database."""
    print("\n" + "=" * 60)
    print("Clearing ALL Data")
    print("=" * 60)
    
    confirm = input("Are you sure you want to delete ALL data? (yes/no): ")
    if confirm.lower() != "yes":
        print("Cancelled.")
        return
    
    print("Deleting all nodes and relationships...")
    while True:
        result = session.run("""
            MATCH (n)
            WITH n LIMIT 10000
            DETACH DELETE n
            RETURN count(*) AS deleted
        """)
        deleted = result.single()["deleted"]
        if deleted == 0:
            break
        print(f"  Deleted {deleted} nodes...")
    
    print("All data cleared.")


def main():
    parser = argparse.ArgumentParser(description="Cleanup duplicate nodes in Neo4j Aura")
    parser.add_argument("--check", action="store_true", help="Check for duplicates (no deletion)")
    parser.add_argument("--delete", action="store_true", help="Delete duplicate nodes")
    parser.add_argument("--cleanup-migration", action="store_true", help="Remove _migration_id properties")
    parser.add_argument("--clear-all", action="store_true", help="Clear ALL data from database")
    args = parser.parse_args()
    
    if not any([args.check, args.delete, args.cleanup_migration, args.clear_all]):
        args.check = True  # デフォルトはチェックのみ
    
    print(f"Connecting to: {AURA_URI}")
    driver = GraphDatabase.driver(AURA_URI, auth=(AURA_USER, AURA_PASSWORD))
    
    try:
        with driver.session() as session:
            # 状態確認
            check_status(session)
            
            if args.clear_all:
                clear_all(session)
                return
            
            if args.check or args.delete:
                has_duplicates = check_duplicates(session)
                
                if args.delete and has_duplicates:
                    confirm = input("\nProceed with deletion? (yes/no): ")
                    if confirm.lower() == "yes":
                        delete_duplicates(session)
                        print("\nAfter deletion:")
                        check_status(session)
            
            if args.cleanup_migration:
                cleanup_migration_ids(session)
    
    finally:
        driver.close()


if __name__ == "__main__":
    main()
