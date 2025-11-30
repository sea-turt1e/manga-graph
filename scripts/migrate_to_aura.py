"""Migrate Neo4j data from local to Aura.

Usage:
    # 環境変数を設定
    export AURA_URI="neo4j+s://xxxxx.databases.neo4j.io"
    export AURA_PASSWORD="your_password"
    
    # 実行
    uv run python scripts/migrate_to_aura.py
    
    # 途中から再開する場合（例: ノード76800から）
    uv run python scripts/migrate_to_aura.py --resume-from 76800
"""

import argparse
import os
import uuid

from neo4j import GraphDatabase
from tqdm import tqdm

# ローカル接続設定
LOCAL_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
LOCAL_USER = os.getenv("NEO4J_USER", "neo4j")
LOCAL_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Aura接続設定
AURA_URI = os.getenv("AURA_URI")  # neo4j+s://xxxxx.databases.neo4j.io
AURA_USER = os.getenv("AURA_USER", "neo4j")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")

BATCH_SIZE = 100  # Auraの負荷軽減のため小さめに


def export_nodes(local_session):
    """Export all nodes from local database using elementId for unique identification."""
    result = local_session.run("""
        MATCH (n)
        RETURN elementId(n) AS element_id,
               labels(n) AS labels, 
               properties(n) AS props
        ORDER BY elementId(n)
    """)
    return [dict(record) for record in result]


def export_relationships(local_session):
    """Export all relationships from local database using elementId."""
    result = local_session.run("""
        MATCH (a)-[r]->(b)
        RETURN elementId(a) AS a_element_id,
               elementId(b) AS b_element_id,
               type(r) AS rel_type,
               properties(r) AS rel_props
    """)
    return [dict(record) for record in result]


def import_nodes(aura_session, nodes, element_id_map, resume_from=0):
    """Import nodes to Aura in batches.
    
    Uses a unique _migration_id to track nodes during migration,
    handling cases where nodes don't have an 'id' property.
    The _migration_id stores the original local elementId for mapping.
    """
    total_batches = (len(nodes) + BATCH_SIZE - 1) // BATCH_SIZE
    start_batch = resume_from // BATCH_SIZE
    
    for i in tqdm(range(start_batch * BATCH_SIZE, len(nodes), BATCH_SIZE), 
                  desc="Importing nodes", 
                  initial=start_batch,
                  total=total_batches):
        batch = nodes[i:i + BATCH_SIZE]
        for node in batch:
            labels = ":".join(node["labels"]) if node["labels"] else "Node"
            props = node["props"].copy()
            
            # embeddingプロパティを除外（サイズが大きいため）
            props = {k: v for k, v in props.items() 
                     if not k.startswith("embedding_")}
            
            # _migration_idにローカルのelementIdを保存（後でマッピング復元に使用）
            props["_migration_id"] = node["element_id"]
            
            # CREATEを使用（MERGEではなく）
            result = aura_session.run(f"""
                CREATE (n:{labels})
                SET n = $props
                RETURN elementId(n) AS new_element_id
            """, props=props)
            
            record = result.single()
            if record:
                element_id_map[node["element_id"]] = record["new_element_id"]


def import_relationships(aura_session, relationships, element_id_map):
    """Import relationships to Aura in batches."""
    failed_count = 0
    
    for i in tqdm(range(0, len(relationships), BATCH_SIZE), desc="Importing relationships"):
        batch = relationships[i:i + BATCH_SIZE]
        for rel in batch:
            a_new_id = element_id_map.get(rel["a_element_id"])
            b_new_id = element_id_map.get(rel["b_element_id"])
            
            if not a_new_id or not b_new_id:
                failed_count += 1
                continue
            
            rel_type = rel["rel_type"]
            rel_props = rel["rel_props"] or {}
            
            aura_session.run(f"""
                MATCH (a) WHERE elementId(a) = $a_id
                MATCH (b) WHERE elementId(b) = $b_id
                CREATE (a)-[r:{rel_type}]->(b)
                SET r = $rel_props
            """, a_id=a_new_id, b_id=b_new_id, rel_props=rel_props)
    
    if failed_count > 0:
        print(f"\n  Warning: {failed_count} relationships skipped (missing nodes)")


def cleanup_migration_ids(aura_session):
    """Remove temporary _migration_id properties."""
    print("\nCleaning up migration IDs...")
    aura_session.run("""
        MATCH (n)
        WHERE n._migration_id IS NOT NULL
        REMOVE n._migration_id
    """)


def restore_element_id_map(aura_session):
    """Restore element_id_map from existing Aura nodes with _migration_id.
    
    This allows resuming migration without losing the mapping.
    """
    print("Restoring element ID mapping from existing Aura nodes...")
    result = aura_session.run("""
        MATCH (n)
        WHERE n._migration_id IS NOT NULL
        RETURN n._migration_id AS local_element_id, elementId(n) AS aura_element_id
    """)
    
    element_id_map = {}
    for record in result:
        element_id_map[record["local_element_id"]] = record["aura_element_id"]
    
    print(f"  Restored {len(element_id_map)} node mappings")
    return element_id_map


def count_aura_nodes(aura_session):
    """Count existing nodes in Aura."""
    result = aura_session.run("MATCH (n) RETURN count(n) AS count")
    return result.single()["count"]


def clear_aura_database(aura_session):
    """Clear all data in Aura database."""
    print("Clearing Aura database...")
    # バッチで削除（大量データ対応）
    while True:
        result = aura_session.run("""
            MATCH (n)
            WITH n LIMIT 10000
            DETACH DELETE n
            RETURN count(*) AS deleted
        """)
        deleted = result.single()["deleted"]
        if deleted == 0:
            break
        print(f"  Deleted {deleted} nodes...")
    print("Aura database cleared.")


def main():
    parser = argparse.ArgumentParser(description="Migrate Neo4j data from local to Aura")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from where it left off (uses existing Aura data)")
    parser.add_argument("--skip-relationships", action="store_true",
                        help="Skip importing relationships (for testing)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear Aura database before migration")
    args = parser.parse_args()

    # 接続確認
    if not all([LOCAL_PASSWORD, AURA_URI, AURA_PASSWORD]):
        print("Error: 環境変数を設定してください")
        print("  NEO4J_PASSWORD (ローカル用)")
        print("  AURA_URI")
        print("  AURA_PASSWORD")
        return

    local_driver = GraphDatabase.driver(LOCAL_URI, auth=(LOCAL_USER, LOCAL_PASSWORD))
    aura_driver = GraphDatabase.driver(AURA_URI, auth=(AURA_USER, AURA_PASSWORD))

    try:
        # Auraをクリア（明示的に指定した場合のみ）
        if args.clear:
            with aura_driver.session() as session:
                clear_aura_database(session)

        # ローカルからエクスポート
        print("Exporting from local Neo4j...")
        with local_driver.session() as session:
            nodes = export_nodes(session)
            relationships = export_relationships(session)
        
        print(f"  Nodes: {len(nodes)}")
        print(f"  Relationships: {len(relationships)}")

        # 既存のマッピングを復元（再開モード）
        element_id_map = {}
        resume_from = 0
        
        if args.resume:
            with aura_driver.session() as session:
                existing_count = count_aura_nodes(session)
                print(f"\nExisting nodes in Aura: {existing_count}")
                
                if existing_count > 0:
                    element_id_map = restore_element_id_map(session)
                    resume_from = len(element_id_map)
                    print(f"  Will resume from node {resume_from}")

        # Auraにインポート
        print("\nImporting to Aura...")
        
        with aura_driver.session() as session:
            # ノードをインポート（再開位置から）
            if resume_from < len(nodes):
                import_nodes(session, nodes, element_id_map, resume_from=resume_from)
            else:
                print("  All nodes already imported, skipping...")
            
            # リレーションシップをインポート
            if not args.skip_relationships:
                import_relationships(session, relationships, element_id_map)
            
            # クリーンアップ
            cleanup_migration_ids(session)

        print("\nMigration completed!")
        
    finally:
        local_driver.close()
        aura_driver.close()


if __name__ == "__main__":
    main()