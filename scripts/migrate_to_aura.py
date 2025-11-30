"""Migrate Neo4j data from local to Aura.

Usage:
    # 環境変数を設定
    export AURA_URI="neo4j+s://xxxxx.databases.neo4j.io"
    export AURA_PASSWORD="your_password"
    
    # 実行
    uv run python scripts/migrate_to_aura.py
    
    # 途中から再開する場合
    uv run python scripts/migrate_to_aura.py --resume
    
    # リレーションシップのみ再インポート
    uv run python scripts/migrate_to_aura.py --relationships-only
    
    # Auraをクリアして最初から
    uv run python scripts/migrate_to_aura.py --clear
"""

import argparse
import os
import uuid

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

load_dotenv()

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
               a.id AS a_id,
               b.id AS b_id,
               a.name AS a_name,
               b.name AS b_name,
               labels(a) AS a_labels,
               labels(b) AS b_labels,
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
    success_count = 0
    
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
            success_count += 1
    
    print(f"\n  Successfully imported: {success_count} relationships")
    if failed_count > 0:
        print(f"  Warning: {failed_count} relationships skipped (missing nodes)")


def import_relationships_by_id(aura_session, relationships):
    """Import relationships to Aura using node id or name property.
    
    This is useful when re-importing relationships after duplicate cleanup,
    where the elementId mapping is no longer valid.
    
    - Work nodes: matched by 'id' property
    - Author/Magazine/Publisher nodes: matched by 'name' property
    """
    failed_count = 0
    success_count = 0
    error_samples = []
    
    for i in tqdm(range(0, len(relationships), BATCH_SIZE), desc="Importing relationships"):
        batch = relationships[i:i + BATCH_SIZE]
        for rel in batch:
            a_id = rel.get("a_id")
            b_id = rel.get("b_id")
            a_name = rel.get("a_name")
            b_name = rel.get("b_name")
            a_labels = rel.get("a_labels", [])
            b_labels = rel.get("b_labels", [])
            
            a_label = a_labels[0] if a_labels else "Node"
            b_label = b_labels[0] if b_labels else "Node"
            
            rel_type = rel["rel_type"]
            rel_props = rel["rel_props"] or {}
            
            # マッチング条件を決定
            # Work は id で、それ以外は name でマッチング
            if a_label == "Work":
                if a_id is None:
                    failed_count += 1
                    continue
                a_match = f"(a:{a_label} {{id: $a_id}})"
                a_params = {"a_id": a_id}
            else:
                if a_name is None:
                    failed_count += 1
                    continue
                a_match = f"(a:{a_label} {{name: $a_name}})"
                a_params = {"a_name": a_name}
            
            if b_label == "Work":
                if b_id is None:
                    failed_count += 1
                    continue
                b_match = f"(b:{b_label} {{id: $b_id}})"
                b_params = {"b_id": b_id}
            else:
                if b_name is None:
                    failed_count += 1
                    continue
                b_match = f"(b:{b_label} {{name: $b_name}})"
                b_params = {"b_name": b_name}
            
            try:
                query = f"""
                    MATCH {a_match}
                    MATCH {b_match}
                    MERGE (a)-[r:{rel_type}]->(b)
                    SET r = $rel_props
                    RETURN count(r) AS created
                """
                params = {**a_params, **b_params, "rel_props": rel_props}
                
                result = aura_session.run(query, **params)
                record = result.single()
                if record and record["created"] > 0:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                if len(error_samples) < 3:
                    error_samples.append(f"{a_label}({a_id or a_name})-[{rel_type}]->{b_label}({b_id or b_name}): {e}")
    
    print(f"\n  Successfully imported: {success_count} relationships")
    if failed_count > 0:
        print(f"  Warning: {failed_count} relationships skipped (missing nodes or errors)")
    if error_samples:
        print("  Error samples:")
        for sample in error_samples:
            print(f"    {sample}")


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


def count_aura_relationships(aura_session):
    """Count existing relationships in Aura."""
    result = aura_session.run("MATCH ()-[r]->() RETURN count(r) AS count")
    return result.single()["count"]


def clear_aura_relationships(aura_session):
    """Clear all relationships in Aura database (keep nodes)."""
    print("Clearing relationships in Aura...")
    while True:
        result = aura_session.run("""
            MATCH ()-[r]->()
            WITH r LIMIT 10000
            DELETE r
            RETURN count(*) AS deleted
        """)
        deleted = result.single()["deleted"]
        if deleted == 0:
            break
        print(f"  Deleted {deleted} relationships...")
    print("All relationships cleared.")


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
    parser.add_argument("--relationships-only", action="store_true",
                        help="Only import relationships (skip nodes). Uses id property for matching.")
    parser.add_argument("--clear", action="store_true",
                        help="Clear Aura database before migration")
    parser.add_argument("--clear-relationships", action="store_true",
                        help="Clear only relationships before re-importing")
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
        
        # リレーションシップのみクリア
        if args.clear_relationships:
            with aura_driver.session() as session:
                clear_aura_relationships(session)

        # リレーションシップのみインポートモード
        if args.relationships_only:
            print("=" * 60)
            print("Relationships-only import mode")
            print("=" * 60)
            
            # Auraの現在の状態を表示
            with aura_driver.session() as session:
                node_count = count_aura_nodes(session)
                rel_count = count_aura_relationships(session)
                print(f"\nCurrent Aura status:")
                print(f"  Nodes: {node_count}")
                print(f"  Relationships: {rel_count}")
            
            # ローカルからリレーションシップをエクスポート
            print("\nExporting relationships from local Neo4j...")
            with local_driver.session() as session:
                relationships = export_relationships(session)
            print(f"  Relationships to import: {len(relationships)}")
            
            # リレーションシップをインポート（idベースで）
            print("\nImporting relationships to Aura (using id property)...")
            with aura_driver.session() as session:
                import_relationships_by_id(session, relationships)
            
            # 最終状態を表示
            with aura_driver.session() as session:
                new_rel_count = count_aura_relationships(session)
                print(f"\nFinal Aura status:")
                print(f"  Nodes: {node_count}")
                print(f"  Relationships: {new_rel_count}")
            
            print("\nRelationship import completed!")
            return

        # 通常のマイグレーション
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