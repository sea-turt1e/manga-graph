"""Migrate embedding vectors from local Neo4j to Aura.

This script copies embedding properties from local Neo4j to Aura,
matching nodes by their 'id' property.

Usage:
    uv run python scripts/migrate_embeddings_to_aura.py
"""

import os

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

load_dotenv()

# ローカル接続設定
LOCAL_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
LOCAL_USER = os.getenv("NEO4J_USER", "neo4j")
LOCAL_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Aura接続設定
AURA_URI = os.getenv("AURA_URI")
AURA_USER = os.getenv("AURA_USER", "neo4j")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")

BATCH_SIZE = 50  # ベクトルデータは大きいので小さめに


def export_embeddings(local_session):
    """Export nodes with embeddings from local database."""
    result = local_session.run("""
        MATCH (w:Work)
        WHERE w.embedding_title_ja IS NOT NULL
           OR w.embedding_title_en IS NOT NULL
           OR w.embedding_description IS NOT NULL
        RETURN w.id AS work_id,
               w.embedding_title_ja AS embedding_title_ja,
               w.embedding_title_en AS embedding_title_en,
               w.embedding_description AS embedding_description
    """)
    return [dict(record) for record in result]


def import_embeddings(aura_session, embeddings):
    """Import embeddings to Aura, matching by work id."""
    success_count = 0
    failed_count = 0
    
    for i in tqdm(range(0, len(embeddings), BATCH_SIZE), desc="Importing embeddings"):
        batch = embeddings[i:i + BATCH_SIZE]
        
        for record in batch:
            work_id = record["work_id"]
            
            if work_id is None:
                failed_count += 1
                continue
            
            # 各embeddingを個別に更新
            props_to_set = {}
            if record["embedding_title_ja"] is not None:
                props_to_set["embedding_title_ja"] = record["embedding_title_ja"]
            if record["embedding_title_en"] is not None:
                props_to_set["embedding_title_en"] = record["embedding_title_en"]
            if record["embedding_description"] is not None:
                props_to_set["embedding_description"] = record["embedding_description"]
            
            if not props_to_set:
                continue
            
            try:
                result = aura_session.run("""
                    MATCH (w:Work {id: $work_id})
                    SET w += $props
                    RETURN count(w) AS updated
                """, work_id=work_id, props=props_to_set)
                
                record_result = result.single()
                if record_result and record_result["updated"] > 0:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                if failed_count <= 3:
                    print(f"\n  Error for work_id={work_id}: {e}")
    
    return success_count, failed_count


def main():
    print("=" * 60)
    print("Embedding Migration: Local → Aura")
    print("=" * 60)
    
    # 接続確認
    if not all([LOCAL_PASSWORD, AURA_URI, AURA_PASSWORD]):
        print("Error: 環境変数を設定してください")
        return
    
    local_driver = GraphDatabase.driver(LOCAL_URI, auth=(LOCAL_USER, LOCAL_PASSWORD))
    aura_driver = GraphDatabase.driver(AURA_URI, auth=(AURA_USER, AURA_PASSWORD))
    
    try:
        # ローカルからエクスポート
        print("\nExporting embeddings from local Neo4j...")
        with local_driver.session() as session:
            embeddings = export_embeddings(session)
        print(f"  Found {len(embeddings)} works with embeddings")
        
        if len(embeddings) == 0:
            print("No embeddings to migrate.")
            return
        
        # サンプルを表示
        sample = embeddings[0]
        print(f"\nSample embedding dimensions:")
        if sample["embedding_title_ja"]:
            print(f"  embedding_title_ja: {len(sample['embedding_title_ja'])} dims")
        if sample["embedding_title_en"]:
            print(f"  embedding_title_en: {len(sample['embedding_title_en'])} dims")
        if sample["embedding_description"]:
            print(f"  embedding_description: {len(sample['embedding_description'])} dims")
        
        # Auraにインポート
        print("\nImporting embeddings to Aura...")
        with aura_driver.session() as session:
            success, failed = import_embeddings(session, embeddings)
        
        print(f"\n  Successfully updated: {success} works")
        if failed > 0:
            print(f"  Failed: {failed} works")
        
        # 確認
        print("\nVerifying on Aura...")
        with aura_driver.session() as session:
            result = session.run("""
                MATCH (w:Work)
                WHERE w.embedding_title_ja IS NOT NULL
                RETURN count(w) AS count
            """)
            count = result.single()["count"]
            print(f"  Works with embedding_title_ja: {count}")
        
        print("\nEmbedding migration completed!")
        
    finally:
        local_driver.close()
        aura_driver.close()


if __name__ == "__main__":
    main()
