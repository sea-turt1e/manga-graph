"""Add embedding_title_en and embedding_title_ja to Aura nodes that don't have them.

This script finds Work nodes in Aura and:
1. If embedding_title_en is missing but title_name exists: generate embedding_title_en from title_name
2. If embedding_title_ja is missing but japanese_name exists: generate embedding_title_ja from japanese_name
3. If embedding_title_ja is missing and japanese_name is also missing but title_name exists:
   generate embedding_title_ja from title_name (fallback)

Usage:
    uv run python scripts/add_embedding_title_en_to_aura.py
    uv run python scripts/add_embedding_title_en_to_aura.py --dry-run
    uv run python scripts/add_embedding_title_en_to_aura.py --batch-size 100
    uv run python scripts/add_embedding_title_en_to_aura.py --limit 1000  # テスト用
    uv run python scripts/add_embedding_title_en_to_aura.py --target en  # embedding_title_en のみ
    uv run python scripts/add_embedding_title_en_to_aura.py --target ja  # embedding_title_ja のみ
    uv run python scripts/add_embedding_title_en_to_aura.py --target all # 両方（デフォルト）
"""

import argparse
import os
import sys
import time
from typing import Any, Dict, List

from dotenv import load_dotenv
from neo4j import GraphDatabase
from tqdm import tqdm

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from domain.services.jina_embedding_client import JinaEmbeddingLocalClient

load_dotenv()

# Aura接続設定
AURA_URI = os.getenv("AURA_URI")
AURA_USER = os.getenv("AURA_USER", "neo4j")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")

DEFAULT_BATCH_SIZE = 50  # 埋め込み生成のバッチサイズ
WRITE_BATCH_SIZE = 100   # DBへの書き込みバッチサイズ


def get_works_without_embedding_en(session, limit: int = None) -> List[Dict[str, Any]]:
    """Get Work nodes that have title_name but no embedding_title_en."""
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # title_name が文字列かつ空でないものを対象にする
    result = session.run(f"""
        MATCH (w:Work)
        WHERE w.embedding_title_en IS NULL
          AND w.title_name IS NOT NULL
          AND w.title_name <> ''
          AND (w.title_name IS :: STRING)
        RETURN w.id AS work_id, w.title_name AS title_name
        {limit_clause}
    """)
    return [dict(record) for record in result]


def get_works_without_embedding_ja(session, limit: int = None) -> List[Dict[str, Any]]:
    """Get Work nodes that need embedding_title_ja.
    
    Returns works where:
    - embedding_title_ja IS NULL
    - AND (japanese_name exists OR title_name exists as fallback)
    
    The 'source_text' field indicates which property was used.
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # japanese_name があればそれを使い、なければ title_name を使う
    result = session.run(f"""
        MATCH (w:Work)
        WHERE w.embedding_title_ja IS NULL
          AND (
            (w.japanese_name IS NOT NULL AND w.japanese_name <> '' AND (w.japanese_name IS :: STRING))
            OR
            (w.title_name IS NOT NULL AND w.title_name <> '' AND (w.title_name IS :: STRING))
          )
        RETURN w.id AS work_id,
               CASE 
                 WHEN w.japanese_name IS NOT NULL AND w.japanese_name <> '' AND (w.japanese_name IS :: STRING)
                 THEN w.japanese_name
                 ELSE w.title_name
               END AS source_text,
               CASE 
                 WHEN w.japanese_name IS NOT NULL AND w.japanese_name <> '' AND (w.japanese_name IS :: STRING)
                 THEN 'japanese_name'
                 ELSE 'title_name'
               END AS source_field
        {limit_clause}
    """)
    return [dict(record) for record in result]


def generate_embeddings_batch(client: JinaEmbeddingLocalClient, texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts."""
    embeddings = []
    for text in texts:
        try:
            embedding = client.encode(text)
            embeddings.append(embedding)
        except Exception as e:
            print(f"\n  Warning: Failed to encode '{text[:50]}...': {e}")
            embeddings.append(None)
    return embeddings


def write_embeddings_batch(session, updates: List[Dict[str, Any]], target_property: str = "embedding_title_en") -> tuple[int, int]:
    """Write embeddings to Aura in batch.
    
    Args:
        session: Neo4j session
        updates: List of dicts with work_id and embedding
        target_property: Property name to write (embedding_title_en or embedding_title_ja)
    """
    success = 0
    failed = 0
    
    for update in updates:
        if update["embedding"] is None:
            failed += 1
            continue
        
        try:
            # 動的にプロパティ名を設定
            cypher = f"""
                MATCH (w:Work {{id: $work_id}})
                SET w.{target_property} = $embedding
                RETURN count(w) AS updated
            """
            result = session.run(cypher, work_id=update["work_id"], embedding=update["embedding"])
            
            record = result.single()
            if record and record["updated"] > 0:
                success += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            if failed <= 5:
                print(f"\n  Error writing work_id={update['work_id']}: {e}")
    
    return success, failed


def process_embedding_en(aura_driver, client: JinaEmbeddingLocalClient, batch_size: int, limit: int = None, dry_run: bool = False) -> tuple[int, int]:
    """Process embedding_title_en generation."""
    print("\n" + "=" * 60)
    print("Processing embedding_title_en")
    print("=" * 60)
    
    # 対象ノードを取得
    print("\n1. Finding works without embedding_title_en...")
    with aura_driver.session() as session:
        works = get_works_without_embedding_en(session, limit)
    
    print(f"   Found {len(works)} works to process")
    
    if len(works) == 0:
        print("   No works need embedding_title_en.")
        return 0, 0
    
    # サンプル表示
    print(f"\n   Sample titles to encode:")
    for w in works[:5]:
        title = w['title_name'][:60] if w['title_name'] else '(empty)'
        print(f"     - {title}...")
    
    if dry_run:
        print(f"\n[DRY RUN] Would generate and write {len(works)} embeddings for embedding_title_en")
        return len(works), 0
    
    # 埋め込み生成とDB書き込み
    print(f"\n2. Generating embeddings and writing to Aura (batch_size={batch_size})...")
    
    total_success = 0
    total_failed = 0
    
    for i in tqdm(range(0, len(works), batch_size), desc="embedding_title_en"):
        batch = works[i:i + batch_size]
        
        texts = [w["title_name"] for w in batch]
        work_ids = [w["work_id"] for w in batch]
        
        embeddings = generate_embeddings_batch(client, texts)
        
        updates = [
            {"work_id": wid, "embedding": emb}
            for wid, emb in zip(work_ids, embeddings)
        ]
        
        with aura_driver.session() as session:
            success, failed = write_embeddings_batch(session, updates, "embedding_title_en")
            total_success += success
            total_failed += failed
    
    print(f"\n   Results: success={total_success}, failed={total_failed}")
    return total_success, total_failed


def process_embedding_ja(aura_driver, client: JinaEmbeddingLocalClient, batch_size: int, limit: int = None, dry_run: bool = False) -> tuple[int, int]:
    """Process embedding_title_ja generation."""
    print("\n" + "=" * 60)
    print("Processing embedding_title_ja")
    print("=" * 60)
    
    # 対象ノードを取得
    print("\n1. Finding works without embedding_title_ja...")
    with aura_driver.session() as session:
        works = get_works_without_embedding_ja(session, limit)
    
    print(f"   Found {len(works)} works to process")
    
    if len(works) == 0:
        print("   No works need embedding_title_ja.")
        return 0, 0
    
    # ソースフィールドの内訳を表示
    from_japanese = sum(1 for w in works if w.get('source_field') == 'japanese_name')
    from_title = sum(1 for w in works if w.get('source_field') == 'title_name')
    print(f"   Source breakdown: japanese_name={from_japanese}, title_name (fallback)={from_title}")
    
    # サンプル表示
    print(f"\n   Sample texts to encode:")
    for w in works[:5]:
        text = w['source_text'][:60] if w['source_text'] else '(empty)'
        src = w.get('source_field', 'unknown')
        print(f"     - [{src}] {text}...")
    
    if dry_run:
        print(f"\n[DRY RUN] Would generate and write {len(works)} embeddings for embedding_title_ja")
        return len(works), 0
    
    # 埋め込み生成とDB書き込み
    print(f"\n2. Generating embeddings and writing to Aura (batch_size={batch_size})...")
    
    total_success = 0
    total_failed = 0
    
    for i in tqdm(range(0, len(works), batch_size), desc="embedding_title_ja"):
        batch = works[i:i + batch_size]
        
        texts = [w["source_text"] for w in batch]
        work_ids = [w["work_id"] for w in batch]
        
        embeddings = generate_embeddings_batch(client, texts)
        
        updates = [
            {"work_id": wid, "embedding": emb}
            for wid, emb in zip(work_ids, embeddings)
        ]
        
        with aura_driver.session() as session:
            success, failed = write_embeddings_batch(session, updates, "embedding_title_ja")
            total_success += success
            total_failed += failed
    
    print(f"\n   Results: success={total_success}, failed={total_failed}")
    return total_success, total_failed


def main():
    parser = argparse.ArgumentParser(description="Add embedding_title_en and embedding_title_ja to Aura")
    parser.add_argument("--dry-run", action="store_true", help="Dry run - show what would be done")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size for embedding generation")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of works to process (for testing)")
    parser.add_argument("--target", choices=["en", "ja", "all"], default="all", 
                        help="Which embeddings to generate: en, ja, or all (default: all)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Add Embeddings to Aura")
    print(f"Target: {args.target}")
    print("=" * 60)
    
    if not all([AURA_URI, AURA_PASSWORD]):
        print("Error: AURA_URI と AURA_PASSWORD を環境変数に設定してください")
        return 1
    
    # Aura接続
    aura_driver = GraphDatabase.driver(AURA_URI, auth=(AURA_USER, AURA_PASSWORD))
    
    try:
        client = None
        
        # dry-run でなければモデルを初期化
        if not args.dry_run:
            print("\nInitializing embedding model...")
            start_init = time.time()
            client = JinaEmbeddingLocalClient()
            print(f"Model loaded in {time.time() - start_init:.1f}s")
        
        total_en_success, total_en_failed = 0, 0
        total_ja_success, total_ja_failed = 0, 0
        
        # embedding_title_en の処理
        if args.target in ["en", "all"]:
            total_en_success, total_en_failed = process_embedding_en(
                aura_driver, client, args.batch_size, args.limit, args.dry_run
            )
        
        # embedding_title_ja の処理
        if args.target in ["ja", "all"]:
            total_ja_success, total_ja_failed = process_embedding_ja(
                aura_driver, client, args.batch_size, args.limit, args.dry_run
            )
        
        # 最終確認
        if not args.dry_run:
            print("\n" + "=" * 60)
            print("Final Verification")
            print("=" * 60)
            with aura_driver.session() as session:
                result = session.run("""
                    MATCH (w:Work)
                    RETURN 
                        count(w) AS total,
                        count(CASE WHEN w.embedding_title_en IS NOT NULL THEN 1 END) AS has_en,
                        count(CASE WHEN w.embedding_title_ja IS NOT NULL THEN 1 END) AS has_ja
                """)
                record = result.single()
                print(f"   Total Works: {record['total']}")
                print(f"   Works with embedding_title_en: {record['has_en']}")
                print(f"   Works with embedding_title_ja: {record['has_ja']}")
        
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        if args.target in ["en", "all"]:
            print(f"   embedding_title_en: success={total_en_success}, failed={total_en_failed}")
        if args.target in ["ja", "all"]:
            print(f"   embedding_title_ja: success={total_ja_success}, failed={total_ja_failed}")
        
        print("\nDone!")
        return 0
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        aura_driver.close()


if __name__ == "__main__":
    sys.exit(main())
