# Neo4j Vector Search Implementation Summary

## 実装内容

Neo4jのベクトル検索機能を manga-graph プロジェクトに統合しました。

### 追加された機能

#### 1. Neo4jMangaRepository の拡張
- `create_vector_index()`: ベクトルインデックスの作成
- `search_by_vector()`: ベクトル類似度検索
- `add_embedding_to_work()`: 作品にembeddingベクトルを追加
- `search_manga_works_with_vector()`: ハイブリッド検索（テキスト + ベクトル）

#### 2. APIエンドポイントの追加
- `POST /api/v1/neo4j/vector/create-index`: ベクトルインデックス作成
- `POST /api/v1/neo4j/vector/search`: ベクトル検索
- `POST /api/v1/neo4j/vector/add-embedding`: embedding追加

#### 3. スキーマの追加
- `VectorSearchRequest`: ベクトル検索リクエスト
- `VectorIndexRequest`: インデックス作成リクエスト  
- `AddEmbeddingRequest`: embedding追加リクエスト

#### 4. ドキュメントとサンプル
- `docs/vector_search_guide.md`: 詳細な使用ガイド
- `examples/vector_search_example.py`: 実装例
- `tests/unit/test_vector_search.py`: ユニットテスト

## 使用方法

### 1. セットアップ
```bash
# 依存関係のインストール
pip install -r requirements.txt

# 環境変数の設定
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j" 
export NEO4J_PASSWORD="password"
export OPENAI_API_KEY="your_api_key"  # OpenAI使用時
```

### 2. ベクトルインデックスの作成
```python
repository = Neo4jMangaRepository()
repository.create_vector_index("Work", "embedding", 1536, "cosine")
```

### 3. embedingの追加
```python
# OpenAI APIでembeddingを生成
import openai
response = openai.Embedding.create(
    input="進撃の巨人",
    model="text-embedding-ada-002"
)
embedding = response['data'][0]['embedding']

# 作品に追加
repository.add_embedding_to_work("work_id", embedding)
```

### 4. ベクトル検索
```python
# 類似作品を検索
results = repository.search_by_vector(embedding, limit=10)

# ハイブリッド検索
results = repository.search_manga_works_with_vector(
    search_term="進撃の巨人",
    embedding=embedding,
    limit=10
)
```

## API使用例

### ベクトル検索
```bash
curl -X POST "http://localhost:8000/api/v1/neo4j/vector/search" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "進撃の巨人",
       "embedding": [0.1, 0.2, ...],
       "limit": 10,
       "use_hybrid": true
     }'
```

## 特徴

1. **ハイブリッド検索**: テキスト検索とベクトル検索を組み合わせ
2. **スコアリング**: 両方で見つかった作品は高いスコアを付与
3. **エラーハンドリング**: インデックス作成の確認、適切なエラー処理
4. **パフォーマンス**: インデックスの重複作成を回避
5. **柔軟性**: 複数のembeddingモデルに対応

## 今後の拡張可能性

1. **他のノードタイプ**: Author、Magazine等へのベクトル検索対応
2. **バッチ処理**: 大量データのembedding生成・追加
3. **キャッシュ**: embedding生成結果のキャッシュ
4. **フィルタリング**: ジャンルや年代によるフィルタ
5. **レコメンデーション**: ユーザー履歴に基づく推薦機能

## 技術詳細

- **Neo4j Vector Index**: `db.index.vector.createNodeIndex`使用
- **類似度計算**: cosine similarity
- **ベクトル次元**: 1536（OpenAI text-embedding-ada-002）
- **検索方式**: `db.index.vector.queryNodes`
- **統合**: 既存の検索機能とシームレスに統合
