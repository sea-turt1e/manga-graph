# Neo4j Vector Search Integration

このドキュメントでは、Neo4jにベクトル検索機能を統合する方法について説明します。

## 概要

Neo4jのベクトル検索機能を使用することで、意味的に類似した漫画作品を検索できます。これにより、タイトルの完全一致だけでなく、内容やテーマが類似した作品を見つけることが可能になります。

## 機能

### 1. ベクトルインデックスの作成

```python
from infrastructure.external.neo4j_repository import Neo4jMangaRepository

repository = Neo4jMangaRepository()

# Workノード用のベクトルインデックスを作成
repository.create_vector_index(
    label="Work",
    property_name="embedding",
    dimension=1536,
    similarity="cosine"
)
```

### 2. 埋め込みベクトルの追加

```python
# 作品にembeddingを追加
work_id = "work_123"
embedding = [0.1, 0.2, 0.3, ...]  # 1536次元のベクトル

success = repository.add_embedding_to_work(work_id, embedding)
```

### 3. ベクトル検索

```python
# ベクトル類似度による検索
query_embedding = [0.1, 0.2, 0.3, ...]  # 検索クエリのembedding

results = repository.search_by_vector(
    embedding=query_embedding,
    label="Work",
    limit=10
)

for work in results:
    print(f"Title: {work['title']}, Score: {work['similarity_score']}")
```

### 4. ハイブリッド検索

```python
# テキスト検索とベクトル検索を組み合わせ
results = repository.search_manga_works_with_vector(
    search_term="進撃の巨人",
    embedding=query_embedding,
    limit=10
)
```

## API エンドポイント

### ベクトルインデックスの作成

```bash
POST /api/v1/neo4j/vector/create-index
```

```json
{
  "label": "Work",
  "property_name": "embedding",
  "dimension": 1536,
  "similarity": "cosine"
}
```

### ベクトル検索

```bash
POST /api/v1/neo4j/vector/search
```

```json
{
  "query": "進撃の巨人",
  "embedding": [0.1, 0.2, 0.3, ...],
  "limit": 10,
  "use_hybrid": true
}
```

### 埋め込みベクトルの追加

```bash
POST /api/v1/neo4j/vector/add-embedding
```

```json
{
  "work_id": "work_123",
  "embedding": [0.1, 0.2, 0.3, ...]
}
```

## 埋め込みベクトルの生成

実際の運用では、OpenAIのAPIやHugging Faceのモデルを使用して埋め込みベクトルを生成します：

### OpenAI APIの例

```python
import openai

def generate_embedding(text: str) -> List[float]:
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-ada-002"
    )
    return response['data'][0]['embedding']

# 使用例
title = "進撃の巨人"
embedding = generate_embedding(title)
```

### Hugging Faceの例

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

def generate_embedding(text: str) -> List[float]:
    embedding = model.encode(text)
    return embedding.tolist()

# 使用例
title = "進撃の巨人"
embedding = generate_embedding(title)
```

## セットアップ

1. Neo4jデータベースが起動していることを確認
2. 必要なPythonパッケージをインストール：

```bash
pip install neo4j openai sentence-transformers
```

3. 環境変数を設定：

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password"
export OPENAI_API_KEY="your_openai_api_key"  # OpenAI使用時
```

## 使用例

詳細な使用例は `examples/vector_search_example.py` を参照してください。

```bash
python examples/vector_search_example.py
```

## パフォーマンス最適化

### インデックスの設定

- `dimension`: 使用する埋め込みモデルの次元数に合わせて設定
- `similarity`: 類似度計算方法（cosine, euclidean, dotProduct）

### 検索の最適化

- `limit`: 結果数を制限してパフォーマンスを向上
- ハイブリッド検索時は、テキスト検索とベクトル検索の結果数を調整

## 注意事項

- ベクトルインデックスの作成は時間がかかる場合があります
- 大量のデータに対する埋め込みベクトルの生成は計算リソースを要します
- 埋め込みベクトルの品質は使用するモデルに依存します

## トラブルシューティング

### よくある問題

1. **インデックスが作成されない**
   - Neo4jのバージョンが4.0以降であることを確認
   - データベースに適切な権限があることを確認

2. **検索結果が返らない**
   - ベクトルの次元数がインデックスと一致することを確認
   - 埋め込みベクトルが正しく保存されていることを確認

3. **パフォーマンスが遅い**
   - インデックスが正しく作成されていることを確認
   - クエリの最適化を検討
