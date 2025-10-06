# Manga Graph

メディア芸術データベースを活用した漫画の関係性を可視化するAPIサーバーです。  
こちらは主にバックエンド側とデータベースのリポジトリになります。  
フロントエンドのリポジトリは[こちら（manga-graph-frontend）](https://github.com/sea-turt1e/manga-graph-frontend)

## デモ
デモは[Manga Graph Visualizer](https://mangagraph.netlify.app/)として公開されており、インタラクティブなグラフ操作が可能です。

[![Manga_Graph_Visualizer_demo](/images/manga_graph_visualizer.gif)](https://mangagraph.netlify.app/)

## 機能
- 📊 漫画グラフデータの提供（Neo4j）
- 🤖 OpenAI APIを使用したテキスト生成（ストリーミング対応）
<!-- - ToBe
  - 📚 作品検索API
  - 👨‍🎨 作者と作品の関係性データ提供
  - 📖 同じ雑誌に掲載された作品の関係性
  - 🔍 柔軟な検索とフィルタリング -->

## 技術スタック

### バックエンド
- Python 3.12.7
- FastAPI
- Neo4j (グラフデータベース)
- CleanArchitecture設計

### インフラ
- Docker & Docker Compose

## セットアップ

### 1. リポジトリのクローン
```bash
git clone git@github.com:sea-turt1e/manga-graph.git
cd manga-graph
```

### 2. Docker Composeを使用した起動
```bash
# すべてのサービスを起動
docker-compose up -d

# ログを確認
docker-compose logs -f
```

### 3. 環境変数の設定
```bash
# .envファイルをコピーして設定
cp .env.example .env

# .envファイルを作成して以下を設定
```bash
LOCALHOST_URL="http://localhost:3000"
PRODUCTION_URL="your_production_url"
NEO4J_URI="your_neo4j_uri" 
NEO4J_USER="your_neo4j_username"
NEO4J_PASSWORD="your_neo4j_password"
```

### 4. データの準備と投入
```bash
# メディア芸術データベースからデータをダウンロード
python scripts/data_import/download_mediaarts_data.py

# Neo4jにデータを投入
python scripts/data_import/import_to_neo4j.py

# ここからはオプション
## 漫画の巻数データをNeo4jに保存
python scripts/update_total_volumes.py --apply

## 漫画のタイトル名のembeddingを生成してNeo4jに保存
python scripts/add_vector_embeddings.py

## 漫画のあらすじ（英語）とそのembeddingを生成してNeo4jに保存。`manga_csv_path`はMyAnimeListから取得したCSVファイルのパス
python scripts/add_synopsis_from_manga_csv.py `manga_csv_path` --create-index

```

### 4. アプリケーションへのアクセス
- バックエンドAPI: http://localhost:8000
- Neo4j Browser: http://localhost:7474 (neo4j/password)
- API Documentation: http://localhost:8000/docs

## 開発

### 開発環境の起動
```bash
# 依存関係のインストール
## pipの場合
pip install -r requirements.txt
# uvの場合
uv sync --frozen --no-dev --index-strategy unsafe-best-match
```
# 日本語形態素解析の辞書をダウンロード
python -m unidic download

# 開発サーバー起動
## pipの場合
uvicorn main:app --reload --host 0.0.0.0 --port 8000
## uvの場合
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```


## API エンドポイント
### 主要API

- `GET /health` - ヘルスチェック
- `GET /api/v1/neo4j/search` - Neo4jグラフデータの検索
- `GET /api/v1/neo4j/vector/title-similarity` - タイトル類似度検索

詳細なAPI仕様は http://localhost:8000/docs で確認できます。

## プロジェクト構造

```
manga-graph/
├── domain/              # ドメイン層
│   ├── entities/        # エンティティ
│   ├── repositories/    # リポジトリインターフェース
│   ├── services/        # ドメインサービス
│   └── use_cases/       # ユースケース
├── infrastructure/      # インフラ層
│   ├── database/        # データベース実装
│   └── external/        # 外部API実装
├── presentation/        # プレゼンテーション層
│   ├── api/            # APIエンドポイント
│   └── schemas/        # リクエスト/レスポンススキーマ
├── scripts/            # データ投入・管理スクリプト
│   └── data_import/    # データインポート関連
├── tests/              # テスト
│   ├── unit/           # ユニットテスト
│   ├── integration/    # 統合テスト
│   └── e2e/            # E2Eテスト
└── static/             # 静的ファイル
```

## データソース

- [メディア芸術データベース](https://mediaarts-db.artmuseums.go.jp/)
- [メタデータファイル（JSON形式）](https://github.com/mediaarts-db/dataset/releases)のデータを取得

## テスト

```bash
python -m pytest tests/unit/
```

<!-- # 全テストの実行
python -m pytest tests/
# # 統合テストのみ
python -m pytest tests/integration/

# E2Eテストのみ
python -m pytest tests/e2e/ -->

## ライセンス

Apache License 2.0

## 出典
このアプリケーションは、以下のデータセットを使用しています：
- [メディア芸術データベース](https://mediaarts-db.artmuseums.go.jp/)
  - 出典：独立行政法人国立美術館国立アートリサーチセンター「メディア芸術データベース」 （https://mediaarts-db.artmuseums.go.jp/）
  - 独立行政法人国立美術館国立アートリサーチセンター「メディア芸術データベース」（https://mediaarts-db.artmuseums.go.jp/）を加工してデータを作成
- [OpenBD](https://openbd.jp/)
  - 「OpenBD」 （https://openbd.jp/） を利用しています。
- [MyAnimeList Dataset](https://www.kaggle.com/datasets/azathoth42/myanimelist)
  - 本プロジェクトはMyAnimeList Dataset（MyAnimeList.net） のデータを利用しています。データベースは Open Database License (ODbL) v1.0、個々のコンテンツは Database Contents License (DbCL) v1.0 に基づきます。ライセンス条件に従い帰属表示と通知保持を行っています。」


## ご協力

プルリクエストやIssueの報告は大歓迎です！改善点や新機能の提案など、どんなフィードバックもお待ちしています。
