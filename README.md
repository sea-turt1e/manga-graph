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
- Python 3.15.1（3.14.9 / 3.13.17 も対応）
- FastAPI
- Neo4j (グラフデータベース)
- CleanArchitecture設計

### インフラ
- Docker & Docker Compose

## 対応 Python バージョンとセキュリティ対応（eMudhra CA 問題）

[eMudhra 中間 CA の秘密鍵漏洩](https://emudhra.we-have-a-problem.com/)により、古い CA バンドルを使う環境では TLS 通信のなりすましを受ける可能性があります。本リポジトリでは以下の対応を行っています。

### 対応 Python バージョン

| バージョン | 状態 |
| --- | --- |
| **3.15.1** | 推奨（Docker / GitHub Actions で使用） |
| 3.14.9 | 対応 |
| 3.13.17 | 対応 |

- 上記以外の Python で API サーバー（`main.py`）や埋め込みスクリプトを起動すると、`UnsupportedEnvironmentWarning` の警告が出ます。certifi が `2026.10.19` より古い場合も警告します。
- 手元の環境は次のコマンドで確認できます（問題があれば終了コード 1）。

```bash
python -m config.python_version_check
```

- 対応バージョンの一覧は [config/python_version_check.py](config/python_version_check.py) の `SUPPORTED_PYTHON_VERSIONS` で管理しています。

```bash
# pyenv で 3.15.1 の環境を作る例
pyenv install 3.15.1
pyenv virtualenv 3.15.1 manga-graph-3.15.1
pyenv local manga-graph-3.15.1
pip install -U "pip>=26.2.2"
pip install -r requirements.txt
```

### 依存パッケージの更新

- `certifi>=2026.10.19`（修正済み CA バンドル）を `requirements.txt` に追加しました。
- Python 3.15 用のホイールが無いため、`pydantic` 2.5.0 → 2.14.1、`aiohttp` 3.9.1 → 3.14.5、`pandas` 2.1.3 → 3.1.0 に更新しました。`requests` も 2.31.0 → 2.34.2 に更新しています。
- Docker イメージは `python:3.15.1-slim` を使い、ビルド時にシステムの `ca-certificates` と `pip`（26.2.2 以上）を更新します。

### 注意: 埋め込み生成（huggingface 方式）は Python 3.15 では使えません

`torch` に Python 3.15 用のホイールがまだ無いため、Python 3.15 では `sentence-transformers` / `sentencepiece` をインストールしません（`requirements.txt` の環境マーカーで除外）。API サーバーはこれらを使わないため影響はありません。

`scripts/add_vector_embeddings.py` と `scripts/add_synopsis_from_manga_csv.py`（`--method huggingface`）を実行する場合は **Python 3.14.9 または 3.13.17** を使ってください。Docker イメージにも torch は含まれません。

### GitHub Actions の安全対策

[.github/workflows/neo4j-keepalive.yml](.github/workflows/neo4j-keepalive.yml) に以下の変更を加えました。

- `permissions: {}` で `GITHUB_TOKEN` の権限をなくしました。
- `actions/setup-python` をタグではなくコミット SHA で固定し（v4 → v7.0.0）、タグの書き換えによる乗っ取りを防ぎます。
- Python を `3.15.1` に固定し、`pip` / `neo4j` / `certifi` のバージョンを指定してインストールします。
- 実行前にランナーの `ca-certificates` を更新します。
- `timeout-minutes` と `concurrency` を設定し、ジョブが止まったり多重に実行されたりしないようにしました。
- ランナーを `ubuntu-latest` から `ubuntu-24.04` に固定しました。
- [.github/dependabot.yml](.github/dependabot.yml) を追加し、SHA で固定した Actions を毎週更新します。公開直後のリリースは乗っ取りのリスクがあるため、7 日待ってから更新します。

### macOS の標準 `pip3` を使っている場合

macOS 標準の `python3` / `pip3`（と、それから作った仮想環境）は独自の古いルート証明書を使います。上記の対応バージョンの Python に切り替えるか、[告知ページ](https://emudhra.we-have-a-problem.com/)の手順に従ってください。

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
pip install -r requirements.txt

# 日本語形態素解析の辞書をダウンロード
python -m unidic download

# 開発サーバー起動
uvicorn main:app --reload --host 0.0.0.0 --port 8000
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
