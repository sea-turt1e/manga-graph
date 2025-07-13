# Manga Graph Visualizer

メディア芸術データベースを活用した漫画の関係性を可視化するAPIサーバーです。

## 機能
- 📊 漫画グラフデータの提供（Neo4j）
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

### 3. データの準備と投入
```bash
# メディア芸術データベースからデータをダウンロード
python scripts/data_import/download_mediaarts_data.py

# Neo4jにデータを投入
python scripts/data_import/import_to_neo4j.py
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

- `GET /health` - ヘルスチェック
- `GET /api/v1/neo4j/search` - Neo4jグラフデータの検索

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
# 全テストの実行
python -m pytest tests/

# ユニットテストのみ
python -m pytest tests/unit/

# 統合テストのみ
python -m pytest tests/integration/

# E2Eテストのみ
python -m pytest tests/e2e/
```

## ライセンス

Apache License 2.0

## データセット
このアプリケーションは、以下のデータセットを使用しています：
- 出典：独立行政法人国立美術館国立アートリサーチセンター「メディア芸術データベース」 （https://mediaarts-db.artmuseums.go.jp/）
  - またこのリポジトリ内のスクリプトによってデータをグラフ化し、Neo4jに投入しています。
- OpenBDのデータ
  - データの詳細は、[OpenBD](https://openbd.jp/)をご覧ください。


## ご協力

プルリクエストやIssueの報告は大歓迎です！改善点や新機能の提案など、どんなフィードバックもお待ちしています。
