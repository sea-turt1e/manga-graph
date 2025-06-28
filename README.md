# Manga Graph Visualizer

メディア芸術データベースを活用した漫画の関係性を可視化するAPIサーバーです。

## 機能

- 📚 作品検索API
- 👨‍🎨 作者と作品の関係性データ提供
- 📖 同じ雑誌に掲載された作品の関係性
- 🔍 柔軟な検索とフィルタリング
- 📊 グラフデータの提供（Neo4j）

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
git clone <repository-url>
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

# データを解析
python scripts/data_import/analyze_data_structure.py

# Neo4jにデータを投入
python import_full_data.py
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

# 開発サーバー起動
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### データ関連の操作
```bash
# メディア芸術データベースからデータを取得
python scripts/data_import/download_mediaarts_data.py

# サンプルデータの作成
python scripts/create_sample_data.py

# テストデータの作成
python scripts/data_import/create_test_data.py

# データベースマイグレーション
python scripts/data_import/migrate_database.py
```

## API エンドポイント

- `GET /health` - ヘルスチェック
- `POST /search` - 作品検索
- `GET /authors` - 作者一覧
- `GET /works` - 作品一覧  
- `GET /magazines` - 雑誌一覧

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
- メタデータファイル（JSON形式）を使用してデータを取得

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

## 貢献

プルリクエストやIssueの報告をお待ちしています。
