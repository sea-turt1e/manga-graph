# Manga Graph Visualizer

メディア芸術データベースを活用した漫画の関係性を可視化するWebアプリケーションです。

## 機能

- 📚 作品検索によるグラフ可視化
- 👨‍🎨 作者と作品の関係性表示
- 📖 同じ雑誌に掲載された作品の関係性
- 🔍 動的な検索とフィルタリング
- 📊 インタラクティブなグラフ操作

## 技術スタック

### フロントエンド
- Vue.js 3
- Cytoscape.js (グラフ可視化)
- Vite (ビルドツール)

### バックエンド
- Python 3.12.7
- FastAPI
- Neo4j (グラフデータベース)

### インフラ
- Docker & Docker Compose
- AWS (デプロイ用)

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

### 3. サンプルデータの作成と投入
```bash
# サンプルデータを作成
cd scripts
python create_sample_data.py

# Neo4jにデータを投入
python neo4j_importer.py
```

### 4. アプリケーションへのアクセス
- フロントエンド: http://localhost:3000
- バックエンドAPI: http://localhost:8000
- Neo4j Browser: http://localhost:7474 (neo4j/password)

## 開発

### フロントエンド開発
```bash
cd frontend
npm install
npm run dev
```

### バックエンド開発
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### データ収集
```bash
cd scripts
# メディア芸術データベースからデータを取得
python media_arts_scraper.py

# Neo4jにデータを投入
python neo4j_importer.py
```

## API エンドポイント

- `GET /health` - ヘルスチェック
- `POST /search` - 作品検索
- `GET /authors` - 作者一覧
- `GET /works` - 作品一覧  
- `GET /magazines` - 雑誌一覧

## 使い方

1. 左側の検索パネルで作品名を入力
2. 検索深度を調整（1-3）
3. 検索ボタンをクリック
4. グラフでノードやエッジをクリックして詳細を確認
5. グラフの操作:
   - ドラッグ: ノードの移動
   - ズーム: マウスホイール
   - フィット: 画面にフィットボタン
   - リセット: レイアウトリセットボタン

## データソース

- [メディア芸術データベース](https://mediaarts-db.artmuseums.go.jp/)
- SPARQLエンドポイントを使用してデータを取得

## ライセンス

MIT License

## 貢献

プルリクエストやIssueの報告をお待ちしています。

## AWS デプロイ

詳細なデプロイ手順については、デプロイ用のドキュメントを参照してください。

### 必要なAWSリソース
- EC2インスタンス
- RDS (Neo4j用)
- S3 (静的ファイル用)
- CloudFront (CDN)
- Route53 (DNS)
