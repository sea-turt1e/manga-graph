# MyAnimeList API v2 データ取得スクリプト

公式 MyAnimeList API v2 を使用してマンガ・アニメデータを取得し、Neo4j にインポートするためのスクリプト群です。

## 概要

これらのスクリプトは、スクレイピングされたデータの代わりに公式 API を使用してデータを取得します。

### ファイル構成

```
domain/services/
└── mal_api_client.py          # MAL API クライアント

scripts/data_import/
├── fetch_manga_from_mal_api.py    # マンガデータ取得スクリプト
├── fetch_anime_from_mal_api.py    # アニメデータ取得スクリプト
└── import_mal_api_to_neo4j.py     # Neo4j インポートスクリプト
```

## セットアップ

### 1. MAL API クレデンシャルの取得

1. [MyAnimeList API](https://myanimelist.net/apiconfig) にアクセス
2. アプリケーションを登録して Client ID を取得
3. `.env` ファイルに追加:

```env
MAL_CLIENT_ID="your_client_id_here"
MAL_CLIENT_SECRET="your_client_secret_here"  # OAuth2を使う場合のみ必要
```

### 2. 依存関係のインストール

```bash
pip install httpx tqdm
```

## 使用方法

### Step 1: マンガデータの取得

```bash
# 全マンガを取得（数時間かかります）
python scripts/data_import/fetch_manga_from_mal_api.py

# テスト用に上位1000件のみ取得
python scripts/data_import/fetch_manga_from_mal_api.py --limit 1000

# 特定のランキングタイプを指定
python scripts/data_import/fetch_manga_from_mal_api.py --ranking-type manga

# 中断後に再開
python scripts/data_import/fetch_manga_from_mal_api.py --resume
```

出力: `data/mal_api/manga_all.json`

### Step 2: アニメデータの取得

```bash
# ランキングベースで取得（高速）
python scripts/data_import/fetch_anime_from_mal_api.py --strategy ranking

# シーズンベースで取得（より網羅的）
python scripts/data_import/fetch_anime_from_mal_api.py --strategy season

# 両方の戦略を使用
python scripts/data_import/fetch_anime_from_mal_api.py --strategy both

# テスト用に上位1000件のみ
python scripts/data_import/fetch_anime_from_mal_api.py --limit 1000
```

出力: `data/mal_api/anime_*.json`

### Step 3: Neo4j へのインポート

```bash
# マンガのみインポート
python scripts/data_import/import_mal_api_to_neo4j.py --type manga

# アニメのみインポート
python scripts/data_import/import_mal_api_to_neo4j.py --type anime

# 両方インポート
python scripts/data_import/import_mal_api_to_neo4j.py --type both

# ドライラン（実際には書き込まない）
python scripts/data_import/import_mal_api_to_neo4j.py --type manga --dry-run

# 既存データをリセットしてインポート
python scripts/data_import/import_mal_api_to_neo4j.py --type both --reset
```

## API レート制限

MAL API の明確なレート制限は公開されていませんが、以下の制限を実装しています:

- デフォルト: 1リクエスト/秒
- 429 (Too Many Requests) 時: 自動バックオフ（指数関数的に待機時間を増加）
- 403 (Forbidden) 時: より長いバックオフで再試行

レート制限を調整する場合:

```bash
# より低速に（安全）
python scripts/data_import/fetch_manga_from_mal_api.py --rate-limit 0.5

# より高速に（リスクあり）
python scripts/data_import/fetch_manga_from_mal_api.py --rate-limit 2.0
```

## データ構造

### 取得されるマンガフィールド

| フィールド | 説明 |
|-----------|------|
| id | MAL ID |
| title_name | メインタイトル |
| japanese_name | 日本語タイトル |
| english_name | 英語タイトル |
| synonymns | 別名リスト |
| description | あらすじ |
| score | 平均スコア |
| ranked | ランキング順位 |
| popularity | 人気順位 |
| members | メンバー数 |
| volumes | 巻数 |
| chapters | 話数 |
| status | 連載状態 |
| genres | ジャンルリスト |
| authors | 著者リスト |
| serialization | 連載雑誌リスト |
| related_manga | 関連マンガ |
| related_anime | 関連アニメ |
| recommendations | おすすめ作品 |

### 取得されるアニメフィールド

上記に加えて:

| フィールド | 説明 |
|-----------|------|
| episodes | エピソード数 |
| studios | 制作スタジオ |
| start_season | 放送開始シーズン |
| source | 原作メディア |
| duration | エピソード時間 |
| age_rating | 年齢制限 |

## 既存スクレイピングデータとの比較

| 機能 | スクレイピング | API v2 |
|-----|--------------|--------|
| 多言語タイトル（独/仏/西） | ✅ | ❌ |
| デモグラフィック | ✅ | ❌（genresに含まれる可能性） |
| 関連作品 | ❌ | ✅ |
| おすすめ作品 | ❌ | ✅ |
| 利用規約 | ⚠️ 違反の可能性 | ✅ 公式サポート |

## トラブルシューティング

### "MAL_CLIENT_ID environment variable is required" エラー

`.env` ファイルに `MAL_CLIENT_ID` が設定されているか確認してください。

### 403 Forbidden エラーが頻発する

レート制限に引っかかっている可能性があります。`--rate-limit 0.5` などでより低速に設定してください。

### インポート中にメモリ不足になる

`--batch-size` を小さくしてください（デフォルト: 500）:

```bash
python scripts/data_import/import_mal_api_to_neo4j.py --batch-size 100
```

## 定期更新

新しいデータを定期的に取得する場合は、`--resume` オプションを使用して差分更新できます:

```bash
# 既存データをスキップして新規のみ取得
python scripts/data_import/fetch_manga_from_mal_api.py --resume
```

cron などで定期実行する例:

```bash
0 3 * * * cd /path/to/manga-graph && python scripts/data_import/fetch_manga_from_mal_api.py --resume >> /var/log/mal_fetch.log 2>&1
```
