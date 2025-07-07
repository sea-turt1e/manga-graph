@scripts/data_import/import_to_neo4j_v4.py
を以下のルールに従って漫画のグラフDBを作成してください。

# データについて
- data/mediaarts/以下にメディア芸術データベースのデータが格納されています。

## 使用するファイル
- `@data/mediaarts/metadata104.json`: 漫画単行本シリーズデータ
- 

# nodeについて
必要なnodeは以下の4つです。
1. work（漫画）
2. creator（原作者）
3. magazine（雑誌）
4. publisher（発行社）

## idの取得方法
`@data/mediaarts/*.json`にそれぞれのデータのidが"@id"としてありますのでそれを使ってください。（例: "@id": "https://mediaarts-db.artmuseums.go.jp/id/C61770"）

## nodeの取得の仕方
1. 

# edgeについて
## 必要なedge
必要なedgeは以下の3つです。
1. work -> creator
2. work -> magazine
3. magazine -> publisher

## edgeの取得の仕方
