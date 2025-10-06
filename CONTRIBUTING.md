# コントリビュートガイド

Manga Graph Visualizerプロジェクトへのコントリビュートをありがとうございます！このガイドでは、プロジェクトへの貢献方法について説明します。

## 開発ワークフロー

### 1. リポジトリのフォーク

```bash
# GitHubでリポジトリをフォークした後
git clone https://github.com/your-username/manga-graph.git
cd manga-graph
```

### 2. 開発環境のセットアップ

```bash
# 依存関係のインストール
uv sync

# 日本語形態素解析の辞書をダウンロード
uv run python -m unidic download

# Docker環境の起動
docker-compose up -d
```

### 3. ブランチ戦略

**重要**: すべてのプルリクエストは`develop`ブランチに対して作成してください。

```bash
# developブランチから作業ブランチを作成
git checkout develop
git pull origin develop
git checkout -b feat/your-feature-name
```

### 4. 開発作業

#### コーディング規約
- Python PEP 8に従ってコードを記述
- クリーンアーキテクチャの原則に従った実装
- 適切なタイプヒントの使用
- 日本語コメントでの説明

#### テストの実行
新機能やバグ修正を行う際は、必ずテストを実行してください：

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

#### 新しいテストの追加
- 新機能には対応するテストを追加
- バグ修正には回帰テストを追加
- テストカバレッジの維持を心がける

### 5. コミットガイドライン

#### コミットメッセージの形式
```
<type>: <description>

<body (optional)>
```

#### コミットタイプ
- `feat`: 新機能
- `fix`: バグ修正
- `docs`: ドキュメント更新
- `style`: コードフォーマット修正
- `refactor`: リファクタリング
- `test`: テスト追加・修正
- `chore`: ビルド・設定変更

#### 例
```
feat: Neo4jクエリのパフォーマンス向上

- インデックスを適用してクエリ速度を改善
- キャッシュ機能を追加
```

### 6. プルリクエストの作成

<!-- #### プルリクエスト前のチェックリスト
- [ ] すべてのテストが通ること
- [ ] コードが既存の規約に従っていること
- [ ] 適切なドキュメントが更新されていること
- [ ] コミットメッセージが規約に従っていること -->

#### プルリクエストの作成
```bash
git push origin feat/your-feature-name
```

GitHubでプルリクエストを作成し、**`develop`ブランチ**をベースブランチとして選択してください。

#### プルリクエストテンプレート
```markdown
## 概要
<!-- 変更内容の簡潔な説明 -->

## 変更点
- [ ] 新機能の追加
- [ ] バグ修正
- [ ] ドキュメント更新
- [ ] リファクタリング


## 影響範囲
<!-- 変更が影響する機能・コンポーネント -->

```

## 開発に関する技術情報

### アーキテクチャ
このプロジェクトはクリーンアーキテクチャを採用しています：

- **Domain Layer**: ビジネスロジック（entities, services, use_cases）
- **Infrastructure Layer**: 外部システムとの連携（database, external）
- **Presentation Layer**: API定義（api, schemas）

### 主要な技術スタック
- **Backend**: Python 3.12.7, FastAPI
- **Database**: Neo4j
- **Testing**: pytest
- **Containerization**: Docker, Docker Compose

### Neo4jクエリのベストプラクティス
- インデックスを適切に使用
- パフォーマンスを考慮したクエリ設計
- Cypherクエリの可読性を重視

## イシューの報告

バグ報告や新機能の提案は、GitHubのIssueで行ってください：

### バグ報告
- 再現手順の詳細な記載
- 期待される動作と実際の動作
- 環境情報（OS、Pythonバージョンなど）

### 機能提案
- 提案の背景と目的
- 具体的な実装案（可能であれば）
- UI/UXの考慮事項

## コードレビュープロセス

1. プルリクエストの作成
2. 自動テストの実行確認
3. メンテナーによるコードレビュー
4. 必要に応じた修正対応
5. approveされた後、developブランチにマージ

## ヘルプが必要な場合

- ドキュメントを確認: README.md
- Issueで質問を投稿
- 既存のIssueやプルリクエストを参考

皆様のコントリビュートをお待ちしています！🚀