import os
import subprocess
import time
from datetime import datetime

from neo4j import GraphDatabase

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")


class ImportProgressMonitor:
    def __init__(self):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def close(self):
        self.driver.close()

    def get_database_status(self):
        """データベースの現在の状況を取得"""
        with self.driver.session() as session:
            status = {}

            # ノード数をカウント
            for label in ["Work", "Author", "Publisher", "Magazine"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                status[label] = result.single()["count"]

            # リレーションシップ数をカウント
            relationships = {}
            for rel_type in ["CREATED_BY", "PUBLISHED_IN", "PUBLISHED_BY"]:
                result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count")
                relationships[rel_type] = result.single()["count"]

            status["Relationships"] = relationships
            return status

    def check_import_process(self):
        """インポートプロセスが実行中かチェック"""
        try:
            result = subprocess.run(["pgrep", "-f", "import_to_neo4j_v3.py"], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                return True, result.stdout.strip().split("\n")
            return False, []
        except Exception:
            return False, []

    def estimate_progress_stage(self, status):
        """現在の進行段階を推定"""
        total_nodes = sum(status[label] for label in ["Work", "Author", "Publisher", "Magazine"])
        total_relationships = sum(status["Relationships"].values())

        if total_nodes == 0:
            return "初期化中またはデータ分類中"
        elif status["Publisher"] > 0 and status["Author"] == 0:
            return "出版社ノード作成中"
        elif status["Author"] > 0 and status["Magazine"] == 0:
            return "著者ノード作成中"
        elif status["Magazine"] > 0 and status["Work"] == 0:
            return "雑誌ノード作成中"
        elif status["Work"] > 0 and total_relationships == 0:
            return "作品ノード作成中"
        elif total_relationships > 0:
            return "リレーションシップ作成中"
        else:
            return "完了またはエラー"

    def display_progress(self):
        """進行状況を表示"""
        print("\n{'='*60}")
        print(f"インポート進行状況チェック - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'=' * 60}")

        # プロセス状況チェック
        is_running, pids = self.check_import_process()
        if is_running:
            print(f"✅ インポートプロセス実行中 (PID: {', '.join(pids)})")
        else:
            print("❌ インポートプロセスが見つかりません")

        print()

        # データベース状況
        try:
            status = self.get_database_status()
            stage = self.estimate_progress_stage(status)

            print(f"📊 現在の段階: {stage}")
            print()

            print("ノード数:")
            for label, count in status.items():
                if label != "Relationships":
                    print(f"  {label}: {count:,}")

            print()
            print("リレーションシップ数:")
            for rel_type, count in status["Relationships"].items():
                print(f"  {rel_type}: {count:,}")

            # 推定完了度
            total_expected = {"Publisher": 8291, "Author": 27574, "Magazine": 214105, "Work": 146619}

            print()
            print("推定進捗:")
            for label, expected in total_expected.items():
                current = status.get(label, 0)
                if expected > 0:
                    progress = min(100, (current / expected) * 100)
                    bar_length = 20
                    filled_length = int(bar_length * progress / 100)
                    bar = "█" * filled_length + "░" * (bar_length - filled_length)
                    print(f"  {label:10}: |{bar}| {progress:5.1f}% ({current:,}/{expected:,})")

        except Exception as e:
            print(f"❌ データベース接続エラー: {e}")

    def monitor_continuously(self, interval=30):
        """継続的に監視"""
        print("Neo4j インポート進行状況監視を開始...")
        print("Ctrl+C で終了")

        try:
            while True:
                self.display_progress()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\n監視を終了します...")
            self.close()


def main():
    monitor = ImportProgressMonitor()

    try:
        # 引数で実行モード選択
        import sys

        if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
            # 継続監視モード
            interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            monitor.monitor_continuously(interval)
        else:
            # 1回だけチェック
            monitor.display_progress()

    except Exception as e:
        print(f"エラー: {e}")
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
