#!/usr/bin/env python3
"""
テスト用の小さなデータセットを作成
"""
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts"
TEST_DATA_DIR = DATA_DIR / "test"
TEST_DATA_DIR.mkdir(exist_ok=True)


def create_test_manga_books(limit: int = 100):
    """テスト用のマンガ単行本データを作成"""
    source_file = DATA_DIR / "metadata101_json" / "metadata101.json"
    test_file = TEST_DATA_DIR / "test_metadata101.json"

    with open(source_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 最初の100件のみ抽出
    test_data = {"@context": data["@context"], "@graph": data["@graph"][:limit]}

    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)

    print(f"Created test data with {len(test_data['@graph'])} items: {test_file}")


def create_test_manga_series(limit: int = 50):
    """テスト用のマンガシリーズデータを作成"""
    source_file = DATA_DIR / "metadata104_json" / "metadata104.json"
    test_file = TEST_DATA_DIR / "test_metadata104.json"

    with open(source_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 最初の50件のみ抽出
    test_data = {"@context": data["@context"], "@graph": data["@graph"][:limit]}

    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)

    print(f"Created test series data with {len(test_data['@graph'])} items: {test_file}")


if __name__ == "__main__":
    create_test_manga_books(100)
    create_test_manga_series(50)
    print("Test data creation completed!")
