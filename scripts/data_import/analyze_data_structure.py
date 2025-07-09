#!/usr/bin/env python3
"""
メディア芸術データベースのデータ構造を分析
"""
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts"


def analyze_json_structure(filename: str, sample_size: int = 5):
    """JSONファイルの構造を分析"""
    filepath = DATA_DIR / f"{filename}_json" / f"{filename}.json"

    logger.info(f"Analyzing: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    # データの基本情報
    if isinstance(data, list):
        logger.info(f"Type: List with {len(data)} items")
        if data:
            logger.info(f"First item keys: {list(data[0].keys())}")

            # サンプルデータを表示
            logger.info(f"\nSample data ({sample_size} items):")
            for i, item in enumerate(data[:sample_size]):
                logger.info(f"\n--- Item {i + 1} ---")
                for key, value in item.items():
                    if isinstance(value, (str, int, float, bool)):
                        logger.info(f"  {key}: {value}")
                    elif isinstance(value, list):
                        logger.info(f"  {key}: List with {len(value)} items")
                        if value and isinstance(value[0], dict):
                            logger.info(f"    First item: {value[0]}")
                    elif isinstance(value, dict):
                        logger.info(f"  {key}: Dict with keys {list(value.keys())}")

    elif isinstance(data, dict):
        logger.info(f"Type: Dict with keys: {list(data.keys())}")
        if "@graph" in data:
            graph_data = data["@graph"]
            logger.info(f"@graph contains {len(graph_data)} items")
            analyze_graph_item(graph_data[0] if graph_data else {})

    return data


def analyze_graph_item(item):
    """グラフアイテムの詳細分析"""
    logger.info("\nGraph item structure:")
    for key, value in item.items():
        if isinstance(value, (str, int, float, bool)):
            logger.info(f"  {key}: {value}")
        elif isinstance(value, list):
            logger.info(f"  {key}: List[{len(value)}]")
        elif isinstance(value, dict):
            logger.info(f"  {key}: Dict")


def search_one_piece(data):
    """ONE PIECEに関連するデータを検索"""
    one_piece_items = []

    items = data if isinstance(data, list) else data.get("@graph", [])

    for item in items:
        # タイトルまたは名前でONE PIECEを検索
        title = item.get("http://schema.org/name", "")
        if isinstance(title, list) and title:
            title = title[0].get("@value", "")
        elif isinstance(title, dict):
            title = title.get("@value", "")

        if "ONE PIECE" in str(title).upper():
            one_piece_items.append(item)

    logger.info(f"\nFound {len(one_piece_items)} ONE PIECE related items")
    for i, item in enumerate(one_piece_items[:3]):
        logger.info(f"\n--- ONE PIECE Item {i + 1} ---")
        logger.info(json.dumps(item, ensure_ascii=False, indent=2)[:500] + "...")

    return one_piece_items


def main():
    """メイン処理"""
    # マンガ単行本データを分析
    logger.info("=== Analyzing metadata101 (マンガ単行本) ===")
    data101 = analyze_json_structure("metadata101", sample_size=3)
    search_one_piece(data101)

    # シリーズデータを分析
    logger.info("\n\n=== Analyzing metadata104 (マンガ単行本シリーズ) ===")
    data104 = analyze_json_structure("metadata104", sample_size=3)
    search_one_piece(data104)

    # 雑誌データを分析
    logger.info("\n\n=== Analyzing metadata105 (マンガ雑誌) ===")
    data105 = analyze_json_structure("metadata105", sample_size=3)
    search_one_piece(data105)


if __name__ == "__main__":
    main()
