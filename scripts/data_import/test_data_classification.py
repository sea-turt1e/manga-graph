#!/usr/bin/env python3
"""
データの分類を簡単にテストするスクリプト
"""

import json
from pathlib import Path

def load_json_ld(filename):
    """JSON-LD形式のファイルを読み込む"""
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("@graph", [])

def main():
    # データを読み込む
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent.parent / "data" / "mediaarts"
    
    print(f"Looking for data in: {data_dir}")
    print(f"Data directory exists: {data_dir.exists()}")
    
    json_files = list(data_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files")
    
    # 最初のファイルを読み込み
    if json_files:
        first_file = json_files[0]
        print(f"Loading first file: {first_file.name}")
        
        try:
            data = load_json_ld(str(first_file))
            print(f"Loaded {len(data)} items")
            
            # 分類のテスト
            manga_books = 0
            manga_series = 0
            magazines = 0
            
            for i, item in enumerate(data):
                if i >= 10000:  # 最初の10000件のみテスト
                    break
                    
                item_type = item.get("@type", "")
                genre = item.get("schema:genre", "")
                
                if i < 10:
                    print(f"Item {i}: type='{item_type}', genre='{genre}'")
                
                if item_type == "class:MangaBook" and genre == "マンガ単行本":
                    manga_books += 1
                elif item_type == "class:MangaBookSeries" and genre == "マンガ単行本シリーズ":
                    manga_series += 1
                elif genre in ["マンガ雑誌", "雑誌", "雑誌全号まとめ"] or "Magazine" in item_type:
                    magazines += 1
            
            print(f"\nResults from first {min(10000, len(data))} items:")
            print(f"  MangaBooks: {manga_books}")
            print(f"  MangaSeries: {manga_series}")
            print(f"  Magazines: {magazines}")
            
        except Exception as e:
            print(f"Error loading {first_file}: {e}")

if __name__ == "__main__":
    main()