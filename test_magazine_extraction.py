#!/usr/bin/env python3
"""
雑誌抽出関数のテスト
"""
import re
import json

def extract_magazines_from_description(description):
    """schema:descriptionフィールドから雑誌名を抽出する"""
    if not description or not isinstance(description, str):
        return []
    
    magazines = []
    # 「初出：」で始まる部分を探す
    if "初出：" in description:
        # 初出：以降の部分を取得
        initial_part = description.split("初出：", 1)[1]
        
        # 「」で囲まれた雑誌名を抽出
        magazine_matches = re.findall(r'「([^」]+)」', initial_part)
        
        for magazine in magazine_matches:
            # 雑誌名をクリーンアップ（余分な文字を除去）
            clean_magazine = magazine.strip()
            if clean_magazine and len(clean_magazine) > 1:  # 空でなく、1文字以上
                magazines.append(clean_magazine)
    
    return magazines

# テストケース
test_descriptions = [
    "初出：「週刊ヤングジャンプ増刊 漫革」「週刊ヤングジャンプ」「コミックマーブル」",
    "初出：「小学五年生」「小学六年生」",
    "初出：「ゲッサン」200906～12",
    "その他の情報",
    "",
    None
]

print("=== 雑誌抽出テスト ===")
for i, desc in enumerate(test_descriptions):
    magazines = extract_magazines_from_description(desc)
    print(f"テスト {i+1}: {desc}")
    print(f"抽出結果: {magazines}")
    print("---")

# 実際のデータをテスト
print("\n=== 実際のデータをテスト ===")
try:
    with open("/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata104.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    count = 0
    magazine_count = 0
    
    for item in data.get("@graph", [])[:100]:  # 最初の100件をテスト
        item_type = item.get("@type", "")
        genre = item.get("schema:genre", "")
        if ("MangaBook" in item_type and "マンガ単行本" in genre) or item_type == "class:MangaBookSeries":
            count += 1
            description = item.get("schema:description", "")
            magazines = extract_magazines_from_description(description)
            
            if magazines:
                magazine_count += 1
                print(f"作品: {item.get('schema:name', ['不明'])[0] if isinstance(item.get('schema:name'), list) else item.get('schema:name', '不明')}")
                print(f"抽出雑誌: {magazines}")
                print("---")
    
    print(f"\n処理した作品数: {count}")
    print(f"雑誌情報があった作品数: {magazine_count}")
    print(f"雑誌情報の割合: {magazine_count/count*100:.1f}%" if count > 0 else "0%")
    
except Exception as e:
    print(f"エラー: {e}")