#!/usr/bin/env python3
"""
小規模なデータセットでのインポートテスト
"""
import json
import os
import sys
import re
from collections import defaultdict
from pathlib import Path

from kanjiconv import KanjiConv
from neo4j import GraphDatabase

# NameNormalizerクラスをコピー
class NameNormalizer:
    """名前の正規化と統一を行うクラス"""

    def __init__(self):
        self.kanjiconv = KanjiConv()
        self.name_map = {}  # 正規化名 -> 表示名のマッピング
        self.reverse_map = {}  # 入力名 -> 正規化名のマッピング

    def normalize(self, name):
        """名前を正規化（ひらがなに変換）"""
        if not name or not isinstance(name, str):
            return None
        # カッコ内の内容を削除
        name_without_paren = name.split("(")[0].split("（")[0].strip()
        # ひらがなに変換
        normalized = self.kanjiconv.to_hiragana(name_without_paren)
        return normalized.lower()

    def register_name(self, name):
        """名前を登録して正規化IDを返す"""
        if not name or not isinstance(name, str):
            return None

        normalized = self.normalize(name)
        if not normalized:
            return None

        # 既に登録されている場合
        if name in self.reverse_map:
            return self.reverse_map[name]

        # 正規化名が既に存在する場合
        if normalized in self.name_map:
            # 優先順位に基づいて表示名を更新
            existing_name = self.name_map[normalized]
            if self._should_update_display_name(existing_name, name):
                self.name_map[normalized] = name
        else:
            # 新規登録
            self.name_map[normalized] = name

        self.reverse_map[name] = normalized
        return normalized

    def get_display_name(self, normalized_name):
        """正規化名から表示名を取得"""
        return self.name_map.get(normalized_name, normalized_name)

    def _should_update_display_name(self, existing, new):
        """表示名を更新すべきかどうかを判定"""

        # 優先順位: 漢字 > ローマ字 > カタカナ > ひらがな
        def get_priority(name):
            if any("\\u4e00" <= char <= "\\u9fff" for char in name):  # 漢字
                return 4
            elif any("A" <= char <= "Z" or "a" <= char <= "z" for char in name):  # ローマ字
                return 3
            elif any("\\u30a0" <= char <= "\\u30ff" for char in name):  # カタカナ
                return 2
            else:  # ひらがな
                return 1

        return get_priority(new) > get_priority(existing)


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


def load_json_ld(filename):
    """JSON-LD形式のファイルを読み込む"""
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("@graph", [])


# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

try:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    # 小規模テスト用に最初の1000件のみ処理
    print("=== 小規模インポートテスト ===")
    
    data_dir = Path("/Users/yamadahikaru/project/manga-graph/data/mediaarts")
    data_file = data_dir / "metadata104.json"
    
    print(f"Loading {data_file.name}...")
    all_data = load_json_ld(str(data_file))
    
    # 最初の1000件のみ
    test_data = all_data[:1000]
    print(f"Test data items: {len(test_data)}")
    
    # 作品と雑誌情報を収集
    work_magazine_map = defaultdict(set)
    manga_books = []
    magazines = set()
    
    print("\\nClassifying data...")
    
    for item in test_data:
        item_type = item.get("@type", "")
        genre = item.get("schema:genre", "")

        if ("MangaBook" in item_type and "マンガ単行本" in genre) or item_type == "class:MangaBookSeries":
            manga_books.append(item)
            
            # 作品名を取得
            full_name = item.get("schema:name", "")
            if isinstance(full_name, list):
                full_name = full_name[0] if full_name else ""
            elif isinstance(full_name, dict):
                full_name = full_name.get("@value", "")
            
            if full_name and isinstance(full_name, str):
                work_key = full_name.upper()
                
                # 雑誌情報を収集（schema:descriptionから初出情報を抽出）
                description = item.get("schema:description", "")
                if description:
                    extracted_magazines = extract_magazines_from_description(description)
                    for magazine_name in extracted_magazines:
                        work_magazine_map[work_key].add(magazine_name)
                        magazines.add(magazine_name)
                        print(f"Found magazine connection: {full_name} -> {magazine_name}")

    print(f"\\nFound {len(manga_books)} manga books")
    print(f"Found {len(magazines)} unique magazines from descriptions")
    print(f"Work-magazine connections: {sum(len(v) for v in work_magazine_map.values())}")
    
    # 雑誌詳細を表示
    print("\\n=== 発見した雑誌 ===")
    for magazine in sorted(magazines):
        print(f"- {magazine}")
    
    print("\\n=== 作品-雑誌の関係 ===")
    for work_key, magazine_set in work_magazine_map.items():
        if magazine_set:
            print(f"{work_key}: {list(magazine_set)}")
    
    driver.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()