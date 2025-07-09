import json
import re
from collections import defaultdict

def extract_brand_magazine_mapping(input_file):
    """metadata105.jsonからブランド/レーベルと雑誌のマッピングを抽出"""
    
    brand_to_magazine = defaultdict(set)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    # @graphのエントリを処理
    for entry in data.get('@graph', []):
        entry_type = entry.get('@type', '')
        
        # マンガ単行本のエントリを探す
        if entry_type == 'class:Manga':
            # ブランド/レーベル情報を探す
            book_format = entry.get('schema:bookFormat', '')
            
            # 雑誌情報を探す（関連フィールドから）
            # schema:isPartOfやma:relatedMagazineなどのフィールドをチェック
            magazine_info = None
            
            # 可能なフィールドをチェック
            for field in ['ma:magazine', 'ma:relatedMagazine', 'schema:isPartOf']:
                if field in entry:
                    magazine_info = entry[field]
                    break
            
            # ブランドと雑誌の関係を記録
            if book_format and magazine_info:
                if isinstance(magazine_info, str):
                    brand_to_magazine[book_format].add(magazine_info)
                elif isinstance(magazine_info, dict) and 'schema:name' in magazine_info:
                    brand_to_magazine[book_format].add(magazine_info['schema:name'])
    
    # 特定のパターンを含むエントリも探す
    for entry in data.get('@graph', []):
        if 'schema:name' in entry:
            names = entry['schema:name']
            if isinstance(names, list):
                for name in names:
                    if isinstance(name, str):
                        # サンデー系のパターンをチェック
                        if 'サンデー' in name and 'コミックス' in name:
                            # ブランド名から雑誌名を推測
                            if 'サンデーうぇぶり' in name:
                                brand_to_magazine[name].add('サンデーうぇぶり')
                            elif '裏サンデー' in name:
                                brand_to_magazine[name].add('裏サンデー')
                            elif 'ゲッサン' in name:
                                brand_to_magazine[name].add('ゲッサン')
    
    return dict(brand_to_magazine)

def update_brand_json(existing_file, new_mappings):
    """既存のbrand_to_magazine.jsonを更新"""
    
    # 既存のデータを読み込む
    with open(existing_file, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
    
    # 新しいマッピングを追加
    for brand, magazines in new_mappings.items():
        # 適切なカテゴリを決定
        category = None
        if 'サンデー' in brand:
            category = 'サンデー系'
        elif 'ジャンプ' in brand:
            category = 'ジャンプ系'
        elif 'マガジン' in brand:
            category = 'マガジン系'
        elif 'チャンピオン' in brand:
            category = 'チャンピオン系'
        else:
            category = 'その他'
        
        # カテゴリが存在しない場合は作成
        if category not in existing_data:
            existing_data[category] = {}
        
        # マッピングを追加（最初の雑誌名を使用）
        if magazines:
            magazine = list(magazines)[0]
            existing_data[category][brand] = magazine
    
    return existing_data

if __name__ == "__main__":
    # metadata105.jsonから抽出
    input_file = "/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json"
    brand_mappings = extract_brand_magazine_mapping(input_file)
    
    print("抽出されたブランド/雑誌マッピング:")
    for brand, magazines in brand_mappings.items():
        print(f"{brand}: {', '.join(magazines)}")
    
    # 既存のJSONファイルを更新
    existing_file = "/Users/yamadahikaru/project/manga-graph/scripts/data_import/brand_to_magazine.json"
    updated_data = update_brand_json(existing_file, brand_mappings)
    
    # 更新されたデータを保存
    output_file = "/Users/yamadahikaru/project/manga-graph/scripts/data_import/brand_to_magazine_updated.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(updated_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n更新されたデータを {output_file} に保存しました。")