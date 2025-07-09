import json
from collections import defaultdict

# metadata105.jsonを読み込んで、マンガ単行本のブランド情報を分析
input_file = "/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json"

# ブランドと雑誌の関係を保存
brand_magazine_map = defaultdict(set)

# サンデー系の特殊なブランド
sunday_brands = {
    "サンデーうぇぶり少年サンデーコミックス": "サンデーうぇぶり",
    "裏少年サンデーコミックス": "裏サンデー",
    "サンデーGXコミックス": "月刊サンデーGX",
    "ゲッサン少年サンデーコミックススペシャル": "ゲッサン",
    "少年サンデーコミックススペシャル": "週刊少年サンデー",
    "MY FIRST BIG SPECIAL": "週刊少年サンデー",
    "ビッグコミックススペシャル": "ビッグコミックスペリオール",
    "ビッグコミックスオリジナル": "ビッグコミックオリジナル"
}

print("metadata105.jsonを分析中...")

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

manga_count = 0
brand_count = defaultdict(int)

# @graphのエントリを処理
for entry in data.get('@graph', []):
    if entry.get('@type') == 'class:Manga':
        manga_count += 1
        
        # ブランド情報を取得
        book_format = entry.get('schema:bookFormat', '')
        if book_format:
            brand_count[book_format] += 1
            
            # 特殊なサンデー系ブランドをチェック
            for brand, magazine in sunday_brands.items():
                if brand in book_format:
                    brand_magazine_map[book_format] = magazine
                    break

print(f"\n総マンガエントリ数: {manga_count}")
print(f"\nブランド/レーベルの出現頻度（上位20）:")

# 頻度順にソート
sorted_brands = sorted(brand_count.items(), key=lambda x: x[1], reverse=True)

for i, (brand, count) in enumerate(sorted_brands[:20]):
    print(f"{i+1}. {brand}: {count}回")

print(f"\n\n特殊なサンデー系ブランドのマッピング:")
for brand, magazine in brand_magazine_map.items():
    print(f"  {brand} → {magazine}")

# 既存のbrand_to_magazine.jsonを読み込む
existing_file = "/Users/yamadahikaru/project/manga-graph/scripts/data_import/brand_to_magazine.json"
with open(existing_file, 'r', encoding='utf-8') as f:
    existing_data = json.load(f)

# 新しいマッピングを追加
updated = False
for brand, magazine in brand_magazine_map.items():
    # サンデー系に追加
    if brand not in existing_data.get('サンデー系', {}):
        if 'サンデー系' not in existing_data:
            existing_data['サンデー系'] = {}
        existing_data['サンデー系'][brand] = magazine
        updated = True
        print(f"\n追加: {brand} → {magazine}")

# その他の頻出ブランドも確認
other_brands_to_add = {
    "ドラゴンコミックスエイジ": "月刊ドラゴンエイジ",
    "電撃コミックス": "電撃大王",
    "電撃コミックスNEXT": "電撃大王NEXT",
    "MFコミックス": "コミックフラッパー",
    "MFコミックス アライブシリーズ": "月刊コミックアライブ",
    "まんがタイムKRコミックス": "まんがタイムきらら",
    "バンブーコミックス": "まんがライフMOMO",
    "ビームコミックス": "月刊コミックビーム",
    "リュウコミックス": "月刊コミックリュウ",
    "ヤングアニマルコミックス": "ヤングアニマル",
    "ブレイドコミックス": "月刊コミックブレイド",
    "REXコミックス": "月刊コミックREX",
    "ガンガンコミックスJOKER": "ガンガンJOKER",
    "Gファンタジーコミックス": "月刊Gファンタジー"
}

for brand, magazine in other_brands_to_add.items():
    # 適切なカテゴリを決定
    category = "その他"
    if 'ガンガン' in brand or 'ファンタジー' in brand:
        category = "スクウェア・エニックス系"
    elif 'アニマル' in brand:
        category = "ヤングアニマル系"
    elif '電撃' in brand:
        category = "角川系"
    
    if category not in existing_data:
        existing_data[category] = {}
    
    if brand not in existing_data[category]:
        existing_data[category][brand] = magazine
        updated = True
        print(f"追加: {brand} → {magazine} (カテゴリ: {category})")

if updated:
    # 更新されたデータを保存
    output_file = "/Users/yamadahikaru/project/manga-graph/scripts/data_import/brand_to_magazine_updated.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
    print(f"\n\n更新されたデータを {output_file} に保存しました。")
else:
    print("\n新しいマッピングは見つかりませんでした。")