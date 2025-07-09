import json

# Load the JSON file
with open('/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Initialize collections
magazine_publishers = {}
manga_brands = {}
sunday_webry_items = []
isagi_items = []

# Process entries
for item in data['@graph']:
    item_type = item.get('@type', '')
    
    # Convert to string for search
    item_str = json.dumps(item, ensure_ascii=False)
    
    # Search for specific patterns
    if 'サンデーうぇぶり' in item_str or 'サンデーウェブリ' in item_str:
        sunday_webry_items.append(item)
    
    if '竜送りのイサギ' in item_str:
        isagi_items.append(item)
    
    # Process MangaMagazine entries
    if item_type == 'class:MangaMagazine':
        name = item.get('schema:name', 'Unknown')
        if isinstance(name, list):
            name = name[0] if name else 'Unknown'
        publisher = item.get('schema:publisher', 'Unknown')
        if isinstance(publisher, list):
            publisher = publisher[0] if publisher else 'Unknown'
        
        if publisher not in magazine_publishers:
            magazine_publishers[publisher] = []
        magazine_publishers[publisher].append(name)
    
    # Process Manga entries
    elif item_type == 'class:Manga':
        title = item.get('schema:name', 'Unknown')
        publisher = item.get('schema:publisher', 'Unknown')
        
        # Extract brand/label from bookFormat
        book_format = item.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            brand = book_format.get('schema:name', None)
            if brand:
                if publisher not in manga_brands:
                    manga_brands[publisher] = set()
                manga_brands[publisher].add(brand)

# Print results
print("=== Magazine Publishers (Top 20) ===")
for i, (publisher, magazines) in enumerate(sorted(magazine_publishers.items())[:20], 1):
    print(f"\n{i}. {publisher}")
    for mag in magazines[:5]:  # Show first 5 magazines
        print(f"   - {mag}")
    if len(magazines) > 5:
        print(f"   ... and {len(magazines) - 5} more")

print("\n=== Manga Brands/Labels by Publisher ===")
for publisher, brands in sorted(manga_brands.items()):
    if brands:
        print(f"\n{publisher}:")
        for brand in sorted(brands):
            print(f"   - {brand}")

print(f"\n=== サンデーうぇぶり Related Items ({len(sunday_webry_items)} found) ===")
for i, item in enumerate(sunday_webry_items[:5], 1):
    print(f"\n{i}. {item.get('@type', 'Unknown')}")
    print(f"   Title: {item.get('schema:name', 'Unknown')}")
    print(f"   Publisher: {item.get('schema:publisher', 'Unknown')}")
    if item.get('@type') == 'class:Manga':
        book_format = item.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            print(f"   Brand: {book_format.get('schema:name', 'N/A')}")

print(f"\n=== 竜送りのイサギ Items ({len(isagi_items)} found) ===")
for i, item in enumerate(isagi_items[:5], 1):
    print(f"\n{i}. {item.get('@type', 'Unknown')}")
    print(f"   Title: {item.get('schema:name', 'Unknown')}")
    print(f"   Publisher: {item.get('schema:publisher', 'Unknown')}")
    if item.get('@type') == 'class:Manga':
        book_format = item.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            print(f"   Brand: {book_format.get('schema:name', 'N/A')}")

# Save results to file
with open('/Users/yamadahikaru/project/manga-graph/brand_relationships.txt', 'w', encoding='utf-8') as f:
    f.write("=== BRAND/LABEL RELATIONSHIPS ===\n\n")
    
    for publisher, brands in sorted(manga_brands.items()):
        if brands:
            f.write(f"{publisher}:\n")
            for brand in sorted(brands):
                f.write(f"   - {brand}\n")
            f.write("\n")

print("\nResults saved to brand_relationships.txt")