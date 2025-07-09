#!/usr/bin/env python3
import json
from collections import defaultdict

# Load JSON file
file_path = '/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json'

print("Loading metadata105.json...")
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Analyze data
type_counts = defaultdict(int)
manga_samples = []
brand_relationships = defaultdict(set)
sunday_webry_entries = []
isagi_entries = []

print(f"Total entries: {len(data['@graph'])}")

# Process entries
for item in data['@graph']:
    item_type = item.get('@type', 'Unknown')
    type_counts[item_type] += 1
    
    # Convert to string to search
    item_str = json.dumps(item, ensure_ascii=False)
    
    # Search for specific patterns
    if 'サンデーうぇぶり' in item_str or 'サンデーウェブリ' in item_str:
        sunday_webry_entries.append(item)
    
    if '竜送りのイサギ' in item_str:
        isagi_entries.append(item)
    
    # Collect Manga samples
    if item_type == 'class:Manga' and len(manga_samples) < 50:
        manga_samples.append(item)
        
        # Extract brand information
        publisher = item.get('schema:publisher', 'Unknown')
        book_format = item.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            brand_name = book_format.get('schema:name', None)
            if brand_name:
                brand_relationships[publisher].add(brand_name)

# Print findings
print("\n=== Type Distribution ===")
for t, count in sorted(type_counts.items()):
    print(f"{t}: {count}")

print(f"\n=== Manga Samples (First 20) ===")
for i, manga in enumerate(manga_samples[:20], 1):
    print(f"\n{i}. Manga:")
    print(f"   Title: {manga.get('schema:name', 'Unknown')}")
    print(f"   Publisher: {manga.get('schema:publisher', 'Unknown')}")
    
    book_format = manga.get('schema:bookFormat', {})
    if isinstance(book_format, dict):
        print(f"   Book Format/Brand: {book_format.get('schema:name', 'N/A')}")
    else:
        print(f"   Book Format/Brand: {book_format}")
    
    is_part_of = manga.get('schema:isPartOf', {})
    if isinstance(is_part_of, dict):
        print(f"   Part of: {is_part_of.get('schema:name', 'N/A')}")

print(f"\n=== Brand/Label Relationships ===")
for publisher, brands in sorted(brand_relationships.items()):
    if brands:
        print(f"\n{publisher}:")
        for brand in sorted(brands):
            print(f"   - {brand}")

print(f"\n=== サンデーうぇぶり Related Entries ({len(sunday_webry_entries)} found) ===")
for i, entry in enumerate(sunday_webry_entries[:10], 1):
    print(f"\n{i}. Entry:")
    print(f"   Type: {entry.get('@type', 'Unknown')}")
    print(f"   Title: {entry.get('schema:name', 'Unknown')}")
    print(f"   Publisher: {entry.get('schema:publisher', 'Unknown')}")
    
    if entry.get('@type') == 'class:Manga':
        book_format = entry.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            print(f"   Book Format/Brand: {book_format.get('schema:name', 'N/A')}")

print(f"\n=== 竜送りのイサギ Related Entries ({len(isagi_entries)} found) ===")
for i, entry in enumerate(isagi_entries[:10], 1):
    print(f"\n{i}. Entry:")
    print(f"   Type: {entry.get('@type', 'Unknown')}")
    print(f"   Title: {entry.get('schema:name', 'Unknown')}")
    print(f"   Publisher: {entry.get('schema:publisher', 'Unknown')}")
    
    if entry.get('@type') == 'class:Manga':
        book_format = entry.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            print(f"   Book Format/Brand: {book_format.get('schema:name', 'N/A')}")
        
        is_part_of = entry.get('schema:isPartOf', {})
        if isinstance(is_part_of, dict):
            print(f"   Series: {is_part_of.get('schema:name', 'N/A')}")

# Find all unique magazine names
print("\n=== All Magazine Names (First 50) ===")
magazine_count = 0
for item in data['@graph']:
    if item.get('@type') == 'class:MangaMagazine':
        magazine_count += 1
        if magazine_count <= 50:
            name = item.get('schema:name', 'Unknown')
            if isinstance(name, list):
                name = name[0] if name else 'Unknown'
            publisher = item.get('schema:publisher', 'Unknown')
            print(f"{magazine_count}. {name} (Publisher: {publisher})")