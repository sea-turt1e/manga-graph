#!/usr/bin/env python3
"""Search metadata105.json for manga-brand relationships"""

import json
import sys
from collections import defaultdict

def main():
    file_path = '/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json'
    
    print("Loading metadata105.json...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Total entries: {len(data['@graph'])}")
    
    # Collect statistics
    type_counts = defaultdict(int)
    manga_brands = defaultdict(set)
    magazine_publishers = defaultdict(set)
    
    # Sample entries for each type
    sample_manga = []
    sample_magazine = []
    
    for item in data['@graph']:
        item_type = item.get('@type', 'Unknown')
        type_counts[item_type] += 1
        
        if item_type == 'class:Manga' and len(sample_manga) < 10:
            sample_manga.append(item)
            
            # Extract brand/label information
            publisher = item.get('schema:publisher', 'Unknown')
            book_format = item.get('schema:bookFormat', {})
            if isinstance(book_format, dict):
                brand = book_format.get('schema:name', 'Unknown')
                if brand != 'Unknown':
                    manga_brands[publisher].add(brand)
        
        elif item_type == 'class:MangaMagazine' and len(sample_magazine) < 10:
            sample_magazine.append(item)
            publisher = item.get('schema:publisher', 'Unknown')
            name = item.get('schema:name', 'Unknown')
            if isinstance(name, list):
                name = name[0] if name else 'Unknown'
            magazine_publishers[publisher].add(name)
    
    # Print statistics
    print("\n=== Type Distribution ===")
    for type_name, count in sorted(type_counts.items()):
        print(f"{type_name}: {count}")
    
    # Print sample Manga entries
    print("\n=== Sample Manga Entries (First 10) ===")
    for i, manga in enumerate(sample_manga, 1):
        print(f"\n{i}. Manga Entry:")
        print(f"   Title: {manga.get('schema:name', 'Unknown')}")
        print(f"   Publisher: {manga.get('schema:publisher', 'Unknown')}")
        print(f"   Book Format: {manga.get('schema:bookFormat', 'Unknown')}")
        is_part_of = manga.get('schema:isPartOf', {})
        if isinstance(is_part_of, dict):
            print(f"   Part of: {is_part_of.get('schema:name', 'Unknown')}")
        print(f"   ID: {manga.get('@id', 'Unknown')}")
    
    # Search for specific patterns
    print("\n=== Searching for サンデーうぇぶり ===")
    sunday_webry_count = 0
    sunday_examples = []
    
    for item in data['@graph']:
        item_str = json.dumps(item, ensure_ascii=False)
        if 'サンデーうぇぶり' in item_str or 'サンデーウェブリ' in item_str:
            sunday_webry_count += 1
            if len(sunday_examples) < 5:
                sunday_examples.append(item)
    
    print(f"Found {sunday_webry_count} entries containing サンデーうぇぶり")
    
    for i, example in enumerate(sunday_examples, 1):
        print(f"\n{i}. Example:")
        print(f"   Type: {example.get('@type', 'Unknown')}")
        print(f"   Title: {example.get('schema:name', 'Unknown')}")
        print(f"   Publisher: {example.get('schema:publisher', 'Unknown')}")
        if example.get('@type') == 'class:Manga':
            print(f"   Book Format: {example.get('schema:bookFormat', 'Unknown')}")
            is_part_of = example.get('schema:isPartOf', {})
            if isinstance(is_part_of, dict):
                print(f"   Part of: {is_part_of.get('schema:name', 'Unknown')}")
    
    # Search for 竜送りのイサギ
    print("\n=== Searching for 竜送りのイサギ ===")
    isagi_count = 0
    isagi_examples = []
    
    for item in data['@graph']:
        item_str = json.dumps(item, ensure_ascii=False)
        if '竜送りのイサギ' in item_str:
            isagi_count += 1
            isagi_examples.append(item)
    
    print(f"Found {isagi_count} entries containing 竜送りのイサギ")
    
    for i, example in enumerate(isagi_examples, 1):
        print(f"\n{i}. Example:")
        print(f"   Type: {example.get('@type', 'Unknown')}")
        print(f"   Title: {example.get('schema:name', 'Unknown')}")
        print(f"   Publisher: {example.get('schema:publisher', 'Unknown')}")
        if example.get('@type') == 'class:Manga':
            print(f"   Book Format: {example.get('schema:bookFormat', 'Unknown')}")
            is_part_of = example.get('schema:isPartOf', {})
            if isinstance(is_part_of, dict):
                print(f"   Part of: {is_part_of.get('schema:name', 'Unknown')}")

if __name__ == '__main__':
    main()