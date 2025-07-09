#!/usr/bin/env python3
import json

# Load and search the data
with open('/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Count types
types = {}
manga_count = 0
magazine_count = 0
manga_samples = []

for item in data['@graph']:
    t = item.get('@type', 'Unknown')
    types[t] = types.get(t, 0) + 1
    
    if t == 'class:Manga' and manga_count < 20:
        manga_count += 1
        manga_samples.append({
            'title': item.get('schema:name', 'Unknown'),
            'publisher': item.get('schema:publisher', 'Unknown'),
            'bookFormat': item.get('schema:bookFormat', {}),
            'isPartOf': item.get('schema:isPartOf', {})
        })

print("=== Type Counts ===")
for t, count in sorted(types.items()):
    print(f"{t}: {count}")

print("\n=== First 20 Manga Samples ===")
for i, m in enumerate(manga_samples, 1):
    print(f"\n{i}. {m['title']}")
    print(f"   Publisher: {m['publisher']}")
    if isinstance(m['bookFormat'], dict):
        print(f"   Book Format: {m['bookFormat'].get('schema:name', 'N/A')}")
    if isinstance(m['isPartOf'], dict):
        print(f"   Part of: {m['isPartOf'].get('schema:name', 'N/A')}")