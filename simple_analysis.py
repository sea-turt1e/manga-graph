import json

# Load data
with open('/Users/yamadahikaru/project/manga-graph/data/mediaarts/metadata105.json', 'r') as f:
    data = json.load(f)

# Process entries
results = {
    'types': {},
    'manga_brands': {},
    'sunday_webry': [],
    'isagi': []
}

for item in data['@graph']:
    item_type = item.get('@type', 'Unknown')
    results['types'][item_type] = results['types'].get(item_type, 0) + 1
    
    # Check for specific content
    item_str = json.dumps(item, ensure_ascii=False)
    if 'サンデーうぇぶり' in item_str:
        results['sunday_webry'].append({
            'type': item_type,
            'title': item.get('schema:name', 'Unknown'),
            'publisher': item.get('schema:publisher', 'Unknown'),
            'bookFormat': item.get('schema:bookFormat', {})
        })
    
    if '竜送りのイサギ' in item_str:
        results['isagi'].append({
            'type': item_type,
            'title': item.get('schema:name', 'Unknown'),
            'publisher': item.get('schema:publisher', 'Unknown'),
            'bookFormat': item.get('schema:bookFormat', {})
        })
    
    # Collect manga brands
    if item_type == 'class:Manga':
        publisher = item.get('schema:publisher', 'Unknown')
        book_format = item.get('schema:bookFormat', {})
        if isinstance(book_format, dict):
            brand = book_format.get('schema:name', None)
            if brand:
                if publisher not in results['manga_brands']:
                    results['manga_brands'][publisher] = set()
                results['manga_brands'][publisher].add(brand)

# Save results
with open('/Users/yamadahikaru/project/manga-graph/analysis_results.json', 'w', encoding='utf-8') as f:
    # Convert sets to lists for JSON serialization
    manga_brands_list = {}
    for pub, brands in results['manga_brands'].items():
        manga_brands_list[pub] = list(brands)
    
    output = {
        'types': results['types'],
        'manga_brands': manga_brands_list,
        'sunday_webry_count': len(results['sunday_webry']),
        'sunday_webry_samples': results['sunday_webry'][:10],
        'isagi_count': len(results['isagi']),
        'isagi_samples': results['isagi'][:10]
    }
    
    json.dump(output, f, ensure_ascii=False, indent=2)

print("Analysis complete. Results saved to analysis_results.json")