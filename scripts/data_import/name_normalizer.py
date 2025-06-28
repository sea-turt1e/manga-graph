#!/usr/bin/env python3
"""
Name normalization utilities for manga creators and publishers
"""
import re
from typing import List, Optional


def normalize_creator_name(name: str) -> str:
    """
    Normalize creator name by removing role prefixes and brackets
    
    Examples:
        "[著]尾田栄一郎" -> "尾田栄一郎"
        "[[著]]尾田栄一郎" -> "尾田栄一郎"
        "[原作]尾田栄一郎" -> "尾田栄一郎"
        "[作画]Boichi" -> "Boichi"
    
    Args:
        name: Original creator name
        
    Returns:
        Normalized creator name
    """
    if not name or not isinstance(name, str):
        return ""
    
    normalized = name.strip()
    
    # Remove role prefixes in brackets: [著], [原作], [作画], [[著]], etc.
    # Pattern matches single or double brackets with any content
    normalized = re.sub(r'\[+[^\]]*\]+', '', normalized)
    
    # Remove any remaining whitespace
    normalized = normalized.strip()
    
    return normalized


def normalize_publisher_name(name: str) -> str:
    """
    Normalize publisher name by removing reading annotations
    
    Examples:
        "集英社　∥　シュウエイシャ" -> "集英社"
        "講談社　∥　コウダンシャ" -> "講談社"
        "小学館" -> "小学館"
    
    Args:
        name: Original publisher name
        
    Returns:
        Normalized publisher name
    """
    if not name or not isinstance(name, str):
        return ""
    
    normalized = name.strip()
    
    # Remove reading annotation part (everything after ∥ symbol)
    normalized = re.sub(r'　*∥.*$', '', normalized)
    
    # Remove any trailing whitespace
    normalized = normalized.strip()
    
    return normalized


def split_multiple_creators(creator_string: str) -> List[str]:
    """
    Split multiple creator names separated by commas or other delimiters
    
    Examples:
        "岸本斉史, 江坂純" -> ["岸本斉史", "江坂純"]
        "[原作]岸本斉史, ひなたしょう" -> ["[原作]岸本斉史", "[原作]ひなたしょう"]
        "安藤英, 尾田栄一郎" -> ["安藤英", "尾田栄一郎"]
    
    Args:
        creator_string: String potentially containing multiple creator names
        
    Returns:
        List of individual creator names
    """
    if not creator_string or not isinstance(creator_string, str):
        return []
    
    # Extract role prefix if present (e.g., "[原作]", "[著]")
    role_prefix = ""
    name_part = creator_string.strip()
    
    # Check for role prefix at the beginning
    role_match = re.match(r'^(\[+[^\]]*\]+)', name_part)
    if role_match:
        role_prefix = role_match.group(1)
        name_part = name_part[len(role_prefix):].strip()
    
    # Split by comma delimiters first (most reliable)
    separators = [',', '、']
    names = [name_part]
    
    for separator in separators:
        new_names = []
        for name in names:
            new_names.extend([n.strip() for n in name.split(separator) if n.strip()])
        names = new_names
    
    # Only use middle dot (・) if we still have a single name and it looks like multiple people
    if len(names) == 1 and '・' in names[0]:
        parts = [n.strip() for n in names[0].split('・') if n.strip()]
        # Only split if all parts look like person names (simple heuristic: short parts, no company words)
        if len(parts) > 1 and all(len(part) <= 8 and not any(word in part for word in ['編集部', '出版', '社', 'プロダクション', 'スタジオ']) for part in parts):
            names = parts
    
    # Add role prefix back to each name if it was present
    if role_prefix:
        names = [f"{role_prefix}{name}" for name in names if name]
    
    return [name for name in names if name]


def normalize_and_split_creators(creator_string: str) -> List[str]:
    """
    Split multiple creators and normalize each one
    
    Args:
        creator_string: String potentially containing multiple creator names
        
    Returns:
        List of normalized individual creator names
    """
    individual_creators = split_multiple_creators(creator_string)
    return [normalize_creator_name(creator) for creator in individual_creators if creator]


def generate_normalized_id(name: str, entity_type: str) -> str:
    """
    Generate normalized ID for entities
    
    Args:
        name: Normalized name
        entity_type: Type of entity ('author' or 'publisher')
        
    Returns:
        Normalized ID string
    """
    if not name:
        return ""
    
    return f"{entity_type}_{abs(hash(name))}"


def test_normalizations():
    """Test function to verify normalization rules"""
    print("=== Creator Name Normalization Tests ===")
    
    creator_tests = [
        "[著]尾田栄一郎",
        "[[著]]尾田栄一郎", 
        "[原作]尾田栄一郎",
        "[作画]Boichi",
        "[デザイン]バナナグローブスタジオ",
        "[監修]Fischer's",
        "尾田栄一郎",  # Already normalized
        "　[編]ジャンプ・コミック出版編集部　",  # With spaces
    ]
    
    for test_name in creator_tests:
        normalized = normalize_creator_name(test_name)
        print(f"'{test_name}' -> '{normalized}'")
    
    print("\n=== Publisher Name Normalization Tests ===")
    
    publisher_tests = [
        "集英社　∥　シュウエイシャ",
        "講談社　∥　コウダンシャ",
        "小学館",
        "集英社",  # Already normalized
        "　小学館　∥　ショウガクカン　",  # With spaces
    ]
    
    for test_name in publisher_tests:
        normalized = normalize_publisher_name(test_name)
        print(f"'{test_name}' -> '{normalized}'")
    
    print("\n=== ID Generation Tests ===")
    
    # Test that different representations produce same ID
    creator_variations = [
        "[著]尾田栄一郎",
        "[[著]]尾田栄一郎",
        "[原作]尾田栄一郎",
    ]
    
    creator_ids = []
    for variation in creator_variations:
        normalized = normalize_creator_name(variation)
        id_val = generate_normalized_id(normalized, "author")
        creator_ids.append(id_val)
        print(f"'{variation}' -> normalized: '{normalized}' -> ID: '{id_val}'")
    
    print(f"All creator IDs same: {len(set(creator_ids)) == 1}")
    
    publisher_variations = [
        "集英社　∥　シュウエイシャ",
        "集英社",
    ]
    
    publisher_ids = []
    for variation in publisher_variations:
        normalized = normalize_publisher_name(variation)
        id_val = generate_normalized_id(normalized, "publisher")
        publisher_ids.append(id_val)
        print(f"'{variation}' -> normalized: '{normalized}' -> ID: '{id_val}'")
    
    print(f"All publisher IDs same: {len(set(publisher_ids)) == 1}")
    
    print("\n=== Multiple Creator Splitting Tests ===")
    
    multiple_creator_tests = [
        "岸本斉史, 江坂純",
        "[原作]岸本斉史, ひなたしょう", 
        "安藤英, 尾田栄一郎",
        "[編]ジャンプ・コミックス出版編集部",  # Single creator
        "[著]武井宏文, 尾田栄一郎",
        "岸本斉史、江坂純",  # Japanese comma
        "作者A・作者B",  # Middle dot separator
    ]
    
    for test_string in multiple_creator_tests:
        split_result = split_multiple_creators(test_string)
        normalized_result = normalize_and_split_creators(test_string)
        print(f"'{test_string}'")
        print(f"  Split: {split_result}")
        print(f"  Normalized: {normalized_result}")
        print()


if __name__ == "__main__":
    test_normalizations()