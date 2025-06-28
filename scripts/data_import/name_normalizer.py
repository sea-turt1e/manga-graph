#!/usr/bin/env python3
"""
Name normalization utilities for manga creators and publishers
"""
import re
from typing import Optional


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


if __name__ == "__main__":
    test_normalizations()