#!/usr/bin/env python3
"""
Test normalization functionality with sample data
"""
import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent / "scripts" / "data_import"
sys.path.append(str(scripts_dir))

from name_normalizer import normalize_creator_name, normalize_publisher_name, generate_normalized_id


def test_one_piece_problem():
    """Test the specific One Piece problem mentioned by the user"""
    print("=== Testing One Piece Problem Cases ===")
    
    # Test creator normalization (the main issue)
    creator_variants = [
        "[著]尾田栄一郎",
        "[[著]]尾田栄一郎", 
        "[原作]尾田栄一郎",
        "尾田栄一郎"  # Expected normalized form
    ]
    
    print("Creator name normalization:")
    normalized_creators = []
    creator_ids = []
    
    for variant in creator_variants:
        normalized = normalize_creator_name(variant)
        creator_id = generate_normalized_id(normalized, "author")
        normalized_creators.append(normalized)
        creator_ids.append(creator_id)
        print(f"  '{variant}' -> '{normalized}' (ID: {creator_id})")
    
    # Check if all variants produce the same result
    all_same_name = len(set(normalized_creators)) == 1
    all_same_id = len(set(creator_ids)) == 1
    
    print(f"\n✅ All creator names normalize to same value: {all_same_name}")
    print(f"✅ All creator IDs are identical: {all_same_id}")
    
    # Test publisher normalization
    publisher_variants = [
        "集英社　∥　シュウエイシャ",
        "集英社"  # Expected normalized form
    ]
    
    print("\nPublisher name normalization:")
    normalized_publishers = []
    publisher_ids = []
    
    for variant in publisher_variants:
        normalized = normalize_publisher_name(variant)
        publisher_id = generate_normalized_id(normalized, "publisher")
        normalized_publishers.append(normalized)
        publisher_ids.append(publisher_id)
        print(f"  '{variant}' -> '{normalized}' (ID: {publisher_id})")
    
    # Check if all variants produce the same result
    all_same_pub_name = len(set(normalized_publishers)) == 1
    all_same_pub_id = len(set(publisher_ids)) == 1
    
    print(f"\n✅ All publisher names normalize to same value: {all_same_pub_name}")
    print(f"✅ All publisher IDs are identical: {all_same_pub_id}")
    
    # Overall result
    success = all_same_name and all_same_id and all_same_pub_name and all_same_pub_id
    print(f"\n{'🎉 NORMALIZATION TEST PASSED' if success else '❌ NORMALIZATION TEST FAILED'}")
    
    return success


def test_edge_cases():
    """Test edge cases for normalization"""
    print("\n=== Testing Edge Cases ===")
    
    edge_cases = [
        ("", "Empty string"),
        ("   ", "Whitespace only"),
        ("[編集]編集部", "Editorial role"),
        ("[作画・原作]複数の役割", "Multiple roles"),
        ("　[構成・編集]　得能久子　", "Spaces around brackets"),
        ("出版社名　∥　読み仮名　∥　追加情報", "Multiple separators"),
    ]
    
    print("Edge case testing:")
    for test_input, description in edge_cases:
        creator_normalized = normalize_creator_name(test_input)
        publisher_normalized = normalize_publisher_name(test_input)
        print(f"  {description}")
        print(f"    Input: '{test_input}'")
        print(f"    Creator: '{creator_normalized}'")
        print(f"    Publisher: '{publisher_normalized}'")
        print()


def test_id_consistency():
    """Test that ID generation is consistent across runs"""
    print("=== Testing ID Consistency ===")
    
    test_name = "尾田栄一郎"
    
    # Generate IDs multiple times
    ids = []
    for i in range(5):
        author_id = generate_normalized_id(test_name, "author")
        ids.append(author_id)
    
    all_same = len(set(ids)) == 1
    print(f"Generated IDs for '{test_name}': {ids[0]}")
    print(f"✅ ID generation is consistent: {all_same}")
    
    return all_same


if __name__ == "__main__":
    print("🧪 Testing Name Normalization Implementation\n")
    
    # Run all tests
    test1_passed = test_one_piece_problem()
    test_edge_cases()
    test2_passed = test_id_consistency()
    
    # Summary
    print("\n" + "="*50)
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED!")
        print("The normalization implementation should resolve the One Piece duplicate issue.")
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please review the implementation before proceeding.")