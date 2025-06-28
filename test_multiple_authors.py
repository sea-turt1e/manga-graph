#!/usr/bin/env python3
"""
Test multiple author splitting functionality
"""
import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent / "scripts" / "data_import"
sys.path.append(str(scripts_dir))

from name_normalizer import normalize_and_split_creators, generate_normalized_id


def test_naruto_problem():
    """Test the specific NARUTO problem cases mentioned by the user"""
    print("=== Testing NARUTO Multiple Author Problem ===")
    
    # Test cases from the NARUTO search result
    test_cases = [
        "岸本斉史, 江坂純",
        "岸本斉史, ひなたしょう", 
        "[原作]岸本斉史, 江坂純",
        "[著]安藤英, 尾田栄一郎",
        "[編]ジャンプ・コミックス出版編集部",  # Should NOT be split
        "[脚本]小太刀右京",  # Single author
        "[漫画]池本幹雄",    # Single author
    ]
    
    print("Testing creator splitting and ID generation:")
    for test_case in test_cases:
        normalized_creators = normalize_and_split_creators(test_case)
        print(f"\nOriginal: '{test_case}'")
        print(f"Split into: {normalized_creators}")
        
        # Generate IDs for each creator
        for creator in normalized_creators:
            creator_id = generate_normalized_id(creator, "author")
            print(f"  '{creator}' -> ID: {creator_id}")
    
    # Check that the main problem case is properly split
    problem_case = "岸本斉史, 江坂純"
    result = normalize_and_split_creators(problem_case)
    
    expected_result = ["岸本斉史", "江坂純"]
    success = result == expected_result
    
    print(f"\n🎯 Main problem case test:")
    print(f"Input: '{problem_case}'")
    print(f"Expected: {expected_result}")
    print(f"Got: {result}")
    print(f"{'✅ SUCCESS' if success else '❌ FAILED'}: Multiple authors properly split")
    
    # Test that single authors with company names are NOT split
    organization_case = "[編]ジャンプ・コミックス出版編集部"
    org_result = normalize_and_split_creators(organization_case)
    
    org_success = len(org_result) == 1
    print(f"\n🏢 Organization name test:")
    print(f"Input: '{organization_case}'")
    print(f"Result: {org_result}")
    print(f"{'✅ SUCCESS' if org_success else '❌ FAILED'}: Organization name not split")
    
    return success and org_success


def demonstrate_id_unification():
    """Demonstrate that the same author gets the same ID regardless of representation"""
    print("\n=== Demonstrating ID Unification ===")
    
    # Different representations of the same author
    kishimoto_variants = [
        "岸本斉史",
        "[著]岸本斉史",
        "[原作]岸本斉史",
        "岸本斉史, 江坂純",  # Will be split, first part should match
    ]
    
    kishimoto_ids = set()
    
    for variant in kishimoto_variants:
        creators = normalize_and_split_creators(variant)
        for creator in creators:
            if creator == "岸本斉史":
                creator_id = generate_normalized_id(creator, "author")
                kishimoto_ids.add(creator_id)
                print(f"'{variant}' -> '{creator}' -> {creator_id}")
    
    print(f"\nUnique IDs for '岸本斉史': {len(kishimoto_ids)}")
    print(f"{'✅ SUCCESS' if len(kishimoto_ids) == 1 else '❌ FAILED'}: All variants produce same ID")


if __name__ == "__main__":
    print("🧪 Testing Multiple Author Splitting for NARUTO Problem\n")
    
    test_passed = test_naruto_problem()
    demonstrate_id_unification()
    
    print("\n" + "="*60)
    if test_passed:
        print("🎉 ALL TESTS PASSED!")
        print("The multiple author problem should now be resolved.")
        print("Each author will get their own individual node.")
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please review the implementation.")