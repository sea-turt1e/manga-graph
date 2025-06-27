#!/usr/bin/env python3
"""
Check current duplicate authors and publishers in the database
"""
import os
import sys
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Add scripts directory to path
scripts_dir = Path(__file__).parent / "scripts" / "data_import"
sys.path.append(str(scripts_dir))

from name_normalizer import normalize_creator_name, normalize_publisher_name

load_dotenv()


def check_author_duplicates():
    """Check for author duplicates that would be resolved by normalization"""
    
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'password')
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        with driver.session() as session:
            print("=== Checking Author Duplicates ===")
            
            # Get authors that contain "尾田"
            query = """
            MATCH (a:Author)
            WHERE a.name CONTAINS "尾田"
            RETURN a.name
            ORDER BY a.name
            """
            
            result = session.run(query)
            authors = [record['a.name'] for record in result]
            
            print(f"Found {len(authors)} authors containing '尾田':")
            for author in authors:
                normalized = normalize_creator_name(author)
                print(f"  '{author}' -> '{normalized}'")
            
            # Check how many unique normalized names
            normalized_authors = list(set(normalize_creator_name(a) for a in authors))
            print(f"\nAfter normalization: {len(normalized_authors)} unique authors")
            for norm_author in normalized_authors:
                print(f"  '{norm_author}'")
            
            print(f"Reduction: {len(authors)} -> {len(normalized_authors)} ({len(authors) - len(normalized_authors)} duplicates removed)")
            
    finally:
        driver.close()


def check_publisher_duplicates():
    """Check for publisher duplicates that would be resolved by normalization"""
    
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'password')
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        with driver.session() as session:
            print("\n=== Checking Publisher Duplicates ===")
            
            # Get publishers that contain "集英社"
            query = """
            MATCH (p:Publisher)
            WHERE p.name CONTAINS "集英社"
            RETURN p.name
            ORDER BY p.name
            """
            
            result = session.run(query)
            publishers = [record['p.name'] for record in result]
            
            print(f"Found {len(publishers)} publishers containing '集英社':")
            for publisher in publishers:
                normalized = normalize_publisher_name(publisher)
                print(f"  '{publisher}' -> '{normalized}'")
            
            # Check how many unique normalized names
            normalized_publishers = list(set(normalize_publisher_name(p) for p in publishers))
            print(f"\nAfter normalization: {len(normalized_publishers)} unique publishers")
            for norm_pub in normalized_publishers:
                print(f"  '{norm_pub}'")
            
            print(f"Reduction: {len(publishers)} -> {len(normalized_publishers)} ({len(publishers) - len(normalized_publishers)} duplicates removed)")
            
    finally:
        driver.close()


def check_bracket_patterns():
    """Check common bracket patterns in author names"""
    
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'password')
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        with driver.session() as session:
            print("\n=== Checking Bracket Patterns ===")
            
            # Find authors with brackets
            query = """
            MATCH (a:Author)
            WHERE a.name CONTAINS "["
            RETURN a.name
            ORDER BY a.name
            LIMIT 20
            """
            
            result = session.run(query)
            authors = [record['a.name'] for record in result]
            
            print(f"Sample authors with brackets (showing first 20):")
            for author in authors:
                normalized = normalize_creator_name(author)
                print(f"  '{author}' -> '{normalized}'")
    
    finally:
        driver.close()


if __name__ == "__main__":
    check_author_duplicates()
    check_publisher_duplicates()
    check_bracket_patterns()