#!/usr/bin/env python3
"""
Detailed analysis of name variations with specific focus on problematic patterns
"""
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple
from neo4j import GraphDatabase
import logging
from collections import defaultdict, Counter
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Add parent directory to path to import modules
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from infrastructure.external.neo4j_repository import Neo4jMangaRepository


class DetailedVariationAnalyzer:
    """Detailed analyzer for name variations"""
    
    def __init__(self):
        self.uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = os.getenv('NEO4J_USER', 'neo4j')
        self.password = os.getenv('NEO4J_PASSWORD', 'password')
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        logger.info(f"Connected to Neo4j at {self.uri}")
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
    
    def analyze_author_role_prefixes(self) -> Dict[str, Any]:
        """Analyze authors with [著], [原作] etc. prefixes in detail"""
        logger.info("Analyzing author role prefixes in detail...")
        
        with self.driver.session() as session:
            # Get all authors with role prefixes
            query = """
            MATCH (a:Author)
            WHERE a.name =~ '.*\\[.*\\].*'
            RETURN a.name as name, a.id as id, count{(a)-[:CREATED]->()} as work_count
            ORDER BY work_count DESC
            """
            
            result = session.run(query)
            authors_with_roles = []
            role_types = Counter()
            
            for record in result:
                name = record['name']
                author_id = record['id']
                work_count = record['work_count']
                
                # Extract role type
                role_match = re.search(r'\[([^\]]+)\]', name)
                if role_match:
                    role = role_match.group(1)
                    role_types[role] += 1
                    
                    # Extract clean name
                    clean_name = re.sub(r'\[[^\]]+\]', '', name).strip()
                    
                    authors_with_roles.append({
                        'original_name': name,
                        'clean_name': clean_name,
                        'role': role,
                        'id': author_id,
                        'work_count': work_count
                    })
            
            # Find duplicate authors (same name with different roles)
            name_groups = defaultdict(list)
            for author in authors_with_roles:
                name_groups[author['clean_name']].append(author)
            
            duplicates = {name: authors for name, authors in name_groups.items() if len(authors) > 1}
            
            return {
                'total_with_roles': len(authors_with_roles),
                'role_distribution': dict(role_types),
                'authors_with_roles': authors_with_roles[:20],  # Top 20
                'potential_duplicates': duplicates
            }
    
    def analyze_publisher_reading_annotations(self) -> Dict[str, Any]:
        """Analyze publishers with reading annotations (∥)"""
        logger.info("Analyzing publisher reading annotations...")
        
        with self.driver.session() as session:
            # Get all publishers with reading annotations
            query = """
            MATCH (p:Publisher)
            WHERE p.name CONTAINS '∥'
            RETURN p.name as name, p.id as id, count{(p)-[:PUBLISHED]->()} as work_count
            ORDER BY work_count DESC
            """
            
            result = session.run(query)
            publishers_with_reading = []
            
            for record in result:
                name = record['name']
                parts = name.split('∥')
                
                publishers_with_reading.append({
                    'original_name': name,
                    'main_name': parts[0].strip() if parts else name,
                    'reading': parts[1].strip() if len(parts) > 1 else '',
                    'id': record['id'],
                    'work_count': record['work_count']
                })
            
            # Check for publishers that exist both with and without reading
            query_check = """
            MATCH (p1:Publisher)
            WHERE p1.name CONTAINS '∥'
            WITH p1, split(p1.name, '∥')[0] as main_name
            MATCH (p2:Publisher)
            WHERE p2.name = trim(main_name)
            RETURN p1.name as with_reading, p2.name as without_reading,
                   p1.id as id1, p2.id as id2,
                   count{(p1)-[:PUBLISHED]->()} as count1,
                   count{(p2)-[:PUBLISHED]->()} as count2
            """
            
            result_check = session.run(query_check)
            duplicates = []
            for record in result_check:
                duplicates.append({
                    'with_reading': record['with_reading'],
                    'without_reading': record['without_reading'],
                    'id_with_reading': record['id1'],
                    'id_without_reading': record['id2'],
                    'work_count_with': record['count1'],
                    'work_count_without': record['count2']
                })
            
            return {
                'total_with_reading': len(publishers_with_reading),
                'publishers': publishers_with_reading[:20],  # Top 20
                'duplicates': duplicates
            }
    
    def analyze_manga_series_patterns(self) -> Dict[str, Any]:
        """Analyze manga series and volume patterns"""
        logger.info("Analyzing manga series patterns...")
        
        with self.driver.session() as session:
            # Find works that share the same base title
            query = """
            MATCH (w:Work)
            WITH w, 
                 CASE 
                    WHEN w.title =~ '.*\\d+$' THEN rtrim(w.title, '0123456789 ')
                    WHEN w.title =~ '.*第\\d+巻?$' THEN split(w.title, '第')[0]
                    WHEN w.title =~ '.*\\(\\d+\\)$' THEN split(w.title, '(')[0]
                    ELSE w.title
                 END as base_title
            WITH base_title, collect(w) as works
            WHERE size(works) > 1
            RETURN base_title, 
                   [w in works | {title: w.title, id: w.id, published_date: w.published_date}] as work_list
            ORDER BY size(works) DESC
            LIMIT 20
            """
            
            result = session.run(query)
            series_groups = []
            
            for record in result:
                base_title = record['base_title'].strip()
                works = record['work_list']
                
                # Sort works by title to show volume order
                works_sorted = sorted(works, key=lambda x: x['title'])
                
                series_groups.append({
                    'base_title': base_title,
                    'work_count': len(works),
                    'works': works_sorted
                })
            
            # Check for works linked to Series nodes
            query_series = """
            MATCH (s:Series)-[:CONTAINS]->(w:Work)
            WITH s, collect(w) as works
            RETURN s.name as series_name, s.id as series_id, 
                   s.volume_count as expected_volumes,
                   size(works) as actual_volumes,
                   [w in works | w.title] as titles
            ORDER BY size(works) DESC
            LIMIT 10
            """
            
            result_series = session.run(query_series)
            series_with_nodes = []
            
            for record in result_series:
                series_with_nodes.append({
                    'series_name': record['series_name'],
                    'series_id': record['series_id'],
                    'expected_volumes': record['expected_volumes'],
                    'actual_volumes': record['actual_volumes'],
                    'titles': sorted(record['titles'])
                })
            
            return {
                'series_groups': series_groups,
                'series_with_nodes': series_with_nodes
            }
    
    def analyze_character_encoding_issues(self) -> Dict[str, Any]:
        """Analyze potential character encoding issues"""
        logger.info("Analyzing character encoding issues...")
        
        with self.driver.session() as session:
            # Find names with potential encoding issues
            issues = {
                'unusual_spaces': [],
                'mixed_width_characters': [],
                'special_characters': []
            }
            
            # Check for unusual spaces
            query_spaces = """
            MATCH (n)
            WHERE n.name CONTAINS '　' OR n.name CONTAINS '  '
            RETURN labels(n)[0] as label, n.name as name, n.id as id
            LIMIT 20
            """
            
            result = session.run(query_spaces)
            for record in result:
                issues['unusual_spaces'].append({
                    'type': record['label'],
                    'name': record['name'],
                    'id': record['id'],
                    'hex_repr': record['name'].encode('utf-8').hex()
                })
            
            # Check for mixed full-width and half-width characters
            query_mixed = """
            MATCH (n)
            WHERE n.name =~ '.*[０-９Ａ-Ｚａ-ｚ].*' AND n.name =~ '.*[0-9A-Za-z].*'
            RETURN labels(n)[0] as label, n.name as name, n.id as id
            LIMIT 20
            """
            
            result = session.run(query_mixed)
            for record in result:
                issues['mixed_width_characters'].append({
                    'type': record['label'],
                    'name': record['name'],
                    'id': record['id']
                })
            
            return issues
    
    def generate_detailed_report(self):
        """Generate detailed analysis report"""
        print("\n" + "="*80)
        print("Detailed Name Variation Analysis Report")
        print("="*80 + "\n")
        
        # Author role prefixes
        print("1. Author Role Prefix Analysis")
        print("-" * 40)
        role_analysis = self.analyze_author_role_prefixes()
        
        print(f"Total authors with role prefixes: {role_analysis['total_with_roles']}")
        print("\nRole type distribution:")
        for role, count in sorted(role_analysis['role_distribution'].items(), key=lambda x: x[1], reverse=True):
            print(f"  - [{role}]: {count}")
        
        if role_analysis['potential_duplicates']:
            print(f"\nPotential duplicate authors (same name, different roles): {len(role_analysis['potential_duplicates'])}")
            for name, authors in list(role_analysis['potential_duplicates'].items())[:5]:
                print(f"\n  Clean name: '{name}'")
                for author in authors:
                    print(f"    - {author['original_name']} (ID: {author['id']}, Works: {author['work_count']})")
        
        # Publisher reading annotations
        print("\n\n2. Publisher Reading Annotation Analysis")
        print("-" * 40)
        publisher_analysis = self.analyze_publisher_reading_annotations()
        
        print(f"Total publishers with reading annotations: {publisher_analysis['total_with_reading']}")
        print("\nTop publishers with reading annotations:")
        for pub in publisher_analysis['publishers'][:10]:
            print(f"  - {pub['main_name']} ∥ {pub['reading']} (Works: {pub['work_count']})")
        
        if publisher_analysis['duplicates']:
            print(f"\nPublishers that exist both with and without reading: {len(publisher_analysis['duplicates'])}")
            for dup in publisher_analysis['duplicates'][:5]:
                print(f"  - '{dup['without_reading']}' (Works: {dup['work_count_without']}) vs")
                print(f"    '{dup['with_reading']}' (Works: {dup['work_count_with']})")
        
        # Manga series patterns
        print("\n\n3. Manga Series Pattern Analysis")
        print("-" * 40)
        series_analysis = self.analyze_manga_series_patterns()
        
        print("Series identified by title patterns:")
        for series in series_analysis['series_groups'][:5]:
            print(f"\n  Base title: '{series['base_title']}' ({series['work_count']} volumes)")
            for work in series['works'][:5]:
                print(f"    - {work['title']} (Date: {work['published_date']})")
            if series['work_count'] > 5:
                print(f"    ... and {series['work_count'] - 5} more")
        
        if series_analysis['series_with_nodes']:
            print("\n\nSeries with explicit Series nodes:")
            for series in series_analysis['series_with_nodes'][:5]:
                print(f"\n  Series: '{series['series_name']}'")
                print(f"    Expected volumes: {series['expected_volumes']}, Actual: {series['actual_volumes']}")
                print(f"    Titles: {', '.join(series['titles'][:3])}")
                if len(series['titles']) > 3:
                    print(f"    ... and {len(series['titles']) - 3} more")
        
        # Character encoding issues
        print("\n\n4. Character Encoding Analysis")
        print("-" * 40)
        encoding_issues = self.analyze_character_encoding_issues()
        
        if encoding_issues['unusual_spaces']:
            print(f"\nNames with unusual spaces: {len(encoding_issues['unusual_spaces'])}")
            for item in encoding_issues['unusual_spaces'][:5]:
                print(f"  - {item['type']}: '{item['name']}'")
                print(f"    Hex: {item['hex_repr']}")
        
        if encoding_issues['mixed_width_characters']:
            print(f"\nNames with mixed full/half-width characters: {len(encoding_issues['mixed_width_characters'])}")
            for item in encoding_issues['mixed_width_characters'][:5]:
                print(f"  - {item['type']}: '{item['name']}'")
        
        print("\n" + "="*80)
        print("End of Detailed Analysis")
        print("="*80 + "\n")


def main():
    """Run the detailed analysis"""
    analyzer = DetailedVariationAnalyzer()
    
    try:
        analyzer.generate_detailed_report()
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()