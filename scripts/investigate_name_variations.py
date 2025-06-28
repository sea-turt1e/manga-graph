#!/usr/bin/env python3
"""
Investigate name variations in Neo4j database for authors and manga titles
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


class NameVariationInvestigator:
    """Investigate name variations in Neo4j database"""
    
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
    
    def get_database_structure(self) -> Dict[str, Any]:
        """Get database structure information"""
        logger.info("Getting database structure...")
        
        with self.driver.session() as session:
            structure = {
                'node_labels': [],
                'relationship_types': [],
                'node_properties': {},
                'relationship_properties': {}
            }
            
            # Get node labels
            result = session.run("CALL db.labels()")
            structure['node_labels'] = [record['label'] for record in result]
            
            # Get relationship types
            result = session.run("CALL db.relationshipTypes()")
            structure['relationship_types'] = [record['relationshipType'] for record in result]
            
            # Get node properties for each label
            for label in structure['node_labels']:
                query = f"""
                MATCH (n:{label})
                WITH n LIMIT 10
                RETURN keys(n) as properties
                """
                result = session.run(query)
                props = set()
                for record in result:
                    props.update(record['properties'])
                structure['node_properties'][label] = list(props)
            
            # Get relationship properties
            for rel_type in structure['relationship_types']:
                query = f"""
                MATCH ()-[r:{rel_type}]->()
                WITH r LIMIT 10
                RETURN keys(r) as properties
                """
                result = session.run(query)
                props = set()
                for record in result:
                    if record['properties']:
                        props.update(record['properties'])
                structure['relationship_properties'][rel_type] = list(props)
            
            return structure
    
    def investigate_author_variations(self, limit: int = 100) -> Dict[str, Any]:
        """Investigate author name variations"""
        logger.info("Investigating author name variations...")
        
        with self.driver.session() as session:
            # Get all author names
            query = """
            MATCH (a:Author)
            RETURN a.name as name, a.id as id
            LIMIT $limit
            """
            
            result = session.run(query, limit=limit)
            authors = [(record['name'], record['id']) for record in result]
            
            # Analyze variations
            variations = {
                'with_role_prefix': [],  # Names with [著], [原作], etc.
                'with_reading': [],      # Names with reading annotations
                'mixed_script': [],      # Names mixing kanji, hiragana, katakana, alphabet
                'potential_duplicates': defaultdict(list),  # Similar names that might be duplicates
                'script_types': defaultdict(int)  # Count of script types used
            }
            
            for name, author_id in authors:
                if not name:
                    continue
                
                # Check for role prefixes
                if re.search(r'\[[^\]]+\]', name):
                    variations['with_role_prefix'].append(name)
                
                # Check for reading annotations
                if '∥' in name or '||' in name:
                    variations['with_reading'].append(name)
                
                # Analyze script types
                has_kanji = bool(re.search(r'[\u4e00-\u9fff]', name))
                has_hiragana = bool(re.search(r'[\u3040-\u309f]', name))
                has_katakana = bool(re.search(r'[\u30a0-\u30ff]', name))
                has_alpha = bool(re.search(r'[a-zA-Z]', name))
                
                script_count = sum([has_kanji, has_hiragana, has_katakana, has_alpha])
                if script_count >= 2:
                    variations['mixed_script'].append(name)
                
                # Count script types
                if has_kanji:
                    variations['script_types']['kanji'] += 1
                if has_hiragana:
                    variations['script_types']['hiragana'] += 1
                if has_katakana:
                    variations['script_types']['katakana'] += 1
                if has_alpha:
                    variations['script_types']['alphabet'] += 1
                
                # Find potential duplicates by removing common variations
                normalized = self._normalize_for_duplicate_check(name)
                if normalized:
                    variations['potential_duplicates'][normalized].append((name, author_id))
            
            # Filter potential duplicates to only show actual variations
            actual_duplicates = {}
            for normalized, names_list in variations['potential_duplicates'].items():
                if len(names_list) > 1:
                    actual_duplicates[normalized] = names_list
            
            variations['potential_duplicates'] = actual_duplicates
            
            return variations
    
    def investigate_manga_title_variations(self, limit: int = 100) -> Dict[str, Any]:
        """Investigate manga title variations"""
        logger.info("Investigating manga title variations...")
        
        with self.driver.session() as session:
            # Get all work titles
            query = """
            MATCH (w:Work)
            RETURN w.title as title, w.id as id
            LIMIT $limit
            """
            
            result = session.run(query, limit=limit)
            works = [(record['title'], record['id']) for record in result]
            
            # Analyze variations
            variations = {
                'with_volume_number': [],     # Titles with volume numbers
                'with_parentheses': [],       # Titles with parentheses content
                'case_variations': defaultdict(list),  # Same title with different cases
                'space_variations': defaultdict(list),  # Same title with different spacing
                'potential_series': defaultdict(list),  # Works that might be part of same series
                'special_characters': []       # Titles with special characters
            }
            
            for title, work_id in works:
                if not title:
                    continue
                
                # Check for volume numbers
                if re.search(r'\s*\d+$|\s*第\d+巻?$|\s*\(\d+\)$|vol\.\s*\d+$|VOLUME\s*\d+$', title, re.IGNORECASE):
                    variations['with_volume_number'].append(title)
                
                # Check for parentheses
                if '(' in title or '（' in title:
                    variations['with_parentheses'].append(title)
                
                # Check for special characters
                if re.search(r'[!?！？・☆★♪♫♬♭♮♯]', title):
                    variations['special_characters'].append(title)
                
                # Normalize for duplicate detection
                normalized_lower = title.lower()
                variations['case_variations'][normalized_lower].append((title, work_id))
                
                # Normalize spaces
                normalized_space = re.sub(r'\s+', '', title)
                variations['space_variations'][normalized_space].append((title, work_id))
                
                # Extract base title for series detection
                base_title = self._extract_base_title(title)
                if base_title:
                    variations['potential_series'][base_title].append((title, work_id))
            
            # Filter to only show actual variations
            actual_case_variations = {}
            for normalized, titles_list in variations['case_variations'].items():
                if len(titles_list) > 1 and len(set(t[0] for t in titles_list)) > 1:
                    actual_case_variations[normalized] = titles_list
            
            actual_space_variations = {}
            for normalized, titles_list in variations['space_variations'].items():
                if len(titles_list) > 1 and len(set(t[0] for t in titles_list)) > 1:
                    actual_space_variations[normalized] = titles_list
            
            actual_series = {}
            for base, titles_list in variations['potential_series'].items():
                if len(titles_list) > 1:
                    actual_series[base] = titles_list
            
            variations['case_variations'] = actual_case_variations
            variations['space_variations'] = actual_space_variations
            variations['potential_series'] = actual_series
            
            return variations
    
    def get_specific_examples(self) -> Dict[str, Any]:
        """Get specific examples of common variation patterns"""
        logger.info("Getting specific examples of variations...")
        
        examples = {
            'author_variations': {},
            'title_variations': {}
        }
        
        with self.driver.session() as session:
            # Example 1: Authors with same normalized name
            query = """
            MATCH (a1:Author), (a2:Author)
            WHERE a1.id < a2.id
            AND (
                replace(replace(a1.name, '[著]', ''), '[原作]', '') = 
                replace(replace(a2.name, '[著]', ''), '[原作]', '')
            )
            RETURN a1.name as name1, a2.name as name2, a1.id as id1, a2.id as id2
            LIMIT 5
            """
            
            result = session.run(query)
            examples['author_variations']['role_prefix_duplicates'] = [
                {
                    'name1': record['name1'],
                    'name2': record['name2'],
                    'id1': record['id1'],
                    'id2': record['id2']
                }
                for record in result
            ]
            
            # Example 2: Manga titles that are part of same series
            query = """
            MATCH (w1:Work), (w2:Work)
            WHERE w1.id < w2.id
            AND w1.title STARTS WITH substring(w2.title, 0, size(w2.title) - 2)
            AND w1.title =~ '.*[0-9]$'
            AND w2.title =~ '.*[0-9]$'
            RETURN w1.title as title1, w2.title as title2, w1.id as id1, w2.id as id2
            LIMIT 5
            """
            
            result = session.run(query)
            examples['title_variations']['series_volumes'] = [
                {
                    'title1': record['title1'],
                    'title2': record['title2'],
                    'id1': record['id1'],
                    'id2': record['id2']
                }
                for record in result
            ]
            
            # Example 3: Publishers with reading annotations
            query = """
            MATCH (p:Publisher)
            WHERE p.name CONTAINS '∥'
            RETURN p.name as name, p.id as id
            LIMIT 5
            """
            
            result = session.run(query)
            examples['publisher_with_reading'] = [
                {'name': record['name'], 'id': record['id']}
                for record in result
            ]
            
        return examples
    
    def _normalize_for_duplicate_check(self, name: str) -> str:
        """Normalize name for duplicate checking"""
        # Remove role prefixes
        normalized = re.sub(r'\[[^\]]+\]', '', name)
        # Remove spaces
        normalized = normalized.replace(' ', '').replace('　', '')
        # Convert to lowercase
        normalized = normalized.lower()
        # Remove common suffixes
        normalized = re.sub(r'(先生|氏|さん|様)$', '', normalized)
        return normalized.strip()
    
    def _extract_base_title(self, title: str) -> str:
        """Extract base title by removing volume indicators"""
        # Remove volume numbers at the end
        patterns = [
            r'\s*\d+$',
            r'\s*第\d+巻?$',
            r'\s*\(\d+\)$',
            r'\s*vol\.\s*\d+$',
            r'\s*VOLUME\s*\d+$',
            r'\s*巻\d+$',
            r'\s*その\d+$'
        ]
        
        base = title
        for pattern in patterns:
            base = re.sub(pattern, '', base, flags=re.IGNORECASE)
        
        return base.strip()
    
    def generate_report(self):
        """Generate comprehensive investigation report"""
        print("\n" + "="*80)
        print("Neo4j Database Name Variation Investigation Report")
        print("="*80 + "\n")
        
        # Database structure
        print("1. Database Structure")
        print("-" * 40)
        structure = self.get_database_structure()
        
        print("Node Labels:")
        for label in structure['node_labels']:
            props = structure['node_properties'].get(label, [])
            print(f"  - {label}: {', '.join(props)}")
        
        print("\nRelationship Types:")
        for rel_type in structure['relationship_types']:
            props = structure['relationship_properties'].get(rel_type, [])
            if props:
                print(f"  - {rel_type}: {', '.join(props)}")
            else:
                print(f"  - {rel_type}")
        
        # Author variations
        print("\n\n2. Author Name Variations")
        print("-" * 40)
        author_vars = self.investigate_author_variations(limit=1000)
        
        print(f"Total authors analyzed: {sum(author_vars['script_types'].values())}")
        print(f"\nScript type distribution:")
        for script_type, count in author_vars['script_types'].items():
            print(f"  - {script_type}: {count}")
        
        print(f"\nNames with role prefixes ([著], [原作], etc.): {len(author_vars['with_role_prefix'])}")
        if author_vars['with_role_prefix'][:5]:
            print("  Examples:")
            for name in author_vars['with_role_prefix'][:5]:
                print(f"    - {name}")
        
        print(f"\nNames with reading annotations: {len(author_vars['with_reading'])}")
        if author_vars['with_reading'][:5]:
            print("  Examples:")
            for name in author_vars['with_reading'][:5]:
                print(f"    - {name}")
        
        print(f"\nPotential duplicate authors: {len(author_vars['potential_duplicates'])}")
        if author_vars['potential_duplicates']:
            print("  Examples:")
            for normalized, names_list in list(author_vars['potential_duplicates'].items())[:5]:
                print(f"    - Normalized: '{normalized}'")
                for name, author_id in names_list[:3]:
                    print(f"      • {name} (ID: {author_id})")
        
        # Manga title variations
        print("\n\n3. Manga Title Variations")
        print("-" * 40)
        title_vars = self.investigate_manga_title_variations(limit=1000)
        
        print(f"\nTitles with volume numbers: {len(title_vars['with_volume_number'])}")
        if title_vars['with_volume_number'][:5]:
            print("  Examples:")
            for title in title_vars['with_volume_number'][:5]:
                print(f"    - {title}")
        
        print(f"\nTitles with parentheses: {len(title_vars['with_parentheses'])}")
        if title_vars['with_parentheses'][:5]:
            print("  Examples:")
            for title in title_vars['with_parentheses'][:5]:
                print(f"    - {title}")
        
        print(f"\nTitles with special characters: {len(title_vars['special_characters'])}")
        if title_vars['special_characters'][:5]:
            print("  Examples:")
            for title in title_vars['special_characters'][:5]:
                print(f"    - {title}")
        
        print(f"\nPotential series (same base title): {len(title_vars['potential_series'])}")
        if title_vars['potential_series']:
            print("  Examples:")
            for base, titles_list in list(title_vars['potential_series'].items())[:3]:
                print(f"    - Base title: '{base}'")
                for title, work_id in titles_list[:3]:
                    print(f"      • {title} (ID: {work_id})")
        
        # Specific examples
        print("\n\n4. Specific Examples of Variations")
        print("-" * 40)
        examples = self.get_specific_examples()
        
        if examples['author_variations']['role_prefix_duplicates']:
            print("\nAuthors with role prefix variations:")
            for ex in examples['author_variations']['role_prefix_duplicates']:
                print(f"  - '{ex['name1']}' (ID: {ex['id1']}) vs '{ex['name2']}' (ID: {ex['id2']})")
        
        if examples['title_variations']['series_volumes']:
            print("\nManga volumes from same series:")
            for ex in examples['title_variations']['series_volumes']:
                print(f"  - '{ex['title1']}' (ID: {ex['id1']}) and '{ex['title2']}' (ID: {ex['id2']})")
        
        if examples.get('publisher_with_reading'):
            print("\nPublishers with reading annotations:")
            for pub in examples['publisher_with_reading']:
                print(f"  - {pub['name']} (ID: {pub['id']})")
        
        print("\n" + "="*80)
        print("End of Report")
        print("="*80 + "\n")


def main():
    """Run the investigation"""
    investigator = NameVariationInvestigator()
    
    try:
        investigator.generate_report()
    except Exception as e:
        logger.error(f"Error during investigation: {e}")
        raise
    finally:
        investigator.close()


if __name__ == "__main__":
    main()