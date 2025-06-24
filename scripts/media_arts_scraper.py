import requests
import json
import pandas as pd
from typing import Dict, List, Any
import time
from urllib.parse import quote
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MediaArtsDataScraper:
    def __init__(self):
        self.sparql_endpoint = "https://mediaarts-db.artmuseums.go.jp/sparql"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def execute_sparql_query(self, query: str) -> List[Dict]:
        """Execute SPARQL query and return results"""
        headers = {
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {'query': query}
        
        try:
            response = self.session.post(self.sparql_endpoint, headers=headers, data=data)
            response.raise_for_status()
            
            results = response.json()
            return results.get('results', {}).get('bindings', [])
        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    def get_manga_works(self, limit: int = 1000) -> List[Dict]:
        """Get manga works from MADB"""
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <http://schema.org/>
        PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/madb/>
        
        SELECT DISTINCT ?work ?title ?creator ?creatorName ?publisher ?publicationDate ?genre
        WHERE {{
            ?work a schema:Book ;
                  schema:name ?title .
            OPTIONAL {{ ?work schema:creator ?creator .
                       ?creator schema:name ?creatorName . }}
            OPTIONAL {{ ?work schema:publisher ?publisher . }}
            OPTIONAL {{ ?work schema:datePublished ?publicationDate . }}
            OPTIONAL {{ ?work schema:genre ?genre . }}
            FILTER(CONTAINS(STR(?work), "manga") || CONTAINS(STR(?work), "comic"))
        }}
        ORDER BY ?title
        LIMIT {limit}
        """
        
        results = self.execute_sparql_query(query)
        works = []
        
        for result in results:
            work = {
                'uri': result.get('work', {}).get('value', ''),
                'title': result.get('title', {}).get('value', ''),
                'creator_uri': result.get('creator', {}).get('value', ''),
                'creator_name': result.get('creatorName', {}).get('value', ''),
                'publisher': result.get('publisher', {}).get('value', ''),
                'publication_date': result.get('publicationDate', {}).get('value', ''),
                'genre': result.get('genre', {}).get('value', '')
            }
            works.append(work)
            
        return works

    def get_authors(self, limit: int = 1000) -> List[Dict]:
        """Get manga authors from MADB"""
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <http://schema.org/>
        PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/madb/>
        
        SELECT DISTINCT ?author ?name ?birthDate ?deathDate ?nationality
        WHERE {{
            ?author a schema:Person ;
                    schema:name ?name .
            ?work schema:creator ?author .
            OPTIONAL {{ ?author schema:birthDate ?birthDate . }}
            OPTIONAL {{ ?author schema:deathDate ?deathDate . }}
            OPTIONAL {{ ?author schema:nationality ?nationality . }}
            FILTER(CONTAINS(STR(?work), "manga") || CONTAINS(STR(?work), "comic"))
        }}
        ORDER BY ?name
        LIMIT {limit}
        """
        
        results = self.execute_sparql_query(query)
        authors = []
        
        for result in results:
            author = {
                'uri': result.get('author', {}).get('value', ''),
                'name': result.get('name', {}).get('value', ''),
                'birth_date': result.get('birthDate', {}).get('value', ''),
                'death_date': result.get('deathDate', {}).get('value', ''),
                'nationality': result.get('nationality', {}).get('value', '')
            }
            authors.append(author)
            
        return authors

    def get_magazines(self, limit: int = 1000) -> List[Dict]:
        """Get manga magazines from MADB"""
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <http://schema.org/>
        PREFIX madb: <https://mediaarts-db.artmuseums.go.jp/madb/>
        
        SELECT DISTINCT ?magazine ?name ?publisher ?startDate ?endDate
        WHERE {{
            ?magazine a schema:Periodical ;
                      schema:name ?name .
            OPTIONAL {{ ?magazine schema:publisher ?publisher . }}
            OPTIONAL {{ ?magazine schema:startDate ?startDate . }}
            OPTIONAL {{ ?magazine schema:endDate ?endDate . }}
            FILTER(CONTAINS(LCASE(?name), "マンガ") || CONTAINS(LCASE(?name), "コミック"))
        }}
        ORDER BY ?name
        LIMIT {limit}
        """
        
        results = self.execute_sparql_query(query)
        magazines = []
        
        for result in results:
            magazine = {
                'uri': result.get('magazine', {}).get('value', ''),
                'name': result.get('name', {}).get('value', ''),
                'publisher': result.get('publisher', {}).get('value', ''),
                'start_date': result.get('startDate', {}).get('value', ''),
                'end_date': result.get('endDate', {}).get('value', '')
            }
            magazines.append(magazine)
            
        return magazines

    def save_to_json(self, data: List[Dict], filename: str):
        """Save data to JSON file"""
        with open(f'../data/{filename}', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(data)} records to {filename}")

    def scrape_all_data(self):
        """Scrape all manga-related data"""
        logger.info("Starting data scraping from Media Arts Database...")
        
        # Get works
        logger.info("Fetching manga works...")
        works = self.get_manga_works(2000)
        self.save_to_json(works, 'manga_works.json')
        
        # Get authors
        logger.info("Fetching manga authors...")
        authors = self.get_authors(1000)
        self.save_to_json(authors, 'manga_authors.json')
        
        # Get magazines
        logger.info("Fetching manga magazines...")
        magazines = self.get_magazines(500)
        self.save_to_json(magazines, 'manga_magazines.json')
        
        logger.info("Data scraping completed!")
        
        return {
            'works': works,
            'authors': authors,
            'magazines': magazines
        }

if __name__ == "__main__":
    scraper = MediaArtsDataScraper()
    data = scraper.scrape_all_data()