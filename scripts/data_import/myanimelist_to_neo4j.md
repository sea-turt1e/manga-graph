# Import MyAnimeList Manga Data into Neo4j

## Import Full Manga Data
```bash
uv run python scripts/data_import/import_myanimelist_to_neo4j.py \
  --csv-path data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv \
  --uri <your-neo4j-uri> --user <your-username> --password <your-password> \
  --batch-size 500
```

## Import Authors to Neo4j
```bash
uv run python scripts/data_import/import_myanimelist_authors_magazines.py \
  --csv-path data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv \
  --uri <your-neo4j-uri> --user <your-username> --password <your-password> \
  --batch-size 500
```

## Import Publishers from Mapping
```bash
uv run python scripts/data_import/import_publishers_from_mapping.py \
  --mapping-path data/myanimelist/myanimelist-scraped-data-2025-July/publisher_magazine_mapping.json \
  --uri <your-neo4j-uri> --user <your-username> --password <your-password>
``` 

## indexを貼る
```
CREATE FULLTEXT INDEX work_titles_fulltext
FOR (w:Work)
ON EACH [w.title_name, w.title, w.english_name, w.japanese_name, w.aliases];
```