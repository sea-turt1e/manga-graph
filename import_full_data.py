#!/usr/bin/env python3
"""
Import full manga dataset to Neo4j for comprehensive search
"""
import os
import subprocess
import sys
from pathlib import Path

from scripts.data_import.import_to_neo4j import Neo4jImporter

# # Add project root to Python path
# sys.path.insert(0, str(Path(__file__).parent))


def check_and_download_data():
    """Check if data exists, download if needed"""
    data_dir = Path(__file__).parent / "data" / "mediaarts"
    book_file = data_dir / "metadata101_json" / "metadata101.json"
    series_file = data_dir / "metadata104_json" / "metadata104.json"

    if book_file.exists() and series_file.exists():
        print("✅ Data files already exist")
        return True

    print("📥 Data files not found. Downloading from GitHub...")

    # Run download script
    download_script = Path(__file__).parent / "scripts" / "data_import" / "download_mediaarts_data.py"

    try:
        result = subprocess.run([sys.executable, str(download_script)], capture_output=True, text=True, check=True)
        print("✅ Download completed successfully")
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Download failed: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False


def main():
    """Import full dataset with auto-download"""
    print("=== Importing Full Manga Dataset to Neo4j ===")
    print("This will import ~380K manga volumes to the database.")
    print("Estimated time: 30-60 minutes")

    # Check and download data if needed
    if not check_and_download_data():
        print("Failed to download required data files")
        return

    # Confirm import
    print(f"\nReady to import full dataset to Neo4j")
    print(f"This will replace any existing data in the database.")

    # Neo4j connection info
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)

    try:
        print("\nClearing existing test data...")
        importer.clear_database()

        print("Creating constraints...")
        importer.create_constraints()

        # Import full manga books dataset
        book_file = Path(__file__).parent / "data" / "mediaarts" / "metadata101_json" / "metadata101.json"
        if book_file.exists():
            print(f"\nImporting full manga books dataset...")
            print(f"File: {book_file}")
            importer.import_manga_books(book_file, batch_size=500)  # Smaller batches for stability
        else:
            print(f"Error: {book_file} not found")
            return

        # Import manga series dataset
        series_file = Path(__file__).parent / "data" / "mediaarts" / "metadata104_json" / "metadata104.json"
        if series_file.exists():
            print(f"\nImporting manga series dataset...")
            print(f"File: {series_file}")
            importer.import_manga_series(series_file, batch_size=500)
        else:
            print(f"Warning: {series_file} not found, skipping series import")

        print("\nCreating additional relationships...")
        importer.create_additional_relationships()

        # Get final statistics
        stats = importer.get_statistics()
        print(f"\n" + "=" * 50)
        print("FULL IMPORT COMPLETED!")
        print("=" * 50)
        for key, value in stats.items():
            print(f"{key}: {value:,}")

        print(f"\nYou can now search for classic manga like 'ONE PIECE'!")

    except Exception as e:
        print(f"Error during import: {e}")
        import traceback

        traceback.print_exc()

    finally:
        importer.close()


if __name__ == "__main__":
    main()
