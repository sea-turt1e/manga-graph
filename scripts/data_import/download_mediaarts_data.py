#!/usr/bin/env python3
"""
メディア芸術データベースから漫画関連データをダウンロードするスクリプト
"""
import os
import sys
import json
import zipfile
import requests
from pathlib import Path
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# データセットのGitHub Release URL
GITHUB_API_URL = "https://api.github.com/repos/mediaarts-db/dataset/releases/latest"
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "mediaarts"

# ダウンロードする漫画関連データ
MANGA_DATASETS = [
    "metadata101_json.zip",  # マンガ単行本
    "metadata104_json.zip",  # マンガ単行本シリーズ
    "metadata105_json.zip",  # マンガ雑誌
    "metadata107_json.zip",  # マンガ作品
]


def ensure_directories():
    """必要なディレクトリを作成"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Data directory: {DATA_DIR}")


def get_latest_release_info() -> Dict[str, Any]:
    """最新リリース情報を取得"""
    try:
        response = requests.get(GITHUB_API_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch release info: {e}")
        sys.exit(1)


def download_file(url: str, filename: str) -> Path:
    """ファイルをダウンロード"""
    filepath = DATA_DIR / filename
    
    if filepath.exists():
        logger.info(f"File already exists: {filename}")
        return filepath
    
    logger.info(f"Downloading {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    progress = (downloaded / total_size) * 100
                    print(f"\rProgress: {progress:.1f}%", end='')
        
        print()  # New line after progress
        logger.info(f"Downloaded: {filename}")
        return filepath
        
    except requests.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")
        if filepath.exists():
            filepath.unlink()
        raise


def extract_zip(filepath: Path):
    """ZIPファイルを解凍"""
    extract_dir = filepath.parent / filepath.stem
    
    if extract_dir.exists():
        logger.info(f"Already extracted: {filepath.name}")
        return extract_dir
    
    logger.info(f"Extracting {filepath.name}...")
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    logger.info(f"Extracted to: {extract_dir}")
    return extract_dir


def main():
    """メイン処理"""
    ensure_directories()
    
    # 最新リリース情報を取得
    logger.info("Fetching latest release information...")
    release_info = get_latest_release_info()
    
    logger.info(f"Latest release: {release_info['name']} ({release_info['published_at']})")
    
    # アセットURLを取得
    assets = {asset['name']: asset['browser_download_url'] 
              for asset in release_info['assets']}
    
    # 漫画データをダウンロード
    downloaded_files = []
    for dataset in MANGA_DATASETS:
        if dataset not in assets:
            logger.warning(f"Dataset not found in release: {dataset}")
            continue
        
        try:
            filepath = download_file(assets[dataset], dataset)
            downloaded_files.append(filepath)
        except Exception as e:
            logger.error(f"Failed to process {dataset}: {e}")
    
    # 解凍
    for filepath in downloaded_files:
        try:
            extract_zip(filepath)
        except Exception as e:
            logger.error(f"Failed to extract {filepath}: {e}")
    
    logger.info("Download completed!")
    logger.info(f"Data location: {DATA_DIR}")


if __name__ == "__main__":
    main()