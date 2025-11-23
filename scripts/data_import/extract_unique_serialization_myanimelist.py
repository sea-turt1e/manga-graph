#!/usr/bin/env python
"""
Extract unique serialization (magazine) names from MyAnimeList CSV.

Output example (serialization_unique.json):
[
  "Young Animal",
  "Ultra Jump",
  "Morning",
  "Shounen Jump (Weekly)",
  "Big Comic Original",
  "Afternoon",
  "Shounen Gangan",
  ...
]
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, List, Optional, Set

import pandas as pd

CSV_PATH = Path("data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv")
OUTPUT_PATH = Path("data/myanimelist/serialization_unique.json")


def try_parse_collection(text: str) -> Optional[Any]:
    if not text:
        return None
    text = text.strip()
    if not text or text[0] not in "[{":
        return None
    # MAL風の二重クォートを軽く補正
    normalized = text.replace('""', '"')
    try:
        return json.loads(normalized)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(normalized)
        except (ValueError, SyntaxError):
            return None


def normalize_list_field(value: str) -> List[str]:
    parsed = try_parse_collection(value)
    if parsed is None:
        return []
    if isinstance(parsed, str):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []

    results: List[str] = []
    for v in parsed:
        if not isinstance(v, str):
            continue
        name = " ".join(v.strip().split())
        if not name:
            continue
        results.append(name)
    return results


def main() -> int:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)

    mags: Set[str] = set()
    for raw in df.get("serialization", []):
        if not isinstance(raw, str) or not raw:
            continue
        for name in normalize_list_field(raw):
            mags.add(name)

    sorted_mags = sorted(mags)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(sorted_mags, f, ensure_ascii=False, indent=2)

    print(f"Found {len(sorted_mags)} unique serialization names.")
    print(f"Written to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())