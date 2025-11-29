#!/usr/bin/env python
"""
Extract unique values for serialization, genres, themes, demographic
from MAL manga_entries.csv and dump them as intermediate JSON.

Usage:
  uv run python scripts/data_import/extract_mal_taxonomies.py \
    --csv data/myanimelist/myanimelist-scraped-data-2025-July/manga_entries.csv \
    --out data/myanimelist/mal_taxonomies_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Set

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract unique MAL taxonomies")
    parser.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Path to manga_entries.csv",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="data/myanimelist/mal_taxonomies_raw.json",
        help="Output JSON file path",
    )
    return parser.parse_args()


def split_multi_value_cell(value: str) -> Set[str]:
    """Split a multi-valued cell (comma/semicolon-separated) into a set of cleaned tokens."""
    if not value or not isinstance(value, str):
        return set()

    parts = []
    for chunk in value.replace(";", ",").split(","):
        item = chunk.strip()
        if item:
            parts.append(item)
    return set(parts)


def main() -> None:
    args = parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    # NOTE: If this is too large for memory, we can switch to chunksize later.
    df = pd.read_csv(csv_path)

    # Adjust these column names if your CSV differs
    serialization_col = "serialization"
    genres_col = "genres"
    themes_col = "themes"
    demographic_col = "demographic"

    for col in [serialization_col, genres_col, themes_col, demographic_col]:
        if col not in df.columns:
            raise KeyError(f"Expected column '{col}' not found in CSV. Actual columns: {list(df.columns)}")

    serializations: Set[str] = set()
    genres: Set[str] = set()
    themes: Set[str] = set()
    demographics: Set[str] = set()

    for _, row in df.iterrows():
        # serialization & demographic are assumed single-valued
        s_raw = row.get(serialization_col)
        d_raw = row.get(demographic_col)

        s_val = str(s_raw).strip() if s_raw is not None and str(s_raw).strip().lower() != "nan" else ""
        if s_val:
            serializations.add(s_val)

        d_val = str(d_raw).strip() if d_raw is not None and str(d_raw).strip().lower() != "nan" else ""
        if d_val:
            demographics.add(d_val)

        # genres & themes are potentially multi-valued
        g_raw = row.get(genres_col)
        t_raw = row.get(themes_col)

        g_val = "" if g_raw is None or str(g_raw).strip().lower() == "nan" else str(g_raw)
        t_val = "" if t_raw is None or str(t_raw).strip().lower() == "nan" else str(t_raw)

        genres.update(split_multi_value_cell(g_val))
        themes.update(split_multi_value_cell(t_val))

    data: Dict[str, Dict[str, str]] = {
        "serialization": {name: "" for name in sorted(serializations)},
        "genres": {name: "" for name in sorted(genres)},
        "themes": {name: "" for name in sorted(themes)},
        "demographic": {name: "" for name in sorted(demographics)},
    }

    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Extracted taxonomies to {out_path}")
    print(
        "Counts:",
        f"serialization={len(serializations)},",
        f"genres={len(genres)},",
        f"themes={len(themes)},",
        f"demographic={len(demographics)}",
    )


if __name__ == "__main__":
    main()
