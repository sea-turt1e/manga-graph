#!/usr/bin/env python
"""Generate Japanese-labelled MAL taxonomies from mal_taxonomies_raw.json.

Steps:
1. Normalize broken keys produced by `extract_mal_taxonomies.py`.
2. Apply English->Japanese mappings for genres/themes/demographic.
3. Handle serialization with special rules:
   - Known major magazines get explicit Japanese labels.
   - Others fall back to original name (or a simple cleaned version).

Usage:
  uv run python scripts/data_import/generate_mal_taxonomies_ja.py \
    --in data/myanimelist/mal_taxonomies_raw.json \
    --out data/myanimelist/mal_taxonomies_ja.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate JA labels for MAL taxonomies")
    parser.add_argument(
        "--in",
        dest="input",
        type=str,
        default="data/myanimelist/mal_taxonomies_raw.json",
        help="Input raw taxonomy JSON path",
    )
    parser.add_argument(
        "--out",
        dest="output",
        type=str,
        default="data/myanimelist/mal_taxonomies_ja.json",
        help="Output JA taxonomy JSON path",
    )
    return parser.parse_args()


# --- helpers for parsing the slightly broken keys -----------------------------------------

import ast


def try_parse_collection(text: str) -> Optional[Any]:
    """Best-effort parse of MAL-style serialized lists.

    Examples of input:
      '["Action", "Adventure"]'
      '"Adventure"'
      '[]'
    """

    if not text:
        return None
    text = text.strip()
    if not text:
        return None

    # First try JSON-like parsing via ast.literal_eval (more forgiving for quotes)
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        # Try to coerce into a list if it looks like JSON-ish
        if text.startswith("[") and text.endswith("]"):
            inner = text[1:-1].strip()
            if not inner:
                return []
            parts = [p.strip().strip("\"'") for p in inner.split(",")]
            return [p for p in parts if p]
        # Fallback: just treat as plain string
        return text.strip("\"'")


def normalize_token_list_value(raw: str) -> List[str]:
    """Normalize a raw cell into a list of clean tokens.

    Used for genres/themes and for demographic (which can be multi-valued).
    """

    if not isinstance(raw, str) or not raw:
        return []

    parsed = try_parse_collection(raw)
    if parsed is None:
        return []

    if isinstance(parsed, str):
        parsed = [parsed]

    if not isinstance(parsed, list):
        return []

    result: List[str] = []
    for v in parsed:
        if not isinstance(v, str):
            continue
        name = " ".join(v.strip().split())
        if not name or name == "[]":
            continue
        result.append(name)
    return result


def normalize_single_or_list_value(raw: str) -> List[str]:
    """Like normalize_token_list_value, but accepts single non-list tokens.

    This is mainly for `serialization`, where the current raw JSON keys
    often look like '["Young Animal"]' etc.
    """

    if not isinstance(raw, str) or not raw:
        return []

    parsed = try_parse_collection(raw)
    if parsed is None:
        return []

    if isinstance(parsed, str):
        parsed = [parsed]

    if not isinstance(parsed, list):
        return []

    result: List[str] = []
    for v in parsed:
        if not isinstance(v, str):
            continue
        name = " ".join(v.strip().split())
        if not name or name == "[]":
            continue
        result.append(name)
    return result


# --- English -> Japanese base mappings ----------------------------------------------------

GENRE_JA_MAP: Dict[str, str] = {
    "Action": "アクション",
    "Adventure": "アドベンチャー",
    "Avant Garde": "アバンギャルド",
    "Award Winning": "受賞作",
    "Boys Love": "ボーイズラブ（BL）",
    "Comedy": "コメディ",
    "Drama": "ドラマ",
    "Ecchi": "エッチ・サービス要素",
    "Erotica": "エロティック",
    "Fantasy": "ファンタジー",
    "Girls Love": "ガールズラブ（GL／百合）",
    "Gourmet": "グルメ・料理",
    "Hentai": "成人向け（H）",
    "Horror": "ホラー",
    "Mystery": "ミステリー",
    "Romance": "恋愛",
    "Sci-Fi": "SF（サイエンスフィクション）",
    "Slice of Life": "日常",
    "Sports": "スポーツ",
    "Supernatural": "オカルト・超自然",
    "Suspense": "サスペンス",
}

THEME_JA_MAP: Dict[str, str] = {
    "Adult Cast": "大人のキャスト",
    "Anthropomorphic": "擬人化",
    "CGDCT": "日常系・ゆるふわ女子（CGDCT）",
    "Childcare": "育児・子育て",
    "Combat Sports": "格闘技",
    "Crossdressing": "女装・男装",
    "Delinquents": "不良・ヤンキー",
    "Detective": "探偵・推理",
    "Educational": "教育・学習",
    "Gag Humor": "ギャグ・ナンセンス",
    "Gore": "流血・グロテスク",
    "Harem": "ハーレム",
    "High Stakes Game": "命懸けゲーム・ギャンブル",
    "Historical": "歴史・時代物",
    "Idols (Female)": "女性アイドル",
    "Idols (Male)": "男性アイドル",
    "Isekai": "異世界",
    "Iyashikei": "癒やし系",
    "Love Polygon": "三角関係・多角関係",
    "Love Status Quo": "現状維持な恋愛関係",
    "Magical Sex Shift": "性別変化（TS）",
    "Mahou Shoujo": "魔法少女",
    "Martial Arts": "武術・格闘技",
    "Mecha": "ロボット・メカ",
    "Medical": "医療・病院",
    "Memoir": "自伝・回想録",
    "Military": "ミリタリー・戦争",
    "Music": "音楽",
    "Mythology": "神話・伝承",
    "Organized Crime": "マフィア・組織犯罪",
    "Otaku Culture": "オタク文化",
    "Parody": "パロディ",
    "Performing Arts": "舞台芸術・芸能",
    "Pets": "ペット・動物",
    "Psychological": "心理サスペンス",
    "Racing": "レース",
    "Reincarnation": "転生",
    "Reverse Harem": "逆ハーレム",
    "Samurai": "サムライ",
    "School": "学園・学校",
    "Showbiz": "芸能界・ショービジネス",
    "Space": "宇宙",
    "Strategy Game": "戦略ゲーム・ボードゲーム",
    "Super Power": "超能力",
    "Survival": "サバイバル",
    "Team Sports": "チームスポーツ",
    "Time Travel": "タイムトラベル",
    "Urban Fantasy": "現代ファンタジー",
    "Vampire": "吸血鬼",
    "Video Game": "ゲーム世界・ゲーム題材",
    "Villainess": "悪役令嬢",
    "Visual Arts": "美術・アート",
    "Workplace": "職場・お仕事",
}

DEMOGRAPHIC_JA_MAP: Dict[str, str] = {
    "Josei": "女性向け（レディース／女性一般）",
    "Kids": "子供向け",
    "Seinen": "青年向け",
    "Shoujo": "少女向け",
    "Shounen": "少年向け",
}


# 代表的な雑誌の個別マッピング（必要に応じて拡張可能）
SERIALIZATION_KNOWN_JA_MAP: Dict[str, str] = {
    "Weekly Shounen Jump": "週刊少年ジャンプ",
    "Shounen Jump (Weekly)": "週刊少年ジャンプ",
    "Shounen Jump": "少年ジャンプ",
    "Shounen Magazine (Weekly)": "週刊少年マガジン",
    "Shounen Magazine": "少年マガジン",
    "Shounen Sunday": "週刊少年サンデー",
    "Young Jump": "ヤングジャンプ",
    "Young Jump (Weekly)": "ヤングジャンプ",
    "Young Animal": "ヤングアニマル",
    "Young Gangan": "ヤングガンガン",
    "Young King": "ヤングキング",
    "Ultra Jump": "ウルトラジャンプ",
    "Big Comic Spirits": "ビッグコミックスピリッツ",
    "Big Comic Original": "ビッグコミックオリジナル",
    "Morning": "モーニング",
    "Afternoon": "アフタヌーン",
    "Evening": "イブニング",
    "Hana to Yume": "花とゆめ",
    "LaLa": "LaLa（ララ）",
    "Margaret": "マーガレット",
    "Bessatsu Margaret": "別冊マーガレット",
    "Ribon": "りぼん",
    "Nakayoshi": "なかよし",
    "Comic HOTMILK": "COMIC ホットミルク",
}


def translate_label(label: str, mapping: Dict[str, str]) -> str:
    """Translate single English label to Japanese using mapping.

    If not found, return the original label.
    """

    return mapping.get(label, label)


def process_genres(raw_genres: Dict[str, str]) -> Dict[str, str]:
    seen: Set[str] = set()
    result: Dict[str, str] = {}
    for raw_key in raw_genres.keys():
        labels = normalize_token_list_value(raw_key)
        for label in labels:
            if label in seen:
                continue
            seen.add(label)
            result[label] = translate_label(label, GENRE_JA_MAP)
    return result


def process_themes(raw_themes: Dict[str, str]) -> Dict[str, str]:
    seen: Set[str] = set()
    result: Dict[str, str] = {}
    for raw_key in raw_themes.keys():
        labels = normalize_token_list_value(raw_key)
        for label in labels:
            if label in seen:
                continue
            seen.add(label)
            result[label] = translate_label(label, THEME_JA_MAP)
    return result


def process_demographic(raw_demo: Dict[str, str]) -> Dict[str, str]:
    seen: Set[str] = set()
    result: Dict[str, str] = {}
    for raw_key in raw_demo.keys():
        labels = normalize_token_list_value(raw_key)
        for label in labels:
            if label in seen:
                continue
            seen.add(label)
            result[label] = translate_label(label, DEMOGRAPHIC_JA_MAP)
    return result


def clean_serialization_name(name: str) -> str:
    """Light normalization for magazine names.

    We keep them mostly as-is, only collapsing whitespace.
    """

    return " ".join(name.strip().split())


def process_serialization(raw_ser: Dict[str, str]) -> Dict[str, str]:
    seen: Set[str] = set()
    result: Dict[str, str] = {}

    for raw_key in raw_ser.keys():
        labels = normalize_single_or_list_value(raw_key)
        for label in labels:
            cleaned = clean_serialization_name(label)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            ja = SERIALIZATION_KNOWN_JA_MAP.get(cleaned, cleaned)
            result[cleaned] = ja

    return result


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_path}")

    with input_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    raw_ser = raw.get("serialization", {}) or {}
    raw_genres = raw.get("genres", {}) or {}
    raw_themes = raw.get("themes", {}) or {}
    raw_demo = raw.get("demographic", {}) or {}

    ser_ja = process_serialization(raw_ser)
    genres_ja = process_genres(raw_genres)
    themes_ja = process_themes(raw_themes)
    demo_ja = process_demographic(raw_demo)

    output: Dict[str, Dict[str, str]] = {
        "serialization": ser_ja,
        "genres": genres_ja,
        "themes": themes_ja,
        "demographic": demo_ja,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written Japanese taxonomies to {output_path}")
    print(
        "Counts:",
        f"serialization={len(ser_ja)}",
        f"genres={len(genres_ja)}",
        f"themes={len(themes_ja)}",
        f"demographic={len(demo_ja)}",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
