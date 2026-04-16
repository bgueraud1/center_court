#!/usr/bin/env python3
"""
build_players_index.py

Construit un index des joueuses/joueurs à partir des CSV WTA/ATP.

Modes:
    - wta : lit docs/matches/wta_matches/ et écrit docs/index/players_wta_index.json
    - atp : lit docs/matches/atp_matches/ et écrit docs/index/players_atp_index.json
    - all : construit les deux

Usage:
    python build_players_index.py --tour wta
    python build_players_index.py --tour atp
    python build_players_index.py --tour all
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# Configuration des dossiers
# -----------------------------
WTA_MATCHES_DIR = os.path.join("docs", "matches", "wta_matches")
ATP_MATCHES_DIR = os.path.join("docs", "matches", "atp_matches")

OUT_INDEX_DIR = os.path.join("docs", "index")
WTA_OUT_INDEX_FILE = os.path.join(OUT_INDEX_DIR, "players_wta_index.json")
ATP_OUT_INDEX_FILE = os.path.join(OUT_INDEX_DIR, "players_atp_index.json")


# -----------------------------
# Colonnes candidates WTA
# -----------------------------
# On privilégie les colonnes explicites winner/loser, puis on garde des fallback.
WTA_WINNER_NAME_CANDIDATES = [
    "winner",
    "winner_player_name",
    "winner_name",
    "player_winner",
]

WTA_LOSER_NAME_CANDIDATES = [
    "loser",
    "loser_player_name",
    "loser_name",
    "player_loser",
]

WTA_WINNER_COUNTRY_CANDIDATES = [
    "winner_country",
    "country_winner",
    "country_a",
]

WTA_LOSER_COUNTRY_CANDIDATES = [
    "loser_country",
    "country_loser",
    "country_b",
]

WTA_WINNER_ID_CANDIDATES = [
    "player_id_winner",
    "PlayerIDA",
    "PlayerIDA2",
]

WTA_LOSER_ID_CANDIDATES = [
    "player_id_loser",
    "PlayerIDB",
    "PlayerIDB2",
]

WTA_MATCH_ID_CANDIDATES = ["match_id", "Match ID", "MatchID", "MatchId", "ls_match_id"]

# Fallback si winner/loser sont absents
WTA_NAME_PAIR_CANDIDATES = [
    ("player_a", "player_b"),
    ("PlayerNameA", "PlayerNameB"),
]


# -----------------------------
# Colonnes candidates ATP
# -----------------------------
ATP_WINNER_NAME_CANDIDATES = ["player_winner", "winner_player_name", "winner_name"]
ATP_LOSER_NAME_CANDIDATES = ["player_loser", "loser_player_name", "loser_name"]

ATP_WINNER_COUNTRY_CANDIDATES = ["country_winner", "winner_country"]
ATP_LOSER_COUNTRY_CANDIDATES = ["country_loser", "loser_country"]

ATP_WINNER_ID_CANDIDATES = ["player_id_winner"]
ATP_LOSER_ID_CANDIDATES = ["player_id_loser"]

ATP_MATCH_ID_CANDIDATES = ["match_id", "Match ID", "MatchID", "MatchId"]


# -----------------------------
# Utilitaires
# -----------------------------
def slugify(name: str) -> str:
    """Transforme un nom en slug sûre: minuscules, sans accents, alnum + '-'."""
    if name is None:
        return ""
    s = str(name).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def make_player_id_from_slug(slug_name: str) -> str:
    """Crée un id stable à partir du slug, utilisé seulement en fallback."""
    h = hashlib.md5(slug_name.encode("utf-8")).hexdigest()[:6].upper()
    return f"W{h}"


def safe_get_series_val(row: pd.Series, cols: List[str]) -> Optional[str]:
    """Renvoie la première valeur non nulle trouvée dans row pour la liste de colonnes."""
    for c in cols:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() != "":
            return str(row[c]).strip()
    return None


def normalize_name(name: str) -> str:
    """Nettoie les espaces superflus."""
    return " ".join(str(name).strip().split())


def iter_csv_files(root_dir: str) -> List[str]:
    """Retourne tous les CSV sous root_dir, de façon récursive."""
    files: List[str] = []
    for base, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith(".csv"):
                files.append(os.path.join(base, filename))
    return sorted(files)


def read_csv_safe(path: str) -> Optional[pd.DataFrame]:
    """Lit un CSV de façon robuste. Retourne DataFrame ou None."""
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        try:
            return pd.read_csv(path, engine="python", low_memory=False)
        except Exception as e2:
            print(f"[WARN] Impossible de lire {path}: {e2}")
            return None


def better_name(current: Optional[str], candidate: str) -> str:
    """
    Garde le nom le plus utile.
    En pratique, on préfère souvent le plus long, car il est plus complet.
    """
    candidate = normalize_name(candidate)
    if not current:
        return candidate
    if len(candidate) > len(current):
        return candidate
    return current


def build_entry(
    *,
    mode: str,
    name_raw: str,
    country_raw: Optional[str],
    match_uid: str,
    player_id_raw: Optional[str] = None,
) -> Tuple[str, Dict]:
    """
    Construit la clé interne + l'objet joueur.
    - Si player_id_raw existe, on l'utilise comme identifiant stable.
    - Sinon, on fabrique un id de fallback basé sur le nom.
    """
    name_norm = normalize_name(name_raw)
    slug_base = slugify(name_norm) or "unknown"

    if player_id_raw and str(player_id_raw).strip():
        stable_id = str(player_id_raw).strip()
        key = f"pid::{stable_id}"
        slug = f"{stable_id.lower()}-{slug_base}"
    else:
        stable_id = make_player_id_from_slug(slug_base)
        key = f"name::{slug_base}"
        slug = f"{stable_id.lower()}-{slug_base}"

    if mode == "wta":
        page_href = f"players/{slug}"
        data_path = f"players/data/{slug}.json"
    else:
        page_href = f"players_atp/{slug}"
        data_path = f"players_atp/data/{slug}.json"

    entry = {
        "player_id": stable_id,
        "name": name_norm,
        "slug": slug,
        "page_href": page_href,
        "data_path": data_path,
        "country": country_raw if country_raw else None,
        "match_ids": {str(match_uid)},
    }
    return key, entry


# -----------------------------
# Extraction WTA
# -----------------------------
def gather_players_from_row_wta(
    row: pd.Series, row_unique_id: str
) -> List[Tuple[str, Optional[str], str, Optional[str]]]:
    """
    Retourne une liste de tuples:
        (name, country, match_uid, player_id_raw)

    WTA:
      - priorité à winner / loser
      - utilisation de player_id_winner / player_id_loser
      - fallback sur player_a / player_b si nécessaire
    """
    results: List[Tuple[str, Optional[str], str, Optional[str]]] = []

    match_id_val = safe_get_series_val(row, WTA_MATCH_ID_CANDIDATES)
    match_uid = match_id_val if match_id_val is not None else row_unique_id

    # 1) Cas normal : winner / loser explicites
    winner_name = safe_get_series_val(row, WTA_WINNER_NAME_CANDIDATES)
    loser_name = safe_get_series_val(row, WTA_LOSER_NAME_CANDIDATES)

    winner_country = safe_get_series_val(row, WTA_WINNER_COUNTRY_CANDIDATES)
    loser_country = safe_get_series_val(row, WTA_LOSER_COUNTRY_CANDIDATES)

    winner_id = safe_get_series_val(row, WTA_WINNER_ID_CANDIDATES)
    loser_id = safe_get_series_val(row, WTA_LOSER_ID_CANDIDATES)

    if winner_name:
        results.append((winner_name, winner_country, match_uid, winner_id))
    if loser_name:
        results.append((loser_name, loser_country, match_uid, loser_id))

    if results:
        return results

    # 2) Fallback : player_a / player_b
    for name_a_col, name_b_col in WTA_NAME_PAIR_CANDIDATES:
        if name_a_col in row.index and name_b_col in row.index:
            name_a = safe_get_series_val(row, [name_a_col])
            name_b = safe_get_series_val(row, [name_b_col])

            country_a = safe_get_series_val(row, ["country_a", "winner_country", "country_winner"])
            country_b = safe_get_series_val(row, ["country_b", "loser_country", "country_loser"])

            if name_a:
                results.append((name_a, country_a, match_uid, None))
            if name_b:
                results.append((name_b, country_b, match_uid, None))

            if results:
                return results

    # 3) Dernier recours : n'importe quelle colonne plausible
    fallback_cols = (
        WTA_WINNER_NAME_CANDIDATES
        + WTA_LOSER_NAME_CANDIDATES
        + [c for pair in WTA_NAME_PAIR_CANDIDATES for c in pair]
    )
    for c in fallback_cols:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() != "":
            results.append((str(row[c]).strip(), None, match_uid, None))
            break

    return results


# -----------------------------
# Extraction ATP
# -----------------------------
def gather_players_from_row_atp(
    row: pd.Series, row_unique_id: str
) -> List[Tuple[str, Optional[str], str, Optional[str]]]:
    """
    Retourne une liste de tuples:
        (name, country, match_uid, player_id_raw)

    ATP:
      - winner/loser explicites
      - utilise player_id_winner / player_id_loser comme identifiant stable
    """
    results: List[Tuple[str, Optional[str], str, Optional[str]]] = []

    match_id_val = safe_get_series_val(row, ATP_MATCH_ID_CANDIDATES)
    match_uid = match_id_val if match_id_val is not None else row_unique_id

    # Winner
    name_w = safe_get_series_val(row, ATP_WINNER_NAME_CANDIDATES)
    country_w = safe_get_series_val(row, ATP_WINNER_COUNTRY_CANDIDATES)
    player_id_w = safe_get_series_val(row, ATP_WINNER_ID_CANDIDATES)

    if name_w:
        results.append((name_w, country_w, match_uid, player_id_w))

    # Loser
    name_l = safe_get_series_val(row, ATP_LOSER_NAME_CANDIDATES)
    country_l = safe_get_series_val(row, ATP_LOSER_COUNTRY_CANDIDATES)
    player_id_l = safe_get_series_val(row, ATP_LOSER_ID_CANDIDATES)

    if name_l:
        results.append((name_l, country_l, match_uid, player_id_l))

    return results


# -----------------------------
# Construction de l'index
# -----------------------------
def build_index(mode: str) -> dict:
    """
    Construit l'index pour un mode donné ("wta" ou "atp").
    Écrit le JSON dans le bon fichier et renvoie le dict final.
    """
    mode = mode.lower().strip()
    if mode not in {"wta", "atp"}:
        raise ValueError("mode doit être 'wta' ou 'atp'")

    if mode == "wta":
        matches_dir = WTA_MATCHES_DIR
        out_file = WTA_OUT_INDEX_FILE
        extractor = gather_players_from_row_wta
    else:
        matches_dir = ATP_MATCHES_DIR
        out_file = ATP_OUT_INDEX_FILE
        extractor = gather_players_from_row_atp

    if not os.path.isdir(matches_dir):
        raise FileNotFoundError(f"Directory not found: {matches_dir}")

    players: Dict[str, dict] = {}

    file_list = iter_csv_files(matches_dir)
    for file_path in file_list:
        df = read_csv_safe(file_path)
        if df is None:
            continue

        for idx, row in df.iterrows():
            row_uid = f"{os.path.basename(file_path)}::{idx}"
            entries = extractor(row, row_uid)

            for name_raw, country_raw, match_uid, player_id_raw in entries:
                if not name_raw or str(name_raw).strip() == "":
                    continue

                key, entry = build_entry(
                    mode=mode,
                    name_raw=name_raw,
                    country_raw=country_raw,
                    match_uid=match_uid,
                    player_id_raw=player_id_raw,
                )

                if key not in players:
                    players[key] = entry
                else:
                    # On garde l'id stable / slug existant, mais on améliore si possible
                    players[key]["name"] = better_name(players[key].get("name"), name_raw)

                    if not players[key].get("country") and country_raw:
                        players[key]["country"] = country_raw

                    players[key]["match_ids"].add(str(match_uid))

    players_list = []
    for v in players.values():
        players_list.append(
            {
                "player_id": v["player_id"],
                "name": v["name"],
                "slug": v["slug"],
                "page_href": v["page_href"],
                "data_path": v["data_path"],
                "country": v["country"] if v["country"] else None,
                "matches_count": len(v["match_ids"]),
            }
        )

    players_list = sorted(players_list, key=lambda x: (-x["matches_count"], x["name"].lower()))

    out = {"players": players_list, "mode": mode}

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[OK] {mode.upper()} index écrit dans {out_file} ({len(players_list)} joueurs/joueuses).")
    return out


def main():
    parser = argparse.ArgumentParser(description="Construit l'index des joueurs/joueuses WTA/ATP.")
    parser.add_argument(
        "--tour",
        choices=["wta", "atp", "all"],
        default="all",
        help="Mode à construire : wta, atp ou all (défaut).",
    )
    args = parser.parse_args()

    try:
        if args.tour in {"wta", "all"}:
            build_index("wta")
        if args.tour in {"atp", "all"}:
            build_index("atp")
    except Exception as e:
        print(f"[ERR] Erreur lors de la construction de l'index: {e}")


if __name__ == "__main__":
    main()