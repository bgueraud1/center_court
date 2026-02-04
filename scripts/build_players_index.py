#!/usr/bin/env python3
"""
build_players_index.py

Parcourt tous les CSV dans matches/wta_matches/ et génère index/players_wta_index.json
contenant la liste des joueuses trouvées et le nombre de matches par joueuse.

Usage:
    python build_players_index.py
"""
from __future__ import annotations
import os
import json
import hashlib
import unicodedata
import re
from typing import Dict, Set, Tuple, Optional
import pandas as pd
import numpy as np

# Répertoires (modifiable si nécessaire)
MATCHES_DIR = os.path.join("matches", "wta_matches")
OUT_INDEX_DIR = "docs/index"
OUT_INDEX_FILE = os.path.join(OUT_INDEX_DIR, "players_wta_index.json")

# Colonnes candidates pour noms / pays / match id (ordre d'essai)
NAME_COL_CANDIDATES = [
    "winner", "loser",
    "winner_player_name", "loser_player_name",
    "player_a", "player_b",
    "PlayerNameA", "PlayerNameB",
    "Home Player", "Away Player",
    "HomePlayer", "AwayPlayer"
]

COUNTRY_COL_CANDIDATES = [
    "winner_country", "loser_country",
    "country_a", "country_b",
    "PlayerCountryA", "PlayerCountryB",
    "Home Country", "Away Country"
]

MATCH_ID_CANDIDATES = [
    "match_id", "Match ID", "MatchID", "MatchId", "ls_match_id"
]

def slugify(name: str) -> str:
    """Transforme un nom en slug sûre: remove accents, keep alnum + '-'."""
    if name is None:
        return ""
    s = str(name).strip().lower()
    # normalize accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # replace non-alnum by hyphen
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def make_player_id_from_slug(slug_name: str) -> str:
    """Crée un id stable à partir du slug. Ex: W + md5(slug)[:6].upper()."""
    h = hashlib.md5(slug_name.encode("utf-8")).hexdigest()[:6].upper()
    return f"W{h}"

def safe_get_series_val(row: pd.Series, cols: list) -> Optional[str]:
    """Renvoie la première valeur non-nulle trouvée dans row pour la liste de colonnes."""
    for c in cols:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
            return str(row[c])
    return None

def gather_players_from_row(row: pd.Series, row_unique_id: str) -> list[Tuple[str, Optional[str], str]]:
    """
    Retourne liste de tuples (name, country, match_unique_id) extraits d'une ligne.
    - match_unique_id = match_id s'il existe, sinon row_unique_id (file+index)
    """
    results = []
    # determine match_id (if any)
    match_id_val = safe_get_series_val(row, MATCH_ID_CANDIDATES)
    if match_id_val is not None:
        match_uid = str(match_id_val)
    else:
        match_uid = row_unique_id

    # Try to map player columns pairs (player_a -> country_a, player_b -> country_b)
    # If both player_a/player_b exist, prefer them (pairing)
    # Otherwise fallback to winner/loser pair
    if ("player_a" in row.index or "PlayerNameA" in row.index) and ("player_b" in row.index or "PlayerNameB" in row.index):
        # player A
        name_a = safe_get_series_val(row, ["player_a", "PlayerNameA", "PlayerNameFirstA", "PlayerNameLastA"])
        country_a = safe_get_series_val(row, ["country_a", "PlayerCountryA"])
        if name_a:
            results.append((name_a.strip(), country_a.strip() if country_a else None, match_uid))
        # player B
        name_b = safe_get_series_val(row, ["player_b", "PlayerNameB", "PlayerNameFirstB", "PlayerNameLastB"])
        country_b = safe_get_series_val(row, ["country_b", "PlayerCountryB"])
        if name_b:
            results.append((name_b.strip(), country_b.strip() if country_b else None, match_uid))
        return results

    # fallback: winner / loser columns
    name_w = safe_get_series_val(row, ["winner", "winner_player_name", "winner_name"])
    country_w = safe_get_series_val(row, ["winner_country"]) or safe_get_series_val(row, ["country_a"])  # try best-effort
    if name_w:
        results.append((name_w.strip(), country_w.strip() if country_w else None, match_uid))
    name_l = safe_get_series_val(row, ["loser", "loser_player_name", "loser_name"])
    country_l = safe_get_series_val(row, ["loser_country"]) or safe_get_series_val(row, ["country_b"])
    if name_l:
        results.append((name_l.strip(), country_l.strip() if country_l else None, match_uid))

    # if nothing found, try any candidate name cols
    if not results:
        for c in NAME_COL_CANDIDATES:
            if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
                results.append((str(row[c]).strip(), None, match_uid))
    return results

def read_csv_safe(path: str) -> Optional[pd.DataFrame]:
    """Lit un csv de façon robuste. Retourne DataFrame ou None."""
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception as e:
        try:
            # second attempt with different engine
            return pd.read_csv(path, engine="python", low_memory=False)
        except Exception as e2:
            print(f"[WARN] Impossible de lire {path}: {e2}")
            return None

def build_index(matches_dir: str = MATCHES_DIR, out_file: str = OUT_INDEX_FILE) -> dict:
    """
    Parcourt tous les CSV dans matches_dir et construit la structure players.
    Retourne le dict complet et écrit out_file.
    """
    # stockage temporaire: slug_base -> player info
    # structure: slug_base -> { 'player_id':..., 'slug':..., 'name':..., 'country':..., 'match_ids': set(...) }
    players: Dict[str, dict] = {}

    if not os.path.isdir(matches_dir):
        raise FileNotFoundError(f"Directory not found: {matches_dir}")

    # list csv files (only top-level)
    file_list = [os.path.join(matches_dir, f) for f in os.listdir(matches_dir) if f.lower().endswith(".csv") and os.path.isfile(os.path.join(matches_dir, f))]
    # iterate files
    for file_path in sorted(file_list):
        df = read_csv_safe(file_path)
        if df is None:
            continue
        # iterate rows
        for idx, row in df.iterrows():
            # unique fallback id if match_id absent: file + row index
            row_uid = f"{os.path.basename(file_path)}::{idx}"
            entries = gather_players_from_row(row, row_uid)
            for name_raw, country_raw, match_uid in entries:
                if not name_raw or str(name_raw).strip() == "":
                    continue
                # normalize name for keying
                name_norm = " ".join(str(name_raw).strip().split())  # collapse whitespace
                slug_name_only = slugify(name_norm)
                slug_base = slug_name_only if slug_name_only else slugify(name_norm or "unknown")
                # generate stable player_id from slug base
                player_id = make_player_id_from_slug(slug_base)
                slug = f"{player_id.lower()}-{slug_base}" if slug_base else f"{player_id.lower()}-player"
                # key players by slug (stable)
                key = slug
                if key not in players:
                    players[key] = {
                        "player_id": player_id,
                        "name": name_norm,
                        "slug": slug,
                        "page_href": f"players_wta/{slug}",
                        "data_path": f"players_wta/data/{slug}.json",
                        "country": country_raw if country_raw else None,
                        "match_ids": set()
                    }
                # update country if missing and we have it now
                if not players[key].get("country") and country_raw:
                    players[key]["country"] = country_raw
                # increment match set
                players[key]["match_ids"].add(str(match_uid))

    # build final list (convert sets -> counts)
    players_list = []
    for key, v in players.items():
        players_list.append({
            "player_id": v["player_id"],
            "name": v["name"],
            "slug": v["slug"],
            "page_href": v["page_href"],
            "data_path": v["data_path"],
            "country": v["country"] if v["country"] else None,
            "matches_count": len(v["match_ids"])
        })

    # Optionnel: trier par matches_count desc puis name
    players_list = sorted(players_list, key=lambda x: (-x["matches_count"], x["name"].lower()))

    # Préparer structure finale
    out = {"players": players_list}

    # ensure out dir
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    # write JSON
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return out

def main():
    try:
        out = build_index()
        print(f"[OK] Index construit et écrit dans {OUT_INDEX_FILE} ({len(out['players'])} joueuses).")
    except Exception as e:
        print(f"[ERR] Erreur lors de la construction de l'index: {e}")

if __name__ == "__main__":
    main()
