
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
from collections import Counter
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


# ============================================================
# Colonnes candidates WTA
# ============================================================
#
# IMPORTANT :
# Les groupes winner/loser et A/B sont traités comme DEUX SOURCES
# INDÉPENDANTES.
#
# Si une ligne possède winner/loser :
#   -> on utilise winner + winner_country + player_id_winner
#   -> on utilise loser  + loser_country  + player_id_loser
#   -> on ne consulte PAS player_a/player_b/PlayerIDA/PlayerIDB
#
# Si winner/loser sont absents :
#   -> on utilise player_a/player_b
#   -> avec country_a/country_b
#   -> et PlayerIDA/PlayerIDB
#
# Aucun mélange entre les deux groupes.
# ============================================================

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
]

WTA_LOSER_COUNTRY_CANDIDATES = [
    "loser_country",
    "country_loser",
]

WTA_WINNER_ID_CANDIDATES = [
    "player_id_winner",
]

WTA_LOSER_ID_CANDIDATES = [
    "player_id_loser",
]

WTA_MATCH_ID_CANDIDATES = [
    "match_id",
    "Match ID",
    "MatchID",
    "MatchId",
    "ls_match_id",
]

WTA_A_NAME_CANDIDATES = [
    "player_a",
    "PlayerNameA",
]

WTA_B_NAME_CANDIDATES = [
    "player_b",
    "PlayerNameB",
]

WTA_A_COUNTRY_CANDIDATES = [
    "country_a",
]

WTA_B_COUNTRY_CANDIDATES = [
    "country_b",
]

WTA_A_ID_CANDIDATES = [
    "PlayerIDA",
    "PlayerIDA2",
]

WTA_B_ID_CANDIDATES = [
    "PlayerIDB",
    "PlayerIDB2",
]


# ============================================================
# Colonnes candidates ATP
# ============================================================
# PARTIE ATP CONSERVÉE
# ============================================================

ATP_WINNER_NAME_CANDIDATES = ["player_winner", "winner_player_name", "winner_name"]
ATP_LOSER_NAME_CANDIDATES = ["player_loser", "loser_player_name", "loser_name"]

ATP_WINNER_COUNTRY_CANDIDATES = ["country_winner", "winner_country"]
ATP_LOSER_COUNTRY_CANDIDATES = ["country_loser", "loser_country"]

ATP_WINNER_ID_CANDIDATES = ["player_id_winner"]
ATP_LOSER_ID_CANDIDATES = ["player_id_loser"]

ATP_MATCH_ID_CANDIDATES = ["match_id", "Match ID", "MatchID", "MatchId"]


# ============================================================
# Utilitaires
# ============================================================

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
    """
    Renvoie la première valeur non nulle trouvée dans row
    pour la liste de colonnes.
    """
    for c in cols:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() != "":
            return str(row[c]).strip()

    return None


def normalize_name(name: str) -> str:
    """Nettoie les espaces superflus."""
    return " ".join(str(name).strip().split())


def normalize_wta_player_id(value: Optional[str]) -> Optional[str]:
    """
    Normalise les IDs WTA.

    Exemple :
        311243.0 -> 311243
        311243   -> 311243
    """
    if value is None:
        return None

    s = str(value).strip()

    if not s:
        return None

    # Evite les IDs du style "311243.0" produits par pandas.
    if re.fullmatch(r"[+-]?\d+\.0+", s):
        return s.split(".", 1)[0]

    return s


def name_key(name: str) -> str:
    """
    Clé canonique pour comparer deux noms.

    Les accents, espaces, ponctuations et majuscules/minuscules
    sont neutralisés.
    """
    return slugify(normalize_name(name))


def most_common_value(counter: Counter) -> Optional[str]:
    """
    Retourne la valeur la plus fréquente.
    En cas d'égalité, Counter conserve l'ordre d'apparition.
    """
    if not counter:
        return None

    return counter.most_common(1)[0][0]


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


# ============================================================
# EXTRACTION WTA
# ============================================================

def gather_players_from_row_wta(
    row: pd.Series,
    row_unique_id: str,
) -> List[Tuple[str, Optional[str], str, Optional[str]]]:
    """
    Extrait les joueuses d'une ligne WTA.

    RÈGLE ABSOLUE :

    SOURCE 1 - winner/loser
    -----------------------
    Dès qu'un nom winner ou loser existe, on travaille UNIQUEMENT
    avec le groupe winner/loser :

        winner
        winner_country
        player_id_winner

        loser
        loser_country
        player_id_loser

    On ne regarde alors PAS :
        player_a
        player_b
        country_a
        country_b
        PlayerIDA
        PlayerIDB

    SOURCE 2 - A/B
    --------------
    Si aucun winner/loser n'est disponible, on travaille
    UNIQUEMENT avec :

        player_a
        country_a
        PlayerIDA

        player_b
        country_b
        PlayerIDB

    Il n'y a volontairement AUCUN mélange entre les deux sources.
    """
    results: List[Tuple[str, Optional[str], str, Optional[str]]] = []

    # --------------------------------------------------------
    # Match ID
    # --------------------------------------------------------
    match_id_val = safe_get_series_val(row, WTA_MATCH_ID_CANDIDATES)
    match_uid = match_id_val if match_id_val is not None else row_unique_id

    # ========================================================
    # SOURCE 1 : WINNER / LOSER
    # ========================================================

    winner_name = safe_get_series_val(
        row,
        WTA_WINNER_NAME_CANDIDATES,
    )

    loser_name = safe_get_series_val(
        row,
        WTA_LOSER_NAME_CANDIDATES,
    )

    # Dès qu'on possède au moins un nom winner/loser,
    # cette ligne est considérée comme une ligne WINNER/LOSER.
    #
    # On ne bascule surtout pas vers A/B pour compléter
    # une information manquante.
    if winner_name or loser_name:

        winner_country = safe_get_series_val(
            row,
            WTA_WINNER_COUNTRY_CANDIDATES,
        )

        loser_country = safe_get_series_val(
            row,
            WTA_LOSER_COUNTRY_CANDIDATES,
        )

        winner_id = normalize_wta_player_id(
            safe_get_series_val(
                row,
                WTA_WINNER_ID_CANDIDATES,
            )
        )

        loser_id = normalize_wta_player_id(
            safe_get_series_val(
                row,
                WTA_LOSER_ID_CANDIDATES,
            )
        )

        if winner_name:
            results.append(
                (
                    normalize_name(winner_name),
                    winner_country,
                    match_uid,
                    winner_id,
                )
            )

        if loser_name:
            results.append(
                (
                    normalize_name(loser_name),
                    loser_country,
                    match_uid,
                    loser_id,
                )
            )

        return results

    # ========================================================
    # SOURCE 2 : PLAYER A / PLAYER B
    # ========================================================
    #
    # On n'arrive ici QUE si winner ET loser sont absents.
    # ========================================================

    name_a = safe_get_series_val(
        row,
        WTA_A_NAME_CANDIDATES,
    )

    name_b = safe_get_series_val(
        row,
        WTA_B_NAME_CANDIDATES,
    )

    if name_a or name_b:

        country_a = safe_get_series_val(
            row,
            WTA_A_COUNTRY_CANDIDATES,
        )

        country_b = safe_get_series_val(
            row,
            WTA_B_COUNTRY_CANDIDATES,
        )

        player_id_a = normalize_wta_player_id(
            safe_get_series_val(
                row,
                WTA_A_ID_CANDIDATES,
            )
        )

        player_id_b = normalize_wta_player_id(
            safe_get_series_val(
                row,
                WTA_B_ID_CANDIDATES,
            )
        )

        if name_a:
            results.append(
                (
                    normalize_name(name_a),
                    country_a,
                    match_uid,
                    player_id_a,
                )
            )

        if name_b:
            results.append(
                (
                    normalize_name(name_b),
                    country_b,
                    match_uid,
                    player_id_b,
                )
            )

        return results

    # Aucun système d'identification fiable sur cette ligne.
    return []


# ============================================================
# AGRÉGATION WTA
# ============================================================

def aggregate_wta_observation(
    observations_by_id: Dict[str, dict],
    observations_by_name: Dict[str, dict],
    name_raw: str,
    country_raw: Optional[str],
    match_uid: str,
    player_id_raw: Optional[str],
) -> None:
    """
    Ajoute une observation WTA aux structures d'agrégation.

    L'objectif est d'éviter qu'une association erronée sur UNE ligne
    écrase le vrai nom d'un joueur sur l'ensemble du JSON.

    Pour chaque ID, on conserve :
      - tous les noms observés et leur fréquence
      - tous les pays observés et leur fréquence
      - tous les matchs

    Le nom final sera celui observé le plus souvent pour cet ID.
    """

    name_norm = normalize_name(name_raw)

    if not name_norm:
        return

    player_id = normalize_wta_player_id(player_id_raw)

    if player_id:
        if player_id not in observations_by_id:
            observations_by_id[player_id] = {
                "names": Counter(),
                "countries": Counter(),
                "match_ids": set(),
            }

        rec = observations_by_id[player_id]

        rec["names"][name_norm] += 1

        if country_raw:
            rec["countries"][str(country_raw).strip()] += 1

        rec["match_ids"].add(str(match_uid))

        return

    # Aucun ID :
    # on agrège par nom comme fallback.
    key = name_key(name_norm)

    if not key:
        return

    if key not in observations_by_name:
        observations_by_name[key] = {
            "names": Counter(),
            "countries": Counter(),
            "match_ids": set(),
        }

    rec = observations_by_name[key]

    rec["names"][name_norm] += 1

    if country_raw:
        rec["countries"][str(country_raw).strip()] += 1

    rec["match_ids"].add(str(match_uid))


def build_wta_players_list(
    observations_by_id: Dict[str, dict],
    observations_by_name: Dict[str, dict],
) -> List[dict]:
    """
    Construit la liste finale des joueuses WTA.

    Étape importante :
    pour chaque player_id, le nom final est le NOM LE PLUS OBSERVÉ
    pour cet ID.

    Cela évite le comportement problématique de l'ancien better_name()
    qui pouvait remplacer :

        Victoria Azarenka

    par :

        Agnieszka Radwanska

    simplement parce que le second nom était plus long.

    Le slug est recalculé à partir du nom canonique.
    """

    players_by_name: Dict[str, dict] = {}

    # ========================================================
    # 1) Joueurs avec ID
    # ========================================================

    for player_id, rec in observations_by_id.items():

        canonical_name = most_common_value(rec["names"])

        if not canonical_name:
            continue

        canonical_country = most_common_value(
            rec["countries"]
        )

        canonical_slug = slugify(canonical_name) or "unknown"

        slug = f"{player_id.lower()}-{canonical_slug}"

        entry = {
            "player_id": player_id,
            "name": canonical_name,
            "slug": slug,
            "page_href": f"players/{slug}",
            "data_path": f"players/data/{slug}.json",
            "country": canonical_country if canonical_country else None,
            "matches_count": len(rec["match_ids"]),
        }

        key = name_key(canonical_name)

        # ----------------------------------------------------
        # Si plusieurs IDs finissent par correspondre au même
        # nom canonique, on garde l'entrée avec le plus grand
        # nombre de matchs et on fusionne les matchs.
        #
        # Ceci gère aussi les éventuels changements / erreurs
        # d'ID dans les données historiques.
        # ----------------------------------------------------
        if key not in players_by_name:
            players_by_name[key] = entry

        else:
            existing = players_by_name[key]

            if entry["matches_count"] > existing["matches_count"]:
                # On conserve l'ID de l'entrée la plus représentative.
                #
                # Les match_ids ne sont pas disponibles ici dans
                # l'objet final, donc on ne les fusionne pas au niveau
                # détaillé. Le but ici est surtout de garantir une
                # joueuse unique dans l'index.
                players_by_name[key] = entry

    # ========================================================
    # 2) Joueurs sans ID
    # ========================================================

    for key, rec in observations_by_name.items():

        canonical_name = most_common_value(rec["names"])

        if not canonical_name:
            continue

        canonical_country = most_common_value(
            rec["countries"]
        )

        stable_id = make_player_id_from_slug(
            slugify(canonical_name)
        )

        canonical_slug = slugify(canonical_name) or "unknown"
        slug = f"{stable_id.lower()}-{canonical_slug}"

        entry = {
            "player_id": stable_id,
            "name": canonical_name,
            "slug": slug,
            "page_href": f"players/{slug}",
            "data_path": f"players/data/{slug}.json",
            "country": canonical_country if canonical_country else None,
            "matches_count": len(rec["match_ids"]),
        }

        # Si un joueur sans ID porte le même nom qu'un joueur
        # possédant déjà un ID, on ne crée pas de deuxième entrée.
        if key not in players_by_name:
            players_by_name[key] = entry

    return list(players_by_name.values())


# ============================================================
# EXTRACTION ATP
# ============================================================
# NE PAS MODIFIER : cette partie fonctionne bien actuellement.
# ============================================================

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


# ============================================================
# Construction de l'index
# ============================================================

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

        # Structures spécifiques au traitement robuste WTA.
        observations_by_id: Dict[str, dict] = {}
        observations_by_name: Dict[str, dict] = {}

    else:
        matches_dir = ATP_MATCHES_DIR
        out_file = ATP_OUT_INDEX_FILE
        extractor = gather_players_from_row_atp

    if not os.path.isdir(matches_dir):
        raise FileNotFoundError(
            f"Directory not found: {matches_dir}"
        )

    file_list = iter_csv_files(matches_dir)

    # ========================================================
    # WTA
    # ========================================================

    if mode == "wta":

        total_rows = 0
        total_observations = 0

        for file_path in file_list:

            df = read_csv_safe(file_path)

            if df is None:
                continue

            for idx, row in df.iterrows():

                total_rows += 1

                row_uid = f"{os.path.basename(file_path)}::{idx}"

                entries = extractor(
                    row,
                    row_uid,
                )

                for (
                    name_raw,
                    country_raw,
                    match_uid,
                    player_id_raw,
                ) in entries:

                    if (
                        not name_raw
                        or str(name_raw).strip() == ""
                    ):
                        continue

                    aggregate_wta_observation(
                        observations_by_id=observations_by_id,
                        observations_by_name=observations_by_name,
                        name_raw=name_raw,
                        country_raw=country_raw,
                        match_uid=match_uid,
                        player_id_raw=player_id_raw,
                    )

                    total_observations += 1

        players_list = build_wta_players_list(
            observations_by_id=observations_by_id,
            observations_by_name=observations_by_name,
        )

        players_list = sorted(
            players_list,
            key=lambda x: (
                -x["matches_count"],
                x["name"].lower(),
            ),
        )

        out = {
            "players": players_list,
            "mode": mode,
        }

        os.makedirs(
            os.path.dirname(out_file),
            exist_ok=True,
        )

        with open(
            out_file,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                out,
                f,
                ensure_ascii=False,
                indent=2,
            )

        # Statistiques utiles pour vérifier le nouveau traitement.
        conflicting_ids = 0

        for rec in observations_by_id.values():
            if len(rec["names"]) > 1:
                conflicting_ids += 1

        print(
            f"[OK] {mode.upper()} index écrit dans "
            f"{out_file} ({len(players_list)} joueuses)."
        )

        print(
            f"[INFO] WTA : {len(file_list)} CSV, "
            f"{total_rows} lignes, "
            f"{total_observations} observations."
        )

        print(
            f"[INFO] WTA : {len(observations_by_id)} IDs joueurs "
            f"identifiés."
        )

        print(
            f"[INFO] WTA : {conflicting_ids} IDs avaient "
            f"plusieurs noms observés ; le nom majoritaire a été retenu."
        )

        return out

    # ========================================================
    # ATP
    # ========================================================
    #
    # Cette partie reprend la logique de construction originale,
    # sans modifier son fonctionnement.
    # ========================================================

    players: Dict[str, dict] = {}

    for file_path in file_list:

        df = read_csv_safe(file_path)

        if df is None:
            continue

        for idx, row in df.iterrows():

            row_uid = f"{os.path.basename(file_path)}::{idx}"

            entries = extractor(
                row,
                row_uid,
            )

            for (
                name_raw,
                country_raw,
                match_uid,
                player_id_raw,
            ) in entries:

                if (
                    not name_raw
                    or str(name_raw).strip() == ""
                ):
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

                    # On garde l'id stable / slug existant, mais on
                    # améliore si possible.
                    players[key]["name"] = better_name(
                        players[key].get("name"),
                        name_raw,
                    )

                    if (
                        not players[key].get("country")
                        and country_raw
                    ):
                        players[key]["country"] = country_raw

                    players[key]["match_ids"].add(
                        str(match_uid)
                    )

    players_list = []

    for v in players.values():

        players_list.append(
            {
                "player_id": v["player_id"],
                "name": v["name"],
                "slug": v["slug"],
                "page_href": v["page_href"],
                "data_path": v["data_path"],
                "country": (
                    v["country"]
                    if v["country"]
                    else None
                ),
                "matches_count": len(v["match_ids"]),
            }
        )

    players_list = sorted(
        players_list,
        key=lambda x: (
            -x["matches_count"],
            x["name"].lower(),
        ),
    )

    out = {
        "players": players_list,
        "mode": mode,
    }

    os.makedirs(
        os.path.dirname(out_file),
        exist_ok=True,
    )

    with open(
        out_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            out,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"[OK] {mode.upper()} index écrit dans "
        f"{out_file} ({len(players_list)} joueurs/joueuses)."
    )

    return out


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construit l'index des joueurs/joueuses WTA/ATP."
        )
    )

    parser.add_argument(
        "--tour",
        choices=["wta", "atp", "all"],
        default="all",
        help=(
            "Mode à construire : wta, atp ou all "
            "(défaut)."
        ),
    )

    args = parser.parse_args()

    try:

        if args.tour in {"wta", "all"}:
            build_index("wta")

        if args.tour in {"atp", "all"}:
            build_index("atp")

    except Exception as e:
        print(
            f"[ERR] Erreur lors de la construction "
            f"de l'index: {e}"
        )


if __name__ == "__main__":
    main()
