
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
#
# IMPORTANT :
# Les colonnes winner/loser sont indépendantes de player_a/player_b.
#
# Exemple :
#   winner = Petra Martic
#   player_a = Marketa Vondrousova
#
# Donc PlayerIDA ne doit PAS être utilisé comme ID du winner
# simplement parce qu'il s'agit du joueur A.
#
# On utilise les colonnes explicites winner/loser en priorité.
#
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

# Fallback si winner/loser sont absents.
# Dans ce cas, on travaille bien en paire A/B.
WTA_NAME_PAIR_CANDIDATES = [
    ("player_a", "player_b"),
    ("PlayerNameA", "PlayerNameB"),
]


# -----------------------------
# Colonnes candidates ATP
# -----------------------------
# NE PAS MODIFIER : cette partie fonctionne correctement.
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


def normalize_wta_player_id(value: Optional[str]) -> Optional[str]:
    """
    Normalise un ID joueur WTA provenant de pandas.

    Exemple :
        "311243.0"  -> "311243"
        "311243.00" -> "311243"
        "311243"    -> "311243"

    On ne modifie rien d'autre afin de préserver les éventuels formats
    particuliers des identifiants.
    """
    if value is None:
        return None

    s = str(value).strip()

    if not s:
        return None

    # Cas classique causé par pandas lorsque la colonne contient des NaN :
    # 311243 devient parfois 311243.0
    if re.fullmatch(r"[+-]?\d+\.0+", s):
        return s.split(".", 1)[0]

    return s


def names_match(name1: Optional[str], name2: Optional[str]) -> bool:
    """
    Compare deux noms de manière robuste.

    On utilise le slug pour neutraliser :
      - majuscules/minuscules
      - accents
      - espaces
      - ponctuation
    """
    if not name1 or not name2:
        return False

    return slugify(normalize_name(name1)) == slugify(normalize_name(name2))


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


# ============================================================
# Extraction WTA
# ============================================================

def get_wta_ab_participants(row: pd.Series) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Récupère les informations player A / player B du CSV WTA.

    IMPORTANT :
    A/B n'indique PAS winner/loser.

    Exemple :
        player_a = Marketa Vondrousova
        player_b = Petra Martic

        alors que :
        winner = Petra Martic
        loser  = Marketa Vondrousova

    On conserve donc A/B comme un système indépendant.
    """
    for name_a_col, name_b_col in WTA_NAME_PAIR_CANDIDATES:
        if name_a_col not in row.index and name_b_col not in row.index:
            continue

        name_a = safe_get_series_val(row, [name_a_col])
        name_b = safe_get_series_val(row, [name_b_col])

        if not name_a and not name_b:
            continue

        country_a = safe_get_series_val(row, ["country_a"])
        country_b = safe_get_series_val(row, ["country_b"])

        player_id_a = normalize_wta_player_id(
            safe_get_series_val(row, ["PlayerIDA", "PlayerIDA2"])
        )

        player_id_b = normalize_wta_player_id(
            safe_get_series_val(row, ["PlayerIDB", "PlayerIDB2"])
        )

        return {
            "a": {
                "name": name_a,
                "country": country_a,
                "player_id": player_id_a,
            },
            "b": {
                "name": name_b,
                "country": country_b,
                "player_id": player_id_b,
            },
        }

    return {}


def find_wta_ab_participant(
    ab_participants: Dict[str, Dict[str, Optional[str]]],
    target_name: Optional[str],
) -> Optional[Dict[str, Optional[str]]]:
    """
    Retrouve A ou B en comparant son nom avec target_name.

    On ne se base JAMAIS simplement sur la position A/B.
    """
    if not target_name:
        return None

    for side in ("a", "b"):
        participant = ab_participants.get(side)

        if not participant:
            continue

        participant_name = participant.get("name")

        if names_match(target_name, participant_name):
            return participant

    return None


def gather_players_from_row_wta(
    row: pd.Series, row_unique_id: str
) -> List[Tuple[str, Optional[str], str, Optional[str]]]:
    """
    Retourne une liste de tuples:
        (name, country, match_uid, player_id_raw)

    Logique WTA corrigée :

    1) winner / loser sont les références principales.

    2) Pour winner :
         - nom    -> winner / winner_player_name / ...
         - pays   -> winner_country / country_winner
         - ID     -> player_id_winner

    3) Pour loser :
         - nom    -> loser / loser_player_name / ...
         - pays   -> loser_country / country_loser
         - ID     -> player_id_loser

    4) Si un ID ou un pays winner/loser manque :
         on peut chercher A/B, MAIS uniquement en faisant
         correspondre le NOM de winner/loser avec player_a/player_b.

         Ainsi :
             winner = Petra Martic
             player_a = Marketa Vondrousova
             player_b = Petra Martic

         donnera bien Petra -> PlayerIDB,
         et non Petra -> PlayerIDA.

    5) Si winner/loser sont totalement absents :
         fallback A/B classique avec leurs propres IDs/pays.
    """
    results: List[Tuple[str, Optional[str], str, Optional[str]]] = []

    # --------------------------------------------------------
    # Identifiant du match
    # --------------------------------------------------------
    match_id_val = safe_get_series_val(row, WTA_MATCH_ID_CANDIDATES)
    match_uid = match_id_val if match_id_val is not None else row_unique_id

    # --------------------------------------------------------
    # Informations winner / loser
    # --------------------------------------------------------
    winner_name = safe_get_series_val(row, WTA_WINNER_NAME_CANDIDATES)
    loser_name = safe_get_series_val(row, WTA_LOSER_NAME_CANDIDATES)

    # IMPORTANT :
    # On ne prend PLUS country_a pour le winner,
    # ni country_b pour le loser.
    winner_country = safe_get_series_val(
        row,
        WTA_WINNER_COUNTRY_CANDIDATES,
    )

    loser_country = safe_get_series_val(
        row,
        WTA_LOSER_COUNTRY_CANDIDATES,
    )

    # IMPORTANT :
    # On ne prend PLUS PlayerIDA pour le winner,
    # ni PlayerIDB pour le loser sans vérification.
    winner_id = normalize_wta_player_id(
        safe_get_series_val(row, WTA_WINNER_ID_CANDIDATES)
    )

    loser_id = normalize_wta_player_id(
        safe_get_series_val(row, WTA_LOSER_ID_CANDIDATES)
    )

    # --------------------------------------------------------
    # Informations A/B
    # --------------------------------------------------------
    ab_participants = get_wta_ab_participants(row)

    # --------------------------------------------------------
    # Si winner est présent, on complète les infos manquantes
    # en retrouvant le bon joueur A ou B PAR SON NOM.
    # --------------------------------------------------------
    if winner_name:
        ab_winner = find_wta_ab_participant(
            ab_participants,
            winner_name,
        )

        if ab_winner:
            if winner_id is None:
                winner_id = normalize_wta_player_id(
                    ab_winner.get("player_id")
                )

            if winner_country is None:
                winner_country = ab_winner.get("country")

        results.append(
            (
                winner_name,
                winner_country,
                match_uid,
                winner_id,
            )
        )

    # --------------------------------------------------------
    # Même logique pour le loser
    # --------------------------------------------------------
    if loser_name:
        ab_loser = find_wta_ab_participant(
            ab_participants,
            loser_name,
        )

        if ab_loser:
            if loser_id is None:
                loser_id = normalize_wta_player_id(
                    ab_loser.get("player_id")
                )

            if loser_country is None:
                loser_country = ab_loser.get("country")

        results.append(
            (
                loser_name,
                loser_country,
                match_uid,
                loser_id,
            )
        )

    # --------------------------------------------------------
    # Si on a trouvé au moins une joueuse via winner/loser,
    # c'est suffisant : on ne bascule pas vers un fallback
    # qui risquerait d'inventer une association.
    # --------------------------------------------------------
    if results:
        return results

    # ========================================================
    # FALLBACK WTA : winner/loser totalement absents
    # ========================================================
    #
    # Dans ce cas, A et B sont utilisés directement avec
    # leurs propres informations.
    #
    # A -> PlayerIDA + country_a
    # B -> PlayerIDB + country_b
    #
    # Ici seulement, la correspondance A/B est directe.
    #
    # ========================================================
    if ab_participants:
        participant_a = ab_participants.get("a")
        participant_b = ab_participants.get("b")

        if participant_a and participant_a.get("name"):
            results.append(
                (
                    str(participant_a["name"]),
                    participant_a.get("country"),
                    match_uid,
                    normalize_wta_player_id(
                        participant_a.get("player_id")
                    ),
                )
            )

        if participant_b and participant_b.get("name"):
            results.append(
                (
                    str(participant_b["name"]),
                    participant_b.get("country"),
                    match_uid,
                    normalize_wta_player_id(
                        participant_b.get("player_id")
                    ),
                )
            )

        if results:
            return results

    # --------------------------------------------------------
    # Dernier recours :
    # n'importe quelle colonne plausible contenant un nom.
    # Aucun ID artificiel ni pays artificiel n'est inventé ici.
    # --------------------------------------------------------
    fallback_cols = (
        WTA_WINNER_NAME_CANDIDATES
        + WTA_LOSER_NAME_CANDIDATES
        + [c for pair in WTA_NAME_PAIR_CANDIDATES for c in pair]
    )

    for c in fallback_cols:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip() != "":
            results.append(
                (
                    str(row[c]).strip(),
                    None,
                    match_uid,
                    None,
                )
            )
            break

    return results


# ============================================================
# Extraction ATP
# ============================================================
# PARTIE ATP CONSERVÉE TELLE QUELLE
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
    else:
        matches_dir = ATP_MATCHES_DIR
        out_file = ATP_OUT_INDEX_FILE
        extractor = gather_players_from_row_atp

    if not os.path.isdir(matches_dir):
        raise FileNotFoundError(
            f"Directory not found: {matches_dir}"
        )

    players: Dict[str, dict] = {}

    file_list = iter_csv_files(matches_dir)

    for file_path in file_list:
        df = read_csv_safe(file_path)

        if df is None:
            continue

        for idx, row in df.iterrows():
            row_uid = f"{os.path.basename(file_path)}::{idx}"
            entries = extractor(row, row_uid)

            for (
                name_raw,
                country_raw,
                match_uid,
                player_id_raw,
            ) in entries:

                if not name_raw or str(name_raw).strip() == "":
                    continue

                #
                # Pour WTA uniquement, on normalise une dernière fois
                # l'ID au cas où il serait passé sous forme "12345.0".
                #
                if mode == "wta":
                    player_id_raw = normalize_wta_player_id(
                        player_id_raw
                    )

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
                    # On garde l'id stable / slug existant,
                    # mais on améliore le nom si possible.
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
