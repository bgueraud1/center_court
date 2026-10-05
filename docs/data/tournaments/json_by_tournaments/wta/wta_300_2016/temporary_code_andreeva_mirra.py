#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd
import pycountry
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


MIRRA_NAME = "Mirra Andreeva"
START_YEAR = 2020

# IOC / tennis codes where ISO3 != IOC code, or where we want a tennis-specific code
ISO3_TO_IOC = {
    "BGR": "BUL",
    "CHE": "SUI",
    "DEU": "GER",
    "DNK": "DEN",
    "GRC": "GRE",
    "HRV": "CRO",
    "NLD": "NED",
    "PRT": "POR",
    "SVN": "SLO",
    "ZAF": "RSA",
    "URY": "URU",
    "TWN": "TPE",
    "IRN": "IRI",
    "CYP": "CYP",
    "CZE": "CZE",
    "ROU": "ROU",
    "SRB": "SRB",
    "SVK": "SVK",
    "HKG": "HKG",
    "KOR": "KOR",
    "PRK": "PRK",
    "MDA": "MDA",
    "BLR": "BLR",
    "KAZ": "KAZ",
    "GEO": "GEO",
    "ARM": "ARM",
    "EST": "EST",
    "LVA": "LAT",
    "LTU": "LTU",
    "LUX": "LUX",
    "MON": "MON",
    "AUS": "AUS",
    "AUT": "AUT",
    "BEL": "BEL",
    "BRA": "BRA",
    "CAN": "CAN",
    "CHN": "CHN",
    "ESP": "ESP",
    "FIN": "FIN",
    "FRA": "FRA",
    "GBR": "GBR",
    "HUN": "HUN",
    "ITA": "ITA",
    "JPN": "JPN",
    "MEX": "MEX",
    "NOR": "NOR",
    "POL": "POL",
    "RUS": "RUS",
    "SWE": "SWE",
    "TUR": "TUR",
    "USA": "USA",
    "UKR": "UKR",
    "SGP": "SGP",
    "THA": "THA",
    "PHI": "PHI",
    "IND": "IND",
    "ARG": "ARG",
    "CHI": "CHI",
    "COL": "COL",
    "PER": "PER",
    "VEN": "VEN",
    "ECU": "ECU",
    "PUR": "PUR",
    "KSA": "KSA",
    "UAE": "UAE",
    "QAT": "QAT",
    "ISR": "ISR",
    "EGY": "EGY",
    "MAR": "MAR",
    "TUN": "TUN",
    "RSA": "RSA",
    "SUI": "SUI",
    "GER": "GER",
    "DEN": "DEN",
    "GRE": "GRE",
    "CRO": "CRO",
    "POR": "POR",
    "NED": "NED",
    "SLO": "SLO",
    "BUL": "BUL",
    "LAT": "LAT",
    "TPE": "TPE",
    "IRI": "IRI",
}

# Fallbacks on country names if reverse geocoding returns a name instead of an ISO code
COUNTRY_NAME_TO_IOC = {
    "ukraine": "UKR",
    "united states": "USA",
    "united states of america": "USA",
    "us": "USA",
    "usa": "USA",
    "united kingdom": "GBR",
    "great britain": "GBR",
    "england": "GBR",
    "scotland": "GBR",
    "wales": "GBR",
    "northern ireland": "GBR",
    "czech republic": "CZE",
    "czechia": "CZE",
    "russia": "RUS",
    "russian federation": "RUS",
    "south korea": "KOR",
    "republic of korea": "KOR",
    "north korea": "PRK",
    "democratic people's republic of korea": "PRK",
    "taiwan": "TPE",
    "chinese taipei": "TPE",
    "switzerland": "SUI",
    "netherlands": "NED",
    "germany": "GER",
    "denmark": "DEN",
    "greece": "GRE",
    "croatia": "CRO",
    "portugal": "POR",
    "slovenia": "SLO",
    "south africa": "RSA",
    "uruguay": "URU",
    "bulgaria": "BUL",
    "france": "FRA",
    "spain": "ESP",
    "italy": "ITA",
    "poland": "POL",
    "japan": "JPN",
    "china": "CHN",
    "canada": "CAN",
    "australia": "AUS",
    "austria": "AUT",
    "belgium": "BEL",
    "brazil": "BRA",
    "finland": "FIN",
    "hungary": "HUN",
    "romania": "ROU",
    "serbia": "SRB",
    "slovakia": "SVK",
    "sweden": "SWE",
    "turkey": "TUR",
    "moldova": "MDA",
    "belarus": "BLR",
    "kazakhstan": "KAZ",
    "georgia": "GEO",
    "armenia": "ARM",
    "estonia": "EST",
    "latvia": "LAT",
    "lithuania": "LTU",
    "luxembourg": "LUX",
    "monaco": "MON",
    "hong kong": "HKG",
    "india": "IND",
    "thailand": "THA",
    "philippines": "PHI",
    "argentina": "ARG",
    "chile": "CHI",
    "colombia": "COL",
    "peru": "PER",
    "venezuela": "VEN",
    "ecuador": "ECU",
    "puerto rico": "PUR",
    "saudi arabia": "KSA",
    "united arab emirates": "UAE",
    "qatar": "QAT",
    "israel": "ISR",
    "egypt": "EGY",
    "morocco": "MAR",
    "tunisia": "TUN",
    "nigeria": "NGR",
    "south sudan": "SSD",
    "costa rica": "CRC",
    "mexico": "MEX",
    "ireland": "IRL",
    "norway": "NOR",
}

WTA_FOLDER_RE = re.compile(r"^wta_(\d+)_(\d{4})$", re.IGNORECASE)


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text.strip().lower())


def folder_year(folder: Path) -> int | None:
    m = WTA_FOLDER_RE.match(folder.name)
    return int(m.group(2)) if m else None


def ioc_from_iso3(iso3: str | None) -> str | None:
    if not iso3:
        return None
    iso3 = iso3.upper()
    return ISO3_TO_IOC.get(iso3, iso3)


def ioc_from_country_name(country_name: str | None) -> str | None:
    if not country_name:
        return None
    key = normalize_text(country_name)
    if key in COUNTRY_NAME_TO_IOC:
        return COUNTRY_NAME_TO_IOC[key]

    try:
        country = pycountry.countries.lookup(country_name)
        return ioc_from_iso3(country.alpha_3)
    except LookupError:
        return None


def ioc_from_iso2(iso2: str | None) -> str | None:
    if not iso2:
        return None
    iso2 = iso2.upper()
    country = pycountry.countries.get(alpha_2=iso2)
    if not country:
        return None
    return ioc_from_iso3(country.alpha_3)


def extract_opponent(match: dict) -> tuple[str | None, str | None, str | None]:
    w_name = match.get("winner_player_name")
    l_name = match.get("loser_player_name")
    w_country = match.get("winner_country")
    l_country = match.get("loser_country")

    if normalize_text(w_name) == normalize_text(MIRRA_NAME):
        return l_name, l_country, "winner"
    if normalize_text(l_name) == normalize_text(MIRRA_NAME):
        return w_name, w_country, "loser"
    return None, None, None


@lru_cache(maxsize=10000)
def country_from_geocode(lat: float, lon: float) -> tuple[str | None, str | None]:
    """
    Returns (tournament_country_ioc, tournament_country_name).
    Uses Nominatim reverse geocoding and caches results.
    """
    geolocator = Nominatim(user_agent="mirra_andreeva_match_extractor")
    reverse = RateLimiter(
        geolocator.reverse,
        min_delay_seconds=1.0,
        max_retries=2,
        error_wait_seconds=2,
        swallow_exceptions=True,
    )

    location = reverse((lat, lon), exactly_one=True, language="en", zoom=3)
    if not location:
        return None, None

    raw = getattr(location, "raw", {}) or {}
    address = raw.get("address", {}) or {}
    country_name = address.get("country")
    country_code = address.get("country_code")

    if country_code:
        return ioc_from_iso2(country_code), country_name

    return ioc_from_country_name(country_name), country_name


def parse_geocode(meta: dict) -> tuple[float | None, float | None]:
    geocode = meta.get("geocode")
    if not isinstance(geocode, (list, tuple)) or len(geocode) != 2:
        return None, None
    try:
        return float(geocode[0]), float(geocode[1])
    except (TypeError, ValueError):
        return None, None


def process_tournament_json(json_path: Path) -> list[dict]:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    meta = data.get("meta", {}) or {}
    matches = data.get("matches", []) or []

    year = meta.get("year")
    try:
        year = int(year)
    except (TypeError, ValueError):
        year = folder_year(json_path.parent)

    if year is None or year < START_YEAR:
        return []

    tourney_id = meta.get("tourney_id")
    tourney_name = meta.get("tourney_name")
    start_date = meta.get("start_date")
    surface = meta.get("surface")
    level = meta.get("level")
    lat, lon = parse_geocode(meta)

    tournament_country_ioc = None
    tournament_country_name = None
    if lat is not None and lon is not None:
        tournament_country_ioc, tournament_country_name = country_from_geocode(lat, lon)

    rows = []
    for match in matches:
        opponent_name, opponent_country, mirra_side = extract_opponent(match)
        if opponent_name is None:
            continue

        opponent_country = (opponent_country or "").strip().upper() or None
        if not opponent_country:
            continue

        is_ukrainian = opponent_country == "UKR"
        is_local = (
            tournament_country_ioc is not None
            and opponent_country == tournament_country_ioc
        )

        if not (is_ukrainian or is_local):
            continue

        if is_ukrainian and is_local:
            relation = "ukrainian_and_local"
        elif is_ukrainian:
            relation = "ukrainian_opponent"
        else:
            relation = "local_opponent"

        rows.append(
            {
                "source_file": str(json_path),
                "tournament_folder": json_path.parent.name,
                "tourney_id": tourney_id,
                "year": year,
                "tourney_name": tourney_name,
                "start_date": start_date,
                "surface": surface,
                "level": level,
                "geocode_lat": lat,
                "geocode_lon": lon,
                "tournament_country_ioc": tournament_country_ioc,
                "tournament_country_name": tournament_country_name,
                "match_id": match.get("match_id"),
                "round": match.get("round"),
                "mirra_side": mirra_side,
                "mirra_player_name": MIRRA_NAME,
                "opponent_player_name": opponent_name,
                "opponent_country": opponent_country,
                "relation": relation,
                "score_string": match.get("score_string"),
                "winner_player_name": match.get("winner_player_name"),
                "loser_player_name": match.get("loser_player_name"),
                "player_id_winner": match.get("player_id_winner"),
                "player_id_loser": match.get("player_id_loser"),
                "winner_seed": match.get("winner_seed"),
                "loser_seed": match.get("loser_seed"),
            }
        )

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Extrait les matchs de Mirra Andreeva (contre joueuses ukrainiennes ou locales) à partir de 2020."
    )
    parser.add_argument(
        "root_folder",
        help="Chemin du dossier racine contenant les sous-dossiers wta_XXX_YYYY",
    )
    parser.add_argument(
        "--output",
        default="mirra_andreeva_ukr_local_matches.csv",
        help="Nom du fichier de sortie CSV",
    )
    args = parser.parse_args()

    root = Path(args.root_folder).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Dossier introuvable ou invalide : {root}")

    json_files = list(root.rglob("tournament.json"))
    all_rows = []

    for json_file in json_files:
        # On ne garde que les dossiers WTA format wta_XXX_YYYY
        if not WTA_FOLDER_RE.match(json_file.parent.name):
            continue
        try:
            all_rows.extend(process_tournament_json(json_file))
        except Exception as e:
            print(f"[WARN] Impossible de traiter {json_file} : {e}")

    df = pd.DataFrame(all_rows)

    if not df.empty:
        sort_cols = [c for c in ["year", "tourney_id", "tourney_name", "match_id"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    output_path = Path(args.output).expanduser().resolve()
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Fichier écrit : {output_path}")
    print(f"Nombre de lignes : {len(df)}")
    if not df.empty:
        print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()