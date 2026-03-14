#!/usr/bin/env python3
"""
scripts/build_tournament_jsons.py

But :
- lire la liste de CSV fournie (created_files.txt)
- pour chaque CSV, parser les lignes (WTA ou ATP)
- grouper les matches par tournoi (event_id / event_year)
- pour chaque tournoi :
    * créer un répertoire de sortie: <out_base>/{atp|wta}/{atp|wta}_{event_id}_{event_year}/
    * écrire un JSON par match (nommé <match_id>.json) contenant toutes les colonnes de la ligne
    * créer un JSON global du tournoi avec la structure demandée :
        { "meta": {...}, "matches": [ {match summary...}, ... ] }
    * ajouter un champ "geocode": [lat, lon] dans meta si on trouve un mapping
Usage:
  python3 scripts/build_tournament_jsons.py --input-list created_files.txt \
      --geocodes docs/tools/geocodes_combined.json \
      --out-base docs/data/tournaments/json_by_tournaments
"""
import argparse
import json
import os
from pathlib import Path
import pandas as pd
import re

def read_geocodes(path):
    """
    Lecture robuste de docs/tools/geocodes_combined.json.
    - accepte ces formes :
      { "geocode": { "City, Country": [lat, lon], ... } }
      ou top-level mapping { "City, Country": [lat,lon], ... }
      ou nested mapping under any top-level key.
    - lève une erreur claire si aucun mapping n'est trouvé.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Geocodes file not found: {path}")

    with path.open('r', encoding='utf-8') as fh:
        try:
            j = json.load(fh)
        except Exception as e:
            raise RuntimeError(f"Failed to parse JSON from {path}: {e}")

    if j is None:
        raise ValueError(f"Geocodes JSON {path} parsed to null (None). Check file contents.")

    # 1) case: { "geocode": { ... } }
    if isinstance(j, dict):
        ge = j.get('geocode')
        if isinstance(ge, dict) and ge:
            return {k: tuple(v) for k, v in ge.items()}

        # 2) case: top-level mapping of keys -> [lat,lon]
        candidates = {k: tuple(v) for k, v in j.items() if isinstance(v, (list, tuple)) and len(v) >= 2}
        if candidates:
            return candidates

        # 3) case: nested under another top-level key (search)
        for v in j.values():
            if isinstance(v, dict):
                cand = {k: tuple(val) for k, val in v.items() if isinstance(val, (list, tuple)) and len(val) >= 2}
                if cand:
                    return cand

    # nothing found
    raise ValueError(
        f"Could not find geocode mapping in {path}. "
        "Expected structure: { 'geocode': { 'City, Country': [lat, lon], ... } } "
        "or top-level mapping. Inspect the file."
    )

def _first_of(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def normalize_str(v):
    if pd.isna(v):
        return None
    if v is None:
        return None
    return str(v).strip()

def find_geocode_for_city_name(geocode_map, city_name):
    """Try to match city_name (token) to keys in geocode_map.
       We'll do a case-insensitive substring match on the geocode keys.
    """
    if not city_name:
        return None, None
    token = re.sub(r"[\.\'\"]", "", city_name.lower()).strip()
    # direct exact-key match first
    for k in geocode_map:
        if k.lower() == token:
            return geocode_map[k], k
    # substring contains:
    for k in geocode_map:
        if token in k.lower():
            return geocode_map[k], k
    # try token split (last word)
    last = token.split()[-1]
    for k in geocode_map:
        if last in k.lower():
            return geocode_map[k], k
    return None, None

def choose_city_for_canada(year):
    # per your rules:
    # - year >= 2022: Montreal if even, Toronto if odd
    # - year <= 2019: Toronto if even, Montreal if odd
    # - years 2020-2021: fallback to Montreal
    y = int(year)
    if y >= 2022:
        return "Montreal, QC, CA" if (y % 2 == 0) else "Toronto, ON, CA"
    if y <= 2019:
        return "Toronto, ON, CA" if (y % 2 == 0) else "Montreal, QC, CA"
    return "Montreal, QC, CA"

def tournament_has_no_location(tname):
    # Finals / Next Gen / Masters Cup -> no location
    lowered = (tname or "").lower()
    keywords = ["finals", "final", "nitto atp finals", "masters cup", "tour world championship", "next gen", "intesa sanpaolo next gen", "next gen atp"]
    return any(k in lowered for k in keywords)

def extract_city_from_atp_tourney_name(tourney_name):
    if not tourney_name:
        return None
    name = tourney_name.strip()
    # Normalize common prefixes
    name = re.sub(r"^(atp|wta)\s+", "", name, flags=re.I).strip()
    # Remove "ATP Masters 1000" prefix etc
    name = re.sub(r"^masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*", "", name, flags=re.I)
    name = re.sub(r"^wta\s*", "", name, flags=re.I)
    # If name contains commas, often the city is first token
    if ',' in name:
        return name.split(',')[0].strip()
    # often last word is city (e.g., "ATP Masters 1000 Madrid")
    parts = name.split()
    # if last token is a country-like word (e.g., "Canada"), return the full name (special handled elsewhere)
    if parts[-1].lower() == "canada":
        return "Canada"
    return parts[-1].strip()

def safe_makedirs(p: Path):
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

def json_serialize_safe(obj):
    # pandas/numpy safe conversion for json.dump
    if isinstance(obj, (pd.Timestamp, )):
        return str(obj)
    return obj

# ---- Main processing ----
def process_csv_file(csv_path: Path, geos, out_base: Path, report):
    # read CSV with pandas (strings to preserve IDs)
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])
    except Exception as e:
        report['errors'].append(f"Failed reading {csv_path}: {e}")
        return

    cols = set(df.columns.tolist())

    # Determine tour type heuristically
    # WTA csvs have 'tourney_id' or 'tourney_year' or 'tournament_name' etc used by your example
    is_wta = False
    if 'tourney_id' in cols or 'tourney_year' in cols or 'tournament_name' in cols:
        is_wta = True
    elif 'event_id' in cols or 'event_year' in cols or 'tourney_name' in cols:
        is_wta = False
    else:
        # fallback: check file path
        is_wta = 'wta' in csv_path.parts or 'WTA' in csv_path.name.lower()

    tour_label = 'wta' if is_wta else 'atp'

    # find relevant column names by trying multiple aliases
    col_event_id = _first_of(cols, ['event_id', 'tourney_id', 'tournament_id', 'tournamentId'])
    col_event_year = _first_of(cols, ['event_year', 'tourney_year', 'year'])
    col_tourney_name = _first_of(cols, ['tourney_name', 'tournament_name', 'tourney'])
    col_city = _first_of(cols, ['city', 'venue_city', 'tourney_city'])
    col_country = _first_of(cols, ['country', 'country_a', 'country_b', 'country_winner'])

    # iterate rows
    for _, row in df.iterrows():
        # normalize keys
        event_id = normalize_str(row.get(col_event_id)) or ""
        event_year = normalize_str(row.get(col_event_year)) or ""
        tourney_name = normalize_str(row.get(col_tourney_name)) or ""
        city_val = normalize_str(row.get(col_city))
        country_val = normalize_str(row.get(col_country))

        if not event_id or not event_year:
            # skip rows without tournament identity
            report['warnings'].append(f"{csv_path}: skipping row without event_id/event_year (tourney_name='{tourney_name}')")
            continue

        # target dir
        folder_name = f"{tour_label}_{event_id}_{event_year}"
        out_dir = out_base.joinpath(tour_label, folder_name)
        safe_makedirs(out_dir)

        # write match-level json: use the full row dict
        row_dict = {k: (None if pd.isna(v) or v == "" else v) for k, v in row.items()}
        match_id = row_dict.get('match_id') or row_dict.get('matchid') or row_dict.get('match') or f"row_{_}"
        match_file = out_dir.joinpath(f"{match_id}.json")

        with open(match_file, 'w', encoding='utf-8') as fh:
            json.dump(row_dict, fh, ensure_ascii=False, default=json_serialize_safe, indent=2)

        # accumulate for tournament summary
        key = (tour_label, event_id, event_year, tourney_name, city_val, country_val)
        if key not in report['tournaments']:
            report['tournaments'][key] = {'rows': [], 'out_dir': out_dir, 'tourney_name': tourney_name, 'city': city_val, 'country': country_val, 'tour_label': tour_label, 'event_id': event_id, 'event_year': event_year}
        report['tournaments'][key]['rows'].append(row_dict)
        report['produced_match_files'].append(str(match_file))

def build_tournament_jsons(report, geos):
    # for each grouped tournament, build tournament JSON
    results = []
    for key, info in report['tournaments'].items():
        tour_label, event_id, event_year, tourney_name, city_val, country_val = key
        rows = info['rows']
        out_dir = info['out_dir']

        # meta fields (try to pick from first row)
        first = rows[0]

        meta = {
            "source": tour_label.upper(),
            "tourney_id": event_id,
            "year": int(event_year) if str(event_year).isdigit() else event_year,
            "tourney_name": tourney_name or normalize_str(first.get('tourney_name') or first.get('tournament_name') or ""),
            "tourney_title": normalize_str(first.get('tournament_title') or first.get('tourney_title') or ""),
            "surface": normalize_str(first.get('surface') or first.get('Surface') or ""),
            "level": normalize_str(first.get('level') or ""),
            "prize_money": normalize_str(first.get('prize_money') or ""),
            "prize_money_currency": normalize_str(first.get('prize_money_currency') or "")
        }

        # attempt singles_draw_size
        singles = first.get('singles_draw_size') or first.get('singles_draw') or first.get('singles_drawsize')
        if singles:
            try:
                meta['singles_draw_size'] = int(singles)
            except:
                meta['singles_draw_size'] = singles

        # city + country
        # WTA: prefer explicit city column
        city_name = city_val or normalize_str(first.get('city') or first.get('venue_city') or "")
        country_name = country_val or normalize_str(first.get('country') or "")

        # For ATP, if city_name empty, try to extract from tourney_name
        if tour_label == 'atp' and (not city_name):
            # special-case Canada
            tn = meta['tourney_name'] or ""
            if "canada" in tn.lower():
                fallback_city = choose_city_for_canada(meta['year'])
                city_name = fallback_city.split(',')[0]
            else:
                extracted = extract_city_from_atp_tourney_name(tn)
                city_name = extracted or city_name

        meta['city'] = city_name or ""
        meta['country'] = country_name or ""

        # geocode lookup
        geocode = None
        matched_key = None
        if meta['city']:
            geocode, matched_key = find_geocode_for_city_name(geos, meta['city'])
        # if still no geocode and meta.tourney_name mentions 'Canada', choose Montreal/Toronto key
        if not geocode and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            geocode = geos.get(special_key)
            matched_key = special_key if geocode else matched_key

        if geocode:
            meta['geocode'] = list(geocode)
            # try to fill country from matched_key last token if not present
            if not meta['country']:
                # extract last token after comma
                if isinstance(matched_key, str) and ',' in matched_key:
                    meta['country'] = matched_key.split(',')[-1].strip()

        # build matches array (reduced summary as requested)
        matches_arr = []
        for r in rows:
            # helpers to find winner/loser/ids
            winner = (r.get('winner_player_name') or r.get('winner') or r.get('player_winner') or r.get('player_a') or r.get('player_a') or "")
            loser = (r.get('loser_player_name') or r.get('loser') or r.get('player_loser') or r.get('player_b') or "")
            pid_w = (r.get('player_id_winner') or r.get('player_a_id') or r.get('playerida') or r.get('PlayerIDA') or r.get('player_a_id') or "")
            pid_l = (r.get('player_id_loser') or r.get('player_b_id') or r.get('playeridb') or r.get('PlayerIDB') or "")
            winner_country = (r.get('winner_country') or r.get('winner_country') or r.get('country_winner') or r.get('country_a') or "")
            loser_country = (r.get('loser_country') or r.get('country_loser') or r.get('country_b') or "")
            winner_seed = r.get('winner_seed') or r.get('seed_a') or r.get('seed_winner') or ""
            loser_seed = r.get('loser_seed') or r.get('seed_b') or r.get('seed_loser') or ""
            match_id = r.get('match_id') or r.get('matchid') or ""
            round_ = r.get('round') or r.get('match_round') or r.get('round_name') or ""
            score = r.get('score_string') or r.get('score') or r.get('set1_score') or ""

            matches_arr.append({
                "match_id": match_id,
                "round": round_,
                "winner_player_name": winner,
                "loser_player_name": loser,
                "score_string": score,
                "player_id_winner": pid_w,
                "player_id_loser": pid_l,
                "winner_country": winner_country,
                "loser_country": loser_country,
                "winner_seed": winner_seed,
                "loser_seed": loser_seed
            })

        tournament_json = {
            "meta": meta,
            "matches": matches_arr
        }

        # write tournament JSON
        tour_json_path = out_dir.joinpath("tournament.json")
        with open(tour_json_path, 'w', encoding='utf-8') as fh:
            json.dump(tournament_json, fh, ensure_ascii=False, indent=2)

        report['produced_tournament_files'].append(str(tour_json_path))
        results.append((out_dir, tour_json_path, len(matches_arr)))

    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-list", "-i", required=True, help="text file listing CSV paths (created_files.txt)")
    ap.add_argument("--geocodes", "-g", required=True, help="path to docs/tools/geocodes_combined.json")
    ap.add_argument("--out-base", "-o", required=True, help="base output directory for per-tournament jsons (e.g. docs/data/tournaments/json_by_tournaments)")
    args = ap.parse_args()

    inp = Path(args.input_list)
    if not inp.exists():
        print(f"[ERROR] input list {inp} not found.")
        raise SystemExit(2)

    geos = read_geocodes(args.geocodes)
    out_base = Path(args.out_base)
    safe_makedirs(out_base)

    report = {
        'tournaments': {}, # key -> rows
        'produced_match_files': [],
        'produced_tournament_files': [],
        'errors': [],
        'warnings': []
    }

    # read list file
    with inp.open('r', encoding='utf-8') as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            p = Path(ln)
            # some entries may be absolute or relative; try both
            if not p.exists():
                # try relative to workspace
                rp = Path.cwd().joinpath(ln)
                if rp.exists():
                    p = rp
                else:
                    report['warnings'].append(f"Listed file not found: {ln}")
                    continue
            process_csv_file(p, geos, out_base, report)

    # build tournament JSONs from the collected rows
    results = build_tournament_jsons(report, geos)

    # summary
    print("=== build_tournament_jsons summary ===")
    print(f"CSV files processed as listed in {inp}:")
    print(f"  match JSONs produced: {len(report['produced_match_files'])}")
    print(f"  tournament JSONs produced: {len(report['produced_tournament_files'])}")
    if report['warnings']:
        print("Warnings:")
        for w in report['warnings'][:50]:
            print("  -", w)
    if report['errors']:
        print("Errors:")
        for e in report['errors'][:50]:
            print("  -", e)

    # print produced tournament files
    for tf in report['produced_tournament_files']:
        print("Produced:", tf)

if __name__ == "__main__":
    main()