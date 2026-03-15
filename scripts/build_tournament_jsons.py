#!/usr/bin/env python3
"""
scripts/build_tournament_jsons.py

Usage:
  python3 scripts/build_tournament_jsons.py --input-list created_files.txt \
      --geocodes docs/tools/geocodes_combined.json \
      --country-map docs/tools/country_to_ioc.json \
      --out-base docs/data/tournaments/json_by_tournaments
"""
import argparse
import json
import os
from pathlib import Path
import pandas as pd
import re
import unicodedata

def read_json_tolerant(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    text = p.read_text(encoding='utf-8')
    try:
        return json.loads(text)
    except Exception:
        # try simple sanitize of trailing commas
        sanitized = re.sub(r',\s*(\]|})', r'\1', text)
        sanitized = re.sub(r'^[ \t]*,[ \t]*$', '', sanitized, flags=re.MULTILINE)
        try:
            return json.loads(sanitized)
        except Exception as e:
            print(f"[WARN] Failed to parse JSON {p}: {e}")
            return None

def read_geocodes(path):
    """
    Lecture tolérante du fichier de geocodes.
    Retourne dict { key: (lat, lon) } (seulement entrées valides).
    """
    j = read_json_tolerant(path)
    if not isinstance(j, dict):
        print(f"[WARN] geocodes JSON root not dict; returning empty mapping")
        return {}

    def extract_map_from_obj(obj):
        out = {}
        if not isinstance(obj, dict):
            return out
        for k, v in obj.items():
            if v is None:
                continue
            if isinstance(v, (list, tuple)) and len(v) >= 2:
                try:
                    lat = float(v[0]); lon = float(v[1])
                    out[str(k)] = (lat, lon)
                except Exception:
                    continue
        return out

    mapping = {}
    ge = j.get('geocode') if isinstance(j, dict) else None
    if isinstance(ge, dict):
        mapping = extract_map_from_obj(ge)
    if not mapping:
        mapping = extract_map_from_obj(j)
    if not mapping:
        for v in j.values():
            if isinstance(v, dict):
                cand = extract_map_from_obj(v)
                if cand:
                    mapping.update(cand)

    print(f"[INFO] geocodes: loaded {len(mapping)} entries")
    return mapping

# CHANGED: load country->IOC mapping (expected JSON: keys = country name variants, values = IOC 3-letter codes)
def read_country_map(path):
    j = read_json_tolerant(path)
    if not isinstance(j, dict):
        print(f"[WARN] country map not a dict or missing: {path}")
        return {}
    # normalize keys (lowercase, strip accents/punct)
    out = {}
    for k, v in j.items():
        nk = _normalize_token(k)
        if isinstance(v, str) and v:
            out[nk] = v.strip().upper()
    print(f"[INFO] country map: loaded {len(out)} normalized entries")
    return out

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
    s = str(v).strip()
    return s if s != "" else None

def _normalize_token(s):
    """Lowercase, remove accents and punctuation, collapse whitespace for matching keys."""
    if not s:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_geocode_for_city_name(geocode_map, city_name):
    """
    CHANGED: scoring-based matching (prefer exact key, then first token before comma, then first word, then substring).
    Returns (geocode_tuple, matched_key) or (None, None)
    """
    if not city_name:
        return None, None
    token = _normalize_token(city_name)
    if not token:
        return None, None

    best = None  # (score, key)
    for k in geocode_map:
        nk = _normalize_token(k)
        score = 0
        if nk == token:
            score = 100
        else:
            if ',' in k:
                first = _normalize_token(k.split(',')[0])
                if first == token:
                    score = 90
            if score == 0:
                first_word = nk.split()[0] if nk.split() else nk
                if first_word == token:
                    score = 80
            if score == 0 and token in nk:
                score = 50
            if score == 0:
                last_word = nk.split()[-1] if nk.split() else nk
                if last_word == token:
                    score = 40
        if score > 0 and geocode_map.get(k) is not None:
            cand = (score, k)
            if best is None:
                best = cand
            else:
                if cand[0] > best[0] or (cand[0] == best[0] and len(cand[1]) < len(best[1])):
                    best = cand

    if best:
        return geocode_map[best[1]], best[1]
    # fallback simple substring
    for k in geocode_map:
        if token in _normalize_token(k):
            return geocode_map[k], k
    return None, None

def choose_city_for_canada(year):
    y = int(year)
    if y >= 2022:
        return "Montreal, QC, CA" if (y % 2 == 0) else "Toronto, ON, CA"
    if y <= 2019:
        return "Toronto, ON, CA" if (y % 2 == 0) else "Montreal, QC, CA"
    return "Montreal, QC, CA"

def tournament_has_no_location(tname):
    lowered = (tname or "").lower()
    keywords = ["finals", "final", "nitto atp finals", "masters cup", "tour world championship", "next gen", "intesa sanpaolo next gen", "next gen atp"]
    return any(k in lowered for k in keywords)

def extract_city_from_atp_tourney_name(tourney_name):
    if not tourney_name:
        return None
    name = tourney_name.strip()
    name = re.sub(r"^(atp|wta)\s+", "", name, flags=re.I).strip()
    name = re.sub(r"^masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*", "", name, flags=re.I)
    name = re.sub(r"^wta\s*", "", name, flags=re.I)
    if ',' in name:
        return name.split(',')[0].strip()
    parts = name.split()
    if parts and parts[-1].lower() == "canada":
        return "Canada"
    return parts[-1].strip() if parts else None

def safe_makedirs(p: Path):
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

def json_serialize_safe(obj):
    if isinstance(obj, (pd.Timestamp, )):
        return str(obj)
    return obj

# ---- Main processing ----

def process_csv_file(csv_path: Path, geos, out_base: Path, report):
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])
    except Exception as e:
        report['errors'].append(f"Failed reading {csv_path}: {e}")
        return

    cols = set(df.columns.tolist())

    is_wta = False
    if 'tourney_id' in cols or 'tourney_year' in cols or 'tournament_name' in cols:
        is_wta = True
    elif 'event_id' in cols or 'event_year' in cols or 'tourney_name' in cols:
        is_wta = False
    else:
        is_wta = 'wta' in csv_path.parts or 'WTA' in csv_path.name.lower()

    tour_label = 'wta' if is_wta else 'atp'

    col_event_id = _first_of(cols, ['event_id', 'tourney_id', 'tournament_id', 'tournamentId'])
    col_event_year = _first_of(cols, ['event_year', 'tourney_year', 'year'])
    col_tourney_name = _first_of(cols, ['tourney_name', 'tournament_name', 'tourney'])
    col_city = _first_of(cols, ['city', 'venue_city', 'tourney_city'])
    col_country = _first_of(cols, ['country', 'country_a', 'country_b', 'country_winner'])

    # iterate rows
    for idx, row in df.iterrows():
        event_id = normalize_str(row.get(col_event_id)) or ""
        event_year = normalize_str(row.get(col_event_year)) or ""
        tourney_name = normalize_str(row.get(col_tourney_name)) or ""
        city_val = normalize_str(row.get(col_city))
        country_val = normalize_str(row.get(col_country))

        if not event_id or not event_year:
            report['warnings'].append(f"{csv_path}: skipping row without event_id/event_year (tourney_name='{tourney_name}')")
            continue

        # CHANGED: group only by tour_label,event_id,event_year so all rows of same tournament are grouped
        key = (tour_label, event_id, event_year)
        folder_name = f"{tour_label}_{event_id}_{event_year}"
        out_dir = out_base.joinpath(tour_label, folder_name)
        safe_makedirs(out_dir)

        # write match-level json: all columns preserved
        row_dict = {k: (None if pd.isna(v) or v == "" else v) for k, v in row.items()}
        match_id = row_dict.get('match_id') or row_dict.get('matchid') or row_dict.get('match') or f"row_{idx}"
        # ensure unique filename: include idx suffix to avoid collisions
        safe_match_id = re.sub(r'[<>:"/\\|?*\s]+', '_', str(match_id)).strip('_')
        match_file = out_dir.joinpath(f"{safe_match_id}_{idx}.json")

        with open(match_file, 'w', encoding='utf-8') as fh:
            json.dump(row_dict, fh, ensure_ascii=False, default=json_serialize_safe, indent=2)

        if key not in report['tournaments']:
            report['tournaments'][key] = {
                'rows': [],
                'out_dir': out_dir,
                'tourney_name': tourney_name or None,
                'city': city_val or None,
                'country': country_val or None,
                'tour_label': tour_label,
                'event_id': event_id,
                'event_year': event_year
            }
        else:
            # CHANGED: ensure we capture first non-empty tourney_name/city/country if missing
            info = report['tournaments'][key]
            if not info.get('tourney_name') and tourney_name:
                info['tourney_name'] = tourney_name
            if not info.get('city') and city_val:
                info['city'] = city_val
            if not info.get('country') and country_val:
                info['country'] = country_val

        report['tournaments'][key]['rows'].append(row_dict)
        report['produced_match_files'].append(str(match_file))


def build_tournament_jsons(report, geos, country_map):
    # helper: normalize & map country name -> IOC 3-letter code (uses outer country_map)
    def map_country_to_ioc(country_value):
        if not country_value:
            return None
        c = str(country_value).strip()
        # If already 3 letters and alpha -> assume IOC
        if re.fullmatch(r'^[A-Za-z]{3}$', c):
            return c.upper()
        nk = _normalize_token(c)
        # direct lookup
        if nk in country_map:
            return country_map[nk]
        # try splitting tokens (last token often is country)
        parts = [p.strip() for p in re.split(r'[,\-()]+', c) if p.strip()]
        for p in reversed(parts):
            np = _normalize_token(p)
            if np in country_map:
                return country_map[np]
        # fallback: try partial tokens (e.g. 'rep chile', 'chilean rep' etc)
        for p in parts:
            np = _normalize_token(p)
            # try progressive shortening
            if np in country_map:
                return country_map[np]
        return None
    
    results = []
    for key, info in report['tournaments'].items():
        tour_label, event_id, event_year = key
        rows = info['rows']
        out_dir = info['out_dir']
        first = rows[0]

        # meta
        meta = {
            "source": tour_label.upper(),
            "tourney_id": event_id,
            "year": int(event_year) if str(event_year).isdigit() else event_year,
            "tourney_name": info.get('tourney_name') or normalize_str(first.get('tourney_name') or first.get('tournament_name') or ""),
            "tourney_title": normalize_str(first.get('tournament_title') or first.get('tourney_title') or None),
            "surface": normalize_str(first.get('surface') or first.get('Surface') or None),
            "level": normalize_str(first.get('level') or None),
            "prize_money": normalize_str(first.get('prize_money') or None),
            "prize_money_currency": normalize_str(first.get('prize_money_currency') or None)
        }

        singles = first.get('singles_draw_size') or first.get('singles_draw') or first.get('singles_drawsize')
        if singles:
            try:
                meta['singles_draw_size'] = int(singles)
            except:
                meta['singles_draw_size'] = singles

        # CHANGED: add start_date if available
        start_date_raw = normalize_str(first.get('start_date') or first.get('startDate') or "")
        if start_date_raw:
            try:
                dt = pd.to_datetime(start_date_raw)
                meta['start_date'] = dt.strftime("%Y-%m-%d")
            except Exception:
                meta['start_date'] = start_date_raw

        # city + country (prefer info captured during processing)
        city_name = info.get('city') or normalize_str(first.get('city') or first.get('venue_city') or "")
        country_name = info.get('country') or normalize_str(first.get('country') or "")

        # For ATP, if city empty, try to extract from tourney_name
        if tour_label == 'atp' and (not city_name):
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

        # Canada special case on tourney_name
        if not geocode and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            geocode = geos.get(special_key)
            matched_key = special_key if geocode else matched_key

        if geocode:
           meta['geocode'] = list(geocode)
           # If we have a matched_key that looks like "City, Country", prefer deriving the country from it
           derived_country = None
           if isinstance(matched_key, str) and ',' in matched_key:
               # last token after comma is usually the country name
               cand = matched_key.split(',')[-1].strip()
               if cand and re.search(r'[A-Za-z]', cand):
                   derived_country = cand
           # If we already have a country from CSV/rows, try to map it first
           mapped_from_csv = None
           if meta.get('country'):
               mapped_from_csv = map_country_to_ioc(meta['country'])
           # Try to map derived_country (from matched_key) preferentially
           mapped_from_derived = None
           if derived_country:
               mapped_from_derived = map_country_to_ioc(derived_country)
           # Decide which to use (prefer derived_mapped, then csv_mapped, then raw derived)
           if mapped_from_derived:
               meta['country'] = mapped_from_derived
               print(f"[INFO] country derived from matched_key '{matched_key}' -> '{derived_country}' mapped to IOC '{mapped_from_derived}'")
           elif mapped_from_csv:
               meta['country'] = mapped_from_csv
               print(f"[INFO] country taken from CSV '{meta.get('country')}' mapped to IOC '{mapped_from_csv}'")
           elif derived_country:
               # keep textual derived country (not mapped)
               meta['country'] = derived_country
               print(f"[INFO] country derived from matched_key (not mappable): '{derived_country}' -- leaving as text")

        mapped = map_country_to_ioc(meta.get('country'))
        if mapped:
            meta['country'] = mapped
        else:
            # if meta.country exists but not mappable, keep as-is (string)
            # but prefer empty string over None
            if meta.get('country') is None:
                meta['country'] = ""

        # build matches array (reduced summary as requested)
        matches_arr = []
        for r in rows:
            winner = (r.get('winner_player_name') or r.get('winner') or r.get('player_winner') or r.get('player_a') or "")
            loser = (r.get('loser_player_name') or r.get('loser') or r.get('player_loser') or r.get('player_b') or "")
            pid_w = (r.get('player_id_winner') or r.get('player_a_id') or r.get('playerida') or r.get('PlayerIDA') or "")
            pid_l = (r.get('player_id_loser') or r.get('player_b_id') or r.get('playeridb') or r.get('PlayerIDB') or "")
            winner_country = (r.get('winner_country') or r.get('country_winner') or r.get('country_a') or "")
            loser_country = (r.get('loser_country') or r.get('country_loser') or r.get('country_b') or "")
            winner_seed = r.get('winner_seed') or r.get('seed_a') or r.get('seed_winner') or ""
            loser_seed = r.get('loser_seed') or r.get('seed_b') or r.get('seed_loser') or ""
            match_id = r.get('match_id') or r.get('matchid') or ""
            round_ = r.get('round') or r.get('match_round') or r.get('round_name') or ""
            score = r.get('score_string') or r.get('score') or ""

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
    ap.add_argument("--country-map", "-c", required=True, help="path to docs/tools/country_to_ioc.json (country name -> IOC 3-letter code)")  # CHANGED: required
    ap.add_argument("--out-base", "-o", required=True, help="base output directory for per-tournament jsons")
    args = ap.parse_args()

    inp = Path(args.input_list)
    if not inp.exists():
        print(f"[ERROR] input list {inp} not found.")
        raise SystemExit(2)

    geos = read_geocodes(args.geocodes)
    country_map = read_country_map(args.country_map)
    out_base = Path(args.out_base)
    safe_makedirs(out_base)

    report = {
        'tournaments': {},
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
            if not p.exists():
                rp = Path.cwd().joinpath(ln)
                if rp.exists():
                    p = rp
                else:
                    report['warnings'].append(f"Listed file not found: {ln}")
                    continue
            process_csv_file(p, geos, out_base, report)

    # Build tournament JSONs
    results = build_tournament_jsons(report, geos, country_map)

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
    for tf in report['produced_tournament_files']:
        print("Produced:", tf)

if __name__ == "__main__":
    main()
