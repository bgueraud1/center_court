#!/usr/bin/env python3
"""
One-shot converter: process all CSVs in a directory and produce per-CSV tournament JSONs.

Usage:
  python3 scripts/build_tournament_jsons_dir.py \
    --input-dir docs/matches/atp_matches \
    --geocodes docs/tools/geocodes_combined.json \
    --country-map docs/tools/country_to_ioc.json \
    --out-base docs/data/tournaments/json_by_tournaments/atp
"""
import argparse
import json
from pathlib import Path
import pandas as pd
import re
import unicodedata
import sys
import os

# ---------------- helpers copied / adapted from your original script ----------------

def read_json_tolerant(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    text = p.read_text(encoding='utf-8')
    try:
        return json.loads(text)
    except Exception:
        sanitized = re.sub(r',\s*(\]|})', r'\1', text)
        sanitized = re.sub(r'^[ \t]*,[ \t]*$', '', sanitized, flags=re.MULTILINE)
        try:
            return json.loads(sanitized)
        except Exception as e:
            print(f"[WARN] Failed to parse JSON {p}: {e}")
            return None

def read_geocodes(path):
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

def read_country_map(path):
    j = read_json_tolerant(path)
    if not isinstance(j, dict):
        print(f"[WARN] country map not a dict or missing: {path}")
        return {}
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
    Return (latlon_tuple, matched_key) or (None, None).
    Matching logic based on normalized tokens with some heuristics.
    """
    if not city_name:
        return None, None
    token = _normalize_token(city_name)
    if not token:
        return None, None

    best = None
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

# ---------------- processing functions ----------------

def process_csv_file(csv_path: Path, geos, out_base: Path, report):
    """
    Process a single CSV and write per-match JSON files and collect tournament rows into report['tournaments'].
    Important: out_base must be the root base **for tournaments** (e.g. docs/.../atp)
    We will create per-tournament directories directly under out_base (no extra tour_label subdir).
    """
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

    for idx, row in df.iterrows():
        event_id = normalize_str(row.get(col_event_id)) or ""
        event_year = normalize_str(row.get(col_event_year)) or ""
        tourney_name = normalize_str(row.get(col_tourney_name)) or ""
        city_val = normalize_str(row.get(col_city))
        country_val = normalize_str(row.get(col_country))

        if not event_id or not event_year:
            report['warnings'].append(f"{csv_path}: skipping row without event_id/event_year (tourney_name='{tourney_name}')")
            continue

        key = (tour_label, event_id, event_year)
        folder_name = f"{tour_label}_{event_id}_{event_year}"
        # Put tournament dirs directly under out_base (out_base expected to already indicate tour_label if desired).
        out_dir = out_base.joinpath(folder_name)
        safe_makedirs(out_dir)

        row_dict = {k: (None if pd.isna(v) or v == "" else v) for k, v in row.items()}

        # === Strict match filename logic to avoid MS007.07 style ===
        raw_match_id = row_dict.get('match_id') or row_dict.get('matchid') or row_dict.get('match') or ""
        raw_match_id_str = str(raw_match_id).strip()
        # take only the portion before the first dot (removes .07 etc.)
        if raw_match_id_str:
            raw_match_id_base = raw_match_id_str.split('.', 1)[0]
        else:
            raw_match_id_base = ""
        if not raw_match_id_base:
            raw_match_id_base = f"match_{idx}"
        # sanitize: keep only word chars, dash; replace others by underscore
        safe_match_id = re.sub(r'[^\w\-]', '_', raw_match_id_base).strip('_')
        if not safe_match_id:
            safe_match_id = f"match_{idx}"

        # Default filename: SAFEID.json (no _{idx} suffix). If file already exists, append _1, _2, ...
        match_file = out_dir.joinpath(f"{safe_match_id}.json")
        if match_file.exists():
            counter = 1
            while True:
                candidate = out_dir.joinpath(f"{safe_match_id}_{counter}.json")
                if not candidate.exists():
                    match_file = candidate
                    break
                counter += 1

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
        # already a 3-letter IOC code?
        if re.fullmatch(r'^[A-Za-z]{3}$', c):
            return c.upper()
        nk = _normalize_token(c)
        if nk in country_map:
            return country_map[nk]
        # try splitting and searching from the end (e.g. "City, State, Country")
        parts = [p.strip() for p in re.split(r'[,\-()]+', c) if p.strip()]
        for p in reversed(parts):
            np = _normalize_token(p)
            if np in country_map:
                return country_map[np]
        for p in parts:
            np = _normalize_token(p)
            if np in country_map:
                return country_map[np]
        return None

    results = []
    for key, info in report['tournaments'].items():
        tour_label, event_id, event_year = key
        rows = info['rows']
        out_dir = info['out_dir']
        first = rows[0]

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

        start_date_raw = normalize_str(first.get('start_date') or first.get('startDate') or "")
        if start_date_raw:
            try:
                dt = pd.to_datetime(start_date_raw)
                meta['start_date'] = dt.strftime("%Y-%m-%d")
            except Exception:
                meta['start_date'] = start_date_raw

        city_name = info.get('city') or normalize_str(first.get('city') or first.get('venue_city') or "")
        country_name = info.get('country') or normalize_str(first.get('country') or "")

        if tour_label == 'atp' and (not city_name):
            tn = meta['tourney_name'] or ""
            if "canada" in tn.lower():
                fallback_city = choose_city_for_canada(meta['year'])
                city_name = fallback_city.split(',')[0]
            else:
                extracted = extract_city_from_atp_tourney_name(tn)
                city_name = extracted or city_name

        meta['city'] = city_name or ""
        # we'll determine the canonical IOC code below; start with CSV country (may be text)
        meta['country'] = country_name or ""

        geocode = None
        matched_key = None
        if meta['city']:
            geocode, matched_key = find_geocode_for_city_name(geos, meta['city'])

        # special-case Canada tournaments fallback geocode (if you still want it)
        if not geocode and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            geocode = geos.get(special_key)
            matched_key = special_key if geocode else matched_key

        if geocode:
            meta['geocode'] = list(geocode)

            # Attempt to derive a country code from the matched_key by testing chunks from the end.
            mapped_from_derived = None
            derived_country_text = None
            if isinstance(matched_key, str) and ',' in matched_key:
                # tokens like ["Auckland", " New Zealand"] or ["Montreal", " QC", " CA"]
                tokens = [t.strip() for t in matched_key.split(',') if t.strip()]
                # try tokens from last to first, looking for a mappable token
                for tok in reversed(tokens):
                    mapped_tok = map_country_to_ioc(tok)
                    if mapped_tok:
                        mapped_from_derived = mapped_tok
                        derived_country_text = tok
                        break
                # if none mapped, optionally try joining last two tokens ("QC, CA" -> "QC CA") etc (rare)
                if not mapped_from_derived and len(tokens) >= 2:
                    # build candidates progressively: last, last two joined, last three...
                    for n in range(1, min(4, len(tokens))+1):
                        cand = ", ".join(tokens[-n:])
                        mapped_cand = map_country_to_ioc(cand)
                        if mapped_cand:
                            mapped_from_derived = mapped_cand
                            derived_country_text = cand
                            break

            # mapped_from_csv: try mapping the CSV-provided country
            mapped_from_csv = None
            if meta.get('country'):
                mapped_from_csv = map_country_to_ioc(meta['country'])

            # Priority: derived from geocodes -> CSV -> nothing
            if mapped_from_derived:
                meta['country'] = mapped_from_derived
                print(f"[INFO] country derived from matched_key '{matched_key}' -> '{derived_country_text}' mapped to IOC '{mapped_from_derived}'")
            elif mapped_from_csv:
                meta['country'] = mapped_from_csv
                print(f"[INFO] country taken from CSV '{meta.get('country')}' mapped to IOC '{mapped_from_csv}'")
            else:
                # No reliable mapping found — per your request, leave blank (don't fallback to arbitrary values).
                print(f"[INFO] no country mapping found for tournament '{meta.get('tourney_name')}' (matched_key='{matched_key}', csv_country='{country_name}') — leaving country empty")
                meta['country'] = ""

        else:
            # No geocode => we still try to map CSV country; if fail, leave empty (no fallback).
            mapped = map_country_to_ioc(meta.get('country'))
            if mapped:
                meta['country'] = mapped
            else:
                meta['country'] = ""

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

        tour_json_path = out_dir.joinpath("tournament.json")
        with open(tour_json_path, 'w', encoding='utf-8') as fh:
            json.dump(tournament_json, fh, ensure_ascii=False, indent=2)

        report['produced_tournament_files'].append(str(tour_json_path))
        results.append((out_dir, tour_json_path, len(matches_arr)))

    return results

# ---------------- main: iterate CSVs in a directory ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", "-d", required=True, help="directory containing CSV files to process")
    ap.add_argument("--geocodes", "-g", required=True, help="path to docs/tools/geocodes_combined.json")
    ap.add_argument("--country-map", "-c", required=True, help="path to docs/tools/country_to_ioc.json")
    ap.add_argument("--out-base", "-o", required=True, help="base output directory for per-csv outputs (one subdir per csv)")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"[ERROR] input-dir not found or not a directory: {input_dir}")
        raise SystemExit(2)

    geos = read_geocodes(args.geocodes)
    country_map = read_country_map(args.country_map)

    out_base = Path(args.out_base)
    safe_makedirs(out_base)

    csv_files = sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == '.csv'])
    if not csv_files:
        print(f"[WARN] No CSV files found in {input_dir}")
        return

    overall_report = {
        'processed_csvs': [],
        'produced_match_files': [],
        'produced_tournament_files': [],
        'errors': [],
        'warnings': []
    }

    # IMPORTANT: pass out_base directly — we create per-tournament folders under out_base
    for csv_path in csv_files:
        print(f"[INFO] Processing CSV: {csv_path}")

        report = {
            'tournaments': {},
            'produced_match_files': [],
            'produced_tournament_files': [],
            'errors': [],
            'warnings': []
        }

        process_csv_file(csv_path, geos, out_base, report)
        results = build_tournament_jsons(report, geos, country_map)

        print(f"[INFO] Results for {csv_path.name}: produced {len(report['produced_tournament_files'])} tournament JSON(s)")
        overall_report['processed_csvs'].append(str(csv_path))
        overall_report['produced_match_files'].extend(report['produced_match_files'])
        overall_report['produced_tournament_files'].extend(report['produced_tournament_files'])
        overall_report['errors'].extend(report['errors'])
        overall_report['warnings'].extend(report['warnings'])

    # final summary
    print("=== Summary ===")
    print(f"CSV files processed: {len(overall_report['processed_csvs'])}")
    print(f"Match JSONs produced: {len(overall_report['produced_match_files'])}")
    print(f"Tournament JSONs produced: {len(overall_report['produced_tournament_files'])}")
    if overall_report['warnings']:
        print("Warnings:")
        for w in overall_report['warnings'][:50]:
            print(" -", w)
    if overall_report['errors']:
        print("Errors:")
        for e in overall_report['errors'][:50]:
            print(" -", e)

if __name__ == "__main__":
    main()