#!/usr/bin/env python3
"""
scripts/build_tournament_jsons.py

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
import unicodedata

def read_geocodes(path):
    """
    Lecture tolérante de docs/tools/geocodes_combined.json.

    Retourne un dict { key: (lat, lon), ... } (peut être vide).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Geocodes file not found: {path}")

    text = path.read_text(encoding='utf-8')

    def try_load(s):
        try:
            return json.loads(s)
        except Exception as e:
            raise

    j = None
    # 1) Try normal load
    try:
        j = json.loads(text)
    except Exception as e:
        # 2) Attempt a simple sanitize: remove trailing commas before } or ]
        sanitized = re.sub(r',\s*(\]|})', r'\1', text)
        sanitized = re.sub(r'^[ \t]*,[ \t]*$', '', sanitized, flags=re.MULTILINE)
        try:
            j = json.loads(sanitized)
            print(f"[INFO] geocodes: JSON parsed after sanitizing trailing commas ({path})")
        except Exception as e2:
            print(f"[WARN] Failed to parse geocodes JSON (even after sanitize): {e2}")
            print(f"[DEBUG] First 800 chars of file:\n{text[:800]}")
            return {}

    if not isinstance(j, dict):
        print(f"[WARN] geocodes JSON root is not an object/dict: {type(j)} — returning empty mapping")
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
                    lat = float(v[0])
                    lon = float(v[1])
                    out[str(k)] = (lat, lon)
                except Exception:
                    continue
        return out

    ge = j.get('geocode') if isinstance(j, dict) else None
    mapping = {}
    if isinstance(ge, dict):
        mapping = extract_map_from_obj(ge)

    if not mapping:
        mapping = extract_map_from_obj(j)

    if not mapping:
        for v in j.values():
            if isinstance(v, dict):
                candidate = extract_map_from_obj(v)
                if candidate:
                    mapping.update(candidate)

    print(f"[INFO] geocodes: loaded {len(mapping)} entries from {path}")
    if len(mapping) > 0:
        sample_k = next(iter(mapping))
        print(f"[INFO] geocodes: sample key -> {sample_k} -> {mapping[sample_k]}")

    return mapping

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
    """Lowercase, strip spaces, remove punctuation and diacritics for comparison."""
    if not s:
        return ""
    s = str(s).strip().lower()
    # remove accents
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    # remove punctuation
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_geocode_for_city_name(geocode_map, city_name):
    """Try to match city_name to keys in geocode_map.
       Returns (geocode_tuple, matched_key) or (None, None)

       Matching strategy (scored):
         - exact key match (normalized) -> highest
         - exact first-comma token match (normalized) -> high
         - first word token match (normalized) -> medium
         - substring match -> low
       If multiple candidates, pick highest score then prefer shorter key (more specific).
    """
    if not city_name:
        return None, None
    token = _normalize_token(city_name)
    if not token:
        return None, None

    best = None  # tuple (score, key)
    for k in geocode_map:
        nk = _normalize_token(k)
        score = 0
        # exact whole-key match
        if nk == token:
            score = 100
        else:
            # if key contains comma, compare first token before comma
            if ',' in k:
                first = _normalize_token(k.split(',')[0])
                if first == token:
                    score = 90
            # compare first word (before space)
            if score == 0:
                first_word = nk.split()[0] if nk.split() else nk
                if first_word == token:
                    score = 80
            # substring
            if score == 0 and token in nk:
                score = 50
            # last-word match (e.g., user passed 'Cacém' etc.)
            if score == 0:
                lw = nk.split()[-1] if nk.split() else nk
                if lw == token:
                    score = 40

        if score > 0:
            # prefer non-null geocode rows (should already be filtered)
            if geocode_map.get(k) is None:
                continue
            cand = (score, k)
            if best is None:
                best = cand
            else:
                # higher score wins; tie-breaker: prefer shorter key (likely exact city)
                if cand[0] > best[0] or (cand[0] == best[0] and len(cand[1]) < len(best[1])):
                    best = cand

    if best:
        return geocode_map[best[1]], best[1]
    # fallback: try simple token substring again
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
    if parts[-1].lower() == "canada":
        return "Canada"
    return parts[-1].strip()

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

    for idx, row in df.iterrows():
        event_id = normalize_str(row.get(col_event_id)) or ""
        event_year = normalize_str(row.get(col_event_year)) or ""
        tourney_name = normalize_str(row.get(col_tourney_name)) or ""
        city_val = normalize_str(row.get(col_city))
        country_val = normalize_str(row.get(col_country))

        if not event_id or not event_year:
            report['warnings'].append(f"{csv_path}: skipping row without event_id/event_year (tourney_name='{tourney_name}')")
            continue

        folder_name = f"{tour_label}_{event_id}_{event_year}"
        out_dir = out_base.joinpath(tour_label, folder_name)
        safe_makedirs(out_dir)

        row_dict = {k: (None if pd.isna(v) or v == "" else v) for k, v in row.items()}
        match_id = row_dict.get('match_id') or row_dict.get('matchid') or row_dict.get('match') or f"row_{idx}"
        # sanitize filename characters
        safe_match_id = re.sub(r'[<>:"/\\|?*\s]+', '_', str(match_id)).strip('_')
        match_file = out_dir.joinpath(f"{safe_match_id}.json")

        with open(match_file, 'w', encoding='utf-8') as fh:
            json.dump(row_dict, fh, ensure_ascii=False, default=json_serialize_safe, indent=2)

        key = (tour_label, event_id, event_year, tourney_name, city_val, country_val)
        if key not in report['tournaments']:
            report['tournaments'][key] = {'rows': [], 'out_dir': out_dir, 'tourney_name': tourney_name, 'city': city_val, 'country': country_val, 'tour_label': tour_label, 'event_id': event_id, 'event_year': event_year}
        report['tournaments'][key]['rows'].append(row_dict)
        report['produced_match_files'].append(str(match_file))

def build_tournament_jsons(report, geos):
    results = []
    for key, info in report['tournaments'].items():
        tour_label, event_id, event_year, tourney_name, city_val, country_val = key
        rows = info['rows']
        out_dir = info['out_dir']
        first = rows[0]

        # meta base
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

        singles = first.get('singles_draw_size') or first.get('singles_draw') or first.get('singles_drawsize')
        if singles:
            try:
                meta['singles_draw_size'] = int(singles)
            except:
                meta['singles_draw_size'] = singles

        # CHANGED: add start_date if present and normalize to YYYY-MM-DD
        start_date_raw = normalize_str(first.get('start_date') or first.get('startDate') or "")
        if start_date_raw:
            try:
                dt = pd.to_datetime(start_date_raw)
                meta['start_date'] = dt.strftime("%Y-%m-%d")
            except Exception:
                meta['start_date'] = start_date_raw

        # city + country
        city_name = city_val or normalize_str(first.get('city') or first.get('venue_city') or "")
        country_name = country_val or normalize_str(first.get('country') or "")

        # CHANGED: if atp and no city, try extract from tourney_name, with Canada special-case
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

        # CHANGED: geocode lookup with improved matching & safer deriving of country
        geocode = None
        matched_key = None
        if meta['city']:
            geocode, matched_key = find_geocode_for_city_name(geos, meta['city'])

        # if still no geocode and tourney_name mentions 'Canada', choose Montreal/Toronto key
        if not geocode and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            geocode = geos.get(special_key)
            matched_key = special_key if geocode else matched_key

        if geocode:
            meta['geocode'] = list(geocode)
            # CHANGED: only set country from matched_key if matched_key contains a comma (so it likely has "City, Country")
            if not meta['country'] and isinstance(matched_key, str) and ',' in matched_key:
                last = matched_key.split(',')[-1].strip()
                # sanity: last token should be alphabetic and longer than 2 (avoid "Cacém" style confusion)
                if last and re.search(r'[A-Za-z]', last) and len(last) > 2:
                    meta['country'] = last

        # build matches array (reduced summary)
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
            # score priority: score_string, score
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
        'tournaments': {},
        'produced_match_files': [],
        'produced_tournament_files': [],
        'errors': [],
        'warnings': []
    }

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

    results = build_tournament_jsons(report, geos)

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
