#!/usr/bin/env python3
"""
One-shot converter with verbose debug to troubleshoot indoor/outdoor missing value.

Usage (example):
  python3 scripts/build_tournament_jsons_oneshot.py \
    --input-dir docs/matches/atp_test \
    --geocodes docs/tools/geocodes_combined.json \
    --country-map docs/tools/country_to_ioc.json \
    --out-base docs/data/tournaments/json_by_tournaments/atp \
    --tourney-overrides docs/tools/tourney_overrides.json \
    --debug

If --atp-tournaments / --wta-tournaments are omitted the script will attempt
to autodiscover likely files/directories under the project (docs/...).
"""
import argparse
import json
from pathlib import Path
import pandas as pd
import re
import unicodedata
import sys
from collections import defaultdict
from typing import Dict, Tuple, Optional, Any, List

# ---------------- helpers ----------------

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

def _normalize_token(s):
    if not s:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def build_geocode_index(geos):
    exact_norm_index = defaultdict(list)
    first_token_index = defaultdict(list)
    last_token_index = defaultdict(list)
    for k, coords in geos.items():
        nk = _normalize_token(k)
        exact_norm_index[nk].append((k, coords))
        if ',' in k:
            first = k.split(',', 1)[0].strip()
        else:
            first = k.split()[0] if k.split() else k
        if first:
            first_token_index[_normalize_token(first)].append((k, coords))
        last_word = nk.split()[-1] if nk.split() else nk
        last_token_index[_normalize_token(last_word)].append((k, coords))
    return exact_norm_index, first_token_index, last_token_index

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

def read_tourney_overrides(path):
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        print(f"[WARN] tourney-overrides file not found: {p}")
        return {}
    j = read_json_tolerant(p)
    if not isinstance(j, dict):
        print(f"[WARN] tourney-overrides not a dict: {path}")
        return {}
    out = {}
    for k, v in j.items():
        nk = _normalize_token(k)
        out[nk] = v
    print(f"[INFO] tourney-overrides: loaded {len(out)} entries")
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

def find_geocode_indexed(geos, exact_index, first_index, last_index, candidate):
    if not candidate:
        return None, None
    cand_norm = _normalize_token(candidate)
    if cand_norm in exact_index:
        candidates = exact_index[cand_norm]
        candidates_sorted = sorted(candidates, key=lambda t: len(t[0]))
        k, coords = candidates_sorted[0]
        return coords, k
    if cand_norm in first_index:
        candidates = first_index[cand_norm]
        candidates_sorted = sorted(candidates, key=lambda t: len(t[0]))
        k, coords = candidates_sorted[0]
        return coords, k
    if cand_norm in last_index:
        candidates = last_index[cand_norm]
        candidates_sorted = sorted(candidates, key=lambda t: len(t[0]))
        k, coords = candidates_sorted[0]
        return coords, k
    for nk, items in exact_index.items():
        if re.search(r'\b' + re.escape(cand_norm) + r'\b', nk):
            k, coords = sorted(items, key=lambda t: len(t[0]))[0]
            return coords, k
    return None, None

def choose_city_for_canada(year):
    y = int(year)
    if y >= 2022:
        return "Montreal, QC, CA" if (y % 2 == 0) else "Toronto, ON, CA"
    if y <= 2019:
        return "Toronto, ON, CA" if (y % 2 == 0) else "Montreal, QC, CA"
    return "Montreal, QC, CA"

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

# ---------------- read ATP/WTA tournaments JSON indexes ----------------

def _load_json_file_safe(p: Path):
    try:
        return read_json_tolerant(p)
    except Exception as e:
        print(f"[WARN] failed to load tournaments JSON {p}: {e}")
        return None

def _extract_atp_entries_from_json(obj) -> list:
    out = []
    if not obj:
        return out
    if isinstance(obj, dict) and 'TournamentDates' in obj:
        for td in obj.get('TournamentDates') or []:
            tlist = td.get('Tournaments') or []
            for t in tlist:
                tid = t.get('Id') or t.get('id') or t.get('TournamentId')
                if tid is None:
                    continue
                raw_inout = t.get('IndoorOutdoor') or t.get('inOutdoor') or t.get('Indoor') or t.get('indoor') or None
                entry = {
                    'id': str(tid),
                    'location': t.get('Location') or t.get('title') or None,
                    'surface': t.get('Surface') or None,
                    'inOutdoor': raw_inout,
                    'IndoorOutdoor': raw_inout,
                    'level': t.get('Type') or t.get('EventType') or None,
                    'title': t.get('Name') or t.get('Title') or None,
                    'city': None,
                    'country': None
                }
                out.append(entry)
        return out
    if isinstance(obj, list):
        for t in obj:
            tid = t.get('Id') or t.get('id') or t.get('TournamentId')
            if tid is None:
                continue
            raw_inout = t.get('IndoorOutdoor') or t.get('inOutdoor') or t.get('Indoor') or t.get('indoor') or None
            entry = {
                'id': str(tid),
                'location': t.get('Location') or t.get('title') or None,
                'surface': t.get('Surface') or None,
                'inOutdoor': raw_inout,
                'IndoorOutdoor': raw_inout,
                'level': t.get('Type') or t.get('EventType') or None,
                'title': t.get('Name') or t.get('Title') or None,
                'city': None,
                'country': None
            }
            out.append(entry)
        return out
    def walk_for_tournaments(o):
        if isinstance(o, dict):
            if 'Tournaments' in o and isinstance(o['Tournaments'], list):
                for t in o['Tournaments']:
                    tid = t.get('Id') or t.get('id') or t.get('TournamentId')
                    if tid is None:
                        continue
                    raw_inout = t.get('IndoorOutdoor') or t.get('inOutdoor') or t.get('Indoor') or t.get('indoor') or None
                    entry = {
                        'id': str(tid),
                        'location': t.get('Location') or t.get('title') or None,
                        'surface': t.get('Surface') or None,
                        'inOutdoor': raw_inout,
                        'IndoorOutdoor': raw_inout,
                        'level': t.get('Type') or t.get('EventType') or None,
                        'title': t.get('Name') or t.get('Title') or None,
                        'city': None,
                        'country': None
                    }
                    out.append(entry)
            for v in o.values():
                if isinstance(v, (dict, list)):
                    walk_for_tournaments(v)
        elif isinstance(o, list):
            for it in o:
                walk_for_tournaments(it)
    walk_for_tournaments(obj)
    return out

def _extract_wta_entries_from_json(obj) -> list:
    out = []
    if not obj:
        return out
    if isinstance(obj, dict) and 'content' in obj:
        for t in obj['content']:
            tid = None
            if isinstance(t, dict):
                tid = t.get('liveScoringId') or (t.get('tournamentGroup') or {}).get('id') or t.get('id')
            if tid is None:
                continue
            raw_inout = t.get('inOutdoor') or t.get('IndoorOutdoor') or t.get('indoor') or None
            entry = {
                'id': str(tid),
                'city': t.get('city') or (t.get('title') and t.get('title').split('-')[-1].strip()) or None,
                'country': t.get('country') or None,
                'inOutdoor': raw_inout,
                'IndoorOutdoor': raw_inout,
                'level': (t.get('level') or (t.get('tournamentGroup') or {}).get('level') or None),
                'title': t.get('title') or None,
                'surface': t.get('surface') or None
            }
            out.append(entry)
        return out
    def walk_for_content(o):
        if isinstance(o, dict):
            if 'tournamentGroup' in o or ('year' in o and ('city' in o or 'country' in o)):
                tid = None
                if 'liveScoringId' in o:
                    tid = o['liveScoringId']
                elif 'tournamentGroup' in o:
                    tid = (o['tournamentGroup'] or {}).get('id')
                if tid is None:
                    return
                raw_inout = o.get('inOutdoor') or o.get('IndoorOutdoor') or o.get('indoor') or None
                entry = {
                    'id': str(tid),
                    'city': o.get('city') or None,
                    'country': o.get('country') or None,
                    'inOutdoor': raw_inout,
                    'IndoorOutdoor': raw_inout,
                    'level': o.get('level') or (o.get('tournamentGroup') or {}).get('level') or None,
                    'title': o.get('title') or None,
                    'surface': o.get('surface') or None
                }
                out.append(entry)
            for v in o.values():
                if isinstance(v, (dict, list)):
                    walk_for_content(v)
        elif isinstance(o, list):
            for it in o:
                walk_for_content(it)
    walk_for_content(obj)
    return out

def build_tournaments_index_from_path(path: Optional[str], tour_label: str) -> Dict[Tuple[int, str], dict]:
    mapping = {}
    if not path:
        return mapping
    p = Path(path)
    files = []
    if p.is_file():
        files = [p]
    elif p.is_dir():
        files = sorted([x for x in p.iterdir() if x.is_file() and x.suffix.lower() == '.json'])
    else:
        print(f"[WARN] tournaments path not found: {p}")
        return mapping

    for f in files:
        try:
            obj = _load_json_file_safe(f)
            if not obj:
                continue
            fn = f.name
            m = re.search(r'(\d{4})', fn)
            year = None
            if m:
                try:
                    year = int(m.group(1))
                except Exception:
                    year = None
            entries = []
            if tour_label.lower() == 'atp':
                entries = _extract_atp_entries_from_json(obj)
            else:
                entries = _extract_wta_entries_from_json(obj)

            if year is None:
                if isinstance(obj, dict) and 'year' in obj and isinstance(obj['year'], int):
                    year = obj['year']

            for e in entries:
                candidate_year = year
                if isinstance(e.get('year'), int):
                    candidate_year = e['year']
                if candidate_year is None:
                    candidate_year = 0

                raw_inout = None
                for fld in ('inOutdoor', 'IndoorOutdoor', 'indoor', 'Indoor'):
                    if fld in e and e.get(fld):
                        raw_inout = e.get(fld)
                        break

                entry_copy = {
                    'id': e.get('id'),
                    'location': e.get('location'),
                    'city': e.get('city'),
                    'country': e.get('country'),
                    'inOutdoor': raw_inout,
                    'IndoorOutdoor': raw_inout,
                    'level': e.get('level'),
                    'surface': e.get('surface'),
                    'title': e.get('title')
                }
                # -- normaliser id & year pour la clé d'index --
                id_raw = entry_copy.get('id')
                id_str = str(id_raw).strip() if id_raw is not None else ""
                # skip empty ids
                if not id_str:
                    continue

                # candidate_year peut être None, string, int...
                try:
                    year_int = int(candidate_year)
                except Exception:
                    year_int = 0

                key = (year_int, id_str)
                mapping[key] = entry_copy
        except Exception as ex:
            print(f"[WARN] failed to index tournaments file {f}: {ex}")
    print(f"[INFO] built tournaments index ({tour_label}): {len(mapping)} entries from {len(files)} file(s)")
    return mapping

# ---------------- processing functions ----------------

def detect_wta_from_path(csv_path: Path) -> bool:
    s = str(csv_path).lower()
    name = csv_path.name.lower()
    if '/wta_' in s or 'matches/wta' in s or 'wta_matches' in s:
        return True
    if '/atp_' in s or 'matches/atp' in s or 'atp_matches' in s:
        return False
    if name.startswith('wta') or name.startswith('wta_'):
        return True
    if name.startswith('atp') or name.startswith('atp_'):
        return False
    if 'wta' in s:
        return True
    return False

def process_csv_file(csv_path: Path, geos, out_base: Path, report):
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])
    except Exception as e:
        report['errors'].append(f"Failed reading {csv_path}: {e}")
        return

    cols = set(df.columns.tolist())
    is_wta = detect_wta_from_path(csv_path)
    tour_label = 'wta' if is_wta else 'atp'

    col_event_id = _first_of(cols, ['event_id', 'tourney_id', 'tournament_id', 'tournamentId'])
    col_event_year = _first_of(cols, ['event_year', 'tourney_year', 'year'])
    col_tourney_name = _first_of(cols, ['tourney_name', 'tournament_name', 'tourney'])
    col_city = _first_of(cols, ['city', 'venue_city', 'tourney_city'])
    col_country = _first_of(cols, ['country', 'venue_country', 'tourney_country', 'country_code', 'country_code3'])

    for idx, row in df.iterrows():
        event_id = normalize_str(row.get(col_event_id)) or ""
        event_year = normalize_str(row.get(col_event_year)) or ""
        tourney_name = normalize_str(row.get(col_tourney_name)) or ""
        city_val = normalize_str(row.get(col_city))
        country_val = normalize_str(row.get(col_country))

        if not event_id or not event_year:
            report['warnings'].append(f"{csv_path}: skipping row without event_id/event_year (tourney_name='{tourney_name}')")
            continue

        # normalize here so keys are consistent
        event_id = str(event_id).strip()
        event_year = str(event_year).strip()

        key = (tour_label, event_id, event_year)
        folder_name = f"{tour_label}_{event_id}_{event_year}"
        out_dir = out_base.joinpath(folder_name)
        safe_makedirs(out_dir)

        row_dict = {k: (None if pd.isna(v) or v == "" else v) for k, v in row.items()}

        raw_match_id = row_dict.get('match_id') or row_dict.get('matchid') or row_dict.get('match') or ""
        raw_match_id_str = str(raw_match_id).strip()
        if raw_match_id_str:
            raw_match_id_base = raw_match_id_str.split('.', 1)[0]
        else:
            raw_match_id_base = ""
        if not raw_match_id_base:
            raw_match_id_base = f"match_{idx}"
        safe_match_id = re.sub(r'[^\w\-]', '_', raw_match_id_base).strip('_')
        if not safe_match_id:
            safe_match_id = f"match_{idx}"

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

# ---------------- build tournament JSONs (robust geocode lookup + tournament JSON enrichment) ----------------

def _normalize_inout(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    up = s.upper()
    if up in ('O', 'OUT', 'OUTDOOR', 'O(UTDOOR)', 'OUTDOOR)'):
        return 'O'
    if up in ('I', 'IND', 'INDOOR', 'INDOOR)'):
        return 'I'
    if up.startswith('O'):
        return 'O'
    if up.startswith('I'):
        return 'I'
    return None

def build_tournament_jsons(report, geos, country_map, tourney_overrides=None, missing_set=None,
                           atp_index=None, wta_index=None, debug=False):
    tourney_overrides = tourney_overrides or {}
    missing_set = missing_set if missing_set is not None else set()
    atp_index = atp_index or {}
    wta_index = wta_index or {}

    exact_index, first_index, last_index = build_geocode_index(geos)

    def map_country_to_ioc(country_value):
        if not country_value:
            return None
        c = str(country_value).strip()
        if re.fullmatch(r'^[A-Za-z]{3}$', c):
            return c.upper()
        nk = _normalize_token(c)
        if nk in country_map:
            return country_map[nk]
        parts = [p.strip() for p in re.split(r'[,\-()]+', c) if p.strip()]
        for p in reversed(parts):
            np = _normalize_token(p)
            if np in country_map:
                return country_map[np]
        for p in parts:
            np = _normalize_token(p)
            if np in country_map:
                return country_map[np]
        if parts:
            for n in range(1, min(4, len(parts))+1):
                cand = ", ".join(parts[-n:])
                np = _normalize_token(cand)
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

        csv_inout_candidates = [
            first.get('indoor_outdoor'),
            first.get('IndoorOutdoor'),
            first.get('inOutdoor'),
            first.get('indoor'),
            first.get('in_outdoor')
        ]
        csv_inout = None
        for cand in csv_inout_candidates:
            if cand:
                csv_inout = _normalize_inout(cand)
                if csv_inout:
                    break
        if csv_inout:
            meta['indoor_outdoor'] = csv_inout
            if tour_label == 'wta':
                meta['inOutdoor'] = csv_inout
            if debug:
                print(f"[DEBUG] CSV provided in/out '{cand}' -> normalized '{csv_inout}' for {meta.get('tourney_name')}")

        # index lookup
        # normaliser event_year et event_id pour la recherche
        idx_year = None
        try:
            idx_year = int(str(event_year).strip())
        except Exception:
            if isinstance(meta.get('year'), int):
                idx_year = meta.get('year')
            else:
                idx_year = None

        event_id_norm = str(event_id).strip()

        idx_entry = None
        if idx_year is not None:
            idx_key = (idx_year, event_id_norm)
            if debug:
                print(f"[DEBUG] Looking up tournament index for key {idx_key} (tour_label={tour_label})")
            if tour_label == 'atp':
                idx_entry = atp_index.get(idx_key)
            else:
                idx_entry = wta_index.get(idx_key)

        if idx_entry is None:
            fallback_key = (0, event_id_norm)
            if debug:
                print(f"[DEBUG] No exact idx entry for {(idx_year, event_id_norm)}, trying fallback {fallback_key}")
            if tour_label == 'atp':
                idx_entry = atp_index.get(fallback_key)
            else:
                idx_entry = wta_index.get(fallback_key)

        if debug:
            print(f"[DEBUG] idx_entry found: {bool(idx_entry)} -> {idx_entry}")

        if idx_entry:
            if idx_entry.get('location'):
                loc = idx_entry.get('location')
                parts = [p.strip() for p in str(loc).split(',') if p.strip()]
                if parts:
                    left = parts[0]
                    if '-' in left:
                        city_guess = left.split('-', 1)[0].strip()
                    elif ' - ' in left:
                        city_guess = left.split(' - ', 1)[0].strip()
                    else:
                        if '/' in left:
                            city_guess = left.split('/', 1)[0].strip()
                        else:
                            city_guess = left
                    city_name = city_guess or city_name
                if len(parts) >= 2:
                    country_guess = parts[-1]
                    country_name = country_guess or country_name
            if idx_entry.get('city'):
                city_name = idx_entry.get('city')
            if idx_entry.get('country'):
                country_name = idx_entry.get('country')

            # Debug: show raw stored values in idx_entry for indoor/outdoor
            idx_inout_raw = idx_entry.get('inOutdoor') or idx_entry.get('IndoorOutdoor') or idx_entry.get('indoor') or None
            if debug:
                print(f"[DEBUG] idx_inout_raw (before normalization) = {repr(idx_inout_raw)} for idx_entry id={idx_entry.get('id')}")
            idx_inout = _normalize_inout(idx_inout_raw)
            if debug:
                print(f"[DEBUG] idx_inout normalized = {repr(idx_inout)}")

            if idx_inout:
                meta['indoor_outdoor'] = idx_inout
                if tour_label == 'wta':
                    meta['inOutdoor'] = idx_inout
                if debug:
                    print(f"[DEBUG] meta['indoor_outdoor'] set to {idx_inout} from tournament index (id={idx_entry.get('id')})")

            if not meta.get('level') and idx_entry.get('level'):
                meta['level'] = idx_entry.get('level')
            if not meta.get('surface') and idx_entry.get('surface'):
                meta['surface'] = idx_entry.get('surface')

        # ATP-specific fallback extraction
        if tour_label == 'atp' and (not city_name):
            tn = meta['tourney_name'] or ""
            if "canada" in tn.lower():
                fallback_city = choose_city_for_canada(meta['year'])
                city_name = fallback_city.split(',')[0]
            else:
                extracted = extract_city_from_atp_tourney_name(tn)
                city_name = extracted or city_name

        meta['city'] = city_name or ""
        meta['country'] = ""

        # Geocode lookup (unchanged)
        geocode = None
        matched_key = None

        tn_norm = _normalize_token(meta.get('tourney_name') or "")
        override = tourney_overrides.get(tn_norm)
        if override is not None:
            print(f"[INFO] override present for '{meta.get('tourney_name')}' -> '{override}' (strict)")
            if isinstance(override, (list, tuple)) and len(override) >= 2:
                try:
                    geocode = (float(override[0]), float(override[1]))
                    matched_key = f"override:{tn_norm}"
                except Exception:
                    geocode = None
            elif isinstance(override, str):
                if override in geos:
                    geocode = geos.get(override)
                    matched_key = override
                else:
                    g, mk = find_geocode_indexed(geos, exact_index, first_index, last_index, override)
                    if g:
                        geocode = g
                        matched_key = mk
                    else:
                        missing_set.add((meta.get('tourney_name') or "", event_id, event_year, str(override)))
                        geocode = None
            else:
                missing_set.add((meta.get('tourney_name') or "", event_id, event_year, str(override)))
                geocode = None
        else:
            if debug:
                print(f"[DEBUG] looking up geocode for tourney '{meta.get('tourney_name')}', city='{city_name}', country='{country_name}'")
            candidates = []
            if city_name and country_name:
                candidates.append(f"{city_name}, {country_name}")
            if city_name:
                candidates.append(city_name)
            ttitle = normalize_str(first.get('tournament_title') or first.get('tournament_name') or "")
            if ttitle and ',' in ttitle:
                after_dash = ttitle.split('-', 1)[-1].strip() if '-' in ttitle else ttitle
                candidates.append(after_dash)
            if meta.get('tourney_name'):
                candidates.append(meta.get('tourney_name'))

            tried = []
            for cand in candidates:
                if not cand:
                    continue
                tried.append(f"exact:'{cand}'")
                if cand in geos:
                    geocode = geos.get(cand)
                    matched_key = cand
                    if debug:
                        print(f"[DEBUG] exact geocode match for candidate '{cand}' -> key '{matched_key}'")
                    break
            if not geocode:
                for cand in candidates:
                    if not cand:
                        continue
                    tried.append(f"indexed:'{cand}'")
                    g, mk = find_geocode_indexed(geos, exact_index, first_index, last_index, cand)
                    if g:
                        geocode = g
                        matched_key = mk
                        if debug:
                            print(f"[DEBUG] indexed geocode match for candidate '{cand}' -> key '{matched_key}'")
                        break

            if not geocode and debug:
                print(f"[DEBUG] tried geocode candidates: {tried} (no match)")

        if geocode is None and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            if special_key in geos:
                geocode = geos.get(special_key)
                matched_key = special_key
                if debug:
                    print(f"[DEBUG] canada special-case matched key '{matched_key}'")

        if geocode is None:
            mapped = map_country_to_ioc(country_name)
            if mapped:
                meta['country'] = mapped
            else:
                missing_set.add((meta.get('tourney_name') or "", event_id, event_year, ""))
                meta['country'] = ""
        else:
            meta['geocode'] = list(geocode)
            mapped_from_derived = None
            derived_country_text = None
            if isinstance(matched_key, str) and ',' in matched_key:
                tokens = [t.strip() for t in matched_key.split(',') if t.strip()]
                for tok in reversed(tokens):
                    mapped_tok = map_country_to_ioc(tok)
                    if mapped_tok:
                        mapped_from_derived = mapped_tok
                        derived_country_text = tok
                        break
                if not mapped_from_derived and len(tokens) >= 2:
                    for n in range(1, min(4, len(tokens))+1):
                        cand = ", ".join(tokens[-n:])
                        mapped_cand = map_country_to_ioc(cand)
                        if mapped_cand:
                            mapped_from_derived = mapped_cand
                            derived_country_text = cand
                            break

            mapped_from_csv = None
            if country_name:
                mapped_from_csv = map_country_to_ioc(country_name)

            if mapped_from_derived:
                meta['country'] = mapped_from_derived
            elif mapped_from_csv:
                meta['country'] = mapped_from_csv
            else:
                meta['country'] = ""

        # build matches array (unchanged)
        matches_arr = []
        for r in rows:
            winner = (r.get('winner_player_name') or r.get('winner') or r.get('player_winner') or r.get('player_a') or "")
            loser = (r.get('loser_player_name') or r.get('loser') or r.get('player_loser') or r.get('player_b') or "")
            pid_w = (r.get('player_id_winner') or r.get('player_a_id') or r.get('playerida') or r.get('PlayerIDA') or r.get('player_winner') or r.get('player_winner_id') or "")
            pid_l = (r.get('player_id_loser') or r.get('player_b_id') or r.get('playeridb') or r.get('PlayerIDB') or r.get('player_loser') or r.get('player_loser_id') or "")
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

        # BYE reconstruction and champion extraction (unchanged logic)
        id_nums = []
        prefix_counts = {}
        width = 3
        for m in matches_arr:
            mid = (m.get('match_id') or "").strip()
            mo = re.match(r'^([A-Za-z]+)(\d+)$', mid)
            if mo:
                pref, num = mo.group(1), mo.group(2)
                try:
                    n = int(num)
                    id_nums.append(n)
                    prefix_counts[pref] = prefix_counts.get(pref, 0) + 1
                    width = max(width, len(num))
                except Exception:
                    continue

        if prefix_counts:
            prefix = max(prefix_counts.items(), key=lambda x: x[1])[0]
        else:
            prefix = 'LS' if meta.get('source', '').upper() == 'WTA' else 'MS'

        existing_indices = set(id_nums)
        expected_matches = None
        sds = meta.get('singles_draw_size')
        if isinstance(sds, int) and sds in (32, 64, 128):
            expected_matches = sds - 1
        else:
            max_existing = max(id_nums) if id_nums else 1
            for cand in (31, 63, 127):
                if max_existing <= cand:
                    expected_matches = cand
                    break
            if expected_matches is None:
                expected_matches = 127

        leaf_start = expected_matches // 2 + 1
        index_to_match = {}
        for m in matches_arr:
            mid = (m.get('match_id') or "").strip()
            mo = re.match(r'^([A-Za-z]+)(\n?\d+)$', mid)
            if mo:
                try:
                    idx = int(mo.group(2))
                    index_to_match[idx] = m
                except Exception:
                    continue

        synthetic_matches = []
        for i in range(leaf_start, expected_matches + 1):
            if i in existing_indices:
                continue
            parent_idx = i // 2
            sibling_idx = i - 1 if (i % 2 == 1) else i + 1
            parent = index_to_match.get(parent_idx)
            sibling = index_to_match.get(sibling_idx)
            if not parent or not sibling:
                continue
            sib_winner = (sibling.get('winner_player_name') or "").strip()
            parent_winner = (parent.get('winner_player_name') or "").strip()
            parent_loser = (parent.get('loser_player_name') or "").strip()
            missing_winner = None
            missing_winner_id = ""
            missing_winner_country = ""
            missing_winner_seed = ""
            if sib_winner and sib_winner == parent_winner:
                missing_winner = parent_loser
                missing_winner_id = parent.get('player_id_loser') or ""
                missing_winner_country = parent.get('loser_country') or ""
                missing_winner_seed = parent.get('loser_seed') or ""
            elif sib_winner and sib_winner == parent_loser:
                missing_winner = parent_winner
                missing_winner_id = parent.get('player_id_winner') or ""
                missing_winner_country = parent.get('winner_country') or ""
                missing_winner_seed = parent.get('winner_seed') or ""
            else:
                if sib_winner and parent_winner and sib_winner.lower() == parent_winner.lower():
                    missing_winner = parent_loser
                    missing_winner_id = parent.get('player_id_loser') or ""
                    missing_winner_country = parent.get('loser_country') or ""
                    missing_winner_seed = parent.get('loser_seed') or ""
                elif sib_winner and parent_loser and sib_winner.lower() == parent_loser.lower():
                    missing_winner = parent_winner
                    missing_winner_id = parent.get('player_id_winner') or ""
                    missing_winner_country = parent.get('winner_country') or ""
                    missing_winner_seed = parent.get('winner_seed') or ""
                else:
                    continue

            padded_num = str(i).zfill(width)
            synthetic_mid = f"{prefix}{padded_num}"
            synthetic = {
                "match_id": synthetic_mid,
                "round": "1",
                "winner_player_name": missing_winner or "",
                "loser_player_name": "BYE",
                "score_string": "",
                "player_id_winner": missing_winner_id or "",
                "player_id_loser": "",
                "winner_country": missing_winner_country or "",
                "loser_country": "",
                "winner_seed": missing_winner_seed or "",
                "loser_seed": ""
            }
            synthetic_matches.append((i, synthetic))
            existing_indices.add(i)
            index_to_match[i] = synthetic

        for _, sm in synthetic_matches:
            matches_arr.append(sm)

        def match_sort_key(m):
            mid = (m.get('match_id') or "").strip()
            mo = re.match(r'^([A-Za-z]+)(\d+)$', mid)
            if mo:
                try:
                    return int(mo.group(2))
                except Exception:
                    return 10**9
            return 10**9
        matches_arr = sorted(matches_arr, key=match_sort_key)

        champion_info = None
        for m in matches_arr:
            mid = (m.get('match_id') or "").strip()
            if not mid:
                continue
            if mid.upper().startswith('MS001') or mid.upper().startswith('LS001'):
                champion_info = {
                    'winner_player_name': m.get('winner_player_name') or "",
                    'player_id_winner': m.get('player_id_winner') or "",
                    'winner_country': m.get('winner_country') or ""
                }
                break
        if champion_info is None:
            finals_candidates = []
            for m in matches_arr:
                rnd = (m.get('round') or "").strip()
                if rnd and re.fullmatch(r'(?i)F|FINAL|FINALS|T|TFINAL|LS', rnd):
                    finals_candidates.append(m)
            if not finals_candidates:
                for m in matches_arr:
                    rnd = (m.get('round') or "").strip()
                    if rnd and 'final' in rnd.lower():
                        finals_candidates.append(m)
            if finals_candidates:
                chosen = None
                for m in finals_candidates:
                    if (m.get('winner_player_name') or "").strip():
                        chosen = m
                        break
                if chosen is None:
                    chosen = finals_candidates[0]
                champion_info = {
                    'winner_player_name': chosen.get('winner_player_name') or "",
                    'player_id_winner': chosen.get('player_id_winner') or "",
                    'winner_country': chosen.get('winner_country') or ""
                }
        if champion_info:
            meta['champion'] = champion_info

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

# ---------------- autotools ----------------

def autodiscover_tournaments_path(prefix: str = 'atp') -> Optional[str]:
    """
    Try several heuristics to find the tournaments file/dir under the repo.
    Return a string path (file or dir) or None.
    """
    doc = Path('docs')
    # common explicit files
    candidates = [
        doc / f"{prefix}_tournaments_2026.json",
        doc / f"{prefix}_tournaments_2025.json",
        doc / f"{prefix}_tournaments.json",
        doc / f"{prefix}_tournaments_json_dir",
        doc / f"{prefix}_tournaments_json_dir".replace('_json_dir', ''),  # just in case
    ]
    for c in candidates:
        if c.exists():
            return str(c)

    # search for any sensible file under docs
    if doc.exists():
        files = list(doc.rglob(f"{prefix}_tournaments*.json"))
        if files:
            # prefer a file (not directory); return the first reasonable pick
            return str(files[0])

        # look for a directory name containing prefix
        for d in doc.rglob(f"{prefix}*"):
            if d.is_dir():
                # check it contains json files
                jfiles = list(d.glob('*.json'))
                if jfiles:
                    return str(d)

    # fallback: search project tree for 'atp' + 'tournaments' substrings
    for f in Path('.').rglob('*.json'):
        if f.name.lower().startswith(f"{prefix}_tournaments") or f.name.lower().find(f"{prefix}_tournaments") >= 0:
            return str(f)

    return None

# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", "-d", required=True, help="directory containing CSV files to process")
    ap.add_argument("--geocodes", "-g", required=True, help="path to docs/tools/geocodes_combined.json")
    ap.add_argument("--country-map", "-c", required=True, help="path to docs/tools/country_to_ioc.json")
    ap.add_argument("--out-base", "-o", required=True, help="base output directory for per-csv outputs (one subdir per csv)")
    ap.add_argument("--tourney-overrides", "-t", required=False, help="optional JSON file mapping ambiguous tourney_name -> preferred geocode key or [lat,lon]")
    # keep args for power-user but not required — script will autodiscover when missing
    ap.add_argument("--atp-tournaments", required=False, help="optional path (file or dir) containing atp_tournaments_YEAR.json files")
    ap.add_argument("--wta-tournaments", required=False, help="optional path (file or dir) containing wta_tournaments_YEAR.json files")
    ap.add_argument("--debug", action="store_true", help="enable verbose debug output")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"[ERROR] input-dir not found or not a directory: {input_dir}")
        raise SystemExit(2)

    geos = read_geocodes(args.geocodes)
    country_map = read_country_map(args.country_map)
    tourney_overrides = read_tourney_overrides(args.tourney_overrides) if args.tourney_overrides else {}

    # if user didn't provide the tournaments index path, try autodiscovery
    atp_path = args.atp_tournaments
    wta_path = args.wta_tournaments

    if not atp_path:
        atp_auto = autodiscover_tournaments_path('atp')
        if atp_auto:
            atp_path = atp_auto
            print(f"[INFO] autodiscovered atp tournaments path: {atp_path}")
        else:
            print("[WARN] no --atp-tournaments provided and autodiscovery failed; continuing without ATP index")

    if not wta_path:
        wta_auto = autodiscover_tournaments_path('wta')
        if wta_auto:
            wta_path = wta_auto
            print(f"[INFO] autodiscovered wta tournaments path: {wta_path}")
        else:
            # not fatal
            if args.debug:
                print("[DEBUG] no --wta-tournaments provided and autodiscovery failed; continuing without WTA index")

    atp_index = build_tournaments_index_from_path(atp_path, 'atp') if atp_path else {}
    wta_index = build_tournaments_index_from_path(wta_path, 'wta') if wta_path else {}

    if args.debug:
        print(f"[DEBUG] atp_index size = {len(atp_index)}")
        # show a small sample of keys to help debug
        sample_keys = list(atp_index.keys())[:30]
        if sample_keys:
            print(f"[DEBUG] atp_index sample keys: {sample_keys[:10]}{' ...' if len(sample_keys)>10 else ''}")

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

    missing_set = set()

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
        results = build_tournament_jsons(report, geos, country_map, tourney_overrides=tourney_overrides,
                                        missing_set=missing_set, atp_index=atp_index, wta_index=wta_index, debug=args.debug)

        print(f"[INFO] Results for {csv_path.name}: produced {len(report['produced_tournament_files'])} tournament JSON(s)")
        overall_report['processed_csvs'].append(str(csv_path))
        overall_report['produced_match_files'].extend(report['produced_match_files'])
        overall_report['produced_tournament_files'].extend(report['produced_tournament_files'])
        overall_report['errors'].extend(report['errors'])
        overall_report['warnings'].extend(report['warnings'])

    if missing_set:
        miss_file = out_base.joinpath("missing_geocodes.txt")
        with miss_file.open('w', encoding='utf-8') as fh:
            for (tn, eid, yr, ov) in sorted(missing_set):
                fh.write(f"{tn} | {eid} | {yr} | {ov}\n")
        print(f"[INFO] Wrote missing geocodes list to: {miss_file}")

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