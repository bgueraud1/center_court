#!/usr/bin/env python3
"""
scripts/build_tournament_jsons.py

Usage:
  python3 scripts/build_tournament_jsons.py --input-list created_files.txt \
      --geocodes docs/tools/geocodes_combined.json \
      --country-map docs/tools/country_to_ioc.json \
      --out-base docs/data/tournaments/json_by_tournaments \
      [--tourney-overrides docs/tools/tourney_overrides.json] \
      [--enable-geocoding] [--geocode-provider nominatim|google] [--geocode-cache path]

Notes:
 - tourney_overrides.json example:
   {
     "Auckland": "Auckland, New Zealand",
     "Stuttgart-1": "Stuttgart, Germany"
   }
 - By default geocoding fallback is DISABLED. To allow the script to call an external geocoder,
   pass --enable-geocoding. The default provider is 'nominatim' (OpenStreetMap). For Google,
   set provider 'google' and provide GOOGLE_API_KEY environment variable.
 - Geocode cache is stored in <out_base>/geocode_cache.json by default (or pass custom path).
"""
import argparse
import json
from pathlib import Path
import pandas as pd
import re
import unicodedata
import sys
import os
import time
from collections import defaultdict

# Optional geocoding (import only if enabled at runtime)
_GEOPY_AVAILABLE = False
try:
    from geopy.geocoders import Nominatim, GoogleV3  # optional import; may fail if geopy not installed
    _GEOPY_AVAILABLE = True
except Exception:
    _GEOPY_AVAILABLE = False

# ---------------- utilitaires ----------------

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
    """Lowercase, remove accents and punctuation, collapse whitespace for matching keys."""
    if not s:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def build_geocode_index(geos):
    """
    Build simple indices for robust matching:
      - exact_norm_index: normalized whole key -> list of (orig_key, coords)
      - first_token_index: first token normalized -> list
      - last_token_index: last token normalized -> list
    """
    exact_norm_index = defaultdict(list)
    first_token_index = defaultdict(list)
    last_token_index = defaultdict(list)
    for k, coords in geos.items():
        nk = _normalize_token(k)
        exact_norm_index[nk].append((k, coords))
        # first token: before comma if comma present, else first word
        if ',' in k:
            first = k.split(',', 1)[0].strip()
        else:
            first = k.split()[0] if k.split() else k
        if first:
            first_token_index[_normalize_token(first)].append((k, coords))
        # last token
        last_word = nk.split()[-1] if nk.split() else nk
        last_token_index[_normalize_token(last_word)].append((k, coords))
    return exact_norm_index, first_token_index, last_token_index

def read_country_map(path):
    """
    Load country->IOC mapping JSON. Normalise les clés pour matching.
    """
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
    """
    Indexed lookup using indices created by build_geocode_index.
    Returns (coords, matched_key) or (None, None)
    """
    if not candidate:
        return None, None
    cand_norm = _normalize_token(candidate)
    # 1) exact normalized match
    if cand_norm in exact_index:
        candidates = exact_index[cand_norm]
        k, coords = sorted(candidates, key=lambda t: len(t[0]))[0]
        return coords, k
    # 2) first token match
    if cand_norm in first_index:
        candidates = first_index[cand_norm]
        k, coords = sorted(candidates, key=lambda t: len(t[0]))[0]
        return coords, k
    # 3) last token match
    if cand_norm in last_index:
        candidates = last_index[cand_norm]
        k, coords = sorted(candidates, key=lambda t: len(t[0]))[0]
        return coords, k
    # 4) whole-word substring match inside normalized keys
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

def tournament_has_no_location(tname):
    lowered = (tname or "").lower()
    keywords = ["finals", "final", "nitto atp finals", "masters cup", "tour world championship", "next gen", "intesa sanpaolo next gen", "next gen atp"]
    return any(k in lowered for k in keywords)

def extract_city_from_atp_tourney_name(tourney_name):
    """
    Heuristics to extract a city token from tourney_name.
    Handles examples like:
      - "Adelaide 125" -> "Adelaide"
      - "Melbourne-1" -> "Melbourne"
      - "Nur Sultan 3" -> "Nur Sultan"
      - strip common prefixes like 'atp', 'wta', 'masters 1000'
    """
    if not tourney_name:
        return None
    name = str(tourney_name).strip()
    # remove known prefixes
    name = re.sub(r"^(atp|wta)\s+", "", name, flags=re.I).strip()
    name = re.sub(r"^masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*masters\s*1000\s*", "", name, flags=re.I)
    name = re.sub(r"^atp\s*", "", name, flags=re.I)
    name = re.sub(r"^wta\s*", "", name, flags=re.I)
    # remove suffix like " - City, State, Country" if present (rare)
    if '-' in name and ',' in name:
        # "Austin 125 - Austin, TX, USA" -> after dash is "Austin, TX, USA"
        right = name.split('-', 1)[-1].strip()
        if right:
            return right.split(',')[0].strip()
    # take part before comma if present
    if ',' in name:
        return name.split(',')[0].strip()
    # strip trailing tokens that are purely numeric or like "#1" or "-1"
    m = re.match(r'^(.+?)(?:[\s\-_#]+?\d+)?$', name)
    if m:
        cand = m.group(1).strip()
        return cand
    # fallback last word
    parts = name.split()
    return parts[0] if parts else None

def safe_makedirs(p: Path):
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

def json_serialize_safe(obj):
    if isinstance(obj, (pd.Timestamp, )):
        return str(obj)
    return obj

# ---------------- traitement CSV -> fichiers match ----------------

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
    # only prefer tournament-level country columns (not match-level)
    col_country = _first_of(cols, ['country', 'venue_country', 'tourney_country', 'country_code', 'country_code3'])

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

        # group by tour_label,event_id,event_year (all rows of same tournoi groupés)
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
            # ensure we capture first non-empty tourney_name/city/country if missing
            info = report['tournaments'][key]
            if not info.get('tourney_name') and tourney_name:
                info['tourney_name'] = tourney_name
            if not info.get('city') and city_val:
                info['city'] = city_val
            if not info.get('country') and country_val:
                info['country'] = country_val

        report['tournaments'][key]['rows'].append(row_dict)
        report['produced_match_files'].append(str(match_file))

# ---------------- construction des tournament.json (incl. BYE + geocode fallback) ----------------

def build_tournament_jsons(report, geos, country_map, tourney_overrides=None,
                           enable_geocoding=False, geocode_provider='nominatim',
                           geocode_cache_path=None, out_base=None):
    """
    build_tournament_jsons(...):
      - tourney_overrides: dict normalized_tourney_name -> override string or [lat,lon]
      - enable_geocoding: if True attempt geocoding for missing cities (requires geopy)
      - geocode_provider: 'nominatim' or 'google'
      - geocode_cache_path: path to JSON cache to store geocoder results
    """
    tourney_overrides = tourney_overrides or {}
    exact_index, first_index, last_index = build_geocode_index(geos)
    missing_set = set()

    # set up geocoder + cache
    geocode_cache_path = Path(geocode_cache_path) if geocode_cache_path else (Path(out_base).joinpath("geocode_cache.json") if out_base else None)
    geocode_cache = {}
    if geocode_cache_path and geocode_cache_path.exists():
        try:
            geocode_cache = read_json_tolerant(geocode_cache_path)
            if not isinstance(geocode_cache, dict):
                geocode_cache = {}
        except Exception:
            geocode_cache = {}

    geolocator = None
    if enable_geocoding:
        if not _GEOPY_AVAILABLE:
            print("[WARN] geocoding requested but geopy not installed. Install geopy or disable --enable-geocoding.")
            enable_geocoding = False
        else:
            if geocode_provider == 'nominatim':
                geolocator = Nominatim(user_agent="tournament-geocoder-utility")
            elif geocode_provider == 'google':
                api_key = os.environ.get("GOOGLE_API_KEY")
                if not api_key:
                    print("[WARN] google geocode provider requested but GOOGLE_API_KEY not set. Disabling geocoding.")
                    enable_geocoding = False
                else:
                    geolocator = GoogleV3(api_key=api_key)
            else:
                print(f"[WARN] unknown geocode provider '{geocode_provider}'; disabling geocoding.")
                enable_geocoding = False

    def geocode_with_cache(query):
        """
        Query could be 'City, Country' or 'City' etc.
        Returns (lat, lon, display_name, raw_address_dict_or_none) or (None, None, None, None)
        """
        if not enable_geocoding or not geolocator:
            return None, None, None, None
        qn = query.strip()
        if not qn:
            return None, None, None, None
        if geocode_cache is None:
            local_cache = {}
        else:
            local_cache = geocode_cache
        if qn in local_cache:
            val = local_cache.get(qn)
            if val and isinstance(val, dict):
                return val.get('lat'), val.get('lon'), val.get('display_name'), val.get('address')
            return None, None, None, None
        # Not cached: call geocoder (rate-limit friendly)
        try:
            # Respect polite delay for Nominatim
            if geocode_provider == 'nominatim':
                # per Nominatim usage policy, keep a pause
                time.sleep(1)
            loc = geolocator.geocode(qn, exactly_one=True, timeout=10)
            if loc:
                lat = float(loc.latitude)
                lon = float(loc.longitude)
                display = getattr(loc, 'address', None) or getattr(loc, 'raw', {}).get('display_name', None)
                address = None
                try:
                    raw = getattr(loc, 'raw', {})
                    address = raw.get('address') if isinstance(raw, dict) else None
                except Exception:
                    address = None
                entry = {'lat': lat, 'lon': lon, 'display_name': display, 'address': address}
                local_cache[qn] = entry
                # persist cache
                if geocode_cache_path:
                    try:
                        geocode_cache_path.write_text(json.dumps(local_cache, ensure_ascii=False, indent=2), encoding='utf-8')
                    except Exception:
                        pass
                return lat, lon, display, address
        except Exception as e:
            print(f"[WARN] geocoder lookup failed for '{qn}': {e}")
            return None, None, None, None
        # cache negative result
        if geocode_cache_path:
            try:
                local_cache[qn] = None
                geocode_cache_path.write_text(json.dumps(local_cache, ensure_ascii=False, indent=2), encoding='utf-8')
            except Exception:
                pass
        return None, None, None, None

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
        # attempt progressive joins
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

        # add start_date if available
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

        # if city empty for ATP, try extract from tourney_name heuristics
        if tour_label == 'atp' and (not city_name):
            tn = meta['tourney_name'] or ""
            if "canada" in tn.lower():
                fallback_city = choose_city_for_canada(meta['year'])
                city_name = fallback_city.split(',')[0]
            else:
                extracted = extract_city_from_atp_tourney_name(tn)
                if extracted:
                    city_name = extracted

        meta['city'] = city_name or ""
        meta['country'] = ""  # will set IOC code only if mappable

        # geocode lookup: use override -> city+country exact -> indexed lookup -> optionally geocode fallback
        geocode = None
        matched_key = None

        tn_norm = _normalize_token(meta.get('tourney_name') or "")
        override = None
        if isinstance(tourney_overrides, dict):
            override = tourney_overrides.get(tn_norm)
        if override is not None:
            # strict override: try an exact key in geos, or treat override as lat/lon tuple if provided
            print(f"[INFO] override present for '{meta.get('tourney_name')}' -> '{override}' (strict mode)")
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
                    # try indexed lookup on override string
                    g, mk = find_geocode_indexed(geos, *build_geocode_index(geos), override)
                    if g:
                        geocode = g
                        matched_key = mk
                    else:
                        missing_set.add((meta.get('tourney_name') or "", event_id, event_year, str(override)))
                        print(f"[WARN] override for '{meta.get('tourney_name')}' did not resolve in geocodes: '{override}'")
                        geocode = None
            else:
                missing_set.add((meta.get('tourney_name') or "", event_id, event_year, str(override)))
                geocode = None
        else:
            # normal sequence
            print(f"[INFO] looking up geocode for tourney '{meta.get('tourney_name')}', city='{city_name}', country='{country_name}'")
            candidates = []
            if city_name and country_name:
                candidates.append(f"{city_name}, {country_name}")
            if city_name:
                candidates.append(city_name)
            # if tournament title has "City, State, Country" try that tail
            ttitle = normalize_str(first.get('tournament_title') or first.get('tournament_name') or "")
            if ttitle and ',' in ttitle:
                after_dash = ttitle.split('-', 1)[-1].strip() if '-' in ttitle else ttitle
                candidates.append(after_dash)
            if meta.get('tourney_name'):
                candidates.append(meta.get('tourney_name'))

            found = False
            tried = []
            # exact key first (literal keys in geos)
            for cand in candidates:
                if not cand:
                    continue
                tried.append(f"exact:'{cand}'")
                if cand in geos:
                    geocode = geos.get(cand)
                    matched_key = cand
                    print(f"[INFO] exact geocode match for candidate '{cand}' -> key '{matched_key}'")
                    found = True
                    break
            # indexed attempts
            if not found:
                for cand in candidates:
                    if not cand:
                        continue
                    tried.append(f"indexed:'{cand}'")
                    g, mk = find_geocode_indexed(geos, exact_index, first_index, last_index, cand)
                    if g:
                        geocode = g
                        matched_key = mk
                        print(f"[INFO] indexed geocode match for candidate '{cand}' -> key '{matched_key}'")
                        found = True
                        break
            # geocode fallback (remote) if enabled and still not found
            if not found and enable_geocoding:
                # prefer queries in order: "City, Country" -> "City" -> "Tourney Name"
                for cand in candidates:
                    if not cand:
                        continue
                    lat, lon, display, address = geocode_with_cache(str(cand))
                    if lat is not None and lon is not None:
                        geocode = (lat, lon)
                        matched_key = f"geocoded:{cand}"
                        print(f"[INFO] geocoded '{cand}' -> ({lat},{lon}) (display: {display})")
                        found = True
                        # store geocode into in-memory geos and indices for immediate reuse
                        try:
                            geos[matched_key] = geocode
                            # update indices (simple append)
                            exact_index[_normalize_token(matched_key)].append((matched_key, geocode))
                            first = cand.split(',', 1)[0].strip()
                            if first:
                                first_index[_normalize_token(first)].append((matched_key, geocode))
                        except Exception:
                            pass
                        break
                    else:
                        print(f"[INFO] geocoder did not resolve '{cand}'")
                        # continue to next candidate
            if not found:
                print(f"[INFO] tried candidates: {tried} (no match)")

        # Canada special-case
        if geocode is None and 'canada' in (meta['tourney_name'] or "").lower():
            special_key = choose_city_for_canada(meta['year'])
            if special_key in geos:
                geocode = geos.get(special_key)
                matched_key = special_key
                print(f"[INFO] canada special-case matched key '{matched_key}'")

        # Post-geocode: determine meta['country'] from matched_key tokens or tournament CSV country (ONLY if provided)
        if geocode is None:
            mapped = map_country_to_ioc(country_name)
            if mapped:
                meta['country'] = mapped
                print(f"[INFO] mapped tournament CSV country '{country_name}' -> IOC '{mapped}' (no geocode found)")
            else:
                missing_set.add((meta.get('tourney_name') or "", event_id, event_year, ""))
                print(f"[WARN] no geocode found for tournament '{meta.get('tourney_name')}', recorded in missing_geocodes")
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
                print(f"[INFO] country derived from matched_key '{matched_key}' -> '{derived_country_text}' mapped to IOC '{mapped_from_derived}'")
            elif mapped_from_csv:
                meta['country'] = mapped_from_csv
                print(f"[INFO] country taken from tournament CSV '{country_name}' mapped to IOC '{mapped_from_csv}'")
            else:
                meta['country'] = ""

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

        # --- BYE reconstruction (same approach as before)
        id_nums = []
        prefix_counts = {}
        width = 3
        for m in matches_arr:
            mid = (m.get('match_id') or "").strip()
            mo = re.match(r'^([A-Za-z]+)(\d+)$', mid)
            if mo:
                pref, num = mo.group(1), mo.group(2)
                n = int(num)
                id_nums.append(n)
                prefix_counts[pref] = prefix_counts.get(pref, 0) + 1
                width = max(width, len(num))

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
            mo = re.match(r'^([A-Za-z]+)(\d+)$', mid)
            if mo:
                idx = int(mo.group(2))
                index_to_match[idx] = m

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
                return int(mo.group(2))
            return 10**9
        matches_arr = sorted(matches_arr, key=match_sort_key)

        tournament_json = {
            "meta": meta,
            "matches": matches_arr
        }

        # write tournament JSON
        tour_json_path = out_dir.joinpath("tournament.json")
        with open(tour_json_path, 'w', encoding='utf-8') as fh:
            json.dump(tournament_json, fh, ensure_ascii=False, indent=2)

        # record
        report['produced_tournament_files'].append(str(tour_json_path))
        results.append((out_dir, tour_json_path, len(matches_arr)))

    # persist final geocode cache if used
    # (geocode cache was saved incrementally during lookups; nothing further required here)
    # write missing_set to out_base/missing_geocodes.txt if any
    if missing_set:
        miss_file = Path(out_base).joinpath("missing_geocodes.txt") if out_base else Path("missing_geocodes.txt")
        try:
            with miss_file.open('w', encoding='utf-8') as fh:
                for (tn, eid, yr, ov) in sorted(missing_set):
                    fh.write(f"{tn} | {eid} | {yr} | {ov}\n")
            print(f"[INFO] Wrote missing geocodes list to: {miss_file}")
        except Exception as e:
            print(f"[WARN] could not write missing geocodes file: {e}")

    return results

# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-list", "-i", required=True, help="text file listing CSV paths (created_files.txt)")
    ap.add_argument("--geocodes", "-g", required=True, help="path to docs/tools/geocodes_combined.json")
    ap.add_argument("--country-map", "-c", required=True, help="path to docs/tools/country_to_ioc.json (country name -> IOC 3-letter code)")
    ap.add_argument("--out-base", "-o", required=True, help="base output directory for per-tournament jsons")
    ap.add_argument("--tourney-overrides", "-t", required=False, help="optional JSON file mapping ambiguous tourney_name -> preferred geocode key or [lat,lon]")
    ap.add_argument("--enable-geocoding", action="store_true", help="If set, attempt remote geocoding for missing cities (requires geopy). Disabled by default.")
    ap.add_argument("--geocode-provider", default="nominatim", choices=("nominatim", "google"), help="Provider for remote geocoding when enabled")
    ap.add_argument("--geocode-cache", default=None, help="Optional path to geocode cache JSON (defaults to <out_base>/geocode_cache.json)")
    args = ap.parse_args()

    inp = Path(args.input_list)
    if not inp.exists():
        print(f"[ERROR] input list {inp} not found.")
        raise SystemExit(2)

    geos = read_geocodes(args.geocodes)
    country_map = read_country_map(args.country_map)
    tourney_overrides = read_tourney_overrides(args.tourney_overrides) if args.tourney_overrides else {}

    out_base = Path(args.out_base)
    safe_makedirs(out_base)

    csv_list = []
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
                    print(f"[WARN] Listed file not found: {ln} (skipping)")
                    continue
            csv_list.append(p)

    report = {
        'tournaments': {},
        'produced_match_files': [],
        'produced_tournament_files': [],
        'errors': [],
        'warnings': []
    }

    for p in csv_list:
        print(f"[INFO] Processing CSV: {p}")
        process_csv_file(p, geos, out_base, report)

    # Build tournament JSONs
    geocode_cache_path = args.geocode_cache if args.geocode_cache else str(out_base.joinpath("geocode_cache.json"))
    results = build_tournament_jsons(report, geos, country_map,
                                     tourney_overrides=tourney_overrides,
                                     enable_geocoding=args.enable_geocoding,
                                     geocode_provider=args.geocode_provider,
                                     geocode_cache_path=geocode_cache_path,
                                     out_base=out_base)

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