#!/usr/bin/env python3
"""
fin_wrong_birthplaces_atp.py

Autonome : détecte automatiquement un CSV ATP si aucun --input n'est fourni,
géocode les birthplaces non encore en cache (via Nominatim) et produit :
  - maps_html/coords_cache_atp.json  (cache JSON des geocodes)
  - failed_geocodes_atp.csv         (lignes avec birthplace mais geocode échoué)

Usage:
  python fin_wrong_birthplaces_atp.py            # auto-detect CSV
  python fin_wrong_birthplaces_atp.py --input path/to/player_data_atp.csv
  SKIP_GEOCODE=1 python fin_wrong_birthplaces_atp.py  # n'effectue pas d'appels réseau
"""
from pathlib import Path
import argparse
import json
import os
import re
import tempfile
import time
from typing import Optional, Tuple, Dict, Any

import pandas as pd

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeopyError
except Exception as e:
    raise SystemExit("geopy is required: pip install geopy\nError: " + str(e))

# ----------------------------
# Defaults & candidate input files
# ----------------------------
CANDIDATE_INPUTS = [
    Path("player_data_atp.csv"),
    Path("player_data_atp.csv"),  # duplicate intentionally harmless
    Path("player_base_and_maps") / "player_data_atp.csv",
]

DEFAULT_CACHE = Path("maps_html") / "coords_cache_atp.json"
DEFAULT_OUTPUT = Path("failed_geocodes_atp.csv")

# ----------------------------
# Cache I/O (robuste)
# ----------------------------
def load_cache(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"geocode": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except UnicodeDecodeError:
        try:
            with path.open("r", encoding="latin-1") as f:
                return json.load(f)
        except Exception:
            try:
                raw = path.read_text(encoding="latin-1")
                cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', raw)
                return json.loads(cleaned)
            except Exception:
                print(f"Warning: failed to parse cache {path}. Returning empty cache.")
                return {"geocode": {}}
    except json.JSONDecodeError as e:
        print(f"Warning: JSON decode error for cache {path}: {e}")
        return {"geocode": {}}
    except Exception as e:
        print(f"Warning: unexpected error reading cache {path}: {e}")
        return {"geocode": {}}

def save_cache(cache: dict, path: Path):
    parent = path.parent
    if parent and not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="cache_", suffix=".json", dir=str(parent or "."))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, str(path))
    except Exception as e:
        print(f"Warning: Failed to save cache {path}: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

# ----------------------------
# Normalisation clé cache
# ----------------------------
def normalize_place(place: str) -> str:
    if not place or not isinstance(place, str):
        return ""
    s = place
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', s)   # remove control chars
    s = s.strip()
    s = re.sub(r'\s*,\s*', ', ', s)              # unify commas spacing
    s = re.sub(r'\s+', ' ', s)                   # collapse spaces
    s = s.strip(' ,;')
    return s

# ----------------------------
# Geocoding wrapper
# ----------------------------
def get_geolocator(user_agent: str = "failed-geocode-checker", timeout: int = 10):
    return Nominatim(user_agent=user_agent, timeout=timeout)

def geocode_with_cache(place: str,
                       cache: Dict[str, Any],
                       cache_file: Path,
                       user_agent: str = "failed-geocode-checker",
                       delay: float = 1.0,
                       timeout: int = 10,
                       max_retries: int = 2) -> Optional[Tuple[float, float]]:
    if not place or not isinstance(place, str):
        return None
    key = normalize_place(place)
    cache_geo = cache.setdefault("geocode", {})
    if key in cache_geo:
        v = cache_geo[key]
        return None if v is None else tuple(v)

    # If SKIP_GEOCODE env var set -> do not call network
    if str(os.environ.get("SKIP_GEOCODE", "0")).strip().lower() in ("1", "true", "yes"):
        return None

    geolocator = get_geolocator(user_agent=user_agent, timeout=timeout)
    for attempt in range(max_retries + 1):
        if delay:
            time.sleep(delay)
        try:
            loc = geolocator.geocode(place)
            if loc:
                coords = (float(loc.latitude), float(loc.longitude))
                cache_geo[key] = [coords[0], coords[1]]
                save_cache(cache, cache_file)
                return coords
            else:
                cache_geo[key] = None
                save_cache(cache, cache_file)
                return None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            if attempt >= max_retries:
                cache_geo[key] = None
                save_cache(cache, cache_file)
                return None
            time.sleep(1 + attempt)
            continue
        except GeopyError:
            cache_geo[key] = None
            save_cache(cache, cache_file)
            return None
        except Exception:
            cache_geo[key] = None
            save_cache(cache, cache_file)
            return None

# ----------------------------
# Main behaviour
# ----------------------------
def find_failed_geocodes(input_csv: Path,
                         cache_file: Path,
                         out_failed_csv: Path,
                         user_agent: str = "failed-geocode-checker",
                         delay: float = 1.0,
                         timeout: int = 10,
                         max_retries: int = 2):
    print(f"Input CSV: {input_csv}")
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    cache = load_cache(cache_file)

    # gather distinct birthplaces that are non-empty and not yet cached
    distinct_places = []
    for p in df.get('birthplace', pd.Series(dtype=str)).unique():
        if p and str(p).strip():
            nk = normalize_place(str(p))
            if nk not in cache.get('geocode', {}):
                distinct_places.append(p)

    distinct_places = sorted(set(distinct_places))
    print(f"Found {len(distinct_places)} distinct birthplace(s) not in cache.")

    # geocode each (this will update cache). If SKIP_GEOCODE is set, this loop will be a no-op.
    for i, place in enumerate(distinct_places, start=1):
        print(f"[{i}/{len(distinct_places)}] Geocoding: {place}")
        geocode_with_cache(place, cache, cache_file, user_agent=user_agent, delay=delay, timeout=timeout, max_retries=max_retries)

    # Now identify rows with birthplace present but cache has None
    failed_rows = []
    for idx, row in df.iterrows():
        birthplace = row.get('birthplace', '')
        if not birthplace or str(birthplace).strip() == '':
            continue
        key = normalize_place(str(birthplace))
        # consider as failure only if we actually tried (key present in cache) and value is None
        if key in cache.get('geocode', {}):
            if cache['geocode'][key] is None:
                failed_rows.append({
                    "row_index": int(idx),
                    "player_id": row.get('player_id', ''),
                    "full_name": row.get('full_name', ''),
                    "birthplace": birthplace
                })

    if failed_rows:
        print(f"\nFound {len(failed_rows)} row(s) where birthplace exists but geocoding FAILED (cached None).")
        out_df = pd.DataFrame(failed_rows)
        out_failed_csv.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(out_failed_csv, index=False, encoding="utf-8")
        print(f"Saved failed rows -> {out_failed_csv.resolve()}")
        for r in failed_rows:
            print(f"  idx={r['row_index']}, name={r['full_name']!r}, id={r['player_id']!r}, birthplace={r['birthplace']!r}")
    else:
        print("\nNo failed geocodes found (either everything geocoded successfully or SKIP_GEOCODE prevented attempts).")

    if str(os.environ.get("SKIP_GEOCODE", "0")).strip().lower() in ("1", "true", "yes"):
        print("\nNote: SKIP_GEOCODE=1 — network geocoding was skipped for uncached places.")

# ----------------------------
# CLI + auto-detect input if not provided
# ----------------------------
def find_input_file(provided: Optional[str]) -> Optional[Path]:
    if provided:
        p = Path(provided)
        return p if p.exists() else None
    # env var override
    env_in = os.environ.get("INPUT_CSV")
    if env_in:
        p = Path(env_in)
        if p.exists():
            return p
    # try candidate list
    for cand in CANDIDATE_INPUTS:
        if cand.exists():
            return cand
    # try any file in cwd matching *atp*.csv or *player*.csv
    for p in Path.cwd().glob("*atp*.csv"):
        return p
    for p in Path.cwd().glob("*player*.csv"):
        return p
    return None

def main():
    parser = argparse.ArgumentParser(description="Liste lignes avec birthplace mais geocode echoué (ATP).")
    parser.add_argument("--input", "-i", help="CSV input (player_data_atp.csv). If omitted, auto-detects common filenames.")
    parser.add_argument("--cache", "-c", default=str(DEFAULT_CACHE), help=f"Cache JSON file (default: {DEFAULT_CACHE})")
    parser.add_argument("--out", "-o", default=str(DEFAULT_OUTPUT), help=f"Output CSV for failed rows (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--user-agent", default="failed-geocode-checker", help="User-Agent for Nominatim")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds delay between geocode requests")
    parser.add_argument("--timeout", type=float, default=10.0, help="Geolocator timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="Retries for each geocode")
    args = parser.parse_args()

    input_file = find_input_file(args.input)
    if not input_file:
        print("Error: could not auto-detect an input CSV. Please provide --input or place a candidate file in the current directory.")
        print("Candidates searched:", [str(p) for p in CANDIDATE_INPUTS])
        raise SystemExit(2)

    find_failed_geocodes(Path(input_file),
                         Path(args.cache),
                         Path(args.out),
                         user_agent=args.user_agent,
                         delay=args.delay,
                         timeout=args.timeout,
                         max_retries=args.retries)

if __name__ == "__main__":
    main()
