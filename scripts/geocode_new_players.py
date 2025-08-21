#!/usr/bin/env python3
"""
Robuste geocode_new_players.py

- Si --csv fourni n'existe pas, il tente :
  1) checher args.csv relatif au cwd
  2) chercher args.csv relatif aux parents (cwd/.., cwd/../.., ...)
  3) rglob() dans l'arborescence courante (pour retrouver le fichier par nom)
- Affiche des diagnostics utiles si non trouvé.
"""
from pathlib import Path
import json, argparse, time, os, sys, csv
from typing import Set
from requests.exceptions import RequestException

try:
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
except Exception:
    print("ERROR: geopy missing. `pip install geopy`", file=sys.stderr)
    raise

def load_cache(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            print(f"Warning: cache JSON unreadable at {path}, starting fresh.")
    return {}

def atomic_write(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def collect_birthplaces_from_csv(csv_path: Path) -> Set[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    places = set()
    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return places
        header_l = [h.strip().lower() for h in header]
        idx = None
        for candidate in ("birthplace","birth_place","birth place"):
            if candidate in header_l:
                idx = header_l.index(candidate); break
        if idx is None:
            for i,h in enumerate(header_l):
                if 'birth' in h:
                    idx = i; break
        if idx is None:
            print("No 'birthplace' column detected in CSV header; aborting.")
            return places
        for row in reader:
            if len(row) <= idx: continue
            bp = row[idx].strip()
            if bp and bp.lower() not in ("nan","none","null"):
                places.add(bp)
    return places

def resolve_csv_path(arg_path: str) -> Path:
    p = Path(arg_path)
    if p.exists():
        return p
    # try relative to cwd parents: cwd/arg_path, cwd/../arg_path, ...
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        candidate = parent / arg_path
        if candidate.exists():
            print(f"Found CSV at {candidate} (resolved from parent search).")
            return candidate
    # try rglob by filename only
    fname = Path(arg_path).name
    matches = list(cwd.rglob(fname))
    if matches:
        print(f"Found CSV by rglob: {matches[0]}")
        return matches[0]
    # not found -> print helpful info and raise
    print("ERROR: CSV not found at any checked location.", file=sys.stderr)
    print("Tried:", file=sys.stderr)
    print(f" - direct: {p}", file=sys.stderr)
    for parent in [cwd] + list(cwd.parents)[:4]:
        print(f" - {parent / arg_path}", file=sys.stderr)
    print("You can run from repo root or pass an absolute path.", file=sys.stderr)
    # also print cwd listing small sample
    try:
        print("\ncwd listing (top-level):")
        for x in sorted(Path.cwd().iterdir())[:40]:
            print("  ", x)
    except Exception:
        pass
    raise FileNotFoundError(arg_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="player CSV path (reads birthplace column)")
    parser.add_argument("--cache", required=True, help="coords cache JSON path (created/updated)")
    parser.add_argument("--user-agent", required=True, help="Nominatim user-agent (include contact email)")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--max", type=int, default=1000, help="max places to geocode this run")
    args = parser.parse_args()

    if os.environ.get("SKIP_GEOCODE") == "1":
        print("SKIP_GEOCODE=1 -> skipping geocoding (env).")
        return

    try:
        csv_path = resolve_csv_path(args.csv)
    except FileNotFoundError as e:
        print("ERROR: CSV not found; aborting.", file=sys.stderr)
        sys.exit(1)

    cache_path = Path(args.cache)
    birthplaces = collect_birthplaces_from_csv(csv_path)
    if not birthplaces:
        print("No birthplaces found -> nothing to geocode.")
        return

    cache = load_cache(cache_path)
    cache_geo = cache.setdefault("geocode", {})
    to_geocode = [p for p in sorted(birthplaces) if p not in cache_geo and (',' in p or len(p.split())>=2)]

    if not to_geocode:
        print("No new places to geocode (cache already covers all).")
        return

    print(f"{len(to_geocode)} places missing in cache; up to {args.max} will be attempted (delay={args.delay}s).")
    geolocator = Nominatim(user_agent=args.user_agent)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=args.delay, max_retries=2, error_wait_seconds=2.0)

    attempted = 0
    for i, place in enumerate(to_geocode):
        if attempted >= args.max: break
        try:
            print(f"[{i+1}/{len(to_geocode)}] Geocoding: {place}")
            loc = geocode(place, timeout=10)
            if loc:
                coords = [float(loc.latitude), float(loc.longitude)]
                cache_geo[place] = coords
                print(f"  -> {coords[0]:.6f},{coords[1]:.6f}")
            else:
                cache_geo[place] = None
                print("  -> not found (stored null)")
        except RequestException as e:
            print(f"  ! network error for {place}: {e}; storing null and continuing")
            cache_geo[place] = None
        except KeyboardInterrupt:
            print("Interrupted; saving cache and exiting.")
            break
        except Exception as e:
            print(f"  ! unexpected error for {place}: {e}; storing null")
            cache_geo[place] = None

        attempted += 1
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write(cache_path, cache)
        except Exception as e:
            print(f"Warning: failed saving cache: {e}")

    print(f"Done. Attempted {attempted} lookups. Cache saved at {cache_path}")

if __name__ == "__main__":
    main()
