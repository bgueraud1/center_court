# fix_coords_cache.py
import json, sys, os, re
from pathlib import Path

def normalize_place(place: str) -> str:
    if not place or not isinstance(place, str):
        return ""
    s = place
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', s)
    s = s.strip()
    s = re.sub(r'^[,;\s]+|[,;\s]+$', '', s)
    s = re.sub(r'\s*,\s*', ', ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

def load_json_flexible(path):
    # try utf-8, then latin-1, then fallback
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        try:
            with open(path, 'r', encoding='latin-1') as f:
                raw = f.read()
                return json.loads(raw)
        except Exception as e2:
            print("Failed to parse JSON:", e2)
            return None

def main():
    p = Path('maps_html/coords_cache_migrations.json')
    if not p.exists():
        print("File not found:", p)
        return
    data = load_json_flexible(p)
    if data is None:
        print("Could not load JSON cache; aborting.")
        return

    geocode = data.get('geocode', {})
    reverse = data.get('reverse', {})

    new_geo = {}
    for k, v in geocode.items():
        nk = normalize_place(k)
        if nk == "":
            continue
        # if duplicate normalized keys, prefer existing new_geo[nk] if present
        if nk in new_geo:
            # keep first non-null value
            if new_geo[nk] is None and v is not None:
                new_geo[nk] = v
        else:
            new_geo[nk] = v

    # Normalize reverse keys to 5 decimals (if possible)
    new_rev = {}
    for k, v in reverse.items():
        try:
            # attempt to parse floats
            parts = [float(x) for x in re.split(r'\s*,\s*', k)]
            if len(parts) >= 2:
                key = f"{parts[0]:.5f},{parts[1]:.5f}"
            else:
                key = k
        except Exception:
            key = k
        new_rev[key] = v

    out = {'geocode': new_geo, 'reverse': new_rev}
    out_path = p.with_name(p.stem + '.fixed.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print("Wrote fixed cache to", out_path)
    print("Sample keys (first 20):")
    for i, k in enumerate(list(new_geo.keys())[:20]):
        print(" ", i, repr(k))

if __name__ == '__main__':
    main()
