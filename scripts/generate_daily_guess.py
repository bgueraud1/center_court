#!/usr/bin/env python3
"""
scripts/generate_daily_guess.py (robuste)

Lit player_data_atp.csv et player_data_wta.csv (à la racine),
lit maps_html/* pour geocodes,
écrit:
 - docs/tools/daily_guess.json
 - docs/tools/players_catalog.json
 - docs/tools/geocodes_combined.json

Usage (exemple):
 python scripts/generate_daily_guess.py \
   --atp player_data_atp.csv \
   --wta player_data_wta.csv \
   --geocodes-dir maps_html \
   --out-daily docs/tools/daily_guess.json \
   --out-players docs/tools/players_catalog.json \
   --out-geocodes docs/tools/geocodes_combined.json
"""
import csv
import json
import argparse
import os
import re
import random
import sys
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

NAME_CANDIDATES = [
    'full_name','full name','Full Name','Full_Name','Full_name','fullname','fullName',
    'name','player_name','player name','display_name','display name'
]

def parse_rank(val):
    if val is None: return None
    s = str(val).strip()
    if s == '' or s == '-' or s.lower() == 'nan': return None
    m = re.search(r'(\d+)', s)
    return int(m.group(1)) if m else None

def parse_height(row):
    # try fields that commonly hold height
    for key in ('height_cm','height_cm_raw','height','height_cm '):
        if key in row and row[key]:
            s = str(row[key]).strip()
            m = re.search(r'([\d.,]+)', s)
            if m:
                try:
                    val = float(m.group(1).replace(',','.'))
                    if val < 5:
                        return int(round(val*100))
                    if 50 < val < 300:
                        return int(round(val))
                except:
                    pass
    # fallback to inches
    for key in ('height_inches','height_inches '):
        if key in row and row[key]:
            s = str(row[key])
            m = re.search(r"(\d+)\s*'\s*(\d+)", s)
            if m:
                feet = int(m.group(1)); inches = int(m.group(2))
                total = feet*12 + inches
                return int(round(total * 2.54))
    return None

def norm_key(k):
    return k.strip() if isinstance(k, str) else k

def load_csv(path, source):
    out = []
    if not os.path.isfile(path):
        print(f"[WARN] CSV introuvable: {path}", file=sys.stderr)
        return out
    # use utf-8-sig to drop BOM if present
    with open(path, newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh)
        # trim header keys
        reader.fieldnames = [norm_key(fn) for fn in (reader.fieldnames or [])]
        for idx, row in enumerate(reader):
            # normalize keys by trimming
            row2 = {norm_key(k): (v.strip() if isinstance(v, str) else v) for k,v in row.items()}
            row2['_source'] = source
            row2['_csv_row_index'] = idx + 1
            out.append(row2)
    return out

def find_name_in_row(row):
    # test candidate keys
    for k in NAME_CANDIDATES:
        if k in row and row.get(k):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    # fallback: search any column that looks like a person's name (letters + space, length > 3)
    for k,v in row.items():
        if not v or k.startswith('_'): continue
        if isinstance(v, str):
            s = v.strip()
            if len(s) > 3 and re.search(r'[A-Za-zÀ-ÖØ-öø-ÿ]', s) and ' ' in s:
                # crude heuristic: contains letters and a space
                return s
    return None

def normalize_bool_play(s):
    if not s: return None
    s2 = str(s).lower()
    if 'right' in s2: return True
    if 'left' in s2: return False
    return None

def normalize_twohand(s):
    if not s: return None
    s2 = str(s).lower()
    if 'two' in s2: return True
    if 'one' in s2: return False
    return None

def build_players(atp_rows, wta_rows):
    players = []
    for row in (atp_rows + wta_rows):
        # attempt to extract name robustly
        full_name = find_name_in_row(row) or ''
        player_id = row.get('player_id') or row.get('id') or row.get('playerid') or row.get('player id') or ''
        country = (row.get('represented_country') or row.get('represented') or row.get('country') or '').strip()
        rank = parse_rank(row.get('highest_ranking') or row.get('best_rank') or row.get('bestRank') or row.get('ranking') or row.get('best_rank'))
        birth_date = row.get('birth_date') or row.get('birthdate') or row.get('birth_date ')
        birthplace = row.get('birthplace') or row.get('birth_place') or row.get('birth place') or row.get('birthplace ')
        height_cm = parse_height(row)
        plays = row.get('plays') or row.get('play')
        backhand = row.get('backhand') or ''
        # compute age if possible
        age = None
        if birth_date:
            try:
                d = None
                try:
                    d = datetime.fromisoformat(birth_date)
                except:
                    try:
                        d = datetime.strptime(birth_date, '%Y-%m-%d')
                    except:
                        try:
                            d = datetime.strptime(birth_date, '%b %d %Y')
                        except:
                            d = None
                if d:
                    today = datetime.now()
                    age = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
            except:
                age = None
        players.append({
            'full_name': full_name,
            'player_id': player_id,
            'represented_country': country,
            'rank': rank,
            'birth_date': birth_date,
            'birthplace': birthplace,
            'height_cm': height_cm,
            'plays': plays,
            'backhand': backhand,
            'right_handed': normalize_bool_play(plays),
            'two_handed': normalize_twohand(backhand),
            'age': age,
            'source': row.get('_source',''),
            '_raw_row': row
        })
    return players

def load_geocodes(dirpath):
    combined = {}
    if not os.path.isdir(dirpath):
        return combined
    for fname in os.listdir(dirpath):
        if not fname.lower().startswith('geocode'):
            continue
        path = os.path.join(dirpath, fname)
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                if 'geocode' in data and isinstance(data['geocode'], dict):
                    for k,v in data['geocode'].items(): combined[k] = v
                else:
                    for k,v in data.items(): combined[k] = v
        except Exception as e:
            print(f"[WARN] lecture geocode {path} failed: {e}", file=sys.stderr)
    return combined

def choose_for_level(players, topN, rng):
    # require full_name present for selection
    pool = [p for p in players if p.get('full_name') and p.get('rank') is not None and p.get('rank') <= topN and p.get('represented_country')]
    if not pool:
        pool = [p for p in players if p.get('full_name') and p.get('represented_country')]
    if not pool:
        # last fallback: any with full_name
        pool = [p for p in players if p.get('full_name')]
    if not pool:
        # nothing usable
        return None, None
    countries = sorted(list({p['represented_country'] for p in pool if p.get('represented_country')}))
    if not countries:
        chosen = rng.choice(pool)
        return chosen, None
    country = rng.choice(countries)
    by_country = [p for p in pool if p.get('represented_country') == country]
    chosen = rng.choice(by_country) if by_country else rng.choice(pool)
    return chosen, country

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--atp', required=True)
    ap.add_argument('--wta', required=True)
    ap.add_argument('--geocodes-dir', default='maps_html')
    ap.add_argument('--out-daily', required=True)
    ap.add_argument('--out-players', required=True)
    ap.add_argument('--out-geocodes', required=True)
    args = ap.parse_args()

    try:
        if ZoneInfo:
            tz = ZoneInfo('Europe/Paris')
            now = datetime.now(tz)
        else:
            now = datetime.now(timezone.utc)
    except:
        now = datetime.now(timezone.utc)

    seed_date = now.strftime('%Y-%m-%d')
    rng = random.Random(seed_date)

    atp_rows = load_csv(args.atp, 'ATP')
    wta_rows = load_csv(args.wta, 'WTA')

    players = build_players(atp_rows, wta_rows)

    # warn about rows without name
    missing_name = [p for p in players if not p.get('full_name')]
    if missing_name:
        print(f"[WARN] {len(missing_name)} lignes sans full_name trouvées (elles seront conservées dans le catalogue mais exclues de la sélection).", file=sys.stderr)
        for i,p in enumerate(missing_name[:20]):
            ridx = p['_raw_row'].get('_csv_row_index') if isinstance(p.get('_raw_row'), dict) else '?'
            print(f"  - idx={ridx} player_id={p.get('player_id')} country={p.get('represented_country')} birth={p.get('birth_date')}", file=sys.stderr)

    # write players_catalog.json
    os.makedirs(os.path.dirname(args.out_players), exist_ok=True)
    with open(args.out_players, 'w', encoding='utf-8') as fh:
        json.dump({'generated_at': now.isoformat(), 'count': len(players), 'players': players}, fh, ensure_ascii=False, indent=2)

    # geocodes
    geos = load_geocodes(args.geocodes_dir)
    os.makedirs(os.path.dirname(args.out_geocodes), exist_ok=True)
    with open(args.out_geocodes, 'w', encoding='utf-8') as fh:
        json.dump({'geocode': geos}, fh, ensure_ascii=False, indent=2)

    # generate daily choices, skipping rows with missing full_name
    out = {'date': seed_date, 'generated_at_iso': now.isoformat(), 'seed': seed_date, 'levels': {}}
    for topN, key in ((20,'top20'), (100,'top100'), (300,'top300')):
        chosen, country = choose_for_level(players, topN, rng)
        if chosen is None:
            print(f"[ERROR] Aucun joueur valide trouvé pour niveau {key} (top{topN})", file=sys.stderr)
            out['levels'][key] = {'chosen': None, 'country': None, 'pool_size': 0}
            continue
        out['levels'][key] = {
            'chosen': {
                'player_id': chosen.get('player_id'),
                'full_name': chosen.get('full_name'),
                'source': chosen.get('source'),
                'represented_country': chosen.get('represented_country'),
                'rank': chosen.get('rank'),
                'height_cm': chosen.get('height_cm'),
                'birth_date': chosen.get('birth_date'),
                'birthplace': chosen.get('birthplace')
            },
            'country': country,
            'pool_size': len([p for p in players if p.get('full_name') and p.get('represented_country') and p.get('rank') is not None and p.get('rank') <= topN])
        }

    os.makedirs(os.path.dirname(args.out_daily), exist_ok=True)
    with open(args.out_daily, 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

    print("Wrote:", args.out_players, args.out_geocodes, args.out_daily)
    sys.exit(0)

if __name__ == '__main__':
    main()
