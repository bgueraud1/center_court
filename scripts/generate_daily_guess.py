#!/usr/bin/env python3
"""
scripts/generate_daily_guess.py

Lit player_data_atp.csv et player_data_wta.csv (à la racine du repo),
lit les fichiers maps_html/geocodes* (s'il existent),
écrit 3 fichiers JSON :
 - daily_guess.json  (sélection déterministe par date Europe/Paris)
 - players_catalog.json (catalogue complet normalisé des joueurs)
 - geocodes_combined.json (mapping city -> [lat,lon], sous clé "geocode")

Usage (example):
 python scripts/generate_daily_guess.py \
   --atp player_data_atp.csv \
   --wta player_data_wta.csv \
   --geocodes-dir maps_html \
   --out-daily out/daily_guess.json \
   --out-players out/players_catalog.json \
   --out-geocodes out/geocodes_combined.json
"""
import csv, json, argparse, sys, os, re, random
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def parse_rank(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == '' or s == '-' or s.lower() == 'nan':
        return None
    m = re.search(r'(\d+)', s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except:
        return None

def parse_height(row):
    # try multiple fields
    for k in ('height_cm','height_cm_raw','height_cm '):
        if k in row and row[k]:
            s = str(row[k]).strip()
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
    h = row.get('height_inches') or row.get('height_inches ')
    if h:
        m = re.search(r"(\d+)\s*'\s*(\d+)", h)
        if m:
            feet = int(m.group(1)); inches = int(m.group(2))
            total = feet*12 + inches
            return int(round(total * 2.54))
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

def load_csv(path, source):
    out = []
    if not os.path.isfile(path):
        return out
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            r2 = {k.strip(): (v.strip() if isinstance(v,str) else v) for k,v in r.items()}
            r2['_source'] = source
            out.append(r2)
    return out

def build_players(atp_rows, wta_rows):
    players = []
    for r in (atp_rows + wta_rows):
        # find likely name fields
        full_name = r.get('full_name') or r.get('Full_name') or r.get('fullName') or r.get('full name') or ''
        player_id = r.get('player_id') or r.get('playerId') or r.get('playerID') or r.get('player id') or ''
        country = (r.get('represented_country') or r.get('representedCountry') or r.get('represented') or '').strip()
        rank = parse_rank(r.get('highest_ranking') or r.get('best_rank') or r.get('bestRank') or r.get('best_rank'))
        birth_date = r.get('birth_date') or r.get('birthDate') or r.get('BirthDate') or r.get('birth date') or ''
        birthplace = r.get('birthplace') or r.get('birth_place') or r.get('Birthplace') or ''
        height_cm = parse_height(r)
        plays = r.get('plays') or r.get('play') or r.get('plays ')
        backhand = r.get('backhand') or ''
        age = None
        if birth_date:
            try:
                # permissive parse (ISO or simple)
                d = None
                try:
                    d = datetime.fromisoformat(birth_date)
                except:
                    try:
                        d = datetime.strptime(birth_date, '%Y-%m-%d')
                    except:
                        # try other forms like "Mar 30 1999"
                        try:
                            d = datetime.strptime(birth_date, '%b %d %Y')
                        except:
                            d = None
                if d:
                    today = datetime.now()
                    age = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
            except Exception:
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
            'source': r.get('_source', '')
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
            # if file is { "geocode": { ... } } or bare mapping
            if isinstance(data, dict):
                if 'geocode' in data and isinstance(data['geocode'], dict):
                    for k,v in data['geocode'].items():
                        combined[k] = v
                else:
                    # assume bare mapping name->coords
                    for k,v in data.items():
                        combined[k] = v
        except Exception:
            # ignore errors but warn
            print("Warning: cannot read geocode file", path, file=sys.stderr)
    return combined

def choose_for_level(players, topN, rng):
    pool = [p for p in players if p.get('represented_country') and p.get('rank') is not None and p.get('rank') <= topN]
    if not pool:
        pool = [p for p in players if p.get('represented_country')]
    if not pool:
        pool = players[:]
    countries = sorted(list({p['represented_country'] for p in pool if p.get('represented_country')}))
    if not countries:
        chosen = rng.choice(pool)
        return chosen, None
    country = rng.choice(countries)
    by_country = [p for p in pool if p.get('represented_country') == country]
    if not by_country:
        chosen = rng.choice(pool)
    else:
        chosen = rng.choice(by_country)
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
    except Exception:
        now = datetime.now(timezone.utc)

    seed_date = now.strftime('%Y-%m-%d')
    rng = random.Random(seed_date)

    atp_rows = load_csv(args.atp, 'ATP')
    wta_rows = load_csv(args.wta, 'WTA')

    players = build_players(atp_rows, wta_rows)

    # write players_catalog.json (full list normalized)
    players_out = players  # already normalized
    os.makedirs(os.path.dirname(args.out_players), exist_ok=True)
    with open(args.out_players, 'w', encoding='utf-8') as fh:
        json.dump({'generated_at': now.isoformat(), 'count': len(players_out), 'players': players_out}, fh, ensure_ascii=False, indent=2)

    # combine geocodes
    geos = load_geocodes(args.geocodes_dir)
    os.makedirs(os.path.dirname(args.out_geocodes), exist_ok=True)
    with open(args.out_geocodes, 'w', encoding='utf-8') as fh:
        json.dump({'geocode': geos}, fh, ensure_ascii=False, indent=2)

    # generate daily choices
    out = {'date': seed_date, 'generated_at_iso': now.isoformat(), 'seed': seed_date, 'levels': {}}
    for topN, key in ((20, 'top20'), (100, 'top100'), (300, 'top300')):
        chosen, country = choose_for_level(players, topN, rng)
        out['levels'][key] = {
            'chosen': {
                'player_id': chosen.get('player_id'),
                'full_name': chosen.get('full_name'),
                'source': chosen.get('source'),
                'represented_country': chosen.get('represented_country'),
                'rank': chosen.get('rank'),
                'height_cm': chosen.get('height_cm'),
                'birth_date': chosen.get('birth_date'),
                'birthplace': chosen.get('birthplace'),
            },
            'country': country,
            'pool_size': len([p for p in players if p.get('represented_country') and p.get('rank') is not None and p.get('rank') <= topN])
        }

    os.makedirs(os.path.dirname(args.out_daily), exist_ok=True)
    with open(args.out_daily, 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

    print("Wrote:", args.out_players, args.out_geocodes, args.out_daily)
    sys.exit(0)

if __name__ == '__main__':
    main()
