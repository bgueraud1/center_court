#!/usr/bin/env python3
"""
scripts/generate_daily_guess.py

Usage:
  python scripts/generate_daily_guess.py --atp path/to/player_data_atp.csv --wta path/to/player_data_wta.csv --out outpath/daily_guess.json

Le script est déterministe par date Europe/Paris (seed = YYYY-MM-DD).
"""
import csv
import json
import argparse
from datetime import datetime, timezone
import random
import sys

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
    # extract digits
    import re
    m = re.search(r'(\d+)', s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except:
        return None

def parse_height(row):
    # attempt to find height in cm in several columns
    for k in ('height_cm','height_cm_raw','height_cm_raw','height_cm_raw '):
        if k in row and row[k]:
            s = row[k].strip()
            # e.g. "1.91m" or "191"
            s2 = ''
            import re
            m = re.search(r'([\d.,]+)', s)
            if m:
                s2 = m.group(1)
                try:
                    val = float(s2.replace(',','.'))
                    if val < 5:  # meters
                        return int(round(val*100))
                    if val > 50 and val < 300:
                        return int(round(val))
                except:
                    pass
    # fallback to height_inches
    if 'height_inches' in row and row['height_inches']:
        s = row['height_inches']
        import re
        m = re.search(r"(\d+)\s*'\s*(\d+)", s)
        if m:
            feet = int(m.group(1)); inches = int(m.group(2))
            total = feet*12 + inches
            return int(round(total * 2.54))
    return None

def load_csv(path, source_tag):
    rows = []
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append((r, source_tag))
    return rows

def build_players(atp_rows, wta_rows):
    players = []
    for row, src in (atp_rows + wta_rows):
        # normalize common fields
        full_name = row.get('full_name') or row.get('Full_name') or row.get('fullName') or ''
        player_id = row.get('player_id') or row.get('playerId') or row.get('playerID') or ''
        country = (row.get('represented_country') or row.get('representedCountry') or row.get('represented') or '').strip()
        if country == '':
            # some WTA sample uses column 'represented_country' yes; leave empty otherwise
            country = row.get('represented_country') or row.get('representedCountry') or ''
        # rank
        rank_raw = row.get('highest_ranking') or row.get('best_rank') or row.get('bestRank') or row.get('rank') or ''
        rank = parse_rank(rank_raw)
        birth_date = row.get('birth_date') or row.get('birthDate') or ''
        birthplace = row.get('birthplace') or row.get('birth_place') or row.get('birthplace') or ''
        height_cm = parse_height(row)
        players.append({
            'full_name': full_name.strip(),
            'player_id': player_id.strip(),
            'represented_country': country.strip(),
            'rank': rank,
            'birth_date': birth_date,
            'birthplace': birthplace,
            'height_cm': height_cm,
            'source': src
        })
    return players

def choose_for_level(players, topN, rng):
    pool = [p for p in players if p.get('represented_country') and p.get('represented_country').strip() and p.get('rank') is not None and p.get('rank') <= topN]
    if not pool:
        # fallback to any player with country
        pool = [p for p in players if p.get('represented_country') and p.get('represented_country').strip()]
    if not pool:
        # final fallback: any player
        pool = players[:]
    countries = sorted(list({p['represented_country'] for p in pool if p.get('represented_country')}))
    if not countries:
        chosen = rng.choice(pool)
        return chosen, None
    # pick country deterministically
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
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    # compute seed: Europe/Paris date (YYYY-MM-DD)
    try:
        if ZoneInfo:
            tz = ZoneInfo("Europe/Paris")
            now = datetime.now(tz)
        else:
            now = datetime.now(timezone.utc)
    except Exception:
        now = datetime.now(timezone.utc)

    seed_date = now.strftime('%Y-%m-%d')
    rng = random.Random(seed_date)  # deterministic per date

    # load CSVs
    atp_rows = load_csv(args.atp, 'ATP')
    wta_rows = load_csv(args.wta, 'WTA')

    players = build_players(atp_rows, wta_rows)

    out = {
        'date': seed_date,
        'generated_at_iso': now.isoformat(),
        'seed': seed_date,
        'levels': {}
    }

    for topN, key in ((20, 'top20'), (100, 'top100'), (300, 'top300')):
        chosen, country = choose_for_level(players, topN, rng)
        # sanitize chosen data to output
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

    # write out
    with open(args.out, 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

    print("Wrote", args.out)
    print("Seed date:", seed_date)
    sys.exit(0)

if __name__ == '__main__':
    main()
