# parse_csvs.py
# Usage: python parse_csvs.py
# Output: writes JSON files into docs/data/tournaments/ and an index docs/data/tournaments_index.json
#         and a calendar listing docs/data/tournaments_calendar.json

import csv
import json
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path('.')
MATCHES_DIR = ROOT / 'matches'
OUTPUT_DIR = ROOT / 'docs' / 'data' / 'tournaments'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IOC_ATP = ROOT / 'ioc_places_atp.json'

# Utilities
def read_csv_rows(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

# Load ioc for ATP
if IOC_ATP.exists():
    with open(IOC_ATP, 'r', encoding='utf-8') as f:
        ioc_atp = json.load(f)
else:
    ioc_atp = {}

index = defaultdict(list)  # {"atp_73": [1997,1998,...]}
calendar_list = []  # list of tournaments (for calendar)

for kind in ('atp_matches','wta_matches'):
    d = MATCHES_DIR / kind
    if not d.exists():
        continue
    for csvfile in d.glob('*.csv'):
        rows = read_csv_rows(csvfile)
        if not rows:
            continue
        # attempt to get tourney id and year
        name_parts = csvfile.stem.split('_')
        if len(name_parts) >= 3:
            _, tourney_id_str, year_str = name_parts[:3]
        else:
            tourney_id_str = rows[0].get('tourney_id') or rows[0].get('event_id') or 'unknown'
            year_str = rows[0].get('tourney_year') or rows[0].get('event_year') or 'unknown'

        tourney_id = tourney_id_str
        year = year_str
        # collect metadata from first row
        first = rows[0]
        # normalize start_date to YYYY-MM-DD if possible
        raw_start = first.get('start_date') or first.get('tourney_start_date') or ''
        start_date = ''
        if raw_start:
            try:
                # some CSVs have ISO datetimes with time
                dt = datetime.fromisoformat(raw_start)
                start_date = dt.date().isoformat()
            except Exception:
                # try to parse YYYY-MM-DD
                try:
                    start_date = datetime.strptime(raw_start[:10], '%Y-%m-%d').date().isoformat()
                except Exception:
                    start_date = raw_start

        meta = {
            'source': 'ATP' if kind.startswith('atp') else 'WTA',
            'tourney_id': tourney_id,
            'year': int(year) if str(year).isdigit() else year,
            'tourney_name': first.get('tourney_name') or first.get('tournament_name') or '',
            'tourney_title': first.get('tournament_title') or first.get('tourney_title') or '',
            'surface': (first.get('surface') or '').title(),
            'level': first.get('level') or '',
            'prize_money': first.get('prize_money') or '',
            'prize_money_currency': first.get('prize_money_currency') or '',
            'singles_draw_size': int(first.get('singles_draw_size')) if first.get('singles_draw_size') and str(first.get('singles_draw_size')).isdigit() else None,
            'city': first.get('city') or '',
            'country': first.get('country') or '',
            'start_date': start_date
        }

        # for ATP, fill city/country/title using ioc_places_atp.json when missing
        if meta['source'] == 'ATP' and tourney_id in ioc_atp:
            years_map = ioc_atp.get(tourney_id, {})
            ystr = str(year)
            if ystr in years_map:
                place = years_map[ystr]
                if len(place) >= 2 and not meta['city']:
                    meta['city'] = place[0]
                if len(place) >= 2 and not meta['country']:
                    meta['country'] = place[1]
                if len(place) >= 3 and not meta['tourney_title']:
                    meta['tourney_title'] = place[2]

        # normalize matches: keep only the columns we need plus match_id and round
        matches = []
        for r in rows:
            m = {
                'match_id': r.get('match_id') or r.get('match') or r.get('event_id'),
                'round': r.get('round') or r.get('round_name') or '',
                'winner_player_name': r.get('winner_player_name') or r.get('player_winner') or r.get('winner') or r.get('winner_player'),
                'loser_player_name': r.get('loser_player_name') or r.get('player_loser') or r.get('loser') or r.get('loser_player'),
                'score_string': r.get('score_string') or r.get('score') or r.get('score_str') or ''
            }
            matches.append(m)

        # basic validation: sort matches by match_id numeric suffix if possible
        def match_sort_key(m):
            mid = m.get('match_id') or ''
            import re
            mnum = re.sub(r'[^0-9]', '', mid) if mid else ''
            return int(mnum) if mnum.isdigit() else 10**9

        matches_sorted = sorted(matches, key=match_sort_key)

        out = {
            'meta': meta,
            'matches': matches_sorted
        }

        # write file
        out_name = f"{meta['source'].lower()}_{tourney_id}_{year}.json"
        out_path = OUTPUT_DIR / out_name
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        # add to index
        index_key = f"{meta['source'].lower()}_{tourney_id}"
        index[index_key].append(int(year) if str(year).isdigit() else year)

        # add to calendar list
        calendar_list.append({
            'source': meta['source'].lower(),
            'tourney_id': tourney_id,
            'year': int(year) if str(year).isdigit() else year,
            'tourney_name': meta['tourney_name'],
            'start_date': meta['start_date'] or '',
            'surface': meta['surface'],
            'level': meta['level']
        })

# write index file
index_out = {k: sorted(v) for k, v in index.items()}
with open(OUTPUT_DIR / 'tournaments_index.json', 'w', encoding='utf-8') as f:
    json.dump(index_out, f, ensure_ascii=False, indent=2)

# write calendar file sorted by start_date
# normalize unknown dates to far future so they sort last
def sort_key(item):
    d = item.get('start_date')
    try:
        if not d:
            return datetime.max
        return datetime.fromisoformat(d)
    except Exception:
        try:
            return datetime.strptime(d[:10], '%Y-%m-%d')
        except Exception:
            return datetime.max

calendar_sorted = sorted(calendar_list, key=sort_key)
with open(OUTPUT_DIR / 'tournaments_calendar.json', 'w', encoding='utf-8') as f:
    json.dump(calendar_sorted, f, ensure_ascii=False, indent=2)

print('Done — JSON files written to', OUTPUT_DIR)


