# parse_csvs.py
# Usage: python parse_csvs.py
# Writes JSON into docs/data/tournaments/
import csv, json, random, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path('.')
MATCHES_DIR = ROOT / 'matches'
OUTPUT_DIR = ROOT / 'docs' / 'data' / 'tournaments'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IOC_ATP = ROOT / 'ioc_places_atp.json'

def read_csv_rows(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

# load IOC mapping if present
if IOC_ATP.exists():
    with open(IOC_ATP, 'r', encoding='utf-8') as f:
        ioc_atp = json.load(f)
else:
    ioc_atp = {}

index = defaultdict(list)
calendar_list = []

def match_sort_key(m):
    mid = m.get('match_id') or ''
    mnum = re.sub(r'[^0-9]', '', mid) if mid else ''
    return int(mnum) if mnum.isdigit() else 10**9

def normalize_name(n):
    if not n:
        return ''
    return re.sub(r'\s+', ' ', str(n).strip()).lower()

def player_present_in_matches(pid, name, matches_list):
    """Return True if player (by id or by normalized name) occurs in any match in matches_list."""
    nnorm = normalize_name(name)
    for pm in matches_list:
        # check ids
        for k in ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id'):
            if pm.get(k) and pid and str(pm.get(k)) == str(pid):
                return True
        # check names
        wn = pm.get('winner_player_name') or pm.get('winner') or pm.get('player_winner') or pm.get('player_winner_name')
        ln = pm.get('loser_player_name') or pm.get('loser') or pm.get('player_loser') or pm.get('player_loser_name')
        if nnorm and (normalize_name(wn) == nnorm or normalize_name(ln) == nnorm):
            return True
    return False

def augment_byes(groups):
    """
    groups: dict round -> list(matches)
    Strategy:
      - determine rounds ordered by size (desc) consistent with frontend
      - for each column index starting at 1 (i.e. for each pair prev/curr):
          * bucket prev matches into child buckets
          * for each match in curr (top->down) check winner then loser:
              - if player not present in original prev, create a synth match in prev (player vs BYE)
              - insert synth before bucket content for that child (so synth are local)
    """
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    # iterate columns
    for col_index in range(1, len(rounds)):
        prev_key = rounds[col_index - 1]
        curr_key = rounds[col_index]
        prev_matches = groups.get(prev_key, [])[:]
        curr_matches = groups.get(curr_key, [])[:]

        prev_count = len(prev_matches)
        curr_count = len(curr_matches)

        # build buckets: distribute prev_matches into curr_count buckets to preserve vertical locality
        buckets = [[] for _ in range(max(1, curr_count))]
        if prev_count > 0 and curr_count > 0:
            for i, pm in enumerate(prev_matches):
                child_idx = int(i * curr_count / prev_count)
                if child_idx < 0: child_idx = 0
                if child_idx >= curr_count: child_idx = curr_count - 1
                buckets[child_idx].append(pm)
        else:
            # keep buckets empty (will add synth if needed)
            pass

        new_prev = []
        inserted_for_prev = set()

        # helper to extract participant id/name/country/seed from a match row
        def get_part_from_match(match_row, role):
            # role is 'winner' or 'loser'
            id_keys = [f'player_id_{role}', f'{role}_player_id', f'{role}_id', f'{role}Id', f'PlayerID{role[0].upper()}{role[0]}']
            name_keys = [f'{role}_player_name', f'{role}_name', role, f'player_{role}', f'player_{role}_name']
            country_keys = [f'{role}_country', f'{role}_nationality', f'{role}_country_code', f'country_{role}']
            seed_keys = [f'{role}_seed', f'seed_{role}']
            pid = ''
            pname = ''
            pcountry = ''
            pseed = ''
            for k in id_keys:
                if match_row.get(k):
                    pid = match_row.get(k)
                    break
            for k in name_keys:
                if match_row.get(k):
                    pname = match_row.get(k)
                    break
            for k in country_keys:
                if match_row.get(k):
                    pcountry = match_row.get(k)
                    break
            for k in seed_keys:
                if match_row.get(k):
                    pseed = match_row.get(k)
                    break
            return {'id': pid, 'name': pname, 'country': pcountry, 'seed': pseed}

        for j, cm in enumerate(curr_matches):
            # collect winner then loser per your requested priority
            for role in ('winner', 'loser'):
                part = get_part_from_match(cm, role)
                pid = part.get('id') or ''
                pname = part.get('name') or ''
                unique_key = str(pid) if pid else normalize_name(pname)
                if not unique_key:
                    continue
                if unique_key in inserted_for_prev:
                    continue
                # check if present in original prev by id or name
                if not player_present_in_matches(pid, pname, prev_matches):
                    # create synthetic match where this player "won" vs BYE
                    # synth id includes some digits to help sorting later
                    synth_id = f"synth{col_index}{j}{random.randint(1,9999)}"
                    synth = {
                        'match_id': synth_id,
                        'round': prev_key,
                        'player_id_winner': pid or '',
                        'winner_player_name': pname or '',
                        'winner_country': part.get('country') or '',
                        'winner_seed': part.get('seed') or '',
                        'player_id_loser': '',
                        'loser_player_name': '',
                        'loser_country': '',
                        'loser_seed': '',
                        'score_string': ''
                    }
                    new_prev.append(synth)
                    inserted_for_prev.add(unique_key)
            # after synthetic inserts for this child, append the original prev matches bucket for this child (if any)
            if curr_count > 0:
                bucket = buckets[j] if j < len(buckets) else []
                for pm in bucket:
                    new_prev.append(pm)

        # edge case: if curr_count == 0 we keep prev as-is
        if curr_count == 0:
            groups[prev_key] = prev_matches
        else:
            groups[prev_key] = new_prev

def flatten_groups_to_matches(groups):
    """Return a flat list of matches by iterating groups in order of descending size (left->right)."""
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    out = []
    for r in rounds:
        out.extend(groups[r])
    return out

for kind in ('atp_matches','wta_matches'):
    d = MATCHES_DIR / kind
    if not d.exists():
        continue
    for csvfile in d.glob('*.csv'):
        rows = read_csv_rows(csvfile)
        if not rows:
            continue

        name_parts = csvfile.stem.split('_')
        if len(name_parts) >= 3:
            _, tourney_id_str, year_str = name_parts[:3]
        else:
            tourney_id_str = rows[0].get('tourney_id') or rows[0].get('event_id') or 'unknown'
            year_str = rows[0].get('tourney_year') or rows[0].get('event_year') or 'unknown'

        tourney_id = tourney_id_str
        year = year_str
        first = rows[0]

        raw_start = first.get('start_date') or first.get('tourney_start_date') or ''
        start_date = ''
        if raw_start:
            try:
                dt = datetime.fromisoformat(raw_start)
                start_date = dt.date().isoformat()
            except Exception:
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

        # Fill ATP missing info from ioc.json
        if meta['source'] == 'ATP' and tourney_id in ioc_atp:
            years_map = ioc_atp.get(tourney_id, {})
            ystr = str(year)
            if ystr in years_map:
                place = years_map[ystr]
                if len(place) >= 1 and not meta['city']:
                    meta['city'] = place[0]
                if len(place) >= 2 and not meta['country']:
                    meta['country'] = place[1]
                if len(place) >= 3 and not meta['tourney_title']:
                    meta['tourney_title'] = place[2]

        matches = []
        for r in rows:
            # collect also ids, seeds and countries when present
            m = {
                'match_id': r.get('match_id') or r.get('match') or r.get('event_id') or '',
                'round': (r.get('round') or r.get('round_name') or '').strip(),
                'winner_player_name': r.get('winner_player_name') or r.get('player_winner') or r.get('winner') or r.get('winner_player_name') or '',
                'loser_player_name': r.get('loser_player_name') or r.get('player_loser') or r.get('loser') or r.get('loser_player_name') or '',
                'score_string': r.get('score_string') or r.get('score') or r.get('score_str') or '',
                'player_id_winner': r.get('player_id_winner') or r.get('player_winner_id') or r.get('winner_id') or r.get('PlayerIDA') or r.get('PlayerIDA2') or '',
                'player_id_loser': r.get('player_id_loser') or r.get('player_loser_id') or r.get('loser_id') or r.get('PlayerIDB') or r.get('PlayerIDB2') or '',
                'winner_country': r.get('winner_country') or r.get('country_winner') or r.get('winner_country_code') or r.get('country_a') or r.get('country_b') or '',
                'loser_country': r.get('loser_country') or r.get('country_loser') or r.get('loser_country_code') or '',
                'winner_seed': r.get('winner_seed') or r.get('seed_winner') or r.get('seed_a') or r.get('seed_winner') or '',
                'loser_seed': r.get('loser_seed') or r.get('seed_loser') or r.get('seed_b') or r.get('seed_loser') or ''
            }
            matches.append(m)

        # group by round
        groups = defaultdict(list)
        for m in matches:
            rk = m.get('round') or ''
            groups[rk].append(m)

        # order rounds left->right (most matches at left)
        rounds_order = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)

        # AUGMENTATION : insérer BYE synthétiques dans groups (localement) si un joueur apparait en T2 sans avoir T1
        augment_byes(groups)

        # flatten groups back into matches array in rounds order (left->right)
        final_matches = flatten_groups_to_matches(groups)

        # as a safe step, sort final_matches with match_sort_key to keep deterministic order (but grouped order is preserved because synth ids have digits)
        final_matches_sorted = sorted(final_matches, key=match_sort_key)

        out = {'meta': meta, 'matches': final_matches_sorted}
        out_name = f"{meta['source'].lower()}_{tourney_id}_{year}.json"
        out_path = OUTPUT_DIR / out_name
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        index_key = f"{meta['source'].lower()}_{tourney_id}"
        index[index_key].append(int(year) if str(year).isdigit() else year)

        calendar_list.append({
            'source': meta['source'].lower(),
            'tourney_id': tourney_id,
            'year': int(year) if str(year).isdigit() else year,
            'tourney_name': meta['tourney_name'],
            'start_date': meta['start_date'] or '',
            'surface': meta['surface'],
            'level': meta['level']
        })

# write index
index_out = {k: sorted(v) for k, v in index.items()}
with open(OUTPUT_DIR / 'tournaments_index.json', 'w', encoding='utf-8') as f:
    json.dump(index_out, f, ensure_ascii=False, indent=2)

# write calendar
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
