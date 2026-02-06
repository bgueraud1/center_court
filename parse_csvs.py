# parse_csvs.py
# Usage: python parse_csvs.py
# Writes JSON into docs/data/tournaments/

import csv, json, random, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from math import ceil

ROOT = Path('.')
MATCHES_DIR = ROOT / 'matches'
OUTPUT_DIR = ROOT / 'docs' / 'data' / 'tournaments'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IOC_ATP = ROOT / 'ioc_places_atp.json'

# debug toggle
VERBOSE = False

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

# === Utilities for expected numbering ===
def next_power_of_two(x):
    p = 1
    while p < x:
        p <<= 1
    return p

def build_level_counts(full_draw):
    """
    Return list of match counts per round in numbering order:
    [1 (Final), 2 (Semis), 4 (QF), 8 (R16), 16 (R32), ... up to full_draw/2]
    """
    counts = []
    v = 1
    while v <= (full_draw // 2):
        counts.append(v)
        v *= 2
    counts = list(reversed(counts))  # now [full_draw/2, ..., 4,2,1]
    # convert to numbering order: final first -> reverse again
    numbering_order = list(reversed(counts))
    return numbering_order  # [1,2,4,8,..., full_draw/2]

def find_numeric_range_for_count(count, full_draw):
    """
    Given expected match count for a round (e.g. 16 for Round of 32), compute
    numeric range based on standard numbering (final=1, semis=2..3, quarters=4..7, ...).
    """
    numbering = build_level_counts(full_draw)  # [1,2,4,8,...]
    start = 1
    for c in numbering:
        if c == count:
            end = start + c - 1
            return start, end
        start += c
    return None

def round_expected_count_from_name(rname, full_draw):
    """Heuristic: map round name to expected count."""
    n = rname.lower()
    if 'final' in n and 'round' not in n:
        return 1
    if 'semi' in n:
        return 2
    if 'quarter' in n:
        return 4
    if 'round of 16' in n or 'r16' in n or 'round of sixteen' in n:
        return 8
    if 'round of 32' in n or 'r32' in n:
        return full_draw // 2
    if 'round of 64' in n or 'r64' in n:
        return (full_draw * 2) // 2  # 32 for full_draw=64 etc.
    # fallback: if name contains a number like 'round of 128' parse it
    m = re.search(r'round of (\d+)', n)
    if m:
        try:
            cnt = int(m.group(1))
            return cnt // 2
        except Exception:
            pass
    return None

# === Nouveau : remplissage direct des numéros manquants dans la plage attendue ===
id_suffix_re = re.compile(r'([A-Za-z]+)(\d+)$')

def insert_missing_sequential_matches(groups, singles_draw_size=None):
    """
    For each round which we can identify (by name), fill the numeric holes in its
    expected interval (derived from singles_draw_size). For each missing number n:
      - compute child_num = ceil(n/2)
      - attempt to find the child match (in the round that has half the matches)
      - set the synthetic match winner to the child match winner when available,
        otherwise leave winner empty.
    This approach guarantees full coverage of the numeric interval.
    """
    # determine full draw from singles_draw_size if available
    if singles_draw_size and isinstance(singles_draw_size, int) and singles_draw_size > 0:
        full_draw = next_power_of_two(singles_draw_size)
    else:
        # fallback: try to infer from groups sizes: pick max round size and next_pow2
        max_matches = max((len(v) for v in groups.values()), default=1)
        # heuristic: if max_matches is itself a power of two, full_draw = max_matches*2
        # else next pow2:
        full_draw = next_power_of_two(max_matches * 2)

    # build a quick map: by numeric -> match for each round to allow child lookup
    round_to_num_map = {}
    for rk, match_list in groups.items():
        num_map = {}
        for m in match_list:
            mid = m.get('match_id') or ''
            mm = id_suffix_re.search(mid)
            if mm:
                prefix = mm.group(1)
                num = int(mm.group(2))
                num_map[num] = m
        round_to_num_map[rk] = num_map

    # For quick access to rounds by expected size (count), compute candidate round names by size
    # We'll attempt to map round names to expected counts via heuristics
    for rk in list(groups.keys()):
        exp_count = round_expected_count_from_name(rk, full_draw)
        if exp_count is None:
            # can't determine, skip this round
            continue

        numeric_range = find_numeric_range_for_count(exp_count, full_draw)
        if not numeric_range:
            continue
        min_n, max_n = numeric_range

        # build child round: the round that should have exp_count/2 matches (if available)
        child_count = exp_count // 2
        # find a candidate child round whose length equals child_count (or closest)
        candidate_child_round = None
        for rk_cand, lst in groups.items():
            if len(lst) == child_count:
                candidate_child_round = rk_cand
                break
        # fallback: pick the round with strictly fewer matches than current
        if candidate_child_round is None:
            for rk_cand, lst in groups.items():
                if len(lst) < len(groups[rk]):
                    candidate_child_round = rk_cand
                    break

        # prepare child_num -> match map if possible
        child_num_map = {}
        if candidate_child_round:
            for knum, mm in round_to_num_map.get(candidate_child_round, {}).items():
                child_num_map[knum] = mm

        # present numbers in this round
        present_nums = set(round_to_num_map.get(rk, {}).keys())

        added_any = False
        cur_matches = groups.get(rk, [])[:]

        # for every expected n in range, create synth if missing
        for n in range(min_n, max_n + 1):
            if n in present_nums:
                continue
            child_num = (n + 1) // 2
            child_match = child_num_map.get(child_num)
            synth_winner_id = ''
            synth_winner_name = ''
            synth_winner_country = ''
            synth_winner_seed = ''
            if child_match:
                # prefer child match winner as the one who had a bye into this round
                synth_winner_id = child_match.get('player_id_winner') or ''
                synth_winner_name = child_match.get('winner_player_name') or ''
                synth_winner_country = child_match.get('winner_country') or ''
                synth_winner_seed = child_match.get('winner_seed') or ''
            # If no child match found, we still create a BYE placeholder (empty winner)
            num_str = str(n).zfill(3)  # default width 3 (MS001 style). We'll try to detect width from existing.
            # try to detect width from existing IDs in this round
            widths = []
            for mid in round_to_num_map.get(rk, {}).keys():
                widths.append(len(str(mid)))
            if widths:
                num_str = str(n).zfill(max(widths))
            # detect prefix
            prefix = None
            for m in groups.get(rk, []):
                mm = id_suffix_re.search(m.get('match_id') or '')
                if mm:
                    prefix = mm.group(1)
                    break
            if not prefix:
                prefix = 'MS'
            synth_id = f"{prefix}{num_str}"

            synth = {
                'match_id': synth_id,
                'round': rk,
                'winner_player_name': synth_winner_name,
                'loser_player_name': 'BYE',
                'score_string': '',
                'player_id_winner': synth_winner_id,
                'player_id_loser': 'XXXX',
                'winner_country': synth_winner_country or '',
                'loser_country': 'XXX',
                'winner_seed': synth_winner_seed or '',
                'loser_seed': ''
            }

            cur_matches.append(synth)
            present_nums.add(n)
            added_any = True
            if VERBOSE:
                print(f"[insert-fill] round={rk} inserted {synth_id} winner={synth_winner_name or synth_winner_id}")

        if added_any:
            # re-index by numeric order
            def cur_key_fn(m):
                mid = m.get('match_id') or ''
                mm = id_suffix_re.search(mid)
                if mm:
                    try:
                        return int(mm.group(2))
                    except Exception:
                        return 10**9
                return 10**9
            cur_matches_sorted = sorted(cur_matches, key=cur_key_fn)
            groups[rk] = cur_matches_sorted

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

        # === APPEL: remplir les intervalles manquants en se basant sur singles_draw_size ===
        insert_missing_sequential_matches(groups, singles_draw_size=meta.get('singles_draw_size'))
        # === FIN APPEL ===

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
