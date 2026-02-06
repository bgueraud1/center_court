# parse_csvs.py (fixed: correct child mapping + absent-player BYE selection)
# Usage: python parse_csvs.py
# Writes JSON into docs/data/tournaments/

import csv, json, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path('.')
MATCHES_DIR = ROOT / 'matches'
OUTPUT_DIR = ROOT / 'docs' / 'data' / 'tournaments'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IOC_ATP = ROOT / 'ioc_places_atp.json'

# debug toggle: set to True to see insertion/logging details
VERBOSE = True

# If True: after safe insertions, forcibly create synthetic BYE matches
# for any remaining numeric IDs missing in their expected interval.
# This ensures NO numeric MS/LS is missing, but may duplicate players.
FORCE_FILL_REMAINING = True

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

# === BYE insertion (robust) ===
id_suffix_re = re.compile(r'([A-Za-z]+)(\d+)$')

# keys to look for ids and names
ID_KEYS = ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id',
           'PlayerIDA', 'PlayerIDB', 'PlayerIDA2', 'PlayerIDB2')
NAME_KEYS = ('winner_player_name', 'player_winner', 'winner', 'player_winner_name',
             'loser_player_name', 'player_loser', 'loser', 'player_loser_name')

def next_power_of_two(n):
    if not n or n < 1:
        return None
    p = 1
    while p < n:
        p <<= 1
    return p

def insert_missing_sequential_matches(groups, singles_draw_size=None):
    """
    Insert synthetic BYE matches for missing numeric match_ids for prefixes like MS/LS.

    Two phases:
    1) Conservative insertion: only when we can identify the child match and exactly one
       of the child players is present in the current round — then the BYE belongs to the other (absent) player.
    2) (Optional) Forced fill of any remaining numeric gaps, preferring the absent sibling logic when possible.
    """
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    if len(rounds) < 2:
        return

    expected_global_max = None
    P = None
    if singles_draw_size and isinstance(singles_draw_size, int):
        np2 = next_power_of_two(singles_draw_size)
        if np2:
            expected_global_max = np2 - 1  # e.g., draw 28 -> next_pow2=32 -> expect 1..31
            P = np2
            if VERBOSE:
                print(f"[insert_missing] singles_draw_size={singles_draw_size} -> next_pow2={np2}, expected ids 1..{expected_global_max}")

    remaining_missing = defaultdict(lambda: defaultdict(list))  # store ids not resolved in conservative pass

    for idx in range(len(rounds) - 1):
        cur_key = rounds[idx]
        next_key = rounds[idx + 1]
        cur_matches = groups.get(cur_key, [])[:]  # working copy
        next_matches = groups.get(next_key, [])[:]
        if not cur_matches or not next_matches:
            continue

        # maps for next round matches
        next_map = {}
        child_num_map = {}
        child_nums = set()
        for nm in next_matches:
            mid = nm.get('match_id') or ''
            m = id_suffix_re.search(mid)
            if m:
                prefix = m.group(1)
                num = int(m.group(2))
                next_map[(prefix, num)] = nm
                if num not in child_num_map:
                    child_num_map[num] = nm
                child_nums.add(num)

        # bucket current round matches by prefix (MS, LS, etc.)
        cur_prefix_buckets = defaultdict(list)
        for cm in cur_matches:
            mid = cm.get('match_id') or ''
            m = id_suffix_re.search(mid)
            if m:
                cur_prefix_buckets[m.group(1)].append(cm)
            else:
                cur_prefix_buckets[''].append(cm)

        if VERBOSE:
            print(f"[insert_missing] round='{cur_key}' prefixes found: {list(cur_prefix_buckets.keys())}")

        added_any_round = False

        for prefix_used, bucket in cur_prefix_buckets.items():
            if not bucket:
                continue

            # present numeric ids and width
            nums_present = set()
            num_width = 0
            for cm in bucket:
                mid = cm.get('match_id') or ''
                m = id_suffix_re.search(mid)
                if m and m.group(1) == prefix_used:
                    try:
                        nums_present.add(int(m.group(2)))
                        num_width = max(num_width, len(m.group(2)))
                    except Exception:
                        pass

            if not nums_present:
                continue

            # expected numeric interval for this round (based on P if available)
            if P:
                i = idx
                min_n = P // (2 ** (i + 1))
                max_n = (P // (2 ** i)) - 1
            else:
                if child_nums:
                    min_n = 2 * min(child_nums)
                    max_n = 2 * max(child_nums) + 1
                else:
                    min_n = min(nums_present)
                    max_n = max(nums_present)

            if expected_global_max is not None:
                min_n = max(1, min_n)
                max_n = min(expected_global_max, max_n)

            if VERBOSE:
                print(f"[insert_missing] prefix={prefix_used!r} present_count={len(nums_present)} expected_range={min_n}..{max_n} num_width={num_width}")

            added_any_prefix = False

            # build sets of ids/names already present in current round
            present_ids = set()
            present_names = set()
            for pm in bucket:
                for k in ID_KEYS:
                    v = pm.get(k)
                    if v:
                        present_ids.add(str(v))
                for nk in NAME_KEYS:
                    nv = pm.get(nk)
                    if nv:
                        present_names.add(normalize_name(nv))

            # iterate expected numbers and insert missing ones
            for n in range(min_n, max_n + 1):
                if n in nums_present:
                    continue

                # CORRECT mapping: child number is floor(n/2) -> parents 2*c and 2*c+1 feed child c
                child_num = n // 2
                if child_num < 1:
                    continue

                # try find child by prefix/num then fallback by num
                child_match = next_map.get((prefix_used, child_num)) if prefix_used else None
                if not child_match:
                    child_match = child_num_map.get(child_num)

                # fallback: try to find a next_match where exactly one child player is already present
                if not child_match:
                    for cand in next_matches:
                        cand_w_id = cand.get('player_id_winner') or cand.get('winner_id') or cand.get('player_winner_id') or ''
                        cand_l_id = cand.get('player_id_loser') or cand.get('loser_id') or cand.get('player_loser_id') or ''
                        cand_w_name = None
                        cand_l_name = None
                        for nk in NAME_KEYS:
                            if not cand_w_name:
                                cand_w_name = cand.get(nk) or cand_w_name
                            if not cand_l_name:
                                cand_l_name = cand.get(nk) or cand_l_name
                        cand_w_name = cand_w_name or ''
                        cand_l_name = cand_l_name or ''
                        cand_w_present = (cand_w_id and str(cand_w_id) in present_ids) or (cand_w_name and normalize_name(cand_w_name) in present_names)
                        cand_l_present = (cand_l_id and str(cand_l_id) in present_ids) or (cand_l_name and normalize_name(cand_l_name) in present_names)
                        if cand_w_present ^ cand_l_present:
                            child_match = cand
                            if VERBOSE:
                                print(f"[insert_missing] fallback matched child for expected n={n}: child_num={child_num}, cand_mid={cand.get('match_id')}")
                            break

                if not child_match:
                    remaining_missing[cur_key][prefix_used].append(n)
                    if VERBOSE:
                        print(f"[insert_missing] no child for expected n={n} (child_num={child_num}) -> will attempt later")
                    continue

                # extract child players
                w_id = child_match.get('player_id_winner') or child_match.get('winner_id') or child_match.get('player_winner_id') or ''
                w_name = child_match.get('winner_player_name') or child_match.get('winner') or ''
                l_id = child_match.get('player_id_loser') or child_match.get('loser_id') or child_match.get('player_loser_id') or ''
                l_name = child_match.get('loser_player_name') or child_match.get('loser') or ''

                # determine which child players are present in current round
                w_present = False
                l_present = False
                if w_id and str(w_id) in present_ids:
                    w_present = True
                elif w_name and normalize_name(w_name) in present_names:
                    w_present = True
                if l_id and str(l_id) in present_ids:
                    l_present = True
                elif l_name and normalize_name(l_name) in present_names:
                    l_present = True

                # safe insertion only if exactly one of the two child players is present
                if (w_present and l_present) or ((not w_present) and (not l_present)):
                    remaining_missing[cur_key][prefix_used].append(n)
                    if VERBOSE:
                        print(f"[insert_missing] skipping n={n} because w_present={w_present}, l_present={l_present} (will attempt later)")
                    continue

                # **CORRECTED RULE**:
                # the player present has actually played in this round (no bye),
                # so the BYE belongs to the other (absent) player -> that absent player is the synthetic winner.
                synth_winner_id = ''
                synth_winner_name = ''
                synth_winner_country = ''
                synth_winner_seed = ''
                if w_present and not l_present:
                    # winner present -> loser absent -> loser had the BYE and should be inserted as winner
                    synth_winner_id = l_id or ''
                    synth_winner_name = l_name or ''
                    synth_winner_country = child_match.get('loser_country') or ''
                    synth_winner_seed = child_match.get('loser_seed') or ''
                elif l_present and not w_present:
                    # loser present -> winner absent -> winner had the BYE and should be inserted
                    synth_winner_id = w_id or ''
                    synth_winner_name = w_name or ''
                    synth_winner_country = child_match.get('winner_country') or ''
                    synth_winner_seed = child_match.get('winner_seed') or ''

                # construct synthetic match id and entry
                num_str = str(n).zfill(num_width or len(str(n)))
                synth_id = f"{prefix_used}{num_str}" if prefix_used else str(n)

                synth = {
                    'match_id': synth_id,
                    'round': cur_key,
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
                nums_present.add(n)
                if synth_winner_id:
                    present_ids.add(str(synth_winner_id))
                elif synth_winner_name:
                    present_names.add(normalize_name(synth_winner_name))

                added_any_prefix = True
                added_any_round = True
                if VERBOSE:
                    print(f"[insert] round={cur_key} inserted {synth_id} winner={synth_winner_name or synth_winner_id}")

            # merge back sorted by numeric suffix
            if added_any_prefix:
                def cur_key_fn(m):
                    mid = m.get('match_id') or ''
                    mm = id_suffix_re.search(mid)
                    if mm and mm.group(1) == prefix_used:
                        try:
                            return int(mm.group(2))
                        except Exception:
                            return 10**9
                    return 10**9
                cur_matches = sorted(cur_matches, key=cur_key_fn)
                groups[cur_key] = cur_matches

        if added_any_round and VERBOSE:
            print(f"[insert_missing] finished round='{cur_key}', total now={len(groups[cur_key])}")

    # ----- LAST-RESORT: force-fill remaining numeric ids if requested -----
    if FORCE_FILL_REMAINING:
        if VERBOSE:
            print("[insert_missing] FORCE_FILL_REMAINING is True -> attempting to fill remaining missing numeric IDs")
        rounds_now = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
        for cur_key, prefixes in remaining_missing.items():
            try:
                cur_idx = rounds_now.index(cur_key)
                next_key = rounds_now[cur_idx + 1]
            except Exception:
                next_key = None
            next_matches = groups.get(next_key, []) if next_key else []

            # rebuild child_num_map for next_matches
            child_num_map = {}
            for nm in next_matches:
                mid = nm.get('match_id') or ''
                m = id_suffix_re.search(mid)
                if m:
                    try:
                        num = int(m.group(2))
                        child_num_map[num] = nm
                    except Exception:
                        pass

            cur_matches = groups.get(cur_key, [])[:]
            for prefix_used, missing_list in prefixes.items():
                num_width = 0
                for cm in cur_matches:
                    mid = cm.get('match_id') or ''
                    m = id_suffix_re.search(mid)
                    if m and m.group(1) == prefix_used:
                        num_width = max(num_width, len(m.group(2)))

                for n in missing_list:
                    # skip if now present
                    present_ids_nums = set()
                    for cm in cur_matches:
                        mid = cm.get('match_id') or ''
                        m = id_suffix_re.search(mid)
                        if m and m.group(1) == prefix_used:
                            try:
                                present_ids_nums.add(int(m.group(2)))
                            except Exception:
                                pass
                    if n in present_ids_nums:
                        continue

                    child_num = n // 2
                    child_match = child_num_map.get(child_num)
                    if not child_match and next_matches:
                        child_match = next_matches[0]

                    synth_winner_id = ''
                    synth_winner_name = ''
                    synth_winner_country = ''
                    synth_winner_seed = ''
                    if child_match:
                        cw = child_match.get('winner_player_name') or child_match.get('winner') or ''
                        cl = child_match.get('loser_player_name') or child_match.get('loser') or ''
                        cw_id = child_match.get('player_id_winner') or child_match.get('winner_id') or ''
                        cl_id = child_match.get('player_id_loser') or child_match.get('loser_id') or ''

                        # Prefer to pick the absent child player by inspecting sibling if available:
                        sibling_num = n+1 if n % 2 == 0 else n-1
                        sibling_mid = f"{prefix_used}{str(sibling_num).zfill(num_width or len(str(sibling_num)))}"
                        sibling = None
                        for cm in cur_matches:
                            if cm.get('match_id') == sibling_mid:
                                sibling = cm
                                break

                        if sibling:
                            sib_names = {normalize_name(sibling.get('winner_player_name') or ''), normalize_name(sibling.get('loser_player_name') or '')}
                            # if cw not in sibling but cl is in sibling -> choose cw (absent one)
                            if cw and normalize_name(cw) not in sib_names and cl and normalize_name(cl) in sib_names:
                                synth_winner_name = cw; synth_winner_id = cw_id
                            elif cl and normalize_name(cl) not in sib_names and cw and normalize_name(cw) in sib_names:
                                synth_winner_name = cl; synth_winner_id = cl_id
                            else:
                                # fallback: choose child winner if present
                                if cw:
                                    synth_winner_name = cw; synth_winner_id = cw_id
                                elif cl:
                                    synth_winner_name = cl; synth_winner_id = cl_id
                        else:
                            # no sibling found -> choose child winner if any
                            if cw:
                                synth_winner_name = cw; synth_winner_id = cw_id
                            elif cl:
                                synth_winner_name = cl; synth_winner_id = cl_id

                        synth_winner_country = child_match.get('winner_country') or child_match.get('loser_country') or ''
                        synth_winner_seed = child_match.get('winner_seed') or child_match.get('loser_seed') or ''
                        if VERBOSE:
                            print(f"[force_fill] filling {prefix_used}{str(n).zfill(num_width or len(str(n)))} from child {child_match.get('match_id')} (sibling {sibling_mid})")
                    else:
                        if VERBOSE:
                            print(f"[force_fill] no child found for n={n}, inserting blank BYE")

                    num_str = str(n).zfill(num_width or len(str(n)))
                    synth_id = f"{prefix_used}{num_str}" if prefix_used else str(n)
                    synth = {
                        'match_id': synth_id,
                        'round': cur_key,
                        'winner_player_name': synth_winner_name,
                        'loser_player_name': 'BYE',
                        'score_string': '',
                        'player_id_winner': synth_winner_id or 'XXXX',
                        'player_id_loser': 'XXXX',
                        'winner_country': synth_winner_country or '',
                        'loser_country': 'XXX',
                        'winner_seed': synth_winner_seed or '',
                        'loser_seed': ''
                    }
                    cur_matches.append(synth)
                    if VERBOSE:
                        print(f"[force_fill] inserted {synth_id} winner={synth_winner_name or synth_winner_id or 'UNKNOWN'}")

            groups[cur_key] = cur_matches

# === end insertion logic ===

def flatten_groups_to_matches(groups):
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    out = []
    for r in rounds:
        out.extend(groups[r])
    return out

# --- main loop (kept as you requested) ---
for kind in ('atp_matches',):
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

        # insert synthetic BYEs
        insert_missing_sequential_matches(groups, singles_draw_size=meta.get('singles_draw_size'))

        # flatten & sort
        final_matches = flatten_groups_to_matches(groups)
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
