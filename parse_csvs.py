# parse_csvs.py
# Minimal parser + deterministic BYE insertion (global, no per-round heuristics)
# Usage: python parse_csvs.py
# Writes JSON into docs/data/tournaments/

import csv
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# optional special parser import (safe)
try:
    from parse_wta_special import process_wta_special_csv
except Exception:
    process_wta_special_csv = None

SPECIAL_PREFIXES = ('wta_901', 'wta_903', 'wta_904', 'wta_905')


ROOT = Path('.')
MATCHES_DIR = ROOT / 'matches'
OUTPUT_DIR = ROOT / 'docs' / 'data' / 'tournaments'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IOC_ATP = ROOT / 'ioc_places_atp.json'

VERBOSE = True  # set False to silence logs
# -- helper to parse ints that may be written like "56.0" or "56,0" --
def parse_int_like(v):
    """Return an int if v looks like an integer (e.g. '56', '56.0', 56.0), else None.
    Accepts strings with commas as decimal separators, trims whitespace, and never raises.
    """
    if v is None:
        return None
    # preserve ints quickly
    if isinstance(v, int):
        return v
    # convert floats safely
    if isinstance(v, float):
        try:
            return int(v)
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    # normalize comma decimal separator -> dot
    s = s.replace(',', '.')
    # if purely digits
    if s.isdigit():
        return int(s)
    # try float conversion then int (will convert '56.0' -> 56)
    try:
        f = float(s)
    except Exception:
        return None
    # If float is NaN/Inf, give up
    if f != f or f in (float('inf'), float('-inf')):
        return None
    # Convert to int (we intentionally accept floats and truncate to int)
    try:
        return int(f)
    except Exception:
        return None


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

id_suffix_re = re.compile(r'([A-Za-z]+)(\d+)$')

def normalize_name(n):
    if not n:
        return ''
    return re.sub(r'\s+', ' ', str(n).strip()).lower()

def next_power_of_two(n):
    if not n or n < 1:
        return None
    p = 1
    while p < n:
        p <<= 1
    return p

def match_sort_key(m):
    mid = m.get('match_id') or ''
    digits = re.sub(r'[^0-9]', '', mid)
    return int(digits) if digits.isdigit() else 10**9

# robust field extraction for different CSV flavors
def build_match_record(row):
    """Return normalized match dict from a CSV row with varying headers."""
    # candidate lookup function
    def first_present(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != '':
                return v
        return ''

    m = {
        'match_id': first_present('match_id','match','event_id'),
        'round': (first_present('round','round_name') or '').strip(),
        'winner_player_name': first_present('winner_player_name','player_winner','winner','winner_player'),
        'loser_player_name': first_present('loser_player_name','player_loser','loser','loser_player'),
        'score_string': first_present('score_string','score','score_str'),
        'player_id_winner': first_present('player_id_winner','player_winner_id','winner_id','PlayerIDA','PlayerIDA2','player_winner'),
        'player_id_loser': first_present('player_id_loser','player_loser_id','loser_id','PlayerIDB','PlayerIDB2','player_loser'),
        'winner_country': first_present('winner_country','country_winner','winner_country_code','country_a','country_b'),
        'loser_country': first_present('loser_country','country_loser','loser_country_code'),
        'winner_seed': first_present('winner_seed','seed_winner','seed_a'),
        'loser_seed': first_present('loser_seed','seed_loser','seed_b')
    }
    return m

def insert_byes_global(matches_list, singles_draw_size=None):
    """
    Insert BYE matches globally for missing numeric indices per prefix.
    Algorithm EXACTLY as requested by the user (no round partitioning):
      - find missing XXX for each prefix (expected 1..P-1 if singles_draw_size given else min..max)
      - for each missing n:
          child = n // 2  => find child match (prefix+child) globally
          sibling = n+1 if n even else n-1 => find sibling (prefix+sibling) globally
          if child exists:
            find child players (p1,p2)
            if sibling exists and exactly one of p1/p2 appears in sibling:
                BYE winner = the other child player
            elif sibling not found but one of p1/p2 appears in any existing match (global presence):
                BYE winner = the other child player
            else:
                BYE winner = UNKNOWN ('XXXX')
          else:
            BYE winner = UNKNOWN ('XXXX')
      - insert synthetic match with match_id prefix+zero_padded(n), loser 'BYE'
    """
    # Build lookup maps
    id_map = {}  # match_id -> match dict
    prefix_nums = defaultdict(set)  # prefix -> set(nums present)
    prefix_width = defaultdict(int)  # prefix -> max width seen

    for m in matches_list:
        mid = m.get('match_id') or ''
        id_map[mid] = m
        mm = id_suffix_re.search(mid)
        if mm:
            pref = mm.group(1)
            num = int(mm.group(2))
            prefix_nums[pref].add(num)
            prefix_width[pref] = max(prefix_width[pref], len(mm.group(2)))

    # determine prefixes to process (only those with letter prefix like MS, LS)
    prefixes = list(prefix_nums.keys())
    if VERBOSE:
        print("[GLOBAL BYE] prefixes found:", prefixes)

    # build global presence sets (all player ids and names appearing anywhere)
    global_ids = set()
    global_names = set()
    for m in matches_list:
        pw = m.get('player_id_winner') or ''
        pl = m.get('player_id_loser') or ''
        if pw:
            global_ids.add(str(pw))
        if pl:
            global_ids.add(str(pl))
        wn = m.get('winner_player_name') or ''
        ln = m.get('loser_player_name') or ''
        if wn:
            global_names.add(normalize_name(wn))
        if ln:
            global_names.add(normalize_name(ln))

    # iterate prefixes
    to_insert = []  # synthetic matches to append
    for pref in prefixes:
        nums = sorted(prefix_nums[pref])
        if not nums:
            continue

        # expected interval
        if singles_draw_size and isinstance(singles_draw_size, int):
            P = next_power_of_two(singles_draw_size)
            if P:
                min_n = 1
                max_n = P - 1
            else:
                min_n = min(nums)
                max_n = max(nums)
        else:
            min_n = min(nums)
            max_n = max(nums)

        # list missing
        missing = [n for n in range(min_n, max_n + 1) if n not in prefix_nums[pref]]
        if not missing:
            continue

        width = prefix_width.get(pref) or 3

        if VERBOSE:
            print(f"[GLOBAL BYE] prefix={pref} expected_range={min_n}..{max_n} present_count={len(nums)} missing={missing}")

        # process each missing n
        for n in missing:
            mid = f"{pref}{str(n).zfill(width)}"
            if VERBOSE:
                print("[MISSING]", mid)

            child_num = n // 2
            child_mid = f"{pref}{str(child_num).zfill(width)}"
            child = id_map.get(child_mid)

            sibling_num = n + 1 if (n % 2 == 0) else n - 1
            sibling_mid = f"{pref}{str(sibling_num).zfill(width)}"
            sibling = id_map.get(sibling_mid)

            chosen_name = ''
            chosen_id = ''

            if child:
                # get child players
                c_w_name = child.get('winner_player_name') or ''
                c_l_name = child.get('loser_player_name') or ''
                c_w_id = child.get('player_id_winner') or ''
                c_l_id = child.get('player_id_loser') or ''

                # helper to check if a player appears in a match object
                def player_in_match(player_id, player_name, match_obj):
                    if not match_obj:
                        return False
                    # check ids
                    if player_id and (player_id == (match_obj.get('player_id_winner') or '') or player_id == (match_obj.get('player_id_loser') or '')):
                        return True
                    # check names
                    pn = normalize_name(player_name)
                    if pn and (pn == normalize_name(match_obj.get('winner_player_name') or '') or pn == normalize_name(match_obj.get('loser_player_name') or '')):
                        return True
                    return False

                # 1) if sibling exists and exactly one of the child players appears in sibling -> select the other
                if sibling:
                    in_w = player_in_match(c_w_id, c_w_name, sibling)
                    in_l = player_in_match(c_l_id, c_l_name, sibling)
                    if in_w ^ in_l:
                        if in_w:
                            chosen_name = c_l_name or ''
                            chosen_id = c_l_id or ''
                            if VERBOSE:
                                print(f"[DECIDE] {mid}: sibling {sibling_mid} contains child-winner -> selecting child-loser {chosen_name or chosen_id}")
                        else:
                            chosen_name = c_w_name or ''
                            chosen_id = c_w_id or ''
                            if VERBOSE:
                                print(f"[DECIDE] {mid}: sibling {sibling_mid} contains child-loser -> selecting child-winner {chosen_name or chosen_id}")
                    else:
                        # sibling exists but ambiguous: fall through to global-presence fallback
                        if VERBOSE:
                            print(f"[DECIDE] {mid}: sibling {sibling_mid} ambiguous (in_w={in_w}, in_l={in_l}) -> try global presence fallback")
                else:
                    if VERBOSE:
                        print(f"[DECIDE] {mid}: sibling {sibling_mid} not found -> try global presence fallback")

                # 2) fallback: if one child player is present anywhere globally (and the other not), pick the other as BYE winner
                if not chosen_id:
                    w_present_glob = (c_w_id and str(c_w_id) in global_ids) or (c_w_name and normalize_name(c_w_name) in global_names)
                    l_present_glob = (c_l_id and str(c_l_id) in global_ids) or (c_l_name and normalize_name(c_l_name) in global_names)
                    if w_present_glob ^ l_present_glob:
                        if w_present_glob:
                            chosen_name = c_l_name or ''
                            chosen_id = c_l_id or ''
                            if VERBOSE:
                                print(f"[DECIDE] {mid}: global presence w_present -> selecting child-loser {chosen_name or chosen_id}")
                        else:
                            chosen_name = c_w_name or ''
                            chosen_id = c_w_id or ''
                            if VERBOSE:
                                print(f"[DECIDE] {mid}: global presence l_present -> selecting child-winner {chosen_name or chosen_id}")
                    else:
                        # unable to decide -> UNKNOWN
                        if VERBOSE:
                            print(f"[DECIDE] {mid}: cannot decide from sibling/global presence -> UNKNOWN BYE")
                        chosen_name = ''
                        chosen_id = 'XXXX'
            else:
                if VERBOSE:
                    print(f"[DECIDE] {mid}: child {child_mid} not found -> UNKNOWN BYE")
                chosen_name = ''
                chosen_id = 'XXXX'

            # set round field: prefer sibling.round (current round) if available, else child.round, else empty
            chosen_round = ''
            if sibling and sibling.get('round'):
                chosen_round = sibling.get('round')
            elif child and child.get('round'):
                chosen_round = child.get('round')
            else:
                chosen_round = ''

            # --- NEW: preserve country and seed for the selected player when available (taken from child) ---
            chosen_country = ''
            chosen_seed = ''
            if child and chosen_id and chosen_id != 'XXXX':
                c_w_id = child.get('player_id_winner') or ''
                c_l_id = child.get('player_id_loser') or ''
                c_w_name = child.get('winner_player_name') or ''
                c_l_name = child.get('loser_player_name') or ''
                if chosen_id and c_w_id and chosen_id == c_w_id:
                    chosen_country = child.get('winner_country') or ''
                    chosen_seed = child.get('winner_seed') or ''
                elif chosen_id and c_l_id and chosen_id == c_l_id:
                    chosen_country = child.get('loser_country') or ''
                    chosen_seed = child.get('loser_seed') or ''
                else:
                    # fallback to name matching
                    if chosen_name and normalize_name(chosen_name) == normalize_name(c_w_name):
                        chosen_country = child.get('winner_country') or ''
                        chosen_seed = child.get('winner_seed') or ''
                    elif chosen_name and normalize_name(chosen_name) == normalize_name(c_l_name):
                        chosen_country = child.get('loser_country') or ''
                        chosen_seed = child.get('loser_seed') or ''
                    else:
                        chosen_country = ''
                        chosen_seed = ''
            # --- end preserve block ---

            # construct synthetic match
            synth = {
                'match_id': mid,
                'round': chosen_round,
                'winner_player_name': chosen_name,
                'loser_player_name': 'BYE',
                'score_string': '',
                'player_id_winner': chosen_id or '',
                'player_id_loser': 'XXXX',
                'winner_country': chosen_country or '',
                'loser_country': '',
                'winner_seed': chosen_seed or '',
                'loser_seed': ''
            }

            # append to insertion list and update maps so subsequent missing can see it
            to_insert.append(synth)
            id_map[mid] = synth
            prefix_nums[pref].add(n)
            global_ids.add(str(chosen_id)) if chosen_id else None
            if chosen_name:
                global_names.add(normalize_name(chosen_name))

    # finally append synthetic matches to original list
    if to_insert:
        if VERBOSE:
            print(f"[GLOBAL BYE] inserting {len(to_insert)} synthetic BYE matches")
        matches_list.extend(to_insert)

# --- Main processing loop (kept 'test' as requested) ---
index = defaultdict(list)
calendar_list = []

for kind in ('atp_matches','wta_matches'):
    d = MATCHES_DIR / kind
    if not d.exists():
        continue

    for csvfile in d.glob('*.csv'):
        # ---- special delegation for some WTA files ----
        stem = csvfile.stem.lower()
        if kind.startswith('wta') and any(stem.startswith(p) for p in SPECIAL_PREFIXES) and process_wta_special_csv:
            if VERBOSE:
                print(f"[SPECIAL] delegating {csvfile.name} to parse_wta_special.py")
            try:
                # process_wta_special_csv reads the CSV and writes the JSON into OUTPUT_DIR.
                out_path = process_wta_special_csv(csvfile, OUTPUT_DIR, verbose=VERBOSE)
                # If it returned a path, load the produced JSON meta to update index/calendar
                if out_path:
                    try:
                        with open(out_path, 'r', encoding='utf-8') as _f:
                            produced = json.load(_f)
                        meta2 = produced.get('meta', {}) or {}
                        src = (meta2.get('source') or 'wta').lower()
                        tid = str(meta2.get('tourney_id') or stem)
                        year_val = meta2.get('year') or ''
                        # update index and calendar exactly like the main loop does
                        index_key = f"{src}_{tid}"
                        index[index_key].append(int(year_val) if str(year_val).isdigit() else year_val)
                        calendar_list.append({
                            'source': src,
                            'tourney_id': tid,
                            'year': int(year_val) if str(year_val).isdigit() else year_val,
                            'tourney_name': meta2.get('tourney_name') or '',
                            'start_date': meta2.get('start_date') or '',
                            'surface': meta2.get('surface') or '',
                            'level': meta2.get('level') or ''
                        })
                    except Exception as e_meta:
                        # don't fail the whole run if reading produced JSON fails
                        print(f"[SPECIAL ERROR] reading produced JSON for {csvfile.name}: {e_meta}")
            except Exception as e:
                print(f"[SPECIAL ERROR] processing {csvfile.name} with parse_wta_special: {e}")
            # skip normal processing for this file
            continue

        # ---- normal processing for non-special files ----
        rows = read_csv_rows(csvfile)
        if not rows:
            continue

        # (the rest of your original code continues unchanged here)
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
            'singles_draw_size': parse_int_like(first.get('singles_draw_size')),
            'city': first.get('city') or '',
            'country': first.get('country') or '',
            'start_date': start_date
        }

        # fill ATP meta from ioc if available
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

        # build normalized matches list
        matches = []
        for r in rows:
            matches.append(build_match_record(r))

        # insert BYE synthetic matches following EXACT algorithm requested (global)
        insert_byes_global(matches, singles_draw_size=meta.get('singles_draw_size'))

        # ---- NEW: For the lowest round (first column), reorder JSON keys for every 1st,3rd,5th... MS/LS match ----
        # Steps:
        #  1) Find counts of rounds among MS/LS matches and pick the round with the highest count (assumed first round).
        #  2) Build a list of MS/LS matches in that round and sort by numeric suffix ascending.
        #  3) For the 1st,3rd,5th... (indices 0,2,4...) in that ordered list, when serializing a match,
        #     output the dict keys in "loser-first" order (loser_player_name, then winner_player_name, player_id_loser then player_id_winner, etc.)
        # Note: We do not change values — only the order of keys in the output dict — and this applies to both original and synthetic BYE matches.
        round_counts = defaultdict(int)
        ms_ls_matches = []
        for m in matches:
            mid = m.get('match_id') or ''
            mm = id_suffix_re.search(mid)
            if mm:
                pref = mm.group(1)
                if pref in ('MS', 'LS'):
                    rname = m.get('round') or ''
                    round_counts[rname] += 1
                    ms_ls_matches.append(m)

        lowest_round_name = None
        if round_counts:
            # pick the round with the largest number of MS/LS matches (assumed to be the earliest round)
            lowest_round_name = max(round_counts.items(), key=lambda x: x[1])[0]
            if VERBOSE:
                print(f"[LOWEST ROUND] detected lowest_round_name='{lowest_round_name}' with count={round_counts[lowest_round_name]}")
        else:
            if VERBOSE:
                print("[LOWEST ROUND] no MS/LS matches detected; skipping loser-first reordering step")

        # determine which match_ids should be serialized loser-first
        reorder_match_ids = set()
        if lowest_round_name:
            # collect MS/LS matches in that round
            matches_in_lowest = []
            for m in matches:
                mid = m.get('match_id') or ''
                mm = id_suffix_re.search(mid)
                if mm:
                    pref = mm.group(1)
                    if pref in ('MS', 'LS') and (m.get('round') or '') == lowest_round_name:
                        num = int(mm.group(2))
                        matches_in_lowest.append((num, mid))

            # order by numeric suffix ascending
            matches_in_lowest.sort(key=lambda x: x[0])
            if VERBOSE:
                print(f"[LOWEST ROUND] MS/LS matches in lowest round (ordered): {[mid for _, mid in matches_in_lowest]}")

            # mark every odd (1st,3rd,5th...) -> indices 0,2,4... for reordering
            for idx, (_, mid) in enumerate(matches_in_lowest):
                if idx % 2 == 0:
                    reorder_match_ids.add(mid)
            if VERBOSE:
                print(f"[LOWEST ROUND] match_ids to output loser-first: {sorted(reorder_match_ids)}")

        # sort matches for deterministic output (we will transform ordering of keys when serializing)
        final_sorted = sorted(matches, key=match_sort_key)

        # build final matches list with possibly reordered key order for selected matches
        final_output_matches = []
        # desired key order when "loser-first" is requested
        loser_first_keys = [
            'match_id', 'round',
            'loser_player_name', 'winner_player_name',
            'score_string',
            'player_id_loser', 'player_id_winner',
            'loser_country', 'winner_country',
            'loser_seed', 'winner_seed'
        ]

        for m in final_sorted:
            mid = m.get('match_id') or ''
            if mid in reorder_match_ids:
                # construct a new dict with keys in loser-first order, but keep values unchanged.
                newm = {}
                # first add the defined loser-first keys in that order, pulling values from original match (or empty string)
                for k in loser_first_keys:
                    newm[k] = m.get(k, '')
                # then append any other keys that were in the original match but not in loser_first_keys,
                # preserving their original relative order to avoid data loss.
                for k in m.keys():
                    if k not in newm:
                        newm[k] = m.get(k, '')
                final_output_matches.append(newm)
            else:
                # keep original field order as in m (python dict preserves insertion order already)
                # However to be deterministic, ensure all expected fields exist
                # We'll produce a shallow copy so further modifications won't affect original.
                copy_m = {}
                for k, v in m.items():
                    copy_m[k] = v
                final_output_matches.append(copy_m)

        out = {'meta': meta, 'matches': final_output_matches}
        out_name = f"{meta['source'].lower()}_{tourney_id}_{year}.json"
        out_path = OUTPUT_DIR / out_name
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        # update index and calendar
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
