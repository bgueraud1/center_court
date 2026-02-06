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

# utilities
def extract_digits(s):
    if not s:
        return ''
    m = re.sub(r'[^0-9]', '', str(s))
    return m

def match_sort_key(m):
    mid = m.get('match_id') or ''
    mnum = extract_digits(mid)
    if mnum.isdigit():
        return int(mnum)
    # fallback: if match_id contains letters only, put after numeric
    return 10**9

def normalize_name(n):
    if not n:
        return ''
    s = re.sub(r'\s+', ' ', str(n).strip())
    return s.lower()

def normalize_pid(pid):
    if not pid:
        return ''
    return str(pid).strip().upper()

def normalize_name_for_cmp(n):
    if not n:
        return ''
    s = re.sub(r'[^a-z0-9\s]', '', str(n).lower())
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def player_present_in_matches(pid, name, matches_list):
    """Return True if player (by id or by normalized name) occurs in any match in matches_list."""
    pid_norm = normalize_pid(pid)
    name_norm = normalize_name_for_cmp(name)
    for pm in matches_list:
        # check ids robustly across possible keys
        for k in ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id', 'PlayerIDA', 'PlayerIDA2','PlayerIDB','PlayerIDB2'):
            v = pm.get(k)
            if v:
                if normalize_pid(v) and pid_norm and normalize_pid(v) == pid_norm:
                    return True
        # check names in many possible fields
        wn = pm.get('winner_player_name') or pm.get('winner') or pm.get('player_winner') or pm.get('player_winner_name') or ''
        ln = pm.get('loser_player_name') or pm.get('loser') or pm.get('player_loser') or pm.get('player_loser_name') or ''
        if name_norm and (normalize_name_for_cmp(wn) == name_norm or normalize_name_for_cmp(ln) == name_norm):
            return True
    return False

# augment_byes reworked to be deterministic and robust
def augment_byes(groups):
    """
    groups: dict round -> list(matches)
    Strategy:
      - determine rounds ordered by size (desc) consistent with frontend
      - for each column index starting at 1 (i.e. for each pair prev/curr):
          * sort prev matches deterministically
          * bucket prev matches to curr_count buckets (preserving bracket locality)
          * for each child (curr match) create synth matches for any participant appearing in curr
            (winner then loser) who is NOT present in prev_matches, and insert the synth immediately
            before the bucket feeding that child.
    """
    if not groups:
        return

    # rounds ordered left->right (most matches -> left)
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)

    # iterate columns
    for col_index in range(1, len(rounds)):
        prev_key = rounds[col_index - 1]
        curr_key = rounds[col_index]
        prev_matches = groups.get(prev_key, [])[:]
        curr_matches = groups.get(curr_key, [])[:]

        prev_count = len(prev_matches)
        curr_count = len(curr_matches)

        # deterministic ordering of prev and curr matches (by match_sort_key)
        prev_matches.sort(key=match_sort_key)
        curr_matches.sort(key=match_sort_key)

        # build buckets: distribute prev_matches into curr_count buckets to preserve vertical locality
        buckets = [[] for _ in range(max(1, curr_count))]
        if prev_count > 0 and curr_count > 0:
            for i, pm in enumerate(prev_matches):
                # integer bucket assignment distributing prev_count across curr_count
                child_idx = int(i * curr_count / prev_count)
                if child_idx < 0:
                    child_idx = 0
                if child_idx >= curr_count:
                    child_idx = curr_count - 1
                buckets[child_idx].append(pm)

        new_prev = []
        inserted_for_prev = set()

        # helper to extract participant id/name/country/seed from a match row
        def get_part_from_match(match_row, role):
            # role is 'winner' or 'loser'
            id_keys = [f'player_id_{role}', f'{role}_player_id', f'{role}_id', f'{role}Id', f'PlayerIDA', f'PlayerIDA2', 'PlayerIDB', 'PlayerIDB2']
            name_keys = [f'{role}_player_name', f'{role}_name', role, f'player_{role}', f'player_{role}_name']
            country_keys = [f'{role}_country', f'{role}_nationality', f'{role}_country_code', f'country_{role}', f'country_{role}']
            seed_keys = [f'{role}_seed', f'seed_{role}']
            pid = ''
            pname = ''
            pcountry = ''
            pseed = ''
            for k in id_keys:
                if k in match_row and match_row.get(k):
                    pid = match_row.get(k)
                    break
            for k in name_keys:
                if k in match_row and match_row.get(k):
                    pname = match_row.get(k)
                    break
            for k in country_keys:
                if k in match_row and match_row.get(k):
                    pcountry = match_row.get(k)
                    break
            for k in seed_keys:
                if k in match_row and match_row.get(k):
                    pseed = match_row.get(k)
                    break
            return {'id': pid or '', 'name': pname or '', 'country': pcountry or '', 'seed': pseed or ''}

        # for each child (curr match), ensure its participants are present in prev (create synth if missing)
        for j, cm in enumerate(curr_matches):
            for role in ('winner', 'loser'):
                part = get_part_from_match(cm, role)
                pid = part.get('id') or ''
                pname = part.get('name') or ''
                unique_key = normalize_pid(pid) if pid else normalize_name_for_cmp(pname)
                if not unique_key:
                    continue
                if unique_key in inserted_for_prev:
                    continue
                # check if present in original prev_matches by id or name
                if not player_present_in_matches(pid, pname, prev_matches):
                    # create synthetic match where this player "won" vs BYE
                    synth_id = f"synth_{col_index}_{j}_{random.randint(1,9999)}"
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
            # after synthetic inserts for this child, append the original prev matches bucket for that child (if any)
            if curr_count > 0:
                bucket = buckets[j] if j < len(buckets) else []
                for pm in bucket:
                    new_prev.append(pm)

        # if there were no curr matches (curr_count==0) keep prev unchanged
        if curr_count == 0:
            groups[prev_key] = prev_matches
        else:
            groups[prev_key] = new_prev

# NEW: flatten groups to matches following bracket ordering rules requested by the user
def get_round_rank_key(rk, source):
    if not rk:
        return 999
    t = str(rk).strip().upper()
    # ATP canonical order: F > SF > QF > R16 > R32 > R64 > R128
    if source == 'ATP':
        order = ['F', 'SF', 'QF', 'R16', 'R32', 'R64', 'R128']
        # normalize obvious variants
        norm = t.replace('FINAL', 'F').replace('SEMIFINAL', 'SF').replace('SEMIFINALS', 'SF').replace('QUARTER', 'QF')
        norm = norm.replace('ROUND ', 'R').replace('RD', 'R')
        # direct match
        for i, name in enumerate(order):
            if norm == name:
                return i
        # try R<number>
        m = re.match(r'^R(\d+)$', norm)
        if m:
            num = int(m.group(1))
            if num >= 128:
                return 6
            if num >= 64:
                return 5
            if num >= 32:
                return 4
            if num >= 16:
                return 3
        # fallback by length (more matches -> earlier in sequence when we sort later)
        return 50
    else:
        # WTA: F > S > Q > 4 > 3 > 2 > 1
        order = ['F', 'S', 'Q', '4', '3', '2', '1']
        norm = t.replace('FINAL', 'F').replace('SEMIFINAL', 'S').replace('SEMIFINALS', 'S').replace('QUARTER', 'Q')
        for i, name in enumerate(order):
            if norm == name:
                return i
        # if round is numeric like '2' etc
        if norm.isdigit() and norm in order:
            return order.index(norm)
        return 50


def participants_of_match(m):
    parts = set()
    # ids
    for k in ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id', 'PlayerIDA', 'PlayerIDA2','PlayerIDB','PlayerIDB2'):
        v = m.get(k)
        if v:
            parts.add(('id', normalize_pid(v)))
    # names
    wn = m.get('winner_player_name') or m.get('winner') or m.get('player_winner') or m.get('player_winner_name') or ''
    ln = m.get('loser_player_name') or m.get('loser') or m.get('player_loser') or m.get('player_loser_name') or ''
    if wn:
        parts.add(('name', normalize_name_for_cmp(wn)))
    if ln:
        parts.add(('name', normalize_name_for_cmp(ln)))
    return parts


def match_contains_player(m, pid_norm, name_norm):
    # check ids first
    for k in ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id', 'PlayerIDA', 'PlayerIDA2','PlayerIDB','PlayerIDB2'):
        v = m.get(k)
        if v and normalize_pid(v) and pid_norm and normalize_pid(v) == pid_norm:
            return True
    # names
    wn = m.get('winner_player_name') or m.get('winner') or m.get('player_winner') or m.get('player_winner_name') or ''
    ln = m.get('loser_player_name') or m.get('loser') or m.get('player_loser') or m.get('player_loser_name') or ''
    if name_norm and (normalize_name_for_cmp(wn) == name_norm or normalize_name_for_cmp(ln) == name_norm):
        return True
    return False


def order_matches_by_bracket(groups, source):
    # Build rounds sequence sorted by rank (final first)
    rounds = list(groups.keys())
    rounds_sorted = sorted(rounds, key=lambda k: get_round_rank_key(k, source))

    # If rank keys tie or unknown, we will keep rounds_sorted stable by number of matches descending
    # but ensure final-like rounds remain first
    # Build mapping round -> ordered matches (we won't re-sort the lists here except for deterministic tie-breakers)
    round_to_matches = {r: list(groups[r])[:] for r in rounds_sorted}

    # Ensure deterministic ordering inside each round initially
    for r in round_to_matches:
        round_to_matches[r].sort(key=match_sort_key)

    # Now reorder each round (except the very top 'final') by looking at its parent round closer to final
    # rounds_sorted[0] is final-like; leave it as single match or its existing order
    for idx in range(1, len(rounds_sorted)):
        curr_r = rounds_sorted[idx]
        parent_r = rounds_sorted[idx - 1]
        curr_matches = round_to_matches.get(curr_r, [])[:]
        parent_matches = round_to_matches.get(parent_r, [])[:]

        # build index of parent matches for quick lookup
        parent_indexed = parent_matches

        # For every curr match compute parent_index (the index in parent_matches it feeds into)
        buckets = defaultdict(list)  # parent_index -> list(matches)
        fallback_bucket_key = 10**9
        for cm in curr_matches:
            # Attempt to find parent by checking if any participant of cm appears in parent match
            found_parent = None
            # try ids first then names
            cm_pid_w = normalize_pid(cm.get('player_id_winner') or cm.get('player_winner_id') or cm.get('winner_id') or '')
            cm_pid_l = normalize_pid(cm.get('player_id_loser') or cm.get('player_loser_id') or cm.get('loser_id') or '')
            cm_name_w = normalize_name_for_cmp(cm.get('winner_player_name') or cm.get('winner') or '')
            cm_name_l = normalize_name_for_cmp(cm.get('loser_player_name') or cm.get('loser') or '')

            for p_idx, pm in enumerate(parent_indexed):
                if match_contains_player(pm, cm_pid_w, cm_name_w) or match_contains_player(pm, cm_pid_l, cm_name_l):
                    found_parent = p_idx
                    break
            if found_parent is None:
                # maybe parent contains one of winners/losers by searching the other way: does any parent participant appear in cm?
                for p_idx, pm in enumerate(parent_indexed):
                    p_pid_w = normalize_pid(pm.get('player_id_winner') or pm.get('player_winner_id') or pm.get('winner_id') or '')
                    p_pid_l = normalize_pid(pm.get('player_id_loser') or pm.get('player_loser_id') or pm.get('loser_id') or '')
                    p_name_w = normalize_name_for_cmp(pm.get('winner_player_name') or pm.get('winner') or '')
                    p_name_l = normalize_name_for_cmp(pm.get('loser_player_name') or pm.get('loser') or '')
                    if (cm_pid_w and (p_pid_w == cm_pid_w or p_pid_l == cm_pid_w)) or (cm_pid_l and (p_pid_w == cm_pid_l or p_pid_l == cm_pid_l)):
                        found_parent = p_idx
                        break
                    if (cm_name_w and (p_name_w == cm_name_w or p_name_l == cm_name_w)) or (cm_name_l and (p_name_w == cm_name_l or p_name_l == cm_name_l)):
                        found_parent = p_idx
                        break
            if found_parent is None:
                buckets[fallback_bucket_key].append(cm)
            else:
                buckets[found_parent].append(cm)

        # Now build ordered list for curr round by iterating parent indices in ascending order
        new_curr = []
        sorted_parent_indices = sorted([k for k in buckets.keys() if k != fallback_bucket_key])
        for pidx in sorted_parent_indices:
            group = buckets[pidx]
            if not group:
                continue
            # determine "first player" of the parent match to choose order inside the pair
            p_match = parent_indexed[pidx]
            first_player_pid = normalize_pid(p_match.get('player_id_winner') or p_match.get('player_winner_id') or p_match.get('winner_id') or '')
            first_player_name = normalize_name_for_cmp(p_match.get('winner_player_name') or p_match.get('winner') or '')

            # sort group so that the one containing first_player goes first
            def group_key(gm):
                if match_contains_player(gm, first_player_pid, first_player_name):
                    return 0
                return 1
            group_sorted = sorted(group, key=lambda x: (group_key(x), match_sort_key(x)))
            new_curr.extend(group_sorted)

        # append fallback bucket at end (those we couldn't map)
        if fallback_bucket_key in buckets:
            fallback_sorted = sorted(buckets[fallback_bucket_key], key=match_sort_key)
            new_curr.extend(fallback_sorted)

        round_to_matches[curr_r] = new_curr

    # Finally, concatenate rounds in rounds_sorted order
    out = []
    for r in rounds_sorted:
        out.extend(round_to_matches.get(r, []))
    return out

# iterate files
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
            # collect also ids, seeds and countries when present (robust to various column names)
            m = {
                'match_id': r.get('match_id') or r.get('match') or r.get('event_id') or r.get('match') or '',
                'round': (r.get('round') or r.get('round_name') or '').strip(),
                'winner_player_name': r.get('winner_player_name') or r.get('player_winner') or r.get('winner') or r.get('winner_player') or '',
                'loser_player_name': r.get('loser_player_name') or r.get('player_loser') or r.get('loser') or r.get('loser_player') or '',
                'score_string': r.get('score_string') or r.get('score') or r.get('score_str') or '',
                'player_id_winner': r.get('player_id_winner') or r.get('player_winner_id') or r.get('winner_id') or r.get('PlayerIDA') or r.get('PlayerIDA2') or r.get('player_id_winner') or r.get('player_id_winner'),
                'player_id_loser': r.get('player_id_loser') or r.get('player_loser_id') or r.get('loser_id') or r.get('PlayerIDB') or r.get('PlayerIDB2') or '',
                'winner_country': r.get('winner_country') or r.get('country_winner') or r.get('winner_country_code') or r.get('country_a') or r.get('country_b') or '',
                'loser_country': r.get('loser_country') or r.get('country_loser') or r.get('loser_country_code') or '',
                'winner_seed': r.get('winner_seed') or r.get('seed_winner') or r.get('seed_a') or r.get('seed_winner') or r.get('seed_winner') or '',
                'loser_seed': r.get('loser_seed') or r.get('seed_loser') or r.get('seed_b') or r.get('seed_loser') or ''
            }
            matches.append(m)

        # group by round
        groups = defaultdict(list)
        for m in matches:
            rk = m.get('round') or ''
            groups[rk].append(m)

        # AUGMENTATION : insérer BYE synthétiques dans groups (localement) si un joueur apparait en T2 sans avoir T1
        augment_byes(groups)

        # flatten groups back into matches array in bracket order (final -> semis -> quarters -> ...)
        final_matches = order_matches_by_bracket(groups, meta['source'])

        out = {'meta': meta, 'matches': final_matches}
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
