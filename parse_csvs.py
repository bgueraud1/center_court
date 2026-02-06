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

# === Nouveau : détecter interruptions LS/MS et insérer match factice BYE (optimisé) ===
id_suffix_re = re.compile(r'([A-Za-z]+)(\d+)$')

# keys to look for ids and names (centralized to avoid repeated lists)
ID_KEYS = ('player_id_winner', 'player_id_loser', 'player_winner_id', 'player_loser_id', 'winner_id', 'loser_id', 'player_id_winner', 'player_id_loser')
NAME_KEYS = ('winner_player_name', 'player_winner', 'winner', 'player_winner_name',
             'loser_player_name', 'player_loser', 'loser', 'player_loser_name')

def extract_ids_and_names_from_row(r):
    """Return sets (ids, names) found in a single match row."""
    ids = set()
    names = set()
    # ids
    for k in ID_KEYS:
        v = r.get(k)
        if v:
            ids.add(str(v))
    # winner name
    wn = r.get('winner_player_name') or r.get('winner') or r.get('player_winner') or r.get('player_winner_name')
    ln = r.get('loser_player_name') or r.get('loser') or r.get('player_loser') or r.get('player_loser_name')
    if wn:
        names.add(normalize_name(wn))
    if ln:
        names.add(normalize_name(ln))
    return ids, names

def insert_missing_sequential_matches(groups):
    """
    Optimized insertion of synthetic BYE matches to fill numeric holes.
    - Precompute present ids/names per round for O(1) membership checks.
    - Build a map child_num -> child_match for fast lookup.
    - Iterate expected numeric interval computed from child numbers.
    """
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    if len(rounds) < 2:
        return

    for idx in range(len(rounds) - 1):
        cur_key = rounds[idx]
        next_key = rounds[idx + 1]
        cur_matches = groups.get(cur_key, [])[:]  # travail sur copie
        next_matches = groups.get(next_key, [])[:]
        if not cur_matches or not next_matches:
            continue

        # construire map des matchs du round suivant par (prefix, num) et une map num -> match
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
                # child_num_map: si plusieurs préfixes existent, garder le premier (tolérance)
                if num not in child_num_map:
                    child_num_map[num] = nm
                child_nums.add(num)

        # récupérer préfixe et largeur chiffres du round courant (si possible)
        nums_present = set()
        prefix_used = None
        num_width = None
        for cm in cur_matches:
            mid = cm.get('match_id') or ''
            m = id_suffix_re.search(mid)
            if m:
                if not prefix_used:
                    prefix_used = m.group(1)
                num = int(m.group(2))
                nums_present.add(num)
                num_width = max(num_width or 0, len(m.group(2)))

        if not prefix_used:
            # si on ne peut pas déterminer le préfixe, on ne modifie pas ce round
            continue

        # Construire ensembles de recherche pour le round courant : ids et noms normalisés
        present_ids = set()
        present_names = set()
        for pm in cur_matches:
            # tirer ids/noms de façon robuste (comme extract_ids_and_names_from_row mais en inline pour perf)
            for k in ID_KEYS:
                v = pm.get(k)
                if v:
                    present_ids.add(str(v))
            wn = pm.get('winner_player_name') or pm.get('winner') or pm.get('player_winner') or pm.get('player_winner_name')
            ln = pm.get('loser_player_name') or pm.get('loser') or pm.get('player_loser') or pm.get('player_loser_name')
            if wn:
                present_names.add(normalize_name(wn))
            if ln:
                present_names.add(normalize_name(ln))

        # Déterminer la plage attendue pour le round courant à partir du round enfant.
        # Si on a les numéros enfant, l'intervalle attendu est :
        # [2 * min(child_nums), 2 * max(child_nums) + 1]
        if child_nums:
            min_n = 2 * min(child_nums)
            max_n = 2 * max(child_nums) + 1
        else:
            # fallback : s'appuyer sur ce qui est présent
            if not nums_present:
                continue
            min_n = min(nums_present)
            max_n = max(nums_present)

        added_any = False

        # itérer sur la plage attendue (généralement petite : ex 16..31)
        for n in range(min_n, max_n + 1):
            if n in nums_present:
                continue  # existant -> rien à faire

            # enfant correspondant : ceil(n/2)
            child_num = (n + 1) // 2
            if child_num < 1:
                continue

            # Chercher d'abord avec même préfixe
            child_match = next_map.get((prefix_used, child_num))
            # sinon fallback rapide via child_num_map (O(1))
            if not child_match:
                child_match = child_num_map.get(child_num)

            if not child_match:
                continue  # on ne peut pas reconstruire sans le match enfant

            # extraire joueurs du match enfant (winner/loser)
            w_id = child_match.get('player_id_winner') or child_match.get('winner_id') or child_match.get('player_winner_id') or ''
            w_name = child_match.get('winner_player_name') or child_match.get('winner') or ''
            l_id = child_match.get('player_id_loser') or child_match.get('loser_id') or child_match.get('player_loser_id') or ''
            l_name = child_match.get('loser_player_name') or child_match.get('loser') or ''

            # déterminer présence via sets (O(1))
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

            # si exactement l'un des deux est absent, on crée le match fictif où ce joueur "gagne" contre BYE
            synth_winner_id = ''
            synth_winner_name = ''
            synth_winner_country = ''
            synth_winner_seed = ''
            if (not w_present) and l_present:
                synth_winner_id = w_id or ''
                synth_winner_name = w_name or ''
                synth_winner_country = child_match.get('winner_country') or ''
                synth_winner_seed = child_match.get('winner_seed') or ''
            elif (not l_present) and w_present:
                synth_winner_id = l_id or ''
                synth_winner_name = l_name or ''
                synth_winner_country = child_match.get('loser_country') or ''
                synth_winner_seed = child_match.get('loser_seed') or ''
            else:
                # cas : soit les deux absents, soit les deux présents -> on ne crée rien
                continue

            # construire match_id synthétique (respecter padding si possible)
            num_str = str(n).zfill(num_width or len(str(n)))
            synth_id = f"{prefix_used}{num_str}"

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

            # insérer (on ajoute puis on marque la présence pour éviter doublons)
            cur_matches.append(synth)
            nums_present.add(n)
            # mettre à jour sets pour ne pas recréer plusieurs fois pour même joueur
            if synth_winner_id:
                present_ids.add(str(synth_winner_id))
            elif synth_winner_name:
                present_names.add(normalize_name(synth_winner_name))
            added_any = True
            if VERBOSE:
                print(f"[insert] round={cur_key} inserted {synth_id} winner={synth_winner_name or synth_winner_id}")

        if added_any:
            # trier cur_matches par numéro extrait de match_id quand c'est possible
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
            groups[cur_key] = cur_matches_sorted
# === fin du nouvel ajout ===

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

        # === APPEL: réparer interruptions séquentielles LS/MS en insérant match BYE ===
        insert_missing_sequential_matches(groups)
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
