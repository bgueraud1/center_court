# parse_wta_special.py
"""
Parser WTA spécial : reconstruction récursive des partitions et attribution LS###
Sortie : JSON minimal (meta + matches) avec uniquement les champs essentiels.
Usage:
  python parse_wta_special.py <csv_path> <output_dir>
"""
import csv, json, math, sys, re
from collections import defaultdict, deque
from pathlib import Path
from datetime import datetime

# ----------------- utilitaires -----------------
def _make_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_serializable(v) for v in obj]
    if isinstance(obj, set):
        return [_make_json_serializable(v) for v in sorted(obj, key=lambda x: str(x))]
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)

def parse_int_like(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    s = s.replace(',', '.')
    try:
        f = float(s)
    except Exception:
        return None
    if f != f or f in (float('inf'), float('-inf')):
        return None
    try:
        return int(f)
    except Exception:
        return None

def normalize_name(n):
    if not n:
        return ''
    return ' '.join(str(n).strip().lower().split())

def read_csv_rows(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

# ----------------- collecte joueurs / matches -----------------
def collect_players_and_matches(rows):
    players = {}   # key -> {names:set, seeds:set, brackets:set}
    matches = []   # list of {winner_key, loser_key, round, raw, winner_seed, loser_seed}
    def add_player(key, name, seed, bracket):
        if key is None:
            return
        entry = players.setdefault(key, {'names': set(), 'seeds': set(), 'brackets': set()})
        if name:
            entry['names'].add(normalize_name(name))
        if seed is not None:
            entry['seeds'].add(seed)
        if bracket is not None:
            entry['brackets'].add(bracket)

    for r in rows:
        w_id = r.get('player_id_winner') or r.get('winner_player_id') or r.get('playerida_raw') or r.get('PlayerIDA')
        l_id = r.get('player_id_loser') or r.get('loser_player_id') or r.get('playeridb_raw') or r.get('PlayerIDB')
        w_name = r.get('winner_player_name') or r.get('winner') or r.get('player_winner')
        l_name = r.get('loser_player_name') or r.get('loser') or r.get('player_loser')
        w_seed = parse_int_like(r.get('winner_seed') or r.get('seed_winner') or r.get('winner_seed_raw'))
        l_seed = parse_int_like(r.get('loser_seed') or r.get('seed_loser') or r.get('loser_seed_raw'))
        try:
            w_br = int(r.get('winner_bracket_number')) if r.get('winner_bracket_number') else None
        except Exception:
            w_br = None
        try:
            l_br = int(r.get('loser_bracket_number')) if r.get('loser_bracket_number') else None
        except Exception:
            l_br = None

        w_key = str(w_id).strip() if w_id else ('name:' + normalize_name(w_name) if w_name else None)
        l_key = str(l_id).strip() if l_id else ('name:' + normalize_name(l_name) if l_name else None)

        add_player(w_key, w_name, w_seed, w_br)
        add_player(l_key, l_name, l_seed, l_br)

        matches.append({
            'winner_key': w_key,
            'loser_key': l_key,
            'round': (r.get('round') or '').strip(),
            'raw': r,
            'winner_seed': w_seed,
            'loser_seed': l_seed
        })
    return players, matches

# ----------------- graphe -----------------
def build_adjacency(matches):
    winner_to_losers = defaultdict(set)
    for m in matches:
        w = m['winner_key']; l = m['loser_key']
        if w and l:
            winner_to_losers[w].add(l)
    return winner_to_losers

# ----------------- seed mapping -----------------
def choose_seed_mapping(players):
    seed_candidates = defaultdict(list)
    for key, info in players.items():
        for s in info['seeds']:
            if s is not None:
                score = len(info['names'])
                seed_candidates[int(s)].append((key, score))
    seed_map = {}
    for s, c in seed_candidates.items():
        c_sorted = sorted(c, key=lambda x: (-x[1], x[0]))
        seed_map[int(s)] = c_sorted[0][0]
    return seed_map

# ----------------- BFS descendant -----------------
def bfs_from_anchor(winner_to_losers, anchor_key, max_depth):
    depths = {}
    if anchor_key is None:
        return depths
    q = deque([(anchor_key, 0)])
    depths[anchor_key] = 0
    while q:
        node, d = q.popleft()
        if d >= max_depth:
            continue
        for ch in winner_to_losers.get(node, ()):
            if ch not in depths:
                depths[ch] = d + 1
                q.append((ch, d + 1))
    return depths

# ----------------- round mapping -----------------
ROUND_ORDER = {'F':0, 'S':1, 'Q':2, '4':3, '3':4, '2':5, '1':6}
def round_label_to_index(rlabel, rounds_total):
    if not rlabel:
        return None
    r = str(rlabel).upper().strip()
    if r in ROUND_ORDER:
        idx = ROUND_ORDER[r]
        if idx < rounds_total:
            return idx
    return None

# ----------------- BUILD EXPLICIT MATCH TREE -----------------
def build_match_tree(singles_draw_size):
    """
    Construit le graphe des matches pour un tableau de taille singles_draw_size.
    Numérotation LS utilisée : final = 1 .. total_matches = n-1
    Chaque niveau idx (0 = final, rounds_total-1 = 1er tour) contient matches [2**idx .. 2**(idx+1)-1]
    Retour : dict {
      'children': { match_num (int) : (child1, child2) or () if leaf },
      'parent': { child_match_num: parent_match_num },
      'round_of': { match_num: idx }  # idx 0 = final
    }
    """
    if singles_draw_size & (singles_draw_size - 1) != 0:
        raise ValueError("singles_draw_size must be a power of two")
    rounds_total = int(math.log2(singles_draw_size))
    total_matches = singles_draw_size - 1
    children = {}
    parent = {}
    round_of = {}
    for idx in range(0, rounds_total):
        start = 2 ** idx
        end = 2 ** (idx + 1) - 1
        child_start = 2 ** (idx + 1)
        for mnum in range(start, end + 1):
            # children are at next level; if next level exceeds total matches, leaf (first round has no children)
            if child_start > total_matches:
                # leaf (first round) => no child matches
                children[mnum] = ()
                round_of[mnum] = idx
                continue
            offset = (mnum - start) * 2
            c1 = child_start + offset
            c2 = c1 + 1
            children[mnum] = (c1, c2)
            parent[c1] = mnum
            parent[c2] = mnum
            round_of[mnum] = idx
    # ensure leaves present (last level)
    leaf_start = 2 ** (rounds_total - 1)
    leaf_end = 2 ** rounds_total - 1
    for mnum in range(leaf_start, leaf_end + 1):
        if mnum not in children:
            children[mnum] = ()
            round_of[mnum] = rounds_total - 1
    return {'children': children, 'parent': parent, 'round_of': round_of, 'rounds_total': rounds_total}

# ----------------- draw groups -----------------
def build_draw_groups(winner_to_losers, seed_map, singles_draw_size, rounds_total):
    draw_groups = {}
    group_count = 2
    # group_count doubles until it exceeds number of players: groups_of_{group_size}
    while group_count <= singles_draw_size:
        group_size = singles_draw_size // group_count
        level_name = f'groups_of_{group_size}'
        draw_groups[level_name] = {}
        # depth_limit: how many win-levels to include when starting from anchor
        if group_size and ((group_size & (group_size - 1)) == 0):
            depth_limit = int(math.log2(group_size))
        else:
            depth_limit = max(0, rounds_total - int(math.log2(group_count)))
        for s in range(1, group_count + 1):
            anchor_key = seed_map.get(s)
            if anchor_key:
                depths = bfs_from_anchor(winner_to_losers, anchor_key, depth_limit)
                group_keys = set(depths.keys())
            else:
                depths = {}
                group_keys = set()
            draw_groups[level_name][s] = {
                'anchor_player': anchor_key,
                'group_size': group_size,
                'depth_limit': depth_limit,
                'group_keys': group_keys,
                'depths': depths
            }
        group_count *= 2
    return draw_groups

# ----------------- anchor ordering -----------------
def representative_of_anchor(players, group_keys, seed_num):
    br_vals = []
    id_vals = []
    for pk in group_keys:
        p = players.get(pk)
        if not p:
            continue
        for b in p.get('brackets', []):
            if isinstance(b, int):
                br_vals.append(b)
        try:
            id_vals.append(int(pk))
        except Exception:
            pass
    if br_vals:
        return min(br_vals)
    if id_vals:
        return min(id_vals)
    return seed_num

def compute_anchor_orders(draw_groups, players, singles_draw_size):
    rounds_total = int(math.log2(singles_draw_size))
    max_index = rounds_total - 1
    anchor_orders = {}
    anchor_orders[0] = [1]
    if max_index >= 1:
        anchor_orders[1] = [1, 2]
    for idx in range(2, max_index + 1):
        group_count = 2 ** idx
        ordered = []
        parent_order = anchor_orders[idx - 1]
        for parent_seed in parent_order:
            left_child = 2 * parent_seed - 1
            right_child = 2 * parent_seed
            left_keys = draw_groups.get(f'groups_of_{singles_draw_size // (2 ** idx)}', {}).get(left_child, {}).get('group_keys', set())
            right_keys = draw_groups.get(f'groups_of_{singles_draw_size // (2 ** idx)}', {}).get(right_child, {}).get('group_keys', set())
            left_rep = representative_of_anchor(players, left_keys, left_child)
            right_rep = representative_of_anchor(players, right_keys, right_child)
            if left_rep <= right_rep:
                ordered.extend([left_child, right_child])
            else:
                ordered.extend([right_child, left_child])
        anchor_orders[idx] = ordered
    return anchor_orders

# ----------------- attribution LS### (graph-aware) -----------------
def assign_ls(match_list, draw_groups, players, seed_map, singles_draw_size, verbose=False):
    """
    Attribution basée sur :
     - le graphe des matches (numérotation LS fixe)
     - l'ordre des ancres (anchor_orders)
     - affectation unique d'une LS par match en évitant collisions.
    """
    # build match tree
    mt = build_match_tree(singles_draw_size)
    rounds_total = mt['rounds_total']
    anchor_orders = compute_anchor_orders(draw_groups, players, singles_draw_size)

    used = set()  # used LS numbers (int)
    def match_sort_key(m):
        r = round_label_to_index(m.get('round'), rounds_total)
        key = f"{m.get('winner_key') or ''}|{m.get('loser_key') or ''}"
        return (r if r is not None else 999, key)
    sorted_matches = sorted(match_list, key=match_sort_key)

    for m in sorted_matches:
        rlabel = m.get('round')
        idx = round_label_to_index(rlabel, rounds_total)
        if idx is None:
            m['match_id'] = ''
            continue

        start = 2 ** idx
        end = 2 ** (idx + 1) - 1
        ordered_anchors = anchor_orders.get(idx, [])

        # if no anchors for this level, fallback hashing within the range
        if not ordered_anchors:
            h = (hash(str(m.get('winner_key')) + '|' + str(m.get('loser_key'))) & 0xffffffff)
            j = h % (end - start + 1)
            lsnum = start + j
            while lsnum in used:
                lsnum = start + ((lsnum - start + 1) % (end - start + 1))
            used.add(lsnum)
            m['match_id'] = f"LS{str(lsnum).zfill(3)}"
            continue

        # try to find which anchor-group contains winner or loser
        chosen_anchor_index = None
        winner = m.get('winner_key'); loser = m.get('loser_key')
        level_group_size = singles_draw_size // (2 ** idx)
        level_name = f'groups_of_{level_group_size}'
        for j, seed_anchor in enumerate(ordered_anchors):
            info = draw_groups.get(level_name, {}).get(seed_anchor, {})
            gkeys = info.get('group_keys', set())
            if (winner in gkeys) or (loser in gkeys):
                chosen_anchor_index = j
                break

        # if not found, try parents (walk up levels)
        if chosen_anchor_index is None and idx > 0:
            parent_idx = idx - 1
            found_parent = None
            while parent_idx >= 0 and found_parent is None:
                parent_level_size = singles_draw_size // (2 ** parent_idx)
                parent_name = f'groups_of_{parent_level_size}'
                parent_order = anchor_orders.get(parent_idx, [])
                for p_j, p_seed in enumerate(parent_order):
                    p_info = draw_groups.get(parent_name, {}).get(p_seed, {})
                    p_gkeys = p_info.get('group_keys', set())
                    if winner in p_gkeys or loser in p_gkeys:
                        found_parent = (parent_idx, p_j, p_seed)
                        break
                parent_idx -= 1
            if found_parent is not None:
                parent_idx, p_j, p_seed = found_parent
                # determine child indices under that parent
                child_pos0 = 2 * p_j
                child_pos1 = 2 * p_j + 1
                # children exist in ordered_anchors for current idx
                # protect bounds
                def rep(seed):
                    if seed is None:
                        return 10**12
                    info = draw_groups.get(level_name, {}).get(seed, {})
                    return representative_of_anchor(players, info.get('group_keys', set()), seed)
                child0_seed = ordered_anchors[child_pos0] if child_pos0 < len(ordered_anchors) else None
                child1_seed = ordered_anchors[child_pos1] if child_pos1 < len(ordered_anchors) else None
                r0 = rep(child0_seed); r1 = rep(child1_seed)
                chosen_anchor_index = child_pos0 if r0 <= r1 else child_pos1

        # final fallback: hash-based within anchors
        if chosen_anchor_index is None:
            h = (hash(str(winner) + '|' + str(loser)) & 0xffffffff)
            chosen_anchor_index = h % len(ordered_anchors)

        lsnum = start + chosen_anchor_index
        # safety clamp
        if lsnum < start or lsnum > end:
            lsnum = start
        attempts = 0
        while lsnum in used and attempts < (end - start + 2):
            lsnum = start + ((lsnum - start + 1) % (end - start + 1))
            attempts += 1
        used.add(lsnum)
        m['match_id'] = f"LS{str(lsnum).zfill(3)}"

    return match_list

# ----------------- production de la sortie minimale -----------------
def score_string_from_raw(raw):
    for k in ('score_string','ScoreString','ScoreString','scoreString','Score'):
        v = raw.get(k)
        if v:
            return v
    s1 = raw.get('set1_score') or raw.get('Set 1 Score') or ''
    s2 = raw.get('set2_score') or raw.get('Set 2 Score') or ''
    s3 = raw.get('set3_score') or raw.get('Set 3 Score') or ''
    parts = [p for p in (s1,s2,s3) if p]
    return ','.join(parts)

def seed_to_string(raw_seed_value, internal_seed_int):
    if raw_seed_value:
        return str(raw_seed_value)
    if internal_seed_int is None:
        return ''
    # keep .0 style if integer (user example used "5.0")
    return f"{internal_seed_int}.0"

def normalize_match_output(m, players):
    raw = m.get('raw') or {}
    # match_id -> use recalculated one (NE JAMAIS utiliser raw['match_id'])
    match_id = m.get('match_id') or ''
    roundv = m.get('round') or raw.get('round') or ''
    winner_name = raw.get('winner_player_name') or raw.get('winner') or ''
    loser_name  = raw.get('loser_player_name')  or raw.get('loser')  or ''
    score_str = score_string_from_raw(raw)
    # prefer explicit player id fields from raw if present else use winner_key/loser_key if they are ids
    pid_w = raw.get('player_id_winner') or raw.get('winner_player_id') or raw.get('playerida_raw') or raw.get('PlayerIDA') or ''
    pid_l = raw.get('player_id_loser')  or raw.get('loser_player_id')  or raw.get('playeridb_raw') or raw.get('PlayerIDB') or ''
    if not pid_w:
        wk = m.get('winner_key')
        if wk and re.fullmatch(r'\d+', str(wk)):
            pid_w = str(wk)
    if not pid_l:
        lk = m.get('loser_key')
        if lk and re.fullmatch(r'\d+', str(lk)):
            pid_l = str(lk)
    winner_country = raw.get('winner_country') or raw.get('country_winner') or raw.get('winner_country_code') or ''
    loser_country  = raw.get('loser_country')  or raw.get('country_loser')  or raw.get('loser_country_code')  or ''
    winner_seed = seed_to_string(raw.get('winner_seed') or raw.get('seed_winner'), m.get('winner_seed'))
    loser_seed  = seed_to_string(raw.get('loser_seed')  or raw.get('seed_loser'),  m.get('loser_seed'))

    return {
        'match_id': match_id,
        'round': roundv,
        'winner_player_name': winner_name or '',
        'loser_player_name': loser_name or '',
        'score_string': score_str or '',
        'player_id_winner': str(pid_w) if pid_w else '',
        'player_id_loser': str(pid_l) if pid_l else '',
        'winner_country': winner_country or '',
        'loser_country': loser_country or '',
        'winner_seed': winner_seed,
        'loser_seed': loser_seed
    }

# ----------------- process CSV -> JSON -----------------
def process_wta_special_csv(csv_path, output_dir, verbose=True):
    csv_path = Path(csv_path)
    out_dir = Path(output_dir)
    rows = read_csv_rows(csv_path)
    if not rows:
        if verbose:
            print("[SPECIAL] CSV vide, rien à faire")
        return
    first = rows[0]
    singles_draw_size = parse_int_like(first.get('singles_draw_size')) or 128
    rounds_total = int(math.log2(singles_draw_size))

    players, matches = collect_players_and_matches(rows)
    winner_to_losers = build_adjacency(matches)
    seed_map = choose_seed_mapping(players)

    if verbose:
        print(f"[SPECIAL] players={len(players)} matches={len(matches)} draw={singles_draw_size} rounds={rounds_total}")
        print(f"[SPECIAL] seed_map keys: {sorted(seed_map.keys())}")

    draw_groups = build_draw_groups(winner_to_losers, seed_map, singles_draw_size, rounds_total)
    matches_with_ids = assign_ls(matches, draw_groups, players, seed_map, singles_draw_size, verbose=verbose)

    # build minimal matches output (use recalculated match_id)
    final_matches = [normalize_match_output(m, players) for m in matches_with_ids]

    # sort matches deterministically by numeric part of match_id (as in parser principal)
    def match_sort_key_out(m):
        mid = m.get('match_id') or ''
        digits = re.sub(r'[^0-9]', '', mid)
        return int(digits) if digits.isdigit() else 10**9
    final_matches_sorted = sorted(final_matches, key=match_sort_key_out)

    # build meta similar to original parser
    meta_out = {
        'source': 'WTA',
        'tourney_id': first.get('tourney_id') or first.get('event_id') or csv_path.stem,
        'year': int(first.get('tourney_year') or first.get('event_year') or first.get('year') or 0),
        'tourney_name': first.get('tourney_name') or first.get('tournament_name') or '',
        'tourney_title': first.get('tournament_title') or first.get('tournament_name') or first.get('tourney_name') or '',
        'surface': (first.get('surface') or '').title(),
        'level': first.get('level') or '',
        'prize_money': first.get('prize_money') or '',
        'prize_money_currency': first.get('prize_money_currency') or '',
        'singles_draw_size': singles_draw_size,
        'city': first.get('city') or '',
        'country': first.get('country') or '',
        'start_date': (first.get('start_date') or first.get('tourney_start_date') or '')[:10]
    }

    out_final = {'meta': meta_out, 'matches': final_matches_sorted}
    out_clean = _make_json_serializable(out_final)

    out_path = out_dir / f"wta_{meta_out['tourney_id']}_{meta_out['year']}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out_clean, f, ensure_ascii=False, indent=2)

    if verbose:
        print(f"[SPECIAL] wrote {out_path}")
    return out_path

# ---- CLI ----
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python parse_wta_special.py <csv_path> <output_dir>")
        sys.exit(1)
    process_wta_special_csv(Path(sys.argv[1]), Path(sys.argv[2]), verbose=True)
