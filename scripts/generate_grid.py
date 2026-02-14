#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_grid_improved.py  Générateur amélioré de grille 3x3 à partir des fichiers aggregate_*_reduced.json,
avec :
 - rééquilibrage pour préférer Winner/Finalist/SF vs QF,
 - encouragement à la diversité des deux "result_at_tourney",
 - historique (dernier 10) pour diminuer réutilisation trop fréquente,
 - génération stochastique (différente à chaque run).
Modifications principales :
 - assouplissement de la contrainte "total == 0" (on pénalise au lieu de rejeter),
 - scoring qui prend en compte le nombre de cellules vides (zeros) et favorise
   result kinds W/F/SF, tout en gardant diversité,
 - réduction de la pénalité quand on choisit deux résultats du même type.
"""
import json, itertools, random, time, re
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path('.')
AGG_ATP = ROOT / 'docs' / 'games' / 'aggregate_ATP_reduced.json'
AGG_WTA = ROOT / 'docs' / 'games' / 'aggregate_WTA_reduced.json'
ATP_CSV = ROOT / 'player_data_atp.csv'
WTA_CSV = ROOT / 'player_data_wta.csv'
OUT_DIR = ROOT / 'docs' / 'games'
HISTORY_PATH = OUT_DIR / 'grid_history.json'
OUT_DIR.mkdir(parents=True, exist_ok=True)

GRID_SIZE = 3
TOP_PREFERRED_RANK = 150

# Sampling / search params (tweak if necessary)
N_TRIES = 1200            # augmenter légèrement pour plus de chances
TOP_K_COUNTRIES = 60
TOP_K_RESULTS = 240      # augmenter pour ne pas tronquer des petits sets "winner" trop tôt
TOP_K_OTHERS = 120

# result kind priority multipliers (favor winners/finalists/semis)
RESULT_KIND_PRIORITY = {'W': 4.0, 'F': 2.5, 'SF': 1.7, 'QF': 1.0}

# ---------------- helpers ----------------
def load_json(path):
    if not path.exists():
        return None
    with path.open(encoding='utf-8') as f:
        return json.load(f)

def save_json(obj, path):
    with path.open('w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_players_min(csv_path):
    players = {}
    if not csv_path.exists():
        return players
    import csv
    with csv_path.open(encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            pid = (r.get('player_id') or r.get('player') or '').strip()
            if not pid: continue
            rank_raw = r.get('highest_ranking') or r.get('best_rank') or ''
            best_rank = None
            try:
                if rank_raw not in (None, ''):
                    best_rank = int(float(str(rank_raw).strip()))
            except:
                best_rank = None
            backhand = (r.get('backhand') or '').strip().lower() or None
            plays = (r.get('plays') or r.get('hand') or '').strip().lower() or None
            players[pid] = {'best_rank': best_rank, 'backhand': backhand, 'plays': plays}
    return players

def label_of(cond):
    return cond.get('label') or str(cond)

# ---------------- pool builder (même logique) ----------------
def build_pools(aggregate, players_info):
    countries_pool = []
    for country, pids in aggregate.get('countries', {}).items():
        s = set(pids)
        if s:
            countries_pool.append({'type':'country','label': country, 'set': s})

    results_pool = []
    for ek, ev in aggregate.get('tournaments', {}).items():
        tn = ev.get('tourney_name') or ek
        mapping = [('W', ev.get('winners', [])),
                   ('F', ev.get('finalists', [])),
                   ('SF', ev.get('semifinalists', [])),
                   ('QF', ev.get('quarterfinalists', []))]
        for rk, entries in mapping:
            pids = [e.get('player_id') for e in entries if isinstance(e, dict) and e.get('player_id')]
            # also accept if entries are strings/ids (robustness)
            if not pids and isinstance(entries, list):
                pids = [e for e in entries if isinstance(e, str)]
            s = set([pid for pid in pids if pid])
            if s:
                label = {'W':'Winner at {}','F':'Finalist at {}','SF':'Semifinalist at {}','QF':'Quarterfinalist at {}'}[rk].format(tn)
                results_pool.append({'type':'result_at_tourney','label':label,'set':s,'event_key':ek,'result_kind':rk,'tourney':tn})

    others_pool = []
    # ranks
    ranks = aggregate.get('ranks', {})
    rank_map = [('Rank #1','top_1'),('Rank #2','top_2'),('Rank #3','top_3'),('Rank #5','top_5'),('Rank #10','top_10'),('Rank #50','top_50')]
    for label, key in rank_map:
        pids = ranks.get(key, [])
        if pids:
            others_pool.append({'type':'rank_number','label': label, 'set': set(pids), 'value': int(re.search(r'\d+', label).group())})
    # heights
    ht = aggregate.get('height', {})
    if ht:
        smaller = set(ht.get('smaller_than', []))
        taller = set(ht.get('taller_than', []))
        thresholds = ht.get('thresholds', {})
        if smaller: others_pool.append({'type':'height_cmp','label': f"Shorter than {thresholds.get('smaller')}", 'set': smaller, 'op':'smaller', 'threshold': thresholds.get('smaller')})
        if taller:  others_pool.append({'type':'height_cmp','label': f"Taller than {thresholds.get('taller')}", 'set': taller, 'op':'taller', 'threshold': thresholds.get('taller')})
    # born_cities
    for city,pids in aggregate.get('born_cities', {}).items():
        if pids:
            others_pool.append({'type':'born_city','label': f"Born in {city}", 'set': set(pids), 'value': city})
    # lefties
    lefties = set(aggregate.get('lefties', []))
    if lefties: others_pool.append({'type':'plays_handed','label':'Left-handed','set':lefties,'value':'left'})
    # one-handed backhand
    oneh = set(aggregate.get('one_handed_backhand', []))
    if oneh: others_pool.append({'type':'backhand_type','label':'one-handed backhand','set':oneh,'value':'one-handed'})
    # positive_h2h grouped by top10 opponent
    pos = aggregate.get('positive_h2h_vs_top10', [])
    pos_by_top10 = defaultdict(set)
    for e in pos:
        a = e.get('player_id') if isinstance(e, dict) else None
        b = e.get('top10_id') if isinstance(e, dict) else None
        if a and b: pos_by_top10[b].add(a)
    for top10_id, s in pos_by_top10.items():
        others_pool.append({'type':'positive_h2h','label': f"Positive H2H vs {top10_id}", 'set': set(s), 'top10_id': top10_id})

    # sort by size descending (richest first)
    countries_pool.sort(key=lambda x: len(x['set']), reverse=True)
    results_pool.sort(key=lambda x: len(x['set']), reverse=True)
    others_pool.sort(key=lambda x: len(x['set']), reverse=True)
    return countries_pool, results_pool, others_pool

# ---------------- weighted sampling utilities ----------------
def weighted_index(weights):
    total = sum(weights)
    if total <= 0:
        return None
    r = random.random() * total
    acc = 0.0
    for i,w in enumerate(weights):
        acc += w
        if r <= acc:
            return i
    return len(weights)-1

def weighted_choice_without_replacement(items, weights, k):
    items2 = list(items)
    weights2 = list(weights)
    chosen = []
    for _ in range(min(k, len(items2))):
        idx = weighted_index(weights2)
        if idx is None:
            break
        chosen.append(items2.pop(idx))
        weights2.pop(idx)
    return chosen

# ---------------- scoring helpers ----------------
def intersection_stats(a_set, b_set, players_info):
    inter = a_set & b_set
    total = len(inter)
    top150 = sum(1 for pid in inter if players_info.get(pid,{}).get('best_rank') is not None and players_info[pid]['best_rank'] <= TOP_PREFERRED_RANK)
    return inter, total, top150

# ---------------- randomized search (improved) ----------------
def find_grid_randomized(countries_pool, results_pool, others_pool, players_info, history_labels, n_tries=N_TRIES):
    recent_counts = Counter()
    for h in history_labels:
        for lbl in h:
            recent_counts[lbl] += 1

    # trim pools
    cp = countries_pool[:TOP_K_COUNTRIES]
    rp = results_pool[:TOP_K_RESULTS]
    op = others_pool[:TOP_K_OTHERS]

    if len(cp) < 2 or len(rp) < 2 or len(op) < 2:
        return None

    # base weight function: favors novelty and size, but for results apply result-kind priority
    def weight_for_label(lbl):
        return 1.0 / (1.0 + recent_counts.get(lbl, 0))

    cp_weights = [weight_for_label(label_of(c)) * max(1, len(c['set'])) for c in cp]

    # for results apply result-kind priority (and normalize a bit)
    rp_weights = []
    for r in rp:
        base = weight_for_label(label_of(r)) * max(1, len(r['set']))
        kind = r.get('result_kind') or 'QF'
        mult = RESULT_KIND_PRIORITY.get(kind, 1.0)
        rp_weights.append(base * mult)

    op_weights = [weight_for_label(label_of(o)) * max(1, len(o['set'])) for o in op]

    best = None
    best_score = (-1, -1, 0)  # (min_top150, min_total, -zeros_count)

    for t in range(n_tries):
        # sample 2 countries weighted
        c_choice = weighted_choice_without_replacement(cp, cp_weights, 2)
        if len(c_choice) < 2: continue

        # sample first result weighted
        idx1 = weighted_index(rp_weights)
        if idx1 is None: continue
        r_first = rp[idx1]

        # build second result weights penalizing same result_kind, but not zeroing them
        rp_rem_weights = []
        remaining_indices = []
        for i, r in enumerate(rp):
            if i == idx1:
                continue
            base = rp_weights[i]
            same_kind = (r.get('result_kind') == r_first.get('result_kind'))
            # less severe penalty than before: prefer diversity but allow same-kind
            factor = 0.6 if same_kind else 1.0
            rp_rem_weights.append(base * factor)
            remaining_indices.append(i)

        idx2_rel = weighted_index(rp_rem_weights)
        if idx2_rel is None:
            continue
        idx2 = remaining_indices[idx2_rel]
        r_second = rp[idx2]

        # sample 2 others
        o_choice = weighted_choice_without_replacement(op, op_weights, 2)
        if len(o_choice) < 2: continue

        # shuffle to add variety in placements
        random.shuffle(c_choice); random.shuffle(o_choice)
        rA, rB = r_first, r_second
        oA, oB = o_choice[0], o_choice[1]
        c1, c2 = c_choice[0], c_choice[1]

        # build candidate layouts (both patterns and both assignments)
        candidate_layouts = []
        for (rX, rY) in ((rA, rB), (rB, rA)):
            rows = [c1, c2, rX]; cols = [rY, oA, oB]
            candidate_layouts.append((rows, cols, 'rows_countries'))
            cols = [c1, c2, rX]; rows = [rY, oA, oB]
            candidate_layouts.append((rows, cols, 'cols_countries'))

        for rows, cols, pattern in candidate_layouts:
            # ensure label uniqueness across rows and cols
            if set(label_of(x) for x in rows) & set(label_of(x) for x in cols):
                continue

            # compute intersections with a soft penalty for empty cells instead of outright rejection
            inters = {}
            zeros_count = 0
            min_top150 = None
            min_total = None
            ok = True
            for i, rcond in enumerate(rows):
                for j, ccond in enumerate(cols):
                    inter, total, top150 = intersection_stats(rcond['set'], ccond['set'], players_info)
                    inters[f"{i}-{j}"] = {'inter': inter, 'total': total, 'top150': top150}
                    if total == 0:
                        zeros_count += 1
                    if min_total is None or total < min_total:
                        min_total = total
                    if min_top150 is None or top150 < min_top150:
                        min_top150 = top150

            # scoring: prefer higher min_top150, then higher min_total, then fewer zeros
            # fewer zeros is better -> use -zeros_count in tuple
            score = (min_top150 if min_top150 is not None else 0,
                     min_total if min_total is not None else 0,
                     -zeros_count)
            # tiny random epsilon to break ties and encourage variety
            eps = random.random() * 1e-6
            score_with_eps = (score[0] + eps, score[1] + eps, score[2] + eps)

            if score_with_eps > best_score:
                best_score = score_with_eps
                best = {'rows': rows, 'cols': cols, 'pattern': pattern, 'intersections': inters, 'zeros': zeros_count}
                # early accept if grid is good enough (no empty cells and some top150)
                if score[2] == 0 and score[0] >= 1:
                    return best

    return best

# ---------------- deterministic fallback with diversity-first ----------------
def deterministic_fallback(cp, rp, op, players_info):
    def test_combo(rows, cols):
        # ensure labels distinct
        if set(label_of(x) for x in rows) & set(label_of(x) for x in cols):
            return None
        inters = {}
        zeros_count = 0
        min_top150 = None; min_total = None
        for i, rcond in enumerate(rows):
            for j, ccond in enumerate(cols):
                inter, total, top150 = intersection_stats(rcond['set'], ccond['set'], players_info)
                inters[f"{i}-{j}"] = {'inter': inter, 'total': total, 'top150': top150}
                if total == 0:
                    zeros_count += 1
                if min_total is None or total < min_total: min_total = total
                if min_top150 is None or top150 < min_top150: min_top150 = top150
        # return structure and score tuple (min_top150, min_total, -zeros_count)
        return {'rows': rows, 'cols': cols, 'intersections': inters, 'score': (min_top150, min_total, -zeros_count), 'zeros': zeros_count}

    best = None
    best_score = (-1, -1, 0)

    # first pass: prefer result kinds different
    for (i1,i2) in itertools.combinations(range(len(cp)), 2):
        c1 = cp[i1]; c2 = cp[i2]
        for (j1,j2) in itertools.combinations(range(len(rp)), 2):
            rA = rp[j1]; rB = rp[j2]
            # require different result kinds first
            if rA.get('result_kind') == rB.get('result_kind'):
                continue
            for (k1,k2) in itertools.combinations(range(len(op)), 2):
                oA = op[k1]; oB = op[k2]
                for rX, rY in ((rA, rB),(rB, rA)):
                    rows = [c1, c2, rX]; cols = [rY, oA, oB]
                    res = test_combo(rows, cols)
                    if res:
                        sc = res['score']
                        if sc > best_score:
                            best_score = sc; best = res
                    rows = [rY, oA, oB]; cols = [c1, c2, rX]
                    res = test_combo(rows, cols)
                    if res:
                        sc = res['score']
                        if sc > best_score:
                            best_score = sc; best = res
    if best:
        return best

    # second pass: allow same result kinds (if no grid found)
    for (i1,i2) in itertools.combinations(range(len(cp)), 2):
        c1 = cp[i1]; c2 = cp[i2]
        for (j1,j2) in itertools.combinations(range(len(rp)), 2):
            rA = rp[j1]; rB = rp[j2]
            for (k1,k2) in itertools.combinations(range(len(op)), 2):
                oA = op[k1]; oB = op[k2]
                for rX, rY in ((rA, rB),(rB, rA)):
                    rows = [c1, c2, rX]; cols = [rY, oA, oB]
                    res = test_combo(rows, cols)
                    if res:
                        sc = res['score']
                        if sc > best_score:
                            best_score = sc; best = res
                    rows = [rY, oA, oB]; cols = [c1, c2, rX]
                    res = test_combo(rows, cols)
                    if res:
                        sc = res['score']
                        if sc > best_score:
                            best_score = sc; best = res
    return best

# ---------------- build output grid ----------------
def build_grid_output(best, players_info, max_sample=12):
    rows = best['rows']; cols = best['cols']; inters = best['intersections']
    grid_cells = []
    used = set()
    for i in range(GRID_SIZE):
        row_cells = []
        for j in range(GRID_SIZE):
            info = inters.get(f"{i}-{j}", {}) or {}
            inter_set = set(info.get('inter', set()))
            ordered = sorted(list(inter_set), key=lambda pid: (players_info.get(pid,{}).get('best_rank') if players_info.get(pid,{}).get('best_rank') is not None else 99999, pid))
            sample = ordered[:max_sample]
            chosen = None
            for pid in ordered:
                br = players_info.get(pid,{}).get('best_rank')
                if pid not in used and br is not None and br <= TOP_PREFERRED_RANK:
                    chosen = pid; break
            if not chosen:
                for pid in ordered:
                    if pid not in used:
                        chosen = pid; break
            if chosen:
                used.add(chosen)
            row_cells.append({'row': i, 'col': j, 'candidates_sample': sample, 'chosen': chosen, 'count': len(inter_set), 'count_top150': info.get('top150',0)})
        grid_cells.append(row_cells)
    out = {
        'rows': [{'index': i, 'label': label_of(rows[i]), 'type': rows[i].get('type')} for i in range(GRID_SIZE)],
        'cols': [{'index': j, 'label': label_of(cols[j]), 'type': cols[j].get('type')} for j in range(GRID_SIZE)],
        'cells': grid_cells,
        'pattern': best.get('pattern', 'n/a'),
        'zeros_in_intersections': best.get('zeros', 0)
    }
    return out

# ---------------- history handling ----------------
def read_history():
    if not HISTORY_PATH.exists():
        return []
    return load_json(HISTORY_PATH) or []

def write_history_entry(labels):
    hist = read_history()
    hist.insert(0, {'ts': time.time(), 'labels': labels})
    hist = hist[:10]
    save_json(hist, HISTORY_PATH)

# ---------------- main wrapper ----------------
def generate_grid_for_circuit(agg_path, players_csv, out_path, circuit_label):
    agg = load_json(agg_path)
    if not agg:
        print(f"[ERROR] missing aggregate: {agg_path}")
        return None
    players_info = load_players_min(players_csv)
    countries_pool, results_pool, others_pool = build_pools(agg, players_info)

    # debug: print counts by result_kind to help diagnose "only QF" issues
    kind_counts = Counter(r.get('result_kind') for r in results_pool)
    print(f"[DEBUG] result kinds counts for {circuit_label}:", dict(kind_counts))

    history = read_history()
    history_labels = [entry.get('labels', []) for entry in history]

    best = find_grid_randomized(countries_pool, results_pool, others_pool, players_info, history_labels, n_tries=N_TRIES)

    if not best:
        print(f"[WARN] Could not find a grid in randomized search for {circuit_label} - trying deterministic fallback.")
        # deterministic fallback (diversity-first)
        best = deterministic_fallback(countries_pool[:40], results_pool[:80], others_pool[:80], players_info)

    if not best:
        print(f"[ERROR] No valid grid found for {circuit_label}.")
        return None

    grid_obj = build_grid_output(best, players_info)
    save_json({'circuit': circuit_label, 'generated_at': time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), 'grid': grid_obj}, out_path)

    # update history with the 6 used labels
    normalized_labels = [label_of(x) for x in (best.get('rows',[]) + best.get('cols',[]))]
    write_history_entry(normalized_labels)

    return out_path

# ---------------- run ----------------
if __name__ == '__main__':
    random.seed()  # use a different seed each run (system time)
    # ATP
    atp_out = generate_grid_for_circuit(AGG_ATP, ATP_CSV, OUT_DIR / 'fill_the_grid_ATP.json', 'ATP')
    if atp_out: print("Wrote ATP grid to", atp_out)
    # WTA
    wta_out = generate_grid_for_circuit(AGG_WTA, WTA_CSV, OUT_DIR / 'fill_the_grid_WTA.json', 'WTA')
    if wta_out: print("Wrote WTA grid to", wta_out)
    print("Done.")
