#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_player_h2h.py

Génère des JSON H2H *par joueur* à partir des CSV matches et player_data CSV.
Sortie:
  - docs/data/h2h_by_player/<PLAYER_ID>.json  (1 fichier par joueur)
  - docs/data/h2h_players_index.json         (index léger)

Usage:
  python generate_player_h2h.py --matches-dir ./matches --outdir ./docs/data/h2h_by_player --player-atp ./player_data_atp.csv --player-wta ./player_data_wta.csv --verbose
"""
import os
import csv
import json
import argparse
from glob import glob
from collections import defaultdict
from datetime import datetime

# -------------------------
# Utilitaires (adaptés)
# -------------------------
def norm_key(k: str) -> str:
    return k.strip().lower().replace(' ', '_')

def normalize_id(s):
    if s is None:
        return ''
    s = str(s).strip()
    return s.upper()

def load_player_csv(path):
    players = {}
    if not os.path.isfile(path):
        return players
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        rows = list(reader)
        if not rows:
            return players
        headers = [norm_key(h) for h in rows[0]]
        for r in rows[1:]:
            if len(r) < len(headers):
                r = r + ['']*(len(headers)-len(r))
            rec = { headers[i]: (r[i].strip() if r[i] is not None else '') for i in range(len(headers)) }
            pid = ''
            for candidate in ('player_id','playerid','player_id_winner','playerid_winner','player_id_loser','playerid_loser','player'):
                if candidate in rec and rec[candidate]:
                    pid = rec[candidate]
                    break
            if not pid:
                for h in headers:
                    if 'id' in h and rec.get(h):
                        pid = rec[h]; break
            pid = normalize_id(pid)
            if pid:
                players[pid] = rec
            else:
                name = rec.get('full_name') or rec.get('fullname') or rec.get('full name') or ''
                if name:
                    synthetic = name.strip().upper().replace(' ', '_')
                    players[synthetic] = rec
    return players

def iter_match_csv_files(matches_dir_root):
    patterns = [
        os.path.join(matches_dir_root, 'atp_matches', '*.csv'),
        os.path.join(matches_dir_root, 'wta_matches', '*.csv'),
        os.path.join(matches_dir_root, '*.csv')
    ]
    seen = []
    for pat in patterns:
        for f in glob(pat):
            if f not in seen:
                seen.append(f)
    return sorted(seen)

def normalize_row_dict(row_dict):
    return { norm_key(k): (v.strip() if isinstance(v, str) else v) for k,v in row_dict.items() }

def get_field(d, *candidates):
    for c in candidates:
        if c in d and d[c] != '':
            return d[c]
    return ''

def parse_matches(matches_files, verbose=False):
    matches = []
    for f in matches_files:
        try:
            with open(f, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for raw in reader:
                    row = normalize_row_dict(raw)
                    row['_source_file'] = os.path.basename(f)
                    matches.append(row)
        except Exception as e:
            if verbose:
                print(f"[WARN] impossible de lire {f}: {e}")
    return matches

def extract_basic_match_info(r):
    m = {}
    m['player_id_winner'] = normalize_id(get_field(r, 'player_id_winner','playerid_winner','playerid','player_id_wta','player_winner_id','player_winner'))
    m['player_id_loser']  = normalize_id(get_field(r, 'player_id_loser','playerid_loser','player_loser_id','player_loser'))
    m['winner_player_name'] = get_field(r, 'winner_player_name','winner','player_winner','player_a','player_a_name')
    m['loser_player_name']  = get_field(r, 'loser_player_name','loser','player_loser','player_b','player_b_name')
    m['event_id'] = get_field(r, 'event_id','tourney_id','eventid','event')
    m['event_year'] = get_field(r, 'event_year','tourney_year','year')
    m['tourney_name'] = get_field(r, 'tourney_name','tournament_name','tournament','tourney')
    m['level'] = get_field(r, 'level')
    m['start_date'] = get_field(r, 'start_date')
    m['surface'] = get_field(r, 'surface')
    m['match_id'] = get_field(r, 'match_id','matchid','msid','match_id_wta','match_id_atp')
    m['round'] = get_field(r, 'round','match_round')
    m['score_string'] = get_field(r, 'score_string','score')
    if not m['score_string']:
        sets = []
        for s in ('set1_score','set2_score','set3_score','set4_score','set5_score'):
            v = get_field(r, s)
            if v:
                sets.append(v)
        if sets:
            m['score_string'] = ' '.join(sets)
    m['match_time_total'] = get_field(r, 'match_time_total','match_time','duration')
    m['match_date'] = get_field(r, 'match_date','date')
    m['winner_country'] = get_field(r, 'winner_country','country_winner','country_a','represented_country','country')
    m['loser_country']  = get_field(r, 'loser_country','country_loser','country_b')
    m['_raw'] = r
    return m

def try_int(v):
    try:
        return int(float(v))
    except Exception:
        return None

# -------------------------
# Génération per-player
# -------------------------
def compute_career_counts(all_matches, verbose=False):
    counts = defaultdict(lambda: {'wins': 0, 'losses': 0})
    for r in all_matches:
        wid = normalize_id(get_field(r, 'player_id_winner','playerid_winner','playerid'))
        lid = normalize_id(get_field(r, 'player_id_loser','playerid_loser','playerid_loser'))
        if wid:
            counts[wid]['wins'] += 1
        if lid:
            counts[lid]['losses'] += 1
    return counts

def accumulate_per_player_matches(all_matches, players_index, verbose=False):
    """
    Retourne player_matches: dict player_id -> list of match_entries (where player is involved)
    Chaque match_entry contient les champs minimaux + opponent_id/opponent_name/is_winner
    """
    player_matches = defaultdict(list)

    for r in all_matches:
        m = extract_basic_match_info(r)
        wid = m.get('player_id_winner') or ''
        lid = m.get('player_id_loser') or ''
        # If both ids exist -> create 2 entries (one for winner, one for loser)
        if wid and lid:
            # winner side
            e1 = {
                'match_id': m.get('match_id') or '',
                'event_id': m.get('event_id') or '',
                'event_year': m.get('event_year') or '',
                'tourney_name': m.get('tourney_name') or '',
                'surface': m.get('surface') or '',
                'round': m.get('round') or '',
                'score': m.get('score_string') or '',
                'match_date': m.get('match_date') or '',
                'opponent_id': lid,
                'opponent_name': m.get('loser_player_name') or '',
                'is_winner': True,
                'match_link': build_match_link(m)
            }
            player_matches[wid].append(e1)
            # loser side
            e2 = dict(e1)
            e2['opponent_id'] = wid
            e2['opponent_name'] = m.get('winner_player_name') or ''
            e2['is_winner'] = False
            player_matches[lid].append(e2)
        else:
            # If no ids, try to match by names to players_index; else create synthetic keys
            wn = (m.get('winner_player_name') or '').strip()
            ln = (m.get('loser_player_name') or '').strip()
            if wn and ln:
                # try lookup id by name in players_index['all']
                id_lookup = players_index.get('all', {})
                # build a simple name -> id mapping (once)
                # do a cheap search: exact match / lower-case substring
                def find_id_by_name(n):
                    if not n: return ''
                    nlc = n.lower()
                    for pid, rec in id_lookup.items():
                        cand = (rec.get('full_name') or rec.get('fullname') or '').lower()
                        if cand == nlc:
                            return pid
                    for pid, rec in id_lookup.items():
                        cand = (rec.get('full_name') or rec.get('fullname') or '').lower()
                        if nlc in cand or cand in nlc:
                            return pid
                    return ''
                id_w = find_id_by_name(wn)
                id_l = find_id_by_name(ln)
                if id_w and id_l:
                    # behave like above
                    e1 = {
                        'match_id': m.get('match_id') or '',
                        'event_id': m.get('event_id') or '',
                        'event_year': m.get('event_year') or '',
                        'tourney_name': m.get('tourney_name') or '',
                        'surface': m.get('surface') or '',
                        'round': m.get('round') or '',
                        'score': m.get('score_string') or '',
                        'match_date': m.get('match_date') or '',
                        'opponent_id': id_l,
                        'opponent_name': ln,
                        'is_winner': True,
                        'match_link': build_match_link(m)
                    }
                    player_matches[id_w].append(e1)
                    e2 = dict(e1)
                    e2['opponent_id'] = id_w
                    e2['opponent_name'] = wn
                    e2['is_winner'] = False
                    player_matches[id_l].append(e2)
                else:
                    # fallback: create synthetic ids from names
                    ida = wn.upper().replace(' ', '_')
                    idb = ln.upper().replace(' ', '_')
                    e1 = {
                        'match_id': m.get('match_id') or '',
                        'event_id': m.get('event_id') or '',
                        'event_year': m.get('event_year') or '',
                        'tourney_name': m.get('tourney_name') or '',
                        'surface': m.get('surface') or '',
                        'round': m.get('round') or '',
                        'score': m.get('score_string') or '',
                        'match_date': m.get('match_date') or '',
                        'opponent_id': idb,
                        'opponent_name': ln,
                        'is_winner': True,
                        'match_link': build_match_link(m)
                    }
                    player_matches[ida].append(e1)
                    e2 = dict(e1)
                    e2['opponent_id'] = ida
                    e2['opponent_name'] = wn
                    e2['is_winner'] = False
                    player_matches[idb].append(e2)
            else:
                # totally missing info -> skip
                continue
    return player_matches

def build_match_link(m):
    if m.get('event_id') and m.get('match_id'):
        return f"/en/scores/match-stats/archive/{m.get('event_id')}/{m.get('match_id')}"
    return ''

def build_player_object(pid, rec, career_counts):
    obj = {
        'id': pid,
        'full_name': rec.get('full_name') or rec.get('fullname') or rec.get('full name') or rec.get('player_name') or '',
        'country': rec.get('represented_country') or rec.get('represented') or rec.get('country') or '',
        'best_rank': try_int(rec.get('highest_ranking') or rec.get('best_rank') or rec.get('best rank')),
        'height': rec.get('height_inches') or rec.get('height') or rec.get('height_cm') or '',
        'plays': rec.get('plays') or '',
        'backhand': rec.get('backhand') or '',
        'turned_pro': rec.get('turned_pro') or rec.get('turnedpro') or '',
        'prize_money': rec.get('prize_money') or rec.get('prize') or '',
        'raw': rec
    }
    career = career_counts.get(pid, {'wins':0,'losses':0})
    obj['career_summary'] = {'wins': career.get('wins',0), 'losses': career.get('losses',0)}
    return obj

def compute_aggregates_for_player(matches_list):
    """
    Compute simple aggregates: career (wins/losses already provided separately),
    by_surface counts and by_year counts (H2H matches as in player's matches).
    """
    agg = {'by_surface': defaultdict(lambda: {'wins':0,'losses':0}), 'by_year': defaultdict(lambda: {'wins':0,'losses':0})}
    for m in matches_list:
        s = m.get('surface') or 'Unknown'
        y = str(m.get('event_year') or (m.get('match_date') or '')[:4] or 'Unknown')
        if m.get('is_winner'):
            agg['by_surface'][s]['wins'] += 1
            agg['by_year'][y]['wins'] += 1
        else:
            agg['by_surface'][s]['losses'] += 1
            agg['by_year'][y]['losses'] += 1
    # convert defaultdicts to normal dicts
    agg['by_surface'] = {k:v for k,v in agg['by_surface'].items()}
    agg['by_year'] = {k:v for k,v in agg['by_year'].items()}
    return agg

# -------------------------
# CLI + Main
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="Génère JSON par joueur pour H2H (performant pour frontend).")
    ap.add_argument('--matches-dir', default='./matches', help='dossier contenant atp_matches/ et wta_matches/')
    ap.add_argument('--outdir', default='./docs/data/h2h_by_player', help='dossier de sortie pour JSON (par joueur)')
    ap.add_argument('--player-atp', default='./player_data_atp.csv', help='fichier player_data_atp.csv')
    ap.add_argument('--player-wta', default='./player_data_wta.csv', help='fichier player_data_wta.csv')
    ap.add_argument('--index-out', default='./docs/data/h2h_players_index.json', help='index JSON (mapping player->file)')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    # load players
    atp = load_player_csv(args.player_atp)
    wta = load_player_csv(args.player_wta)
    all_players = {}
    all_players.update(atp)
    for k,v in wta.items():
        if k not in all_players: all_players[k] = v
    players_index = {'atp': atp, 'wta': wta, 'all': all_players}

    # load matches
    match_files = iter_match_csv_files(args.matches_dir)
    if args.verbose:
        print(f"[INFO] fichiers matches détectés: {len(match_files)}")
        for mf in match_files: print("  -", mf)
    all_matches = parse_matches(match_files, verbose=args.verbose)
    if args.verbose:
        print(f"[INFO] matches totaux: {len(all_matches)}")

    # compute career counts (global)
    career_counts = compute_career_counts(all_matches, verbose=args.verbose)
    if args.verbose:
        print(f"[INFO] career counts calculés pour {len(career_counts)} ids")

    # accumulate per player matches
    player_matches = accumulate_per_player_matches(all_matches, players_index, verbose=args.verbose)
    if args.verbose:
        print(f"[INFO] players with matches: {len(player_matches)}")

    # write per-player files
    index_map = {}
    n_written = 0
    for pid, matches_list in player_matches.items():
        # player record from catalog if exists
        rec = all_players.get(pid, {})
        player_obj = build_player_object(pid, rec, career_counts)
        aggregates = compute_aggregates_for_player(matches_list)
        payload = {
            'meta': {
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'player_id': pid,
                'matches_count': len(matches_list)
            },
            'player': player_obj,
            'aggregates': {
                'career': player_obj.get('career_summary', {}),
                'by_surface': aggregates.get('by_surface', {}),
                'by_year': aggregates.get('by_year', {})
            },
            'matches': sorted(matches_list, key=lambda x: (x.get('event_year') or '', x.get('match_date') or ''), reverse=True)
        }
        outpath = os.path.join(args.outdir, f"{pid}.json")
        with open(outpath, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        index_map[pid] = {
            'file': os.path.relpath(outpath, start=os.path.dirname(args.index_out)),
            'matches_count': len(matches_list),
            'full_name': player_obj.get('full_name',''),
            'country': player_obj.get('country','')
        }
        n_written += 1
        if args.verbose and n_written % 200 == 0:
            print(f"[INFO] written {n_written} players...")

    # save index
    index_payload = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'players_count': n_written,
        'players': index_map
    }
    with open(args.index_out, 'w', encoding='utf-8') as fh:
        json.dump(index_payload, fh, ensure_ascii=False, indent=2)

    print(f"[OK] wrote {n_written} player JSONs to {args.outdir}")
    print(f"[OK] wrote index to {args.index_out}")

if __name__ == '__main__':
    main()
