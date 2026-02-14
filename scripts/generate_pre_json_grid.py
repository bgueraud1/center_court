#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_aggregates_reduced.py

Sortie réduite (IDs uniquement) pour ATP et WTA, selon la spec :
 - tournaments : for each event (filtered by level), lists of winners/finalists/semifinalists/quarterfinalists
   each entry: player_id and defeated_opponents (list of opponent_ids when available, else names)
 - countries -> list of player_ids
 - ranks -> lists for 1,2,3,5,10,50 (player_ids)
 - heights -> lists of player_ids shorter/taller than thresholds
 - born_cities -> only cities with > MIN_CITY_PLAYERS players (default 10)
 - lefties -> list of player_ids
 - one_handed_backhand -> list of player_ids
 - positive_h2h_vs_top10 -> list of records {player_id, top10_id, wins, losses}

Writes:
  docs/games/aggregate_ATP_reduced.json
  docs/games/aggregate_WTA_reduced.json
"""
import csv, json, re, traceback
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path('.')
ATP_CSV = ROOT / 'player_data_atp.csv'
WTA_CSV = ROOT / 'player_data_wta.csv'

ATP_MATCHES_DIR_CANDIDATES = [
    ROOT / 'matches' / 'atp_matches',
    ROOT / 'matches',
    ROOT / 'matches_atp',
    ROOT / 'atp_matches'
]
WTA_MATCHES_DIR_CANDIDATES = [
    ROOT / 'matches' / 'wta_matches',
    ROOT / 'matches',
    ROOT / 'matches_wta',
    ROOT / 'wta_matches'
]

OUT_DIR = ROOT / 'docs' / 'games'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# thresholds
CITY_MIN_PLAYERS = 10
HEIGHT_THRESHOLDS = {'ATP': {'smaller':1.70, 'taller':2.00}, 'WTA': {'smaller':1.65, 'taller':1.83}}

# acceptable event level keywords for inclusion
EVENT_KEYWORDS = ('grand', 'slam', 'major', '1000', 'pm', 'premier', 'mandatory', 'masters', '500')

# round -> coarse score mapping
ROUND_SCORE = {
    'R128':1,'R64':2,'R32':3,'R16':4,
    'R32':3,'R64':2,'R128':1,
    'R64Q':2,'R32Q':3,
    'QF':5,'SF':6,'F':7,'FINAL':7,'W':8,'W/O':0,
    'R':0,'RR':0,'BR':0
}
ALLOWED_RESULT_KINDS = {'QF', 'SF', 'F', 'FINAL', 'Q', 'S'}  # will be normalized

def round_to_score(r):
    if not r: return 0
    r2 = str(r).upper().strip()
    if r2 in ROUND_SCORE:
        return ROUND_SCORE[r2]
    if r2 == 'Q': return 5
    if r2 == 'S': return 6
    if r2 == 'F': return 7
    if 'QUART' in r2: return 5
    if 'SEMI' in r2: return 6
    if 'FINAL' in r2: return 7
    return 0

def find_matches_dir(candidates):
    for p in candidates:
        if p.exists() and p.is_dir():
            return p
    fallback = ROOT / 'matches'
    if fallback.exists() and fallback.is_dir():
        return fallback
    return None

def load_players(csv_path, circuit):
    players = []
    if not csv_path.exists():
        print(f"[WARN] players CSV missing: {csv_path}")
        return players
    with csv_path.open(encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            pid = (row.get('player_id') or row.get('player') or '').strip()
            if not pid:
                continue
            fullname = (row.get('full_name') or row.get('full name') or row.get('name') or '').strip()
            country = (row.get('represented_country') or row.get('represented country') or row.get('country') or '').strip().upper() or None
            height_raw = (row.get('height_cm') or row.get('height') or '').strip()
            height_m = None
            if height_raw:
                m = re.search(r'([\d.,]+)\s*m', height_raw)
                if m:
                    try:
                        height_m = float(m.group(1).replace(',','.'))
                    except:
                        height_m = None
                else:
                    m2 = re.search(r'(\d{3})', height_raw)
                    if m2:
                        try:
                            height_m = int(m2.group(1)) / 100.0
                        except:
                            height_m = None
            plays = (row.get('plays') or row.get('hand') or '').strip() or None
            backhand = (row.get('backhand') or '').strip() or None
            birthplace = (row.get('birthplace') or '').strip() or None
            rank_raw = row.get('highest_ranking') or row.get('best_rank') or row.get('best rank') or ''
            best_rank = None
            try:
                if rank_raw not in (None,''):
                    best_rank = int(float(str(rank_raw).strip()))
            except:
                best_rank = None
            players.append({
                'player_id': pid,
                'full_name': fullname or None,
                'country': country,
                'height_m': height_m,
                'plays': plays,
                'backhand': backhand,
                'birthplace': birthplace,
                'best_rank': best_rank,
                'circuit': circuit
            })
    return players

def load_events(matches_dir):
    """
    Read all CSV matches (non recursive) and produce event_index:
      event_index[event_key] = {tourney_name, year, level, matches:[{round,winner_id,loser_id,winner_name,loser_name}], players: {pid: best_round_score, winner:bool}}
    """
    event_index = {}
    if not matches_dir or not matches_dir.exists():
        return event_index
    for f in sorted(matches_dir.glob('*.csv')):
        try:
            with f.open(encoding='utf-8', errors='ignore') as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)
                if not rows: continue
                first = rows[0]
                eid = first.get('event_id') or first.get('event') or ''
                year = first.get('event_year') or first.get('eventyear') or ''
                tn = first.get('tourney_name') or first.get('tourney') or f.stem
                level = (first.get('level') or first.get('category') or '').strip()
                event_key = f"{eid}__{year}" if eid else f"{f.stem}"
                if event_key not in event_index:
                    event_index[event_key] = {'tourney_name': tn, 'year': year, 'level': level, 'matches': [], 'players': defaultdict(lambda: {'best_round':0,'winner':False})}
                for r in rows:
                    rr = (r.get('round') or r.get('round_name') or '').strip()
                    sc = round_to_score(rr)
                    winner_id = (r.get('player_id_winner') or r.get('winner_id') or r.get('winner') or '').strip()
                    loser_id = (r.get('player_id_loser') or r.get('loser_id') or r.get('loser') or '').strip()
                    winner_name = (r.get('winner_player_name') or r.get('winner_name') or '').strip()
                    loser_name = (r.get('loser_player_name') or r.get('loser_name') or '').strip()
                    evm = {'round': rr, 'round_score': sc, 'winner_id': winner_id or None, 'loser_id': loser_id or None, 'winner_name': winner_name or None, 'loser_name': loser_name or None}
                    event_index[event_key]['matches'].append(evm)
                    if winner_id:
                        event_index[event_key]['players'][winner_id]['best_round'] = max(event_index[event_key]['players'][winner_id]['best_round'], sc)
                    if loser_id:
                        event_index[event_key]['players'][loser_id]['best_round'] = max(event_index[event_key]['players'][loser_id]['best_round'], sc)
                    if rr and rr.upper() in ('F','FINAL') and winner_id:
                        event_index[event_key]['players'][winner_id]['winner'] = True
        except Exception as e:
            print(f"[WARN] could not parse {f}: {e}")
    return event_index

def event_allowed(ev):
    text = ((ev.get('tourney_name') or '') + ' ' + (ev.get('level') or '')).lower()
    for kw in EVENT_KEYWORDS:
        if kw in text:
            return True
    return False

def build_h2h(event_index):
    """
    Build H2H mapping: h2h[a][b] = {'wins':n, 'losses':m}
    'wins' = number of matches a beat b, 'losses' = number of matches a lost to b
    """
    h2h_counts = defaultdict(lambda: defaultdict(lambda: {'wins':0,'losses':0}))
    for ek, ev in event_index.items():
        for m in ev['matches']:
            a = m.get('winner_id'); b = m.get('loser_id')
            if a and b:
                h2h_counts[a][b]['wins'] += 1
                h2h_counts[b][a]['losses'] += 1
    # normalize losses for same pair: if two entries exist, ensure consistent view
    # produce final mapping: player -> opponent -> {'wins':w,'losses':l}
    final = {}
    for a, opps in h2h_counts.items():
        final[a] = {}
        for b, rec in opps.items():
            w = rec.get('wins',0)
            l = h2h_counts.get(a,{}).get(b,{}).get('losses',0)
            # more reliable: recompute losses from reverse record if present
            l_rev = h2h_counts.get(b,{}).get(a,{}).get('wins',0)
            # choose wins = w, losses = l_rev
            final[a][b] = {'wins': w, 'losses': l_rev}
    return final

def aggregate_for_circuit(circuit_label, players_csv, matches_dir_candidates, out_filename):
    players = load_players(players_csv, circuit_label)
    players_index = {p['player_id']: p for p in players}
    matches_dir = find_matches_dir(matches_dir_candidates)
    event_index = load_events(matches_dir) if matches_dir else {}

    # 1) tournaments filtered
    tournaments = {}
    for ek, ev in event_index.items():
        if not event_allowed(ev):
            continue
        # only keep players who are QF / SF / F / winners
        winners = []
        finalists = []
        semifinalists = []
        quarterfinalists = []
        # Build defeated lists per player within this event
        # Map player_id -> list(opponent_id or opponent_name)
        defeated_map = defaultdict(list)
        for m in ev['matches']:
            a = m.get('winner_id'); b = m.get('loser_id')
            # if id missing, try names
            if a:
                defeated_map[a].append(b or m.get('loser_name'))
            elif m.get('winner_name'):
                defeated_map[m.get('winner_name')].append(m.get('loser_name') or b)
        # Now look at ev['players']
        for pid, info in ev['players'].items():
            br = info.get('best_round', 0)
            if br >= 8 or info.get('winner'):
                winners.append({'player_id': pid, 'defeated': [x for x in defeated_map.get(pid, [])]})
            elif br >= 7:
                finalists.append({'player_id': pid, 'defeated': [x for x in defeated_map.get(pid, [])]})
            elif br >= 6:
                semifinalists.append({'player_id': pid, 'defeated': [x for x in defeated_map.get(pid, [])]})
            elif br >= 5:
                quarterfinalists.append({'player_id': pid, 'defeated': [x for x in defeated_map.get(pid, [])]})
        # Only include event if at least one of these lists non-empty
        if winners or finalists or semifinalists or quarterfinalists:
            tournaments[ek] = {
                'tourney_name': ev.get('tourney_name'),
                'year': ev.get('year'),
                'level': ev.get('level'),
                'winners': winners,
                'finalists': finalists,
                'semifinalists': semifinalists,
                'quarterfinalists': quarterfinalists
            }

    # 2) countries -> player ids
    countries = defaultdict(list)
    for pid,p in players_index.items():
        c = p.get('country')
        if c:
            countries[c].append(pid)

    # 3) ranks lists
    ranks = {'top_1':[], 'top_2':[], 'top_3':[], 'top_5':[], 'top_10':[], 'top_50':[]}
    for pid,p in players_index.items():
        br = p.get('best_rank')
        if br is None: continue
        if br <= 1: ranks['top_1'].append(pid)
        if br <= 2: ranks['top_2'].append(pid)
        if br <= 3: ranks['top_3'].append(pid)
        if br <= 5: ranks['top_5'].append(pid)
        if br <= 10: ranks['top_10'].append(pid)
        if br <= 50: ranks['top_50'].append(pid)

    # 4) heights
    th = HEIGHT_THRESHOLDS.get(circuit_label, HEIGHT_THRESHOLDS['ATP'])
    smaller = []
    taller = []
    for pid,p in players_index.items():
        h = p.get('height_m')
        if h is None: continue
        if h < th['smaller']:
            smaller.append(pid)
        if h > th['taller']:
            taller.append(pid)

    # 5) born cities with > CITY_MIN_PLAYERS players
    city_map = defaultdict(list)
    for pid,p in players_index.items():
        bp = p.get('birthplace') or ''
        city = bp.split(',')[0].strip() if bp else ''
        if city:
            city_map[city].append(pid)
    born_cities = {city: ids for city, ids in city_map.items() if len(ids) > CITY_MIN_PLAYERS}

    # 6) lefties
    lefties = [pid for pid,p in players_index.items() if p.get('plays') and 'left' in p.get('plays').lower()]

    # 7) one-handed backhands
    one_handed = [pid for pid,p in players_index.items() if p.get('backhand') and ('one' in p.get('backhand').lower() or 'une' in p.get('backhand').lower())]

    # 8) positive H2H vs top10
    h2h = build_h2h(event_index)
    top10 = set([pid for pid,p in players_index.items() if p.get('best_rank') is not None and p['best_rank'] <= 10])
    positive_h2h = []
    # For each player a, check each top10 b: if a has record vs b and wins > losses
    for a, opponents in h2h.items():
        for b, rec in opponents.items():
            if b in top10:
                wins = rec.get('wins',0)
                losses = rec.get('losses',0)
                if (wins + losses) >= 1 and wins > losses:
                    positive_h2h.append({'player_id': a, 'top10_id': b, 'wins': wins, 'losses': losses})

    aggregate = {
        'circuit': circuit_label,
        'tournaments': tournaments,
        'countries': dict(countries),
        'ranks': ranks,
        'height': {'smaller_than': smaller, 'taller_than': taller, 'thresholds': th},
        'born_cities': born_cities,
        'lefties': lefties,
        'one_handed_backhand': one_handed,
        'positive_h2h_vs_top10': positive_h2h
    }

    with out_filename.open('w', encoding='utf-8') as out:
        json.dump(aggregate, out, ensure_ascii=False, indent=2)

    print(f"Wrote {out_filename} (tournaments: {len(tournaments)}, players: {len(players_index)})")

def main():
    try:
        aggregate_for_circuit('ATP', ATP_CSV, ATP_MATCHES_DIR_CANDIDATES, OUT_DIR / 'aggregate_ATP_reduced.json')
    except Exception as e:
        print("Exception ATP:", e)
        traceback.print_exc()
    try:
        aggregate_for_circuit('WTA', WTA_CSV, WTA_MATCHES_DIR_CANDIDATES, OUT_DIR / 'aggregate_WTA_reduced.json')
    except Exception as e:
        print("Exception WTA:", e)
        traceback.print_exc()

if __name__ == '__main__':
    main()
