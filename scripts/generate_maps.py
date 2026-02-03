#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Module 5: generation des cartes (geo aggregates)
Usage:
  python generate_maps.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200

Sorties:
  - dist/players/{PLAYER_ID}.maps.json
"""

import argparse
import os
import json
import glob
import re
from collections import defaultdict, Counter
from datetime import datetime
import pandas as pd

# ----------------- Helpers -----------------

def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid: str) -> str:
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date_only(val):
    if val is None:
        return ''
    try:
        if isinstance(val, str):
            v = val.strip()
            if v == '':
                return ''
            try:
                dt = datetime.fromisoformat(v)
                return dt.date().isoformat()
            except Exception:
                pass
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
            m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
            if m:
                return m.group(1)
            return v
        else:
            dt = pd.to_datetime(val, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
    except Exception:
        return ''
    return ''

def read_matches_from_dir(matches_dir):
    pattern = os.path.join(matches_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {matches_dir}")
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"[generate_maps] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files could be read.")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

# ----------------- Core maps builder -----------------

def build_maps_for_player(matches_df: pd.DataFrame, player_id: str, sample_limit=6):
    """
    Build maps (country aggregates) for one player given full matches_df.
    Returns a dict suitable for JSON dumping.
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # select matches where player participates
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        # nothing found
        return {
            'meta': {'player_id': pid, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'matches': 0},
            'opponent_countries': {},
            'host_countries': {}
        }
    df = pd.concat(frames, ignore_index=True, sort=False)

    # determine canonical player name if available
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            name_candidates.extend([str(x) for x in df[col].dropna().astype(str).tolist()])
    player_name = ''
    if name_candidates:
        # choose most frequent
        player_name = Counter(name_candidates).most_common(1)[0][0]

    # maps
    opp_map = {}  # country -> {wins, losses, matches, sample_matches}
    host_map = {}  # country -> {wins, losses, matches, titles, sample_matches}

    # titles detected if player won and round in W/WIN/F or finals? use common tokens
    title_rounds = set(['W','WIN','F'])

    for idx, r in df.iterrows():
        # determine if player won
        is_win = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_win = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_win = False
        else:
            # fallback by comparing names
            # less reliable; skip if unknown
            is_win = None

        # opponent country detection
        opp_country = ''
        if is_win is True:
            for col in ('country_loser', 'loser_country', 'country_loser_1', 'loser_country_1'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    opp_country = str(r.get(col, '')).strip().upper()
                    break
            if not opp_country:
                # fallback: if loser country missing, perhaps use country columns with mapping
                opp_country = ''
        elif is_win is False:
            for col in ('country_winner', 'winner_country', 'country_winner_1', 'winner_country_1'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    opp_country = str(r.get(col, '')).strip().upper()
                    break

        # host country detection: try HOST_COUNTRY_TO_EVENT_IDS if available via event_id mapping in row
        host_country = ''
        ev = str(r.get('event_id') or '')
        ev_year = str(r.get('event_year') or '')
        # Try to find host country in explicit columns first
        for col in ('host_country','event_country','country_event'):
            if col in r.index and str(r.get(col, '')).strip() != '':
                host_country = str(r.get(col, '')).strip().upper()
                break
        # If not found, try winner/loser country for the match (less reliable)
        if not host_country:
            # choose winner_country if present (this is a best-effort fallback)
            for col in ('country_winner', 'winner_country', 'country_loser', 'loser_country'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    host_country = str(r.get(col, '')).strip().upper()
                    break

        match_entry = {
            'match_id': str(r.get('match_id') or ''),
            'event_id': str(r.get('event_id') or ''),
            'event_year': ev_year,
            'match_date': parse_date_only(r.get('start_date') or r.get('match_date') or ''),
            'opponent_country': opp_country,
            'host_country': host_country,
            'is_win': bool(is_win) if is_win is not None else None,
            'score': str(r.get('score_string') or r.get('score') or '')
        }

        # update opponent map
        if opp_country:
            o = opp_map.get(opp_country, {'wins':0,'losses':0,'matches':0,'sample_matches':[]})
            if is_win is True:
                o['wins'] += 1
            elif is_win is False:
                o['losses'] += 1
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append(match_entry)
            opp_map[opp_country] = o

        # update host map
        if host_country:
            h = host_map.get(host_country, {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]})
            if is_win is True:
                h['wins'] += 1
            elif is_win is False:
                h['losses'] += 1
            h['matches'] += 1
            # titles
            rnd = str(r.get('round') or '')
            if is_win is True and rnd and any(tok in rnd.upper() for tok in title_rounds):
                h['titles'] += 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append(match_entry)
            host_map[host_country] = h

    # compute derived win_rate fields
    for c, o in list(opp_map.items()):
        matches_n = o.get('matches', 0)
        wins_n = o.get('wins', 0)
        o['win_rate'] = (wins_n / matches_n) if matches_n else None
        opp_map[c] = o
    for c, h in list(host_map.items()):
        matches_n = h.get('matches', 0)
        wins_n = h.get('wins', 0)
        h['win_rate'] = (wins_n / matches_n) if matches_n else None
        host_map[c] = h

    result = {
        'meta': {
            'player_id': pid,
            'player_name': player_name,
            'matches': int(len(df)),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'version': 'v1'
        },
        'opponent_countries': opp_map,
        'host_countries': host_map
    }
    return result

# convenience wrapper: read matches and call build_maps_for_player
def build_maps_for_player_from_matches_dir(matches_dir, player_id, sample_limit=6):
    matches = read_matches_from_dir(matches_dir)
    return build_maps_for_player(matches, player_id, sample_limit=sample_limit)

# ----------------- CLI Main -----------------

def main(matches_dir, out_dir, player_list=None, limit_players=None):
    """
    If player_list is provided (list of player ids), process only them; otherwise, discover players from CSV winner/loser columns.
    """
    print("[generate_maps] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[generate_maps] matches rows:", len(matches), "columns:", len(matches.columns))
    # discover players
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    print(f"[generate_maps] discovered {len(player_ids)} player ids")

    if player_list:
        # filter to provided list
        player_ids = [p for p in player_ids if p in set(player_list)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    # ensure out dirs
    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, start=1):
        try:
            print(f"[generate_maps] [{i}/{len(player_ids)}] building maps for {pid} ...")
            maps_obj = build_maps_for_player(matches, pid)
            out_path = os.path.join(players_dir, f"{pid}.maps.json")
            with open(out_path, 'w', encoding='utf8') as f:
                json.dump(maps_obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[generate_maps] ERROR building maps for {pid}: {e}")

    print("[generate_maps] Done. Maps written to", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate per-player maps (opponent/host country aggregates) from matches CSVs.")
    ap.add_argument("--matches-dir", required=True, help="Directory containing matches CSV files")
    ap.add_argument("--out-dir", default="./dist", help="Output directory")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process")
    ap.add_argument("--player", help="Process a single player id (e.g. S0AG)")
    args = ap.parse_args()
    plist = [args.player] if args.player else None
    main(args.matches_dir, args.out_dir, player_list=plist, limit_players=args.limit_players)
