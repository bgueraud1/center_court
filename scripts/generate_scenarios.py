#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scenarios.py - Module 3: generate scenario datasets per player

Usage:
  python generate_scenarios.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200
"""

import argparse
import os
import glob
import json
import re
from collections import defaultdict, Counter
from datetime import datetime
import pandas as pd

# ---------- Helpers ----------
def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid):
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date(val):
    try:
        return pd.to_datetime(val, errors='coerce').date().isoformat()
    except Exception:
        return ''

def read_matches_from_dir(matches_dir):
    pattern = os.path.join(matches_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"[scenarios] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files read")
    return pd.concat(frames, ignore_index=True, sort=False)

# ---------- Core per-match helpers ----------
def parse_set_scores(row):
    """Return list of (left,right,raw) for set1..set5 present in row."""
    arr = []
    for i in range(1,6):
        k = f"set{i}_score"
        if k in row.index:
            v = row.get(k) or ''
            if isinstance(v, str) and '-' in v:
                left, right = v.split('-', 1)
                arr.append((left.strip(), right.strip(), v))
    return arr

def player_won_row(row, player_id):
    """True if row indicates player_id is winner, False if loser, None if unknown."""
    pid = normalize_player_id(player_id)
    if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
        return True
    if 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
        return False
    # fallback to names (best-effort) - not used here
    return None

# ---------- Scenario builders ----------
def build_scenarios_for_player(matches_df, player_id, sample_limit=6):
    pid = normalize_player_id(player_id)
    if not pid:
        return None
    # select player's matches
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {'meta': {'player_id': pid, 'matches': 0, 'generated_at': datetime.utcnow().isoformat()+'Z'}, 'scenarios': {}}
    df = pd.concat(frames, ignore_index=True, sort=False)
    scenarios = defaultdict(list)

    # iterate
    for idx, r in df.iterrows():
        is_win = player_won_row(r, pid)
        sets = parse_set_scores(r)
        # convert to numeric list (winner-side left)
        sets_num = []
        for left,right,raw in sets:
            try:
                a = int(re.sub(r'[^0-9]', '', left)) if left else None
                b = int(re.sub(r'[^0-9]', '', right)) if right else None
            except Exception:
                a=b=None
            if a is not None and b is not None:
                sets_num.append((a,b,raw))
        # scenario: won after losing first set
        if len(sets_num) >= 2 and is_win is True:
            a1,b1,_ = sets_num[0]
            # player is winner in row: CSV left is player's games for that set
            if a1 < b1:
                scenarios['won_after_losing_first_set'].append({
                    'match_id': str(r.get('match_id') or ''),
                    'event_id': str(r.get('event_id') or ''),
                    'match_date': parse_date(r.get('start_date') or r.get('match_date')),
                    'score': r.get('score_string') or r.get('score') or ''
                })
        # scenario: won after winning first two sets
        if len(sets_num) >= 2 and is_win is True:
            a1,b1,_ = sets_num[0]; a2,b2,_ = sets_num[1]
            if a1 > b1 and a2 > b2:
                scenarios['won_after_winning_first_two'].append({
                    'match_id': str(r.get('match_id') or ''),
                    'event_id': str(r.get('event_id') or ''),
                    'match_date': parse_date(r.get('start_date') or r.get('match_date')),
                    'score': r.get('score_string') or r.get('score') or ''
                })
        # comeback from 0-2
        if len(sets_num) >= 3 and is_win is True:
            a1,b1,_ = sets_num[0]; a2,b2,_ = sets_num[1]
            if a1 < b1 and a2 < b2:
                scenarios['comeback_from_0_2'].append({
                    'match_id': str(r.get('match_id') or ''),
                    'event_id': str(r.get('event_id') or ''),
                    'match_date': parse_date(r.get('start_date') or r.get('match_date')),
                    'score': r.get('score_string') or r.get('score') or ''
                })
        # tie-breaks clutch: identify tiebreak in deciding set (set3 for best-of-3, set5 for best-of-5)
        # heuristic: if match has parentheses in final set
        if isinstance(r.get('score_string') or '', str):
            s = r.get('score_string') or ''
            # final set tiebreak detection
            # if match ended in 3 sets and '(...)' in last set
            parts = [p.strip() for p in s.split(',') if p.strip()]
            if len(parts) >= 1:
                last = parts[-1]
                if '(' in last and ')' in last:
                    scenarios['tiebreaks_clutch'].append({
                        'match_id': str(r.get('match_id') or ''),
                        'event_id': str(r.get('event_id') or ''),
                        'match_date': parse_date(r.get('start_date') or r.get('match_date')),
                        'score': s,
                        'is_win': bool(is_win) if is_win is not None else None
                    })
        # breakpoint_comebacks: if row has breakpointssaved/divisor fields and player saved many
        # try to detect player side
        side = 'winner' if is_win else 'loser'
        bps_div = None
        bps_saved = None
        for c in (f'breakpointssaved_divisor_{side}', f'breakpointssaved_divisor_tot_{side}', 'breakpointssaved_divisor'):
            if c in r.index and str(r.get(c) or '').strip() != '':
                try:
                    bps_div = float(re.sub(r'[^\d\.]', '', str(r.get(c))))
                    break
                except Exception:
                    pass
        for c in (f'breakpointssaved_dividend_{side}', f'breakpointssaved_dividend_tot_{side}', 'breakpointssaved_dividend'):
            if c in r.index and str(r.get(c) or '').strip() != '':
                try:
                    bps_saved = float(re.sub(r'[^\d\.]', '', str(r.get(c))))
                    break
                except Exception:
                    pass
        if bps_div is not None and bps_saved is not None:
            saved = bps_saved
            faced = bps_div
            saved_pct = (saved/faced) if faced>0 else None
            if saved_pct is not None and saved_pct >= 0.8 and is_win:
                scenarios['breakpoint_comebacks'].append({
                    'match_id': str(r.get('match_id') or ''),
                    'event_id': str(r.get('event_id') or ''),
                    'match_date': parse_date(r.get('start_date') or r.get('match_date')),
                    'score': r.get('score_string') or r.get('score') or '',
                    'saved': saved,
                    'faced': faced,
                    'saved_pct': saved_pct
                })

    # trim samples to sample_limit
    out = {}
    for k, arr in scenarios.items():
        out[k] = {'count': len(arr), 'sample': arr[:sample_limit], 'all': arr}  # keep 'all' (could be big)
    return {'meta': {'player_id': pid, 'player_name': '', 'generated_at': datetime.utcnow().isoformat()+'Z', 'version':'v1', 'matches': len(df)}, 'scenarios': out}

# ---------- CLI ----------
def main(matches_dir, out_dir, player_list=None, limit_players=None):
    print("[scenarios] loading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[scenarios] rows:", len(matches))
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    if player_list:
        player_ids = [p for p in player_ids if p in set(player_list)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]
    players_dir = os.path.join(out_dir, "players")
    safe_mkdir(players_dir)
    for i, pid in enumerate(player_ids, start=1):
        print(f"[scenarios] [{i}/{len(player_ids)}] building scenarios for {pid}")
        obj = build_scenarios_for_player(matches, pid)
        out_path = os.path.join(players_dir, f"{pid}.scenarios.json")
        with open(out_path, 'w', encoding='utf8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    print("[scenarios] done. files in", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate scenarios")
    ap.add_argument("--matches-dir", required=True)
    ap.add_argument("--out-dir", default="./dist")
    ap.add_argument("--limit-players", type=int, default=None)
    ap.add_argument("--player", default=None)
    args = ap.parse_args()
    plist = [args.player] if args.player else None
    main(args.matches_dir, args.out_dir, player_list=plist, limit_players=args.limit_players)
