#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_time_series.py - Module 4: build time-series per player

Usage:
  python generate_time_series.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200 --rolling-window 20
"""

import argparse
import os
import glob
import json
from datetime import datetime
import pandas as pd
import numpy as np
import re

# ---------- Helpers ----------
def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid):
    if pid is None:
        return ''
    return str(pid).strip().upper()

def read_matches_from_dir(matches_dir):
    pattern = os.path.join(matches_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"[ts] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files read")
    return pd.concat(frames, ignore_index=True, sort=False)

def parse_date(val):
    try:
        return pd.to_datetime(val, errors='coerce')
    except Exception:
        return pd.NaT

def to_float_safe(s):
    try:
        if s is None: return None
        ss = str(s).strip()
        if ss == '': return None
        ss = ss.replace('%','').replace(',','.')
        ss2 = re.sub(r'[^\d\.\-eE]', '', ss)
        if ss2 == '': return None
        return float(ss2)
    except Exception:
        return None

# ---------- Build TS per player ----------
def build_time_series_for_player(matches_df, player_id, rolling_window=20):
    pid = normalize_player_id(player_id)
    if not pid:
        return None
    # select rows
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {'meta': {'player_id': pid, 'matches': 0, 'generated_at': datetime.utcnow().isoformat()+'Z'}, 'ts': {}}
    df = pd.concat(frames, ignore_index=True, sort=False)

    # build DataFrame with canonical columns: date, is_win, firstserve_pct, tiebreak_win (per match)
    rows = []
    for idx, r in df.iterrows():
        date = parse_date(r.get('start_date') or r.get('match_date') or '')
        if pd.isna(date):
            continue
        is_win = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_win = 1
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_win = 0
        else:
            continue
        fs_pct = None
        # attempt to find first serve percent variants
        for cand in ('firstserve_percent_tot_winner','firstserve_percent_tot_loser','firstserve_percent'):
            if cand in r.index and str(r.get(cand) or '').strip() != '':
                fs_pct = to_float_safe(r.get(cand))
                break
        # tiebreak: detect via explicit columns or setN score
        tb_won = 0; tb_played = 0
        for i in range(1,6):
            tw = r.get(f'tiebreak_set{i}_winner') if f'tiebreak_set{i}_winner' in r.index else None
            tl = r.get(f'tiebreak_set{i}_loser') if f'tiebreak_set{i}_loser' in r.index else None
            if (tw is not None and str(tw).strip()!='') or (tl is not None and str(tl).strip()!=''):
                tb_played += 1
                try:
                    tw_n = int(re.sub(r'[^0-9]', '', str(tw))) if tw and str(tw).strip()!='' else None
                    tl_n = int(re.sub(r'[^0-9]', '', str(tl))) if tl and str(tl).strip()!='' else None
                except Exception:
                    tw_n=tl_n=None
                if tw_n is not None and tl_n is not None:
                    # if player is winner in row, player's tiebreak games are left
                    if is_win == 1:
                        if tw_n > tl_n: tb_won += 1
                    else:
                        if tl_n > tw_n: tb_won += 1
        # fallback parse setN_score parentheses
        if tb_played == 0:
            sstr = r.get('score_string') or r.get('score') or ''
            if isinstance(sstr, str):
                parts = [p.strip() for p in sstr.split(',') if p.strip()]
                for p in parts:
                    if '(' in p and ')' in p:
                        tb_played += 1
                        # naive winner detection not computed here
        rows.append({'date': date, 'is_win': is_win, 'firstserve_pct': fs_pct, 'tb_played': tb_played, 'score': r.get('score_string') or r.get('score') or '', 'match_id': str(r.get('match_id') or '')})

    if not rows:
        return {'meta': {'player_id': pid, 'matches': 0, 'generated_at': datetime.utcnow().isoformat()+'Z'}, 'ts': {}}

    dfp = pd.DataFrame(rows)
    dfp = dfp.sort_values('date').reset_index(drop=True)

    # basic point series: date + is_win
    win_series = [{'date': d.strftime('%Y-%m-%d'), 'value': int(v)} for d,v in zip(dfp['date'], dfp['is_win'])]

    # rolling win rate
    dfp['win_roll_mean'] = dfp['is_win'].rolling(window=rolling_window, min_periods=1).mean()
    win_rate_rolling = [{'date': d.strftime('%Y-%m-%d'), 'value': round(v,4) if not pd.isna(v) else None} for d,v in zip(dfp['date'], dfp['win_roll_mean'])]

    # first serve rolling if available
    ts_firstserve = []
    if 'firstserve_pct' in dfp.columns and dfp['firstserve_pct'].notna().any():
        dfp['fs_roll_mean'] = dfp['firstserve_pct'].rolling(window=rolling_window, min_periods=1).mean()
        ts_firstserve = [{'date': d.strftime('%Y-%m-%d'), 'value': round(v,4) if not pd.isna(v) else None} for d,v in zip(dfp['date'], dfp['fs_roll_mean'])]

    # tiebreak rolling win rate (approx): compute per-match tb result indicator if tb_played>0, use is_win for tb result (approx)
    tb_idx = dfp[dfp['tb_played']>0].copy()
    ts_tiebreak = []
    if not tb_idx.empty:
        tb_idx['tb_result'] = tb_idx['is_win']  # approximation
        tb_idx['tb_roll'] = tb_idx['tb_result'].rolling(window=rolling_window, min_periods=1).mean()
        ts_tiebreak = [{'date': d.strftime('%Y-%m-%d'), 'value': round(v,4) if not pd.isna(v) else None} for d,v in zip(tb_idx['date'], tb_idx['tb_roll'])]

    result = {
        'meta': {'player_id': pid, 'matches': len(dfp), 'generated_at': datetime.utcnow().isoformat()+'Z', 'rolling_window': rolling_window},
        'ts': {
            'win_point_series': win_series,
            'win_rate_rolling': win_rate_rolling,
            'firstserve_pct_rolling': ts_firstserve,
            'tiebreak_win_rate_rolling': ts_tiebreak,
            # include compact raw per-match series for charts if needed
            'matches': [{'date': d.strftime('%Y-%m-%d'), 'match_id': m, 'score': s, 'is_win': int(w)} for d,m,s,w in zip(dfp['date'], dfp['match_id'], dfp['score'], dfp['is_win'])]
        }
    }
    return result

# ---------- CLI ----------
def main(matches_dir, out_dir, rolling_window=20, player_list=None, limit_players=None):
    print("[ts] loading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
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
        print(f"[ts] [{i}/{len(player_ids)}] building ts for {pid}")
        obj = build_time_series_for_player(matches, pid, rolling_window=rolling_window)
        out_path = os.path.join(players_dir, f"{pid}.ts.json")
        with open(out_path, 'w', encoding='utf8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    print("[ts] done.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate time series per player")
    ap.add_argument("--matches-dir", required=True)
    ap.add_argument("--out-dir", default="./dist")
    ap.add_argument("--rolling-window", type=int, default=20)
    ap.add_argument("--limit-players", type=int, default=None)
    ap.add_argument("--player", default=None)
    args = ap.parse_args()
    plist = [args.player] if args.player else None
    main(args.matches_dir, args.out_dir, rolling_window=args.rolling_window, player_list=plist, limit_players=args.limit_players)
