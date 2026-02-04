#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_detailed_stats.py - Detailed statistics per player (full version)

Usage:
  python generate_detailed_stats.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200 --player S0AG

Outputs:
  - {out_dir}/players_atp/{PLAYER_ID}.stats.json
"""

import argparse
import os
import glob
import json
import re
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import pandas as pd
import math

# ---------------- Helpers ----------------

def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

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
            print(f"[dstats] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files could be read.")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

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

def to_float_safe(v):
    try:
        if v is None:
            return None
        if v == '':
            return None
        s = str(v).strip()
        if s == '':
            return None
        s = s.replace(',', '.')
        if s.endswith('%'):
            s = s[:-1]
        s2 = re.sub(r'[^\d\.\-eE]', '', s)
        if s2 == '' or s2 == '.' or s2 == '-':
            return None
        val = float(s2)
        if math.isfinite(val):
            return val
        return None
    except Exception:
        return None

def safe_int(v):
    try:
        if v is None: return None
        if v == '': return None
        i = int(float(str(v)))
        return i
    except Exception:
        return None

def parse_time_to_seconds(s):
    if s is None:
        return None
    try:
        st = str(s).strip()
        if st == '':
            return None
        # pattern hh:mm:ss or mm:ss
        parts = st.split(':')
        if len(parts) == 3:
            h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
            return h*3600 + m*60 + sec
        if len(parts) == 2:
            m = int(parts[0]); sec = int(parts[1])
            return m*60 + sec
        # maybe numeric seconds
        val = to_float_safe(st)
        if val is not None:
            return float(val)
    except Exception:
        pass
    return None

def round_smart(v):
    """Round following rule: max 2 decimals; if effectively integer -> int"""
    if v is None:
        return None
    try:
        if isinstance(v, (int,)):
            return v
        f = float(v)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return round(f, 2)
    except Exception:
        return v

# ---------------- Aggregation utilities ----------------

def add_numeric_agg(agg_dict, key, value):
    """
    Maintains agg_dict[key] = {'count':int, 'sum':float, 'min':float, 'max':float}
    If value is None -> do nothing.
    """
    if value is None:
        return
    try:
        v = float(value)
    except Exception:
        return
    if key not in agg_dict:
        agg_dict[key] = {'count': 0, 'sum': 0.0, 'min': v, 'max': v}
    entry = agg_dict[key]
    entry['count'] += 1
    entry['sum'] += v
    if entry['min'] is None or v < entry['min']:
        entry['min'] = v
    if entry['max'] is None or v > entry['max']:
        entry['max'] = v
    agg_dict[key] = entry

def finalize_numeric_agg(agg_dict, is_rate=False):
    """
    Transform internal agg_dict to final form with mean.
    For rates (is_rate True) we omit sum (user requested: no Sum for rates).
    Returns mapping: key -> {count, mean, sum (unless rate), min, max}
    Values rounded smartly.
    """
    out = {}
    for k, e in agg_dict.items():
        cnt = int(e.get('count', 0)) if e and 'count' in e else 0
        s = float(e.get('sum', 0.0)) if e and 'sum' in e else 0.0
        mn = e.get('min', None)
        mx = e.get('max', None)
        mean = (s / cnt) if cnt > 0 else None
        obj = {'count': cnt, 'mean': round_smart(mean) if mean is not None else None,
               'min': round_smart(mn) if mn is not None else None,
               'max': round_smart(mx) if mx is not None else None}
        if not is_rate:
            obj['sum'] = round_smart(s) if cnt>0 else None
        out[k] = obj
    return out

# ---------------- Extractor: per-match metrics ----------------

def first_available(row, candidates):
    for c in candidates:
        if c in row.index:
            v = row.get(c)
            if v is not None and str(v).strip() != '':
                return v
    return None

def extract_player_match_metrics(r, player_id):
    """
    Given a pandas Series row r and player's normalized player_id, return computed metrics dict.
    """
    pid = normalize_player_id(player_id)
    m = {}

    # determine winner
    is_winner = None
    if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
        is_winner = True
    elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
        is_winner = False

    # which side is "player" in column suffix
    side = 'winner' if is_winner else 'loser' if is_winner is False else None

    # --- aces & double faults ---
    aces = None
    dfaults = None
    # try common candidates
    for cand in (f'aces_tot_{side}', f'aces_tot_{side}', f'aces_tot_{side}', f'aces_tot_{side}', f'aces_tot_{side}', f'aces_tot_{side}'):
        pass
    # broader approach: try many candidate names
    ac_cands = []
    df_cands = []
    if side:
        ac_cands += [f'aces_tot_{side}', f'aces_{side}', f'aces_tot_{side}'.replace('__','_')]
        df_cands += [f'doublefaults_tot_{side}', f'doublefaults_{side}']
    ac_cands += ['aces', 'aces_tot_winner', 'aces_tot_loser', 'aces_tot']
    df_cands += ['doublefaults', 'doublefaults_tot_winner', 'doublefaults_tot_loser', 'doublefaults_tot']
    for c in ac_cands:
        if c in r.index and str(r.get(c, '')).strip() != '':
            aces = to_float_safe(r.get(c))
            break
    for c in df_cands:
        if c in r.index and str(r.get(c, '')).strip() != '':
            dfaults = to_float_safe(r.get(c))
            break
    m['aces'] = aces
    m['doublefaults'] = dfaults

    # --- service points played (try many likely columns) ---
    serv_points = None
    serv_cands = []
    if side:
        serv_cands += [
            f'totalservicepointswon_divisor_tot_{side}', f'totalservicepointswon_divisor_{side}',
            f'totalservicepointswon_divisor_tot_{side}'.replace('__','_'),
            f'firstserve_divisor_tot_{side}', f'servicegamesplayed_tot_{side}'
        ]
    serv_cands += ['totalservicepointswon_divisor', 'totalservicepointswon_divisor_tot',
                   'servicegamesplayed', 'servicegamesplayed_tot', 'servicegamesplayed_tot_winner', 'servicegamesplayed_tot_loser']
    # pick first suitable
    for c in serv_cands:
        if c in r.index and str(r.get(c, '')).strip() != '':
            serv_points = to_float_safe(r.get(c))
            if serv_points is not None:
                break
    # fallback: estimate from service games * 4 points per game (very rough)
    if serv_points is None:
        sg = first_available(r, ['servicegamesplayed_tot_'+side if side else '', 'servicegamesplayed', 'servicegamesplayed_tot_winner', 'servicegamesplayed_tot_loser'])
        sg_n = to_float_safe(sg)
        if sg_n is not None:
            serv_points = sg_n * 4.0

    m['service_points_played'] = serv_points

    # aces per service point & double fault rate
    if m['aces'] is not None and m['service_points_played']:
        m['aces_per_service_point'] = (float(m['aces']) / float(m['service_points_played'])) if m['service_points_played']>0 else None
    else:
        m['aces_per_service_point'] = None

    if m['doublefaults'] is not None and m['service_points_played']:
        m['doublefaults_per_service_point'] = (float(m['doublefaults']) / float(m['service_points_played'])) if m['service_points_played']>0 else None
    else:
        m['doublefaults_per_service_point'] = None

    # --- serve/return percent fields (look for winner/loser variants) ---
    def find_pct(base_names):
        # attempt winner/loser variants first
        names = []
        if side:
            for b in base_names:
                names += [f"{b}_tot_{side}", f"{b}_{side}", f"{b}_tot_{side}".replace('__','_')]
        names += base_names + [b + '_tot' for b in base_names]
        for n in names:
            if n in r.index and str(r.get(n, '')).strip() != '':
                v = to_float_safe(r.get(n))
                if v is not None:
                    return v
        return None

    m['firstserve_percent'] = find_pct(['firstserve_percent'])
    m['firstserve_points_won_percent'] = find_pct(['firstservepointswon_percent'])
    m['secondserve_points_won_percent'] = find_pct(['secondservepointswon_percent'])
    m['service_points_won_percent'] = find_pct(['totalservicepointswon_percent','totalservicepointswon_percent'])
    m['return_points_won_percent'] = find_pct(['totalreturnpointswon_percent','totalreturnpointswon_percent'])
    # normalize percents if >1 assume given in percent (e.g., 65)
    for k in ('firstserve_percent','firstserve_points_won_percent','secondserve_points_won_percent','service_points_won_percent','return_points_won_percent'):
        if m.get(k) is not None:
            v = m[k]
            if v is not None and v > 1.0:
                m[k] = float(v) / 100.0

    # --- breakpoints faced/converted: opponent's breakpointssaved_divisor/dividend ---
    # opponent side
    opp_side = 'loser' if is_winner else 'winner' if is_winner is not None else None
    opp_bps_div = None
    opp_bps_dvd = None
    if opp_side:
        div_cands = [f'breakpointssaved_divisor_tot_{opp_side}', f'breakpointssaved_divisor_{opp_side}', 'breakpointssaved_divisor_tot', 'breakpointssaved_divisor']
        dvd_cands = [f'breakpointssaved_dividend_tot_{opp_side}', f'breakpointssaved_dividend_{opp_side}', 'breakpointssaved_dividend_tot', 'breakpointssaved_dividend']
    else:
        div_cands = ['breakpointssaved_divisor_tot','breakpointssaved_divisor']
        dvd_cands = ['breakpointssaved_dividend_tot','breakpointssaved_dividend']
    for c in div_cands:
        if c in r.index and str(r.get(c,'')).strip() != '':
            opp_bps_div = to_float_safe(r.get(c))
            if opp_bps_div is not None: break
    for c in dvd_cands:
        if c in r.index and str(r.get(c,'')).strip() != '':
            opp_bps_dvd = to_float_safe(r.get(c))
            if opp_bps_dvd is not None: break
    m['breakpoints_faced'] = opp_bps_div
    if opp_bps_div is not None and opp_bps_dvd is not None:
        conv = max(0.0, opp_bps_div - opp_bps_dvd)
        m['breakpoints_converted'] = conv
        m['breakpoints_converted_rate'] = (conv / opp_bps_div) if opp_bps_div>0 else None
    else:
        # try explicit fields
        explicit = first_available(r, ['breakpoints_converted', f'breakpoints_converted_{opp_side}' if opp_side else 'breakpoints_converted'])
        conv_fallback = to_float_safe(explicit) if explicit is not None else None
        m['breakpoints_converted'] = conv_fallback
        if conv_fallback is not None and opp_bps_div:
            m['breakpoints_converted_rate'] = (conv_fallback / opp_bps_div) if opp_bps_div and opp_bps_div>0 else None
        else:
            m['breakpoints_converted_rate'] = None

    # --- service games lost rate (breaks conceded by player) ---
    # According to instructions: service_games_lost_rate = (break_points converted by opponent) / servicegamesplayed_tot_{player}
    # break_points converted by opponent is the player's breakpoints_converted when opponent is receiver, but simpler: use breakpoints_converted from m (which measured opponent converting on player's serve)
    player_service_games_played = None
    sg_cands = []
    if side:
        sg_cands += [f'servicegamesplayed_tot_{side}', f'servicegamesplayed_{side}']
    sg_cands += ['servicegamesplayed', 'servicegamesplayed_tot_winner', 'servicegamesplayed_tot_loser', 'servicegamesplayed_tot']
    for c in sg_cands:
        if c in r.index and str(r.get(c,'')).strip() != '':
            player_service_games_played = to_float_safe(r.get(c))
            if player_service_games_played is not None: break
    # breaks conceded by player = opponent converted on player's serve => that is m['breakpoints_converted'] (we set it from opponent stats)
    if m.get('breakpoints_converted') is not None and player_service_games_played and player_service_games_played>0:
        try:
            m['service_games_lost_rate'] = float(m.get('breakpoints_converted')) / float(player_service_games_played)
        except Exception:
            m['service_games_lost_rate'] = None
    else:
        m['service_games_lost_rate'] = None

    # --- tiebreaks played/won via explicit columns ---
    tb_played = 0
    tb_won = 0
    for i in range(1, 6):
        tw_col = f'tiebreak_set{i}_winner'
        tl_col = f'tiebreak_set{i}_loser'
        tw = r.get(tw_col) if tw_col in r.index else None
        tl = r.get(tl_col) if tl_col in r.index else None
        if (tw is not None and str(tw).strip() != '') or (tl is not None and str(tl).strip() != ''):
            tb_played += 1
            try:
                tw_n = int(re.sub(r'[^0-9]', '', str(tw))) if (tw is not None and str(tw).strip() != '') else None
                tl_n = int(re.sub(r'[^0-9]', '', str(tl))) if (tl is not None and str(tl).strip() != '') else None
            except Exception:
                tw_n = tl_n = None
            if tw_n is not None and tl_n is not None:
                if is_winner:
                    if tw_n > tl_n: tb_won += 1
                else:
                    if tl_n > tw_n: tb_won += 1
            else:
                # if only one side present guess
                if is_winner and tw and not tl:
                    tb_won += 1
                if (not is_winner) and tl and not tw:
                    tb_won += 1
    m['tiebreaks_played'] = int(tb_played)
    m['tiebreaks_won'] = int(tb_won)
    m['tiebreak_win_rate'] = (float(tb_won)/tb_played) if tb_played>0 else None

    # --- match time ---
    mtsec = None
    for c in ('match_time_total', 'match_time', 'match_time_seconds', 'match_time_total'):
        if c in r.index and str(r.get(c,'')).strip() != '':
            mtsec = parse_time_to_seconds(r.get(c))
            if mtsec is not None: break
    # fallback to settime columns (some CSVs)
    if mtsec is None:
        if is_winner:
            key = 'settime_tot_winner'
        elif is_winner is False:
            key = 'settime_tot_loser'
        else:
            key = None
        if key and key in r.index and str(r.get(key,'')).strip() != '':
            mtsec = to_float_safe(r.get(key))
    m['match_time_seconds'] = float(mtsec) if mtsec is not None else None
    m['match_time_hours'] = (float(mtsec)/3600.0) if mtsec is not None else None

    # --- ranking/seeding extraction for upset/bucket calculations ---
    # possible columns: winner_seed, loser_seed, seed_winner, seed_loser, winner_seed, loser_seed, seed_winner, seed_loser
    def extract_seed(side_opt):
        candidates = []
        if side_opt:
            candidates += [f'{side_opt}_seed', f'seed_{side_opt}', f'seed{("_" + side_opt) if side_opt else ""}']
        candidates += ['winner_seed', 'loser_seed', 'seed_winner', 'seed_loser', 'seed_winner', 'seed_loser']
        for c in candidates:
            if c in r.index:
                v = r.get(c)
                n = safe_int(v)
                if n is not None:
                    return n
        return None

    winner_seed = extract_seed('winner')
    loser_seed = extract_seed('loser')
    m['winner_seed'] = winner_seed
    m['loser_seed'] = loser_seed

    return m

# ---------------- Build detailed stats per player ----------------

RANK_BUCKETS = [
    (1,10,'1-10'),
    (11,20,'11-20'),
    (21,30,'21-30'),
    (31,50,'31-50'),
    (51,100,'51-100'),
    (101,200,'101-200'),
    (201,500,'201-500'),
    (501,10**9,'500+')
]

def rank_bucket_from_rank(r):
    if r is None:
        return None
    try:
        r = int(r)
    except Exception:
        return None
    for lo,hi,label in RANK_BUCKETS:
        if lo <= r <= hi:
            return label
    return None

def build_detailed_stats(matches_df, player_id, host_event_map=None):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # collect rows where player appears (by ID or name fallback)
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    # fallback by name columns if no ID found
    if not frames:
        # attempt match by winner/loser name equals pid (loose)
        nm_w = 'player_winner' in matches_df.columns
        nm_l = 'player_loser' in matches_df.columns
        if nm_w:
            cond = matches_df['player_winner'].astype(str).str.strip().str.upper() == pid
            if cond.any(): frames.append(matches_df[cond])
        if nm_l:
            cond = matches_df['player_loser'].astype(str).str.strip().str.upper() == pid
            if cond.any(): frames.append(matches_df[cond])

    if not frames:
        return {
            'player_id': pid,
            'player_name': pid,
            'meta': {'matches': 0, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v1'},
            'career': {},
            'stats_by_year': {},
            'stats_by_month': {},
            'matches': []
        }

    df = pd.concat(frames, ignore_index=True, sort=False)

    # canonical name
    player_name = pid
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            vals = df[col].dropna().astype(str).tolist()
            if vals:
                player_name = vals[0]
                break

    # containers
    career_stat_agg = {}  # for numeric aggregation across matches
    # we'll treat rates separately so that we can omit sum for them: maintain a set of rate keys
    rate_keys = set([
        'aces_per_service_point','doublefaults_per_service_point','firstserve_percent',
        'firstserve_points_won_percent','secondserve_points_won_percent','service_points_won_percent','return_points_won_percent',
        'breakpoints_converted_rate','service_games_lost_rate','tiebreak_win_rate'
    ])

    # counters and totals
    career_counts = {
        'matches_played': 0, 'matches_won': 0, 'matches_lost': 0,
        'sets_won': 0, 'sets_lost': 0,
        'tiebreaks_played': 0, 'tiebreaks_won': 0,
        'total_match_time_seconds': 0.0, 'match_time_count': 0
    }

    ranking_buckets = defaultdict(lambda: {'matches':0, 'wins':0})
    upsets = {'wins_vs_better':0, 'matches_vs_better':0, 'losses_vs_worse':0, 'matches_vs_worse':0}

    matches_out = []

    for idx, r in df.iterrows():
        is_winner = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_winner = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_winner = False

        match_date = parse_date_only(r.get('start_date') or r.get('match_date') or '')
        e_year = str(r.get('event_year') or (match_date[:4] if match_date else ''))
        event_id = str(r.get('event_id') or '')
        tourney_name = r.get('tourney_name') or ''
        surface = (r.get('surface') or '').strip().lower()
        round_tok = r.get('round') or ''
        # opponent info
        opponent_name = ''
        opponent_id = None
        if is_winner is True:
            opponent_name = r.get('player_loser') or r.get('loser_player_name') or ''
            opponent_id = r.get('player_id_loser') if 'player_id_loser' in r.index else None
        elif is_winner is False:
            opponent_name = r.get('player_winner') or r.get('winner_player_name') or ''
            opponent_id = r.get('player_id_winner') if 'player_id_winner' in r.index else None

        # extract per-match metrics
        m = extract_player_match_metrics(r, pid)
        if m is None:
            m = {}

        # ensure keys
        m.setdefault('aces', None)
        m.setdefault('doublefaults', None)
        m.setdefault('aces_per_service_point', None)
        m.setdefault('doublefaults_per_service_point', None)
        m.setdefault('firstserve_percent', None)
        m.setdefault('firstserve_points_won_percent', None)
        m.setdefault('secondserve_points_won_percent', None)
        m.setdefault('service_points_won_percent', None)
        m.setdefault('return_points_won_percent', None)
        m.setdefault('breakpoints_faced', None)
        m.setdefault('breakpoints_converted', None)
        m.setdefault('breakpoints_converted_rate', None)
        m.setdefault('service_games_lost_rate', None)
        m.setdefault('tiebreaks_played', 0)
        m.setdefault('tiebreaks_won', 0)
        m.setdefault('tiebreak_win_rate', None)
        m.setdefault('match_time_seconds', None)
        m.setdefault('match_time_hours', None)

        # append per-match minimal record (frontend may use)
        matches_out.append({
            'match_id': str(r.get('match_id') or ''),
            'event_id': event_id,
            'event_year': e_year,
            'match_date': match_date,
            'opponent': str(opponent_name) if opponent_name else '',
            'opponent_id': str(opponent_id).strip().upper() if opponent_id not in (None, '') else None,
            'is_win': bool(is_winner) if is_winner is not None else None,
            'score': str(r.get('score_string') or r.get('score') or ''),
            'aces': round_smart(m.get('aces')),
            'doublefaults': round_smart(m.get('doublefaults')),
            'aces_per_service_point': round_smart(m.get('aces_per_service_point')),
            'doublefaults_per_service_point': round_smart(m.get('doublefaults_per_service_point')),
            'firstserve_percent': round_smart(m.get('firstserve_percent')),
            'firstserve_points_won_percent': round_smart(m.get('firstserve_points_won_percent')),
            'secondserve_points_won_percent': round_smart(m.get('secondserve_points_won_percent')),
            'service_points_won_percent': round_smart(m.get('service_points_won_percent')),
            'return_points_won_percent': round_smart(m.get('return_points_won_percent')),
            'breakpoints_faced': round_smart(m.get('breakpoints_faced')),
            'breakpoints_converted': round_smart(m.get('breakpoints_converted')),
            'breakpoints_converted_rate': round_smart(m.get('breakpoints_converted_rate')),
            'service_games_lost_rate': round_smart(m.get('service_games_lost_rate')),
            'tiebreaks_played': m.get('tiebreaks_played'),
            'tiebreaks_won': m.get('tiebreaks_won'),
            'tiebreak_win_rate': round_smart(m.get('tiebreak_win_rate')),
            'match_time_seconds': m.get('match_time_seconds'),
            'match_time_hours': round_smart(m.get('match_time_hours')),
            'round': str(round_tok),
            'surface': surface
        })

        # career counters
        career_counts['matches_played'] += 1
        if is_winner is True:
            career_counts['matches_won'] += 1
        elif is_winner is False:
            career_counts['matches_lost'] += 1

        # sets won/lost try parse
        s_w = 0; s_l = 0
        for sc in ['set1_score','set2_score','set3_score','set4_score','set5_score']:
            if sc in r.index:
                val = r.get(sc) or ''
                if isinstance(val, str) and '-' in val:
                    left, right = val.split('-', 1)
                    left_n = re.sub(r'[^0-9]', '', left)
                    right_n = re.sub(r'[^0-9]', '', right)
                    try:
                        a = int(left_n) if left_n != '' else None
                        b = int(right_n) if right_n != '' else None
                    except Exception:
                        a = b = None
                    if a is None or b is None:
                        continue
                    if is_winner:
                        if a > b: s_w += 1
                        else: s_l += 1
                    else:
                        if b > a: s_w += 1
                        else: s_l += 1
        career_counts['sets_won'] += s_w
        career_counts['sets_lost'] += s_l

        # tiebreak counters
        career_counts['tiebreaks_played'] += int(m.get('tiebreaks_played') or 0)
        career_counts['tiebreaks_won'] += int(m.get('tiebreaks_won') or 0)

        # match time
        if m.get('match_time_seconds') is not None:
            try:
                career_counts['total_match_time_seconds'] += float(m.get('match_time_seconds'))
                career_counts['match_time_count'] += 1
            except Exception:
                pass

        # aggregate numeric fields
        numeric_fields = {
            'aces': to_float_safe(m.get('aces')),
            'doublefaults': to_float_safe(m.get('doublefaults')),
            'aces_per_service_point': to_float_safe(m.get('aces_per_service_point')),
            'doublefaults_per_service_point': to_float_safe(m.get('doublefaults_per_service_point')),
            'firstserve_percent': to_float_safe(m.get('firstserve_percent')),
            'firstserve_points_won_percent': to_float_safe(m.get('firstserve_points_won_percent')),
            'secondserve_points_won_percent': to_float_safe(m.get('secondserve_points_won_percent')),
            'service_points_won_percent': to_float_safe(m.get('service_points_won_percent')),
            'return_points_won_percent': to_float_safe(m.get('return_points_won_percent')),
            'breakpoints_faced': to_float_safe(m.get('breakpoints_faced')),
            'breakpoints_converted': to_float_safe(m.get('breakpoints_converted')),
            'breakpoints_converted_rate': to_float_safe(m.get('breakpoints_converted_rate')),
            'service_games_lost_rate': to_float_safe(m.get('service_games_lost_rate')),
            'tiebreak_win_rate': to_float_safe(m.get('tiebreak_win_rate')),
            'match_time_hours': to_float_safe(m.get('match_time_hours'))
        }

        for k,v in numeric_fields.items():
            add_numeric_agg(career_stat_agg, k, v)

        # ranking/upset buckets: try to determine opponent ranking/seed
        # prefer explicit seed columns
        winner_seed = None
        loser_seed = None
        for cand in ('winner_seed','loser_seed','seed_winner','seed_loser','seed_winner','seed_loser'):
            if cand in r.index:
                try:
                    if cand.startswith('winner'):
                        winner_seed = safe_int(r.get(cand))
                    elif cand.startswith('loser'):
                        loser_seed = safe_int(r.get(cand))
                except Exception:
                    pass
        # also try other patterns
        if winner_seed is None and 'winner_seed' in r.index:
            winner_seed = safe_int(r.get('winner_seed'))
        if loser_seed is None and 'loser_seed' in r.index:
            loser_seed = safe_int(r.get('loser_seed'))

        # opponent rank numeric (we interpret seed as ranking proxy)
        opp_rank = None
        if is_winner is True:
            opp_rank = safe_int(loser_seed)
            player_seed = safe_int(winner_seed)
        elif is_winner is False:
            opp_rank = safe_int(winner_seed)
            player_seed = safe_int(loser_seed)
        else:
            opp_rank = None
            player_seed = None

        # bucket update
        b = rank_bucket_from_rank(opp_rank)
        if b:
            ranking_buckets[b]['matches'] += 1
            if is_winner:
                ranking_buckets[b]['wins'] += 1

        # upsets:
        # define "better" as lower seed number (1 better than 10). If opponent has numeric seed and player's seed numeric:
        # - win vs better: player wins while player's seed > opponent's seed (player worse ranked)
        # - loss vs worse: player loses while player's seed < opponent's seed (player better ranked)
        if player_seed is not None and opp_rank is not None:
            # matches_vs_better: when opponent is better (opp_rank < player_seed)
            if opp_rank < player_seed:
                upsets['matches_vs_better'] += 1
                if is_winner:
                    upsets['wins_vs_better'] += 1
            if opp_rank > player_seed:
                upsets['matches_vs_worse'] += 1
                if not is_winner:
                    upsets['losses_vs_worse'] += 1

    # finalize career stats
    career_stats = {
        'matches_played': career_counts['matches_played'],
        'matches_won': career_counts['matches_won'],
        'matches_lost': career_counts['matches_lost'],
        'sets_won': career_counts['sets_won'],
        'sets_lost': career_counts['sets_lost'],
        'tiebreaks_played': career_counts['tiebreaks_played'],
        'tiebreaks_won': career_counts['tiebreaks_won'],
        'total_match_time_seconds': career_counts['total_match_time_seconds'],
        'total_match_time_hours': round_smart(career_counts['total_match_time_seconds'] / 3600.0) if career_counts['total_match_time_seconds'] else 0,
        'match_time_count': career_counts['match_time_count'],
        'stat_agg': finalize_numeric_agg(career_stat_agg)
    }

    # finalize ranking buckets to include win_rate
    ranking_buckets_out = {}
    for k, v in ranking_buckets.items():
        matches = int(v['matches'])
        wins = int(v['wins'])
        win_rate = (wins / matches) if matches>0 else None
        ranking_buckets_out[k] = {'matches': matches, 'wins': wins, 'win_rate': round_smart(win_rate) if win_rate is not None else None}

    # finalize upsets rates
    upsets_out = {
        'wins_vs_better_count': int(upsets.get('wins_vs_better',0)),
        'matches_vs_better': int(upsets.get('matches_vs_better',0)),
        'wins_vs_better_rate': round_smart((upsets.get('wins_vs_better',0) / upsets.get('matches_vs_better',1)) if upsets.get('matches_vs_better',0)>0 else None),
        'losses_vs_worse_count': int(upsets.get('losses_vs_worse',0)),
        'matches_vs_worse': int(upsets.get('matches_vs_worse',0)),
        'losses_vs_worse_rate': round_smart((upsets.get('losses_vs_worse',0) / upsets.get('matches_vs_worse',1)) if upsets.get('matches_vs_worse',0)>0 else None)
    }

    result = {
        'player_id': pid,
        'player_name': player_name,
        'meta': {'matches': len(matches_out), 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v1'},
        'career': career_stats,
        'ranking_buckets': ranking_buckets_out,
        'upsets': upsets_out,
        'matches': matches_out
    }

    return result

# ---------------- CLI Main ----------------

def main(matches_dir, out_dir, host_event_map_path=None, player_list=None, limit_players=None):
    print("[dstats] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[dstats] matches rows:", len(matches), "columns:", len(matches.columns))

    host_map = None
    if host_event_map_path:
        try:
            with open(host_event_map_path, 'r', encoding='utf8') as f:
                host_map = json.load(f)
            print("[dstats] Loaded host_event_map from", host_event_map_path)
        except Exception as e:
            print("[dstats] Warning: failed to load host_event_map:", e)
            host_map = None

    # discover players by id columns
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    # fallback: if none found, try names
    if not player_ids:
        for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
            if col in matches.columns:
                player_ids.update([normalize_player_id(x) for x in matches[col].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    if player_list:
        player_ids = [p for p in player_ids if p in set(player_list)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]
    print(f"[dstats] processing {len(player_ids)} players")

    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, start=1):
        try:
            print(f"[dstats] [{i}/{len(player_ids)}] building stats for {pid} ...")
            obj = build_detailed_stats(matches, pid, host_event_map=host_map)
            out_path = os.path.join(players_dir, f"{pid}.stats.json")
            with open(out_path, 'w', encoding='utf8') as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[dstats] ERROR building stats for {pid}: {e}")

    print("[dstats] Done. Stats written to", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate detailed statistics per player from matches CSVs.")
    ap.add_argument("--matches-dir", required=True, help="Directory containing matches CSV files")
    ap.add_argument("--out-dir", default="./dist", help="Output directory")
    ap.add_argument("--host-event-map", default=None, help="Optional JSON file path containing HOST_COUNTRY_TO_EVENT_IDS mapping")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process")
    ap.add_argument("--player", help="Process a single player id (e.g. S0AG)")
    args = ap.parse_args()
    plist = [args.player] if args.player else None
    main(args.matches_dir, args.out_dir, host_event_map_path=args.host_event_map, player_list=plist, limit_players=args.limit_players)
