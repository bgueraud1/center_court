#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_detailed_stats.py - Module 2: Detailed statistics per player

Usage:
  python generate_detailed_stats.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200 --host-event-map ./host_map.json

Outputs:
  - dist/players/{PLAYER_ID}.stats.json  (detailed stats: career, stats_by_year, stats_by_month, stat_agg)
"""

import argparse
import os
import glob
import json
import re
from collections import defaultdict, Counter
from datetime import datetime
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
        # some values may be '—' or '-'
        s = str(v).strip()
        if s == '':
            return None
        # replace commas
        s = s.replace(',', '.')
        # remove trailing % if any
        if s.endswith('%'):
            s = s[:-1]
        # remove non-numeric except dot and minus
        s2 = re.sub(r'[^\d\.\-eE]', '', s)
        if s2 == '' or s2 == '.' or s2 == '-':
            return None
        val = float(s2)
        if math.isfinite(val):
            return val
        return None
    except Exception:
        return None

def safe_float(v):
    return to_float_safe(v)

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

def finalize_numeric_agg(agg_dict):
    """
    Transform internal agg_dict to final form with mean.
    Result: { stat: {'count':..., 'sum':..., 'min':..., 'max':..., 'mean':...}, ...}
    """
    out = {}
    for k, e in agg_dict.items():
        cnt = int(e.get('count', 0)) if e and 'count' in e else 0
        s = float(e.get('sum', 0.0)) if e and 'sum' in e else 0.0
        mn = e.get('min', None)
        mx = e.get('max', None)
        mean = (s / cnt) if cnt > 0 else None
        # coerce ints where appropriate: keep floats for mean/sum
        out[k] = {'count': cnt, 'sum': (s if cnt>0 else None), 'min': (mn if cnt>0 else None), 'max': (mx if cnt>0 else None), 'mean': (round(mean, 6) if mean is not None else None)}
    return out

# ---------------- Extractor: per-match metrics ----------------

def extract_player_match_metrics(r, player_id):
    """
    Given a pandas Series row r (one match) and the player's player_id (normalized),
    return a dict m containing keys used by the pipeline:
      - raw_stats (dict of raw percent columns if present)
      - aces, doublefaults
      - breakpoints_faced, breakpoints_converted (counts), breakpoints_converted_pct
      - service_games_lost_rate
      - tiebreaks_played, tiebreaks_won, tiebreak_win_rate
      - match_time_seconds, match_time_hours
    If insufficient data, return values set to None as appropriate.
    """
    pid = normalize_player_id(player_id)
    m = {}

    # determine if player is winner in this row
    is_winner = None
    if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
        is_winner = True
    elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
        is_winner = False
    else:
        # fallback by matching name fields (best-effort)
        # If names not present, assume None
        if 'player_winner' in r.index and 'player_loser' in r.index:
            # compare string presence - not guaranteed, leave as None
            if isinstance(r.get('player_winner'), str) and isinstance(r.get('player_loser'), str):
                # can't be sure which is ours; don't guess
                pass

    # --- raw stats extraction via STAT_COLUMN_CANDIDATES if present in globals (optional) ---
    raw_stats = {}
    if 'STAT_COLUMN_CANDIDATES' in globals():
        for stat_key, (wc, lc) in STAT_COLUMN_CANDIDATES.items():
            val = None
            if is_winner is True and wc in r.index:
                val = to_float_safe(r.get(wc))
            elif is_winner is False and lc in r.index:
                val = to_float_safe(r.get(lc))
            raw_stats[stat_key] = val
    else:
        # fallback common names: look for suffixed columns (winner/loser) first
        for key in ['aces', 'doublefaults', 'firstserve_percent', 'firstservepointswon_percent',
                    'secondservepointswon_percent', 'totalservicepointswon_percent', 'totalreturnpointswon_percent',
                    'breakpointssaved_percent']:
            # check winner/loser variants first
            found = None
            # try winner/loser suffix
            cand_w = f"{key}_tot_winner" if f"{key}_tot_winner" in r.index else f"{key}_winner"
            cand_l = f"{key}_tot_loser" if f"{key}_tot_loser" in r.index else f"{key}_loser"
            if is_winner is True and cand_w in r.index:
                found = to_float_safe(r.get(cand_w))
            elif is_winner is False and cand_l in r.index:
                found = to_float_safe(r.get(cand_l))
            else:
                # try generic column
                if key in r.index:
                    found = to_float_safe(r.get(key))
            raw_stats[key] = found
    m['raw_stats'] = raw_stats

    # aces / doublefaults counts (try many variants)
    aces = None
    for cand in ('aces', 'aces_tot_winner', 'aces_tot_loser', 'aces_tot'):
        if cand in r.index and str(r.get(cand, '')).strip() != '':
            aces = to_float_safe(r.get(cand))
            break
    m['aces'] = aces

    dfaults = None
    for cand in ('doublefaults', 'doublefaults_tot_winner', 'doublefaults_tot_loser', 'doublefaults_tot'):
        if cand in r.index and str(r.get(cand, '')).strip() != '':
            dfaults = to_float_safe(r.get(cand))
            break
    m['doublefaults'] = dfaults

    # --- match time: try match_time_total/match_time or settime columns ---
    mtsec = None
    mtcol = None
    for c in ('match_time_total', 'match_time', 'match_time_seconds'):
        if c in r.index and str(r.get(c, '')).strip() != '':
            mtcol = r.get(c)
            break
    if mtcol:
        try:
            s = str(mtcol).strip()
            # hh:mm:ss or mm:ss or "02:12:00"
            parts = s.split(':')
            if len(parts) == 3:
                h = int(parts[0]); mm = int(parts[1]); ss = int(parts[2])
                mtsec = h*3600 + mm*60 + ss
            elif len(parts) == 2:
                mm = int(parts[0]); ss = int(parts[1])
                mtsec = mm*60 + ss
            else:
                # maybe it's numeric seconds or float
                mtsec = to_float_safe(s)
        except Exception:
            mtsec = to_float_safe(mtcol)
    # fallback: settime_tot_winner/loser
    if mtsec is None:
        key = 'settime_tot_winner' if is_winner else 'settime_tot_loser'
        if key in r.index and str(r.get(key, '')).strip() != '':
            mtsec = to_float_safe(r.get(key))
    m['match_time_seconds'] = (float(mtsec) if mtsec is not None else None)
    m['match_time_hours'] = (float(mtsec)/3600.0) if mtsec is not None else None

    # --- tiebreaks: prefer explicit tiebreak_set{n}_winner/loser columns (user requested rule) ---
    tb_played = 0
    tb_won = 0
    for i in range(1, 6):
        tw_col = f'tiebreak_set{i}_winner'
        tl_col = f'tiebreak_set{i}_loser'
        tw = r.get(tw_col) if tw_col in r.index else None
        tl = r.get(tl_col) if tl_col in r.index else None
        if (tw is not None and str(tw).strip() != '') or (tl is not None and str(tl).strip() != ''):
            tb_played += 1
            # numeric compare if possible
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
                # best-effort: if only one side has value, assume that side's owner won
                if is_winner and tw and not tl:
                    tb_won += 1
                if (not is_winner) and tl and not tw:
                    tb_won += 1

    # fallback: parse setN_score fields containing parentheses "7-6(7)"
    for i in range(1, 6):
        sc = f'set{i}_score'
        if sc in r.index:
            val = r.get(sc, '') or ''
            if isinstance(val, str) and '(' in val and ')' in val and '-' in val:
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
                # left is winner games in CSV record
                tb_played += 1
                if is_winner:
                    if a > b: tb_won += 1
                else:
                    if b > a: tb_won += 1

    m['tiebreaks_played'] = int(tb_played)
    m['tiebreaks_won'] = int(tb_won)
    m['tiebreak_win_rate'] = (float(tb_won) / tb_played) if tb_played > 0 else None

    # --- breakpoints: compute from opponent's breakpointssaved_divisor/dividend columns when present (user rule) ---
    # opponent side
    opp_side = 'loser' if is_winner else 'winner'
    # try common column patterns
    opp_bps_div = None
    opp_bps_dvd = None
    cand_divs = [
        f'breakpointssaved_divisor_tot_{opp_side}', f'breakpointssaved_divisor_{opp_side}',
        f'breakpointssaved_divisor_tot_winner', f'breakpointssaved_divisor_tot_loser',
        'breakpointssaved_divisor', 'breakpointssaved_divisor_tot'
    ]
    cand_dvds = [
        f'breakpointssaved_dividend_tot_{opp_side}', f'breakpointssaved_dividend_{opp_side}',
        f'breakpointssaved_dividend_tot_winner', f'breakpointssaved_dividend_tot_loser',
        'breakpointssaved_dividend', 'breakpointssaved_dividend_tot'
    ]
    for c in cand_divs:
        if c in r.index and str(r.get(c, '')).strip() != '':
            opp_bps_div = to_float_safe(r.get(c))
            if opp_bps_div is not None:
                break
    for c in cand_dvds:
        if c in r.index and str(r.get(c, '')).strip() != '':
            opp_bps_dvd = to_float_safe(r.get(c))
            if opp_bps_dvd is not None:
                break

    m['breakpoints_faced'] = (opp_bps_div if opp_bps_div is not None else None)
    if (opp_bps_div is not None) and (opp_bps_dvd is not None):
        conv_count = max(0.0, opp_bps_div - opp_bps_dvd)
        m['breakpoints_converted'] = conv_count
        m['breakpoints_converted_pct'] = (conv_count / opp_bps_div) if opp_bps_div > 0 else None
    else:
        # fallback: try explicit fields in row
        conv_fallback = None
        for cand in ('breakpoints_converted', f'breakpoints_converted_tot_{opp_side}', f'breakpoints_converted_{opp_side}'):
            if cand in r.index and str(r.get(cand, '')).strip() != '':
                conv_fallback = to_float_safe(r.get(cand))
                break
        m['breakpoints_converted'] = conv_fallback
        if conv_fallback is not None and opp_bps_div:
            m['breakpoints_converted_pct'] = (conv_fallback / opp_bps_div) if opp_bps_div and opp_bps_div > 0 else None
        else:
            m['breakpoints_converted_pct'] = None

    # service games lost rate = breaks_conceded / player's servicegamesplayed_tot_{side}
    player_sg_played = None
    side_suffix = 'winner' if is_winner else 'loser'
    for cand in (f'servicegamesplayed_tot_{side_suffix}', f'servicegamesplayed_{side_suffix}', 'servicegamesplayed', 'servicegamesplayed_tot_winner', 'servicegamesplayed_tot_loser'):
        if cand in r.index and str(r.get(cand, '')).strip() != '':
            player_sg_played = to_float_safe(r.get(cand))
            break
    if m.get('breakpoints_converted') is not None and player_sg_played and player_sg_played > 0:
        try:
            m['service_games_lost_rate'] = float(m.get('breakpoints_converted')) / float(player_sg_played)
        except Exception:
            m['service_games_lost_rate'] = None
    else:
        m['service_games_lost_rate'] = None

    return m

# ---------------- Build detailed stats per player ----------------

def build_detailed_stats(matches_df, player_id, host_event_map=None):
    """
    Build detailed stats object for a player given full matches_df.
    host_event_map (optional) is a dict mapping event_id -> {year: country} or similar (passed through).
    Returns a dictionary ready to be JSON dumped.
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # collect all rows where player appears
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        # nothing for player
        return {
            'player_id': pid,
            'career': {},
            'stats_by_year': {},
            'stats_by_month': {},
            'meta': {'matches': 0, 'generated_at': datetime.utcnow().isoformat() + 'Z'}
        }
    df = pd.concat(frames, ignore_index=True, sort=False)

    # find canonical player name
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            name_candidates.extend([str(x) for x in df[col].dropna().astype(str).tolist()])
    player_name = name_candidates[0] if name_candidates else pid

    # containers
    career_counts = {
        'matches_played': 0, 'matches_won': 0, 'matches_lost': 0,
        'sets_won': 0, 'sets_lost': 0,
        'tiebreaks_played': 0, 'tiebreaks_won': 0,
        'total_match_time_seconds': 0.0, 'match_time_count': 0,
        'straight_set_wins': 0, 'three_set_wins': 0,
        'wins_after_losing_first_set': 0, 'losses_after_winning_first_set': 0,
        'gs_won_after_winning_first_two': 0, 'gs_lost_after_winning_first_two': 0,
        'gs_won_after_losing_first_two': 0, 'gs_lost_after_losing_first_two': 0
    }
    career_stat_agg = {}
    stats_by_year = {}
    stats_by_month = {}
    matches_out = []

    # iterate rows
    for idx, r in df.iterrows():
        # determine is_winner
        is_winner = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_winner = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_winner = False

        # basic metadata
        e_year = str(r.get('event_year') or '')
        match_date = parse_date_only(r.get('start_date') or r.get('match_date') or '')
        event_id = str(r.get('event_id') or '')
        tourney_name = r.get('tourney_name') or ''
        level = r.get('level') or ''
        surface = (r.get('surface') or '').strip().lower()
        round_tok = r.get('round') or ''
        category = None
        # user may have detect_category elsewhere; leave category empty for now
        # opponent name/id
        opponent_name = ''
        opponent_id = None
        if is_winner is True:
            opponent_name = r.get('player_loser') or r.get('loser_player_name') or ''
            opponent_id = r.get('player_id_loser') if 'player_id_loser' in r.index else None
        elif is_winner is False:
            opponent_name = r.get('player_winner') or r.get('winner_player_name') or ''
            opponent_id = r.get('player_id_winner') if 'player_id_winner' in r.index else None

        # use extract function
        try:
            m = extract_player_match_metrics(r, pid)
            if m is None:
                m = {}
        except Exception as e:
            # In case extractor crashes for a row, continue with minimal m
            m = {}

        # ensure presence of keys
        m.setdefault('tiebreaks_played', 0)
        m.setdefault('tiebreaks_won', 0)
        m.setdefault('tiebreak_win_rate', None)
        m.setdefault('breakpoints_faced', None)
        m.setdefault('breakpoints_converted', None)
        m.setdefault('breakpoints_converted_pct', None)
        m.setdefault('service_games_lost_rate', None)
        m.setdefault('match_time_seconds', None)
        m.setdefault('match_time_hours', None)

        # append match-level record minimal (so frontend can show per-match stats if desired)
        matches_out.append({
            'match_id': str(r.get('match_id') or ''),
            'event_id': event_id,
            'event_year': e_year,
            'match_date': match_date,
            'opponent': str(opponent_name) if opponent_name else '',
            'opponent_id': str(opponent_id).strip().upper() if opponent_id not in (None, '') else None,
            'is_win': bool(is_winner) if is_winner is not None else None,
            'score': str(r.get('score_string') or r.get('score') or ''),
            # include selected stats
            'aces': m.get('aces'),
            'doublefaults': m.get('doublefaults'),
            'breakpoints_faced': m.get('breakpoints_faced'),
            'breakpoints_converted': m.get('breakpoints_converted'),
            'breakpoints_converted_pct': m.get('breakpoints_converted_pct'),
            'service_games_lost_rate': m.get('service_games_lost_rate'),
            'tiebreaks_played': m.get('tiebreaks_played'),
            'tiebreaks_won': m.get('tiebreaks_won'),
            'tiebreak_win_rate': m.get('tiebreak_win_rate'),
            'match_time_seconds': m.get('match_time_seconds'),
            'match_time_hours': m.get('match_time_hours'),
            'round': str(round_tok),
            'surface': surface
        })

        # career counters
        career_counts['matches_played'] += 1
        if is_winner is True:
            career_counts['matches_won'] += 1
        elif is_winner is False:
            career_counts['matches_lost'] += 1

        # sets won/lost attempt: try parse set scores
        s_w = 0
        s_l = 0
        for sc in ['set1_score', 'set2_score', 'set3_score', 'set4_score', 'set5_score']:
            if sc in r.index:
                val = r.get(sc, '') or ''
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

        # tie-break counters
        career_counts['tiebreaks_played'] += int(m.get('tiebreaks_played') or 0)
        career_counts['tiebreaks_won'] += int(m.get('tiebreaks_won') or 0)

        # match time counters
        if m.get('match_time_seconds') is not None:
            try:
                career_counts['total_match_time_seconds'] += float(m.get('match_time_seconds'))
                career_counts['match_time_count'] += 1
            except Exception:
                pass

        # straight/three set wins detection
        total_sets = s_w + s_l
        if is_winner:
            if total_sets == 2:
                career_counts['straight_set_wins'] += 1
            if total_sets == 3:
                career_counts['three_set_wins'] += 1

        # first-set scenario counters
        fs = (r.get('set1_score') or '')
        if isinstance(fs, str) and '-' in fs:
            left, right = fs.split('-', 1)
            left_n = re.sub(r'[^0-9]', '', left)
            right_n = re.sub(r'[^0-9]', '', right)
            try:
                a = int(left_n) if left_n != '' else None
                b = int(right_n) if right_n != '' else None
            except Exception:
                a = b = None
            if a is not None and b is not None:
                player_won_first = (a > b) if is_winner else (b > a)
                player_won_match = bool(is_winner)
                if (not player_won_first) and player_won_match:
                    career_counts['wins_after_losing_first_set'] += 1
                if player_won_first and (not player_won_match):
                    career_counts['losses_after_winning_first_set'] += 1

        # GS-specific counters: attempt detection via tourney_name or level
        is_grand_slam = False
        tn_lower = (str(tourney_name or '')).lower()
        if tn_lower and any(x in tn_lower for x in ['wimbledon','roland','french open','us open','australian open','roland-garros','usopen','australianopen']):
            is_grand_slam = True
        won_first_two = None
        set1 = (r.get('set1_score') or '')
        set2 = (r.get('set2_score') or '')
        def player_won_set_raw(setval, player_is_winner):
            try:
                if isinstance(setval, str) and '-' in setval:
                    left, right = setval.split('-', 1)
                    a = int(re.sub(r'[^0-9]', '', left)) if left.strip() != '' else None
                    b = int(re.sub(r'[^0-9]', '', right)) if right.strip() != '' else None
                    if a is None or b is None:
                        return None
                    return (a > b) if player_is_winner else (b > a)
            except Exception:
                pass
            return None
        s1_won = player_won_set_raw(set1, is_winner)
        s2_won = player_won_set_raw(set2, is_winner)
        if s1_won is not None and s2_won is not None:
            won_first_two = (s1_won and s2_won)
            if is_grand_slam:
                if won_first_two:
                    won_after_winning_first_two = 1 if is_winner else 0
                    lost_after_winning_first_two = 0 if is_winner else 1
                    career_counts['gs_won_after_winning_first_two'] += (won_after_winning_first_two or 0)
                    career_counts['gs_lost_after_winning_first_two'] += (lost_after_winning_first_two or 0)
                else:
                    won_after_losing_first_two = 1 if is_winner else 0
                    lost_after_losing_first_two = 0 if is_winner else 1
                    career_counts['gs_won_after_losing_first_two'] += (won_after_losing_first_two or 0)
                    career_counts['gs_lost_after_losing_first_two'] += (lost_after_losing_first_two or 0)

        # --- prepare numeric fields to aggregate via add_numeric_agg ---
        numeric_fields = {
            'aces': to_float_safe(m.get('aces')),
            'doublefaults': to_float_safe(m.get('doublefaults')),
            'firstserve_percent': to_float_safe(m.get('raw_stats', {}).get('firstserve_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'firstservepointswon_percent': to_float_safe(m.get('raw_stats', {}).get('firstservepointswon_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'secondservepointswon_percent': to_float_safe(m.get('raw_stats', {}).get('secondservepointswon_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'totalservicepointswon_percent': to_float_safe(m.get('raw_stats', {}).get('totalservicepointswon_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'totalreturnpointswon_percent': to_float_safe(m.get('raw_stats', {}).get('totalreturnpointswon_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'breakpointssaved_percent': to_float_safe(m.get('raw_stats', {}).get('breakpointssaved_percent')) if isinstance(m.get('raw_stats'), dict) else None,
            'breakpoints_converted': to_float_safe(m.get('breakpoints_converted')),
            'breakpoints_faced': to_float_safe(m.get('breakpoints_faced')),
            'breakpoints_converted_rate': to_float_safe(m.get('breakpoints_converted_pct')),
            'service_games_lost_rate': to_float_safe(m.get('service_games_lost_rate')),
            'tiebreaks_played': to_float_safe(m.get('tiebreaks_played')),
            'tiebreaks_won': to_float_safe(m.get('tiebreaks_won')),
            'tiebreak_win_rate': to_float_safe(m.get('tiebreak_win_rate')),
            'match_time_hours': to_float_safe(m.get('match_time_hours'))
        }

        for k, v in numeric_fields.items():
            add_numeric_agg(career_stat_agg, k, v)

        # --- per-year aggregates
        if e_year:
            if e_year not in stats_by_year:
                stats_by_year[e_year] = {'matches_played': 0, 'matches_won': 0, 'matches_lost': 0, 'sets_won': 0, 'sets_lost': 0, 'tiebreaks_played': 0, 'tiebreaks_won': 0, 'total_match_time_seconds': 0.0, 'match_time_count': 0, 'stat_agg': {}, 'by_surface': defaultdict(lambda: {'stat_agg': {}}), 'ranking_buckets': Counter()}
            sy = stats_by_year[e_year]
            sy['matches_played'] += 1
            if is_winner:
                sy['matches_won'] += 1
            else:
                sy['matches_lost'] += 1
            sy['sets_won'] += s_w
            sy['sets_lost'] += s_l
            sy['tiebreaks_played'] += int(m.get('tiebreaks_played') or 0)
            sy['tiebreaks_won'] += int(m.get('tiebreaks_won') or 0)
            if m.get('match_time_seconds'):
                try:
                    sy['total_match_time_seconds'] += float(m.get('match_time_seconds'))
                    sy['match_time_count'] += 1
                except Exception:
                    pass
            # aggregate numeric fields for year
            for k, v in numeric_fields.items():
                add_numeric_agg(sy['stat_agg'], k, v)
            # by surface
            surfkey = surface or 'unknown'
            for k, v in numeric_fields.items():
                add_numeric_agg(sy['by_surface'][surfkey]['stat_agg'], k, v)

        # --- per-month aggregates
        month_key = None
        if match_date:
            try:
                dt = pd.to_datetime(match_date, errors='coerce')
                if not pd.isna(dt):
                    month_key = dt.strftime('%Y-%m')
            except Exception:
                month_key = None
        if month_key:
            if month_key not in stats_by_month:
                stats_by_month[month_key] = {'matches_played': 0, 'matches_won': 0, 'matches_lost': 0, 'stat_agg': {}, 'by_surface': defaultdict(lambda: {'stat_agg': {}})}
            sm = stats_by_month[month_key]
            sm['matches_played'] += 1
            if is_winner:
                sm['matches_won'] += 1
            else:
                sm['matches_lost'] += 1
            for k, v in numeric_fields.items():
                add_numeric_agg(sm['stat_agg'], k, v)
            surfkey = surface or 'unknown'
            for k, v in numeric_fields.items():
                add_numeric_agg(sm['by_surface'][surfkey]['stat_agg'], k, v)

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
        'total_match_time_hours': (career_counts['total_match_time_seconds'] / 3600.0) if career_counts['total_match_time_seconds'] else 0,
        'match_time_count': career_counts['match_time_count'],
        'straight_set_wins': career_counts['straight_set_wins'],
        'three_set_wins': career_counts['three_set_wins'],
        'wins_after_losing_first_set': career_counts['wins_after_losing_first_set'],
        'losses_after_winning_first_set': career_counts['losses_after_winning_first_set'],
        'stat_agg': finalize_numeric_agg(career_stat_agg)
    }

    # finalize per-year and per-month
    sby = {}
    for y, sy in stats_by_year.items():
        sby[y] = {
            'matches_played': sy['matches_played'],
            'matches_won': sy['matches_won'],
            'matches_lost': sy['matches_lost'],
            'sets_won': sy['sets_won'],
            'sets_lost': sy['sets_lost'],
            'tiebreaks_played': sy['tiebreaks_played'],
            'tiebreaks_won': sy['tiebreaks_won'],
            'total_match_time_seconds': sy['total_match_time_seconds'],
            'total_match_time_hours': (sy['total_match_time_seconds'] / 3600.0) if sy['total_match_time_seconds'] else 0,
            'match_time_count': sy['match_time_count'],
            'stat_agg': finalize_numeric_agg(sy['stat_agg']),
            'by_surface': {},
            'ranking_buckets': dict(sy['ranking_buckets'])
        }
        for surf, obj in sy['by_surface'].items():
            sby[y]['by_surface'][surf] = {'stat_agg': finalize_numeric_agg(obj['stat_agg'])}
    smb = {}
    for mkey, sm in stats_by_month.items():
        smb[mkey] = {'matches_played': sm['matches_played'], 'matches_won': sm['matches_won'], 'matches_lost': sm['matches_lost'], 'stat_agg': finalize_numeric_agg(sm['stat_agg']), 'by_surface': {}}
        for surf, obj in sm['by_surface'].items():
            smb[mkey]['by_surface'][surf] = {'stat_agg': finalize_numeric_agg(obj['stat_agg'])}

    result = {
        'player_id': pid,
        'player_name': player_name,
        'meta': {'matches': len(matches_out), 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v1'},
        'career': career_stats,
        'stats_by_year': sby,
        'stats_by_month': smb,
        # optionally include per-match list (lightweight)
        'matches': matches_out
    }

    return result

# ---------------- CLI Main ----------------

def main(matches_dir, out_dir, host_event_map_path=None, player_list=None, limit_players=None):
    print("[dstats] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[dstats] matches rows:", len(matches), "columns:", len(matches.columns))

    # optional host_event_map loading
    host_map = None
    if host_event_map_path:
        try:
            with open(host_event_map_path, 'r', encoding='utf8') as f:
                host_map = json.load(f)
            print("[dstats] Loaded host_event_map from", host_event_map_path)
        except Exception as e:
            print("[dstats] Warning: failed to load host_event_map:", e)
            host_map = None

    # discover players
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    if player_list:
        # filter
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
