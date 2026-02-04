#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_detailed_stats.py - Detailed statistics per player (improved)

- Fix tie-break counting using explicit tiebreak_setN_winner/tiebreak_setN_loser columns.
- Compute upsets using ATP snapshot CSVs in directory atp_rankings (files named data_YYYY_MM_DD*.csv).
- Produce career_by_surface and lists available_years/available_surfaces in output JSON.
- Keeps stat_agg structure: { stat: {'count', 'sum', 'min','max','mean'} }
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
import unicodedata

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
            # --- ADD: tag each row with source filename and extracted year from filename ---
            basename = os.path.basename(f)
            df['__src_file'] = basename
            # extract a 4-digit year from the filename (e.g. atp_id_2025.csv or ..._2025_...)
            m = re.search(r'_(\d{4})', basename)
            if not m:
                m2 = re.search(r'(\d{4})', basename)
                m = m2
            df['__src_year'] = m.group(1) if m else ''
            # ---------------------------------------------------------------------------
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
        parts = st.split(':')
        if len(parts) == 3:
            h = int(parts[0]); m = int(parts[1]); sec = int(parts[2])
            return h*3600 + m*60 + sec
        if len(parts) == 2:
            m = int(parts[0]); sec = int(parts[1])
            return m*60 + sec
        val = to_float_safe(st)
        if val is not None:
            return float(val)
    except Exception:
        pass
    return None

def round_smart(v):
    if v is None:
        return None
    try:
        if isinstance(v, (int,)):
            return v
        f = float(v)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return round(f, 6)  # keep precision internal; presentation handled client-side
    except Exception:
        return v

# ---------------- Aggregation utilities ----------------

def add_numeric_agg(agg_dict, key, value):
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
    out = {}
    for k, e in agg_dict.items():
        cnt = int(e.get('count', 0)) if e and 'count' in e else 0
        s = float(e.get('sum', 0.0)) if e and 'sum' in e else 0.0
        mn = e.get('min', None)
        mx = e.get('max', None)
        mean = (s / cnt) if cnt > 0 else None
        out[k] = {'count': cnt, 'sum': (s if cnt>0 else None), 'min': (mn if cnt>0 else None), 'max': (mx if cnt>0 else None), 'mean': (mean if mean is not None else None)}
    return out

# ---------------- ATP rankings loader (atp_rankings dir) ----------------

def normalize_name_for_match(full_name):
    # produce "Initial. Lastname" style from "Roger Federer" -> "R. Federer"
    if not isinstance(full_name, str):
        return ''
    s = full_name.strip()
    if not s:
        return ''
    parts = s.split()
    if len(parts) == 1:
        return parts[0].strip()
    first = parts[0]
    last = parts[-1]
    initial = first[0].upper() if first else ''
    return f"{initial}. {last}".strip()

def normalize_str(s):
    if s is None: return ''
    s2 = str(s).strip().lower()
    # remove diacritics
    s2 = ''.join(c for c in unicodedata.normalize('NFKD', s2) if not unicodedata.combining(c))
    s2 = re.sub(r'[^a-z0-9\.\s\-]', '', s2)
    s2 = re.sub(r'\s+', ' ', s2).strip()
    return s2

def load_atp_rankings(rankings_dir):
    """
    Load CSV ranking snapshots from rankings_dir with filenames like data_YYYY_MM_DD*.csv
    Return sorted list of tuples (date(datetime.date), mapping) where mapping maps:
      - short_name_norm (e.g. 'r. federer') -> ranking (int)
      - last_name_norm -> list of possible rankings (we will pick first)
    """
    if not rankings_dir:
        return []
    pattern = os.path.join(rankings_dir, "data_*.csv")
    files = sorted(glob.glob(pattern))
    entries = []
    for f in files:
        m = re.search(r'data_(\d{4})[_\-]?(\d{2})[_\-]?(\d{2})', os.path.basename(f))
        if not m:
            # try any date in filename
            m2 = re.search(r'(\d{4})[_\-]?(\d{2})[_\-]?(\d{2})', os.path.basename(f))
            if m2:
                m = m2
        if not m:
            continue
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        except Exception:
            continue
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception as e:
            print("[rankings] warning failed to read", f, e)
            continue
        mapping = {}
        last_map = defaultdict(list)
        for _, row in df.iterrows():
            full = row.get('full_name') or row.get('name') or ''
            rank = safe_int(row.get('ranking') or row.get('rank') or row.get('position'))
            if full and rank:
                short = normalize_name_for_match(full)
                shortn = normalize_str(short)
                mapping[shortn] = rank
                ln = normalize_str(full.split()[-1] if isinstance(full, str) else full)
                last_map[ln].append(rank)
        entries.append((dt, mapping, last_map))
    # sort by date ascending
    entries.sort(key=lambda x: x[0])
    return entries

def get_rank_for_player_on_date(name_short, match_date, rankings_entries):
    """
    name_short: e.g. "T. Henman" or "T Henman" etc.
    match_date: datetime.date
    rankings_entries: list as returned above
    Returns ranking int or None
    """
    if not name_short or not match_date or not rankings_entries:
        return None
    norm_short = normalize_str(name_short)
    # find latest snapshot with date <= match_date
    candidate = None
    for dt, mapping, last_map in rankings_entries:
        if dt <= match_date:
            candidate = (dt, mapping, last_map)
        else:
            break
    if not candidate:
        return None
    dt, mapping, last_map = candidate
    # try short match
    if norm_short in mapping:
        return mapping[norm_short]
    # fallback: compare by last name
    last = ''
    parts = name_short.split()
    if parts:
        last = normalize_str(parts[-1])
    if last and last in last_map and last_map[last]:
        # pick the most common / first
        return last_map[last][0]
    # no match
    return None

# ---------------- Extractor: per-match metrics (with tie-break fix) ----------------

def extract_player_match_metrics(r, player_id):
    pid = normalize_player_id(player_id)
    m = {}

    # determine if player is winner
    is_winner = None
    if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
        is_winner = True
    elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
        is_winner = False

    # --- raw stats as before (look for many column variants) ---
    # aces / doublefaults
    aces = None
    for cand in ('aces', 'aces_tot_winner', 'aces_tot_loser', 'aces_tot', 'aces_tot_'+('winner' if is_winner else 'loser') if is_winner is not None else ''):
        if cand and cand in r.index and str(r.get(cand, '')).strip() != '':
            aces = to_float_safe(r.get(cand)); break
    dfaults = None
    for cand in ('doublefaults', 'doublefaults_tot_winner', 'doublefaults_tot_loser', 'doublefaults_tot', 'doublefaults_tot_'+('winner' if is_winner else 'loser') if is_winner is not None else ''):
        if cand and cand in r.index and str(r.get(cand, '')).strip() != '':
            dfaults = to_float_safe(r.get(cand)); break
    m['aces'] = aces
    m['doublefaults'] = dfaults

    # service points (approx) & per-point rates
    service_points = None
    # try servicegamesplayed * 4 as fallback
    side = 'winner' if is_winner else 'loser' if is_winner is not None else None
    for cand in (f'totalservicepointswon_divisor_tot_{side}' if side else None, f'firstserve_divisor_tot_{side}' if side else None, f'servicegamesplayed_tot_{side}' if side else None, 'servicegamesplayed', 'servicegamesplayed_tot'):
        if cand and cand in r.index and str(r.get(cand, '')).strip() != '':
            # if it's service games, multiply by 4 as rough estimate
            val = to_float_safe(r.get(cand))
            if val is not None:
                if 'servicegamesplayed' in cand:
                    service_points = val * 4.0
                else:
                    service_points = val
                break
    m['service_points_played'] = service_points
    m['aces_per_service_point'] = (float(aces)/service_points) if (aces is not None and service_points) else None
    m['doublefaults_per_service_point'] = (float(dfaults)/service_points) if (dfaults is not None and service_points) else None

    # percents
    def find_pct(names):
        for n in names:
            if n and n in r.index and str(r.get(n, '')).strip() != '':
                v = to_float_safe(r.get(n))
                if v is not None:
                    # normalize >1 -> assume percent
                    if v > 1.0:
                        v = v / 100.0
                    return v
        return None

    m['firstserve_percent'] = find_pct([f'firstserve_percent_tot_{side}' if side else None, f'firstserve_percent_{side}' if side else None, 'firstserve_percent'])
    m['firstserve_points_won_percent'] = find_pct([f'firstservepointswon_percent_tot_{side}' if side else None, 'firstservepointswon_percent'])
    m['secondserve_points_won_percent'] = find_pct([f'secondservepointswon_percent_tot_{side}' if side else None, 'secondservepointswon_percent'])
    m['service_points_won_percent'] = find_pct([f'totalservicepointswon_percent_tot_{side}' if side else None, 'totalservicepointswon_percent'])
    m['return_points_won_percent'] = find_pct([f'totalreturnpointswon_percent_tot_{side}' if side else None, 'totalreturnpointswon_percent'])

    # breakpoints faced/converted using opponent's breakpointssaved_divisor/dividend
    opp_side = 'loser' if is_winner else 'winner' if is_winner is not None else None
    opp_bps_div = None
    opp_bps_dvd = None
    if opp_side:
        for c in (f'breakpointssaved_divisor_tot_{opp_side}', f'breakpointssaved_divisor_{opp_side}', 'breakpointssaved_divisor_tot', 'breakpointssaved_divisor'):
            if c in r.index and str(r.get(c, '')).strip() != '':
                opp_bps_div = to_float_safe(r.get(c)); break
        for c in (f'breakpointssaved_dividend_tot_{opp_side}', f'breakpointssaved_dividend_{opp_side}', 'breakpointssaved_dividend_tot', 'breakpointssaved_dividend'):
            if c in r.index and str(r.get(c, '')).strip() != '':
                opp_bps_dvd = to_float_safe(r.get(c)); break
    m['breakpoints_faced'] = opp_bps_div
    if opp_bps_div is not None and opp_bps_dvd is not None:
        conv = max(0.0, opp_bps_div - opp_bps_dvd)
        m['breakpoints_converted'] = conv
        m['breakpoints_converted_pct'] = (conv / opp_bps_div) if opp_bps_div>0 else None
    else:
        conv_fallback = None
        for cand in ('breakpoints_converted', f'breakpoints_converted_{opp_side}' if opp_side else None):
            if cand and cand in r.index and str(r.get(cand, '')).strip() != '':
                conv_fallback = to_float_safe(r.get(cand)); break
        m['breakpoints_converted'] = conv_fallback
        m['breakpoints_converted_pct'] = (conv_fallback / opp_bps_div) if (conv_fallback is not None and opp_bps_div and opp_bps_div>0) else None

    # service games lost rate
    player_sg_played = None
    for cand in (f'servicegamesplayed_tot_{side}' if side else None, f'servicegamesplayed_{side}' if side else None, 'servicegamesplayed', 'servicegamesplayed_tot'):
        if cand and cand in r.index and str(r.get(cand, '')).strip() != '':
            player_sg_played = to_float_safe(r.get(cand)); break
    if m.get('breakpoints_converted') is not None and player_sg_played and player_sg_played>0:
        try:
            m['service_games_lost_rate'] = float(m.get('breakpoints_converted')) / float(player_sg_played)
        except Exception:
            m['service_games_lost_rate'] = None
    else:
        m['service_games_lost_rate'] = None

    # tie-breaks fix: explicitly parse tiebreak_setN_winner / loser columns and compare
    tb_played = 0
    tb_won = 0
    for i in range(1, 6):
        tw_col = f'tiebreak_set{i}_winner'
        tl_col = f'tiebreak_set{i}_loser'
        tw = r.get(tw_col) if tw_col in r.index else None
        tl = r.get(tl_col) if tl_col in r.index else None
        if (tw is not None and str(tw).strip() != '') or (tl is not None and str(tl).strip() != ''):
            # both should be present in many CSVs but guard otherwise
            tb_played += 1
            try:
                tw_n = int(re.sub(r'[^0-9]', '', str(tw))) if (tw is not None and str(tw).strip() != '') else None
                tl_n = int(re.sub(r'[^0-9]', '', str(tl))) if (tl is not None and str(tl).strip() != '') else None
            except Exception:
                tw_n = tl_n = None
            # determine player's score vs opponent: if player is winner, player's tb value is in *_winner columns, else *_loser columns
            if tw_n is not None and tl_n is not None:
                if is_winner:
                    if tw_n > tl_n: tb_won += 1
                else:
                    if tl_n > tw_n: tb_won += 1
            else:
                # if only one side present, infer conservatively
                if is_winner and tw and not tl:
                    tb_won += 1
                if (not is_winner) and tl and not tw:
                    tb_won += 1

    m['tiebreaks_played'] = int(tb_played)
    m['tiebreaks_won'] = int(tb_won)
    m['tiebreak_win_rate'] = (float(tb_won) / tb_played) if tb_played > 0 else None

    # match_time
    mtsec = None
    for c in ('match_time_total', 'match_time', 'match_time_seconds'):
        if c in r.index and str(r.get(c, '')).strip() != '':
            mtsec = parse_time_to_seconds(r.get(c)); break
    if mtsec is None:
        key = 'settime_tot_winner' if is_winner else 'settime_tot_loser' if is_winner is not None else None
        if key and key in r.index and str(r.get(key, '')).strip() != '':
            mtsec = to_float_safe(r.get(key))
    m['match_time_seconds'] = float(mtsec) if mtsec is not None else None
    m['match_time_hours'] = (float(mtsec)/3600.0) if mtsec is not None else None

    # ranking seeds from csv if present (may be blank)
    try:
        m['winner_seed'] = safe_int(r.get('winner_seed') if 'winner_seed' in r.index else r.get('winner_seed') if 'winner_seed' in r.index else r.get('winner_seed'))
    except Exception:
        m['winner_seed'] = None
    try:
        m['loser_seed'] = safe_int(r.get('loser_seed') if 'loser_seed' in r.index else r.get('loser_seed') if 'loser_seed' in r.index else r.get('loser_seed'))
    except Exception:
        m['loser_seed'] = None

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

def build_detailed_stats(matches_df, player_id, rankings_entries=None, host_event_map=None):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # collect rows where player appears
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {
            'player_id': pid,
            'career': {},
            'stats_by_year': {},
            'stats_by_month': {},
            'career_by_surface': {},
            'available_years': [],
            'available_surfaces': [],
            'meta': {'matches': 0, 'generated_at': datetime.utcnow().isoformat() + 'Z'}
        }
    df = pd.concat(frames, ignore_index=True, sort=False)

    # canonical player name
    player_name = pid
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            vals = df[col].dropna().astype(str).tolist()
            if vals:
                player_name = vals[0]
                break

    # containers
    career_stat_agg = {}
    career_by_surface = defaultdict(lambda: {})
    stats_by_year = {}
    stats_by_month = {}
    matches_out = []

    career_counts = {'matches_played':0,'matches_won':0,'matches_lost':0,'sets_won':0,'sets_lost':0,'tiebreaks_played':0,'tiebreaks_won':0,'total_match_time_seconds':0.0,'match_time_count':0}
    ranking_buckets = defaultdict(lambda: {'matches':0,'wins':0})
    upsets = {'wins_vs_better':0,'matches_vs_better':0,'losses_vs_worse':0,'matches_vs_worse':0}

    available_surfaces = set()
    available_years = set()

    for idx, r in df.iterrows():
        # determine winner flag
        is_winner = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_winner = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_winner = False

        # basic metadata
        # determine event year: prefer event_year column, otherwise fallback to __src_year extracted from filename
        e_year = str(r.get('event_year') or '').strip()
        if not e_year:
            e_year = str(r.get('__src_year') or '').strip()

        match_date_raw = r.get('start_date') or r.get('match_date') or ''
        match_date_iso = parse_date_only(match_date_raw)
        match_date_dt = None
        try:
            if match_date_iso:
                match_date_dt = datetime.fromisoformat(match_date_iso).date()
        except Exception:
            match_date_dt = None
        event_id = str(r.get('event_id') or '')
        tourney_name = r.get('tourney_name') or ''
        surface = (r.get('surface') or '').strip().lower() or 'unknown'
        if surface: available_surfaces.add(surface)
        if e_year: available_years.add(e_year)

        # opponent info
        opponent_name = ''
        opponent_id = None
        if is_winner is True:
            opponent_name = r.get('player_loser') or r.get('loser_player_name') or ''
            opponent_id = r.get('player_id_loser') if 'player_id_loser' in r.index else None
        elif is_winner is False:
            opponent_name = r.get('player_winner') or r.get('winner_player_name') or ''
            opponent_id = r.get('player_id_winner') if 'player_id_winner' in r.index else None

        # extract metrics
        m = extract_player_match_metrics(r, pid)
        m.setdefault('tiebreaks_played', 0); m.setdefault('tiebreaks_won', 0); m.setdefault('tiebreak_win_rate', None)
        m.setdefault('breakpoints_faced', None); m.setdefault('breakpoints_converted', None); m.setdefault('breakpoints_converted_pct', None)
        m.setdefault('service_games_lost_rate', None); m.setdefault('match_time_seconds', None); m.setdefault('match_time_hours', None)

        # append match record
        matches_out.append({
            'match_id': str(r.get('match_id') or ''),
            'event_id': event_id,
            'event_year': e_year,
            'start_date': match_date_iso,
            'opponent': str(opponent_name) if opponent_name else '',
            'opponent_id': str(opponent_id).strip().upper() if opponent_id not in (None,'') else None,
            'is_win': bool(is_winner) if is_winner is not None else None,
            'score': str(r.get('score_string') or r.get('score') or ''),
            'aces': m.get('aces'),
            'doublefaults': m.get('doublefaults'),
            'aces_per_service_point': m.get('aces_per_service_point'),
            'doublefaults_per_service_point': m.get('doublefaults_per_service_point'),
            'firstserve_percent': m.get('firstserve_percent'),
            'firstserve_points_won_percent': m.get('firstserve_points_won_percent'),
            'secondserve_points_won_percent': m.get('secondserve_points_won_percent'),
            'service_points_won_percent': m.get('service_points_won_percent'),
            'return_points_won_percent': m.get('return_points_won_percent'),
            'breakpoints_faced': m.get('breakpoints_faced'),
            'breakpoints_converted': m.get('breakpoints_converted'),
            'breakpoints_converted_rate': m.get('breakpoints_converted_pct'),
            'service_games_lost_rate': m.get('service_games_lost_rate'),
            'tiebreaks_played': m.get('tiebreaks_played'),
            'tiebreaks_won': m.get('tiebreaks_won'),
            'tiebreak_win_rate': m.get('tiebreak_win_rate'),
            'match_time_seconds': m.get('match_time_seconds'),
            'match_time_hours': m.get('match_time_hours'),
            'surface': surface,
            'round': str(r.get('round') or '')
        })

        # update career counts
        career_counts['matches_played'] += 1
        if is_winner is True:
            career_counts['matches_won'] += 1
        elif is_winner is False:
            career_counts['matches_lost'] += 1

        # sets parsing (best-effort)
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

        career_counts['tiebreaks_played'] += int(m.get('tiebreaks_played') or 0)
        career_counts['tiebreaks_won'] += int(m.get('tiebreaks_won') or 0)

        if m.get('match_time_seconds') is not None:
            try:
                career_counts['total_match_time_seconds'] += float(m.get('match_time_seconds'))
                career_counts['match_time_count'] += 1
            except Exception:
                pass

        # aggregate numeric fields to career and per-year and per-month and per-surface
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
            'breakpoints_converted_rate': to_float_safe(m.get('breakpoints_converted_pct')),
            'service_games_lost_rate': to_float_safe(m.get('service_games_lost_rate')),
            'tiebreak_win_rate': to_float_safe(m.get('tiebreak_win_rate')),
            'match_time_hours': to_float_safe(m.get('match_time_hours'))
        }
        for k,v in numeric_fields.items():
            add_numeric_agg(career_stat_agg, k, v)
            # per-surface
            add_numeric_agg(career_by_surface[surface].setdefault('stat_agg', {}), k, v)

        # per-year
        if e_year:
            if e_year not in stats_by_year:
                stats_by_year[e_year] = {'matches_played':0,'matches_won':0,'matches_lost':0,'sets_won':0,'sets_lost':0,'tiebreaks_played':0,'tiebreaks_won':0,'total_match_time_seconds':0.0,'match_time_count':0,'stat_agg':{}, 'by_surface': defaultdict(lambda: {'stat_agg': {}})}
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
                    sy['total_match_time_seconds'] += float(m.get('match_time_seconds')); sy['match_time_count'] += 1
                except Exception: pass
            for k,v in numeric_fields.items():
                add_numeric_agg(sy['stat_agg'], k, v)
                add_numeric_agg(sy['by_surface'][surface]['stat_agg'], k, v)

        # per-month
        month_key = None
        if match_date_dt:
            month_key = match_date_dt.strftime('%Y-%m')
            if month_key not in stats_by_month:
                stats_by_month[month_key] = {'matches_played':0,'matches_won':0,'matches_lost':0,'stat_agg':{}, 'by_surface': defaultdict(lambda: {'stat_agg': {}})}
            sm = stats_by_month[month_key]
            sm['matches_played'] += 1
            if is_winner:
                sm['matches_won'] += 1
            else:
                sm['matches_lost'] += 1
            for k,v in numeric_fields.items():
                add_numeric_agg(sm['stat_agg'], k, v)
                add_numeric_agg(sm['by_surface'][surface]['stat_agg'], k, v)

        # ranking/upset logic using rankings_entries (if available)
        player_rank = None
        opp_rank = None
        if rankings_entries and match_date_dt:
            # find full names in CSV to match
            # prefer winner_player_name / loser_player_name or player_winner/player_loser
            winner_name = r.get('winner_player_name') or r.get('player_winner') or ''
            loser_name = r.get('loser_player_name') or r.get('player_loser') or ''
            # short forms
            winner_short = normalize_name_for_match(winner_name) if winner_name else ''
            loser_short = normalize_name_for_match(loser_name) if loser_name else ''
            # find ranking snapshot nearest <= match_date
            # get player and opponent depending on which side
            if is_winner is True:
                player_rank = get_rank_for_player_on_date(winner_short, match_date_dt, rankings_entries)
                opp_rank = get_rank_for_player_on_date(loser_short, match_date_dt, rankings_entries)
            elif is_winner is False:
                player_rank = get_rank_for_player_on_date(loser_short, match_date_dt, rankings_entries)
                opp_rank = get_rank_for_player_on_date(winner_short, match_date_dt, rankings_entries)
            # bucket mapping for opponent
            if opp_rank is not None:
                b = rank_bucket_from_rank(opp_rank)
                if b:
                    ranking_buckets[b]['matches'] += 1
                    if is_winner:
                        ranking_buckets[b]['wins'] += 1
            # upsets counting
            if player_rank is not None and opp_rank is not None:
                if opp_rank < player_rank:
                    upsets['matches_vs_better'] += 1
                    if is_winner:
                        upsets['wins_vs_better'] += 1
                if opp_rank > player_rank:
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
        'total_match_time_hours': (career_counts['total_match_time_seconds'] / 3600.0) if career_counts['total_match_time_seconds'] else 0,
        'match_time_count': career_counts['match_time_count'],
        'stat_agg': finalize_numeric_agg(career_stat_agg)
    }

    # finalize per-year & per-month & career_by_surface
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
            'by_surface': {}
        }
        for surf, obj in sy['by_surface'].items():
            sby[y]['by_surface'][surf] = {'stat_agg': finalize_numeric_agg(obj['stat_agg'])}

    smb = {}
    for mkey, sm in stats_by_month.items():
        smb[mkey] = {'matches_played': sm['matches_played'], 'matches_won': sm['matches_won'], 'matches_lost': sm['matches_lost'], 'stat_agg': finalize_numeric_agg(sm['stat_agg']), 'by_surface': {}}
        for surf, obj in sm['by_surface'].items():
            smb[mkey]['by_surface'][surf] = {'stat_agg': finalize_numeric_agg(obj['stat_agg'])}

    career_by_surface_out = {}
    for surf, obj in career_by_surface.items():
        career_by_surface_out[surf] = {'stat_agg': finalize_numeric_agg(obj.get('stat_agg', {}))}

    ranking_buckets_out = {k: {'matches': v['matches'], 'wins': v['wins'], 'win_rate': ( (v['wins'] / v['matches']) if v['matches']>0 else None )} for k,v in ranking_buckets.items()}

    upsets_out = {
        'wins_vs_better_count': int(upsets.get('wins_vs_better',0)),
        'matches_vs_better': int(upsets.get('matches_vs_better',0)),
        'wins_vs_better_rate': ( (upsets.get('wins_vs_better',0) / upsets.get('matches_vs_better',1)) if upsets.get('matches_vs_better',0)>0 else None ),
        'losses_vs_worse_count': int(upsets.get('losses_vs_worse',0)),
        'matches_vs_worse': int(upsets.get('matches_vs_worse',0)),
        'losses_vs_worse_rate': ( (upsets.get('losses_vs_worse',0) / upsets.get('matches_vs_worse',1)) if upsets.get('matches_vs_worse',0)>0 else None )
    }

    result = {
        'player_id': pid,
        'player_name': player_name,
        'meta': {'matches': len(matches_out), 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v2'},
        'career': career_stats,
        'career_by_surface': career_by_surface_out,
        'stats_by_year': sby,
        'stats_by_month': smb,
        'ranking_buckets': ranking_buckets_out,
        'upsets': upsets_out,
        'available_years': sorted(list(available_years)),
        'available_surfaces': sorted(list(available_surfaces)),
        'matches': matches_out
    }

    return result

# ---------------- CLI Main ----------------

def main(matches_dir, out_dir, rankings_dir=None, host_event_map_path=None, player_list=None, limit_players=None):
    print("[dstats] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[dstats] matches rows:", len(matches), "columns:", len(matches.columns))

    # load rankings
    rankings_entries = []
    if rankings_dir:
        print("[dstats] Loading rankings from", rankings_dir)
        rankings_entries = load_atp_rankings(rankings_dir)
        print(f"[dstats] {len(rankings_entries)} ranking snapshots loaded")

    host_map = None
    if host_event_map_path:
        try:
            with open(host_event_map_path, 'r', encoding='utf8') as f:
                host_map = json.load(f)
            print("[dstats] Loaded host_event_map from", host_event_map_path)
        except Exception as e:
            print("[dstats] Warning: failed to load host_event_map:", e)
            host_map = None

    # discover player ids
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
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
            obj = build_detailed_stats(matches, pid, rankings_entries=rankings_entries, host_event_map=host_map)
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
    ap.add_argument("--rankings-dir", default="./atpp_rankings", help="Directory containing ATP ranking CSV snapshots")
    ap.add_argument("--host-event-map", default=None, help="Optional JSON file path containing HOST_COUNTRY_TO_EVENT_IDS mapping")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process")
    ap.add_argument("--player", help="Process a single player id (e.g. S0AG)")
    args = ap.parse_args()
    plist = [args.player] if args.player else None
    main(args.matches_dir, args.out_dir, rankings_dir=args.rankings_dir, host_event_map_path=args.host_event_map, player_list=plist, limit_players=args.limit_players)
