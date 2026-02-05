#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scenarios.py - Module 3: generate scenario datasets per player

Usage:
  python generate_scenarios.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200

Produces JSON per player at: <out_dir>/players/<PLAYER>.scenarios.json

Output schema (high level):
{
  "meta": {...},
  "scenarios": {
    "non_gs": { "wins_in_2_sets": {"count":N,"denominator":D}, ... },
    "gs": { "wins_after_losing_first_two_sets": {"count":N,"denominator":D}, ... },
    "retirements": {
       "count": total_retirements,
       "by_set": { "2": 12, "3": 5, ... },
       "examples": [ {match...}, ... ]
    },
    "samples": { "<scenario_key>": [ {...}, ... ] }
  }
}
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

def str_safe(v):
    """
    Return a safe string for values that may be None / NaN / floats.
    - None or pd.isna(v) -> ''
    - otherwise -> str(v)
    Use this before calling .strip() or doing string ops.
    """
    try:
        if v is None:
            return ''
        # pandas NA / numpy nan handling
        if isinstance(v, float) and pd.isna(v):
            return ''
        # pd.NaT etc.
        if hasattr(v, 'dtype') and pd.isna(v):
            return ''
        return str(v)
    except Exception:
        return ''


def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid):
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ''
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
            if isinstance(v, str) and v.strip() != '':
                raw = str(v).strip()
                # accept also comma separated in single field
                if '-' in raw:
                    left, right = raw.split('-', 1)
                    arr.append((left.strip(), right.strip(), raw))
                else:
                    # if not in left-right form, skip
                    arr.append((raw, '', raw))
    return arr

def player_won_row(row, player_id):
    """True if row indicates player_id is winner, False if loser, None if unknown."""
    pid = normalize_player_id(player_id)
    if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
        return True
    if 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
        return False
    # fallback to names (best-effort)
    if 'winner_player_name' in row.index and 'loser_player_name' in row.index:
        wn = (row.get('winner_player_name') or '').strip()
        ln = (row.get('loser_player_name') or '').strip()
        if wn and ln:
            # if player's name appears exactly in winner_player_name we assume winner
            # else if equals loser_player_name assume loser
            # else unknown
            # This is a weak fallback but rarely needed
            # normalized minimal compare
            try:
                if isinstance(pid, str) and pid and '.' in pid:
                    # pid like "J. DOE" maybe matching initials - skip fallback
                    return None
            except Exception:
                pass
    return None

def _parse_int_safe(s):
    try:
        if s is None: return None
        s2 = str(s).strip()
        if s2 == '': return None
        # remove parentheses or tiebreak suffix e.g. "7(7)" -> "7"
        m = re.match(r'^\s*(\d+)', s2)
        if m:
            return int(m.group(1))
        # last resort extract digits
        d = re.sub(r'[^0-9]', '', s2)
        if d == '': return None
        return int(d)
    except Exception:
        return None

def is_complete_set_score(left_str, right_str):
    """
    Decide whether a set score represents a *complete* set.
    Rules (heuristic, robust):
      - both sides parse to integers
      - winner_games >= 6 and (winner_games - loser_games >= 2 OR winner_games == 7)
      - this accepts 6-4, 7-6, 8-6 etc.
    """
    a = _parse_int_safe(left_str)
    b = _parse_int_safe(right_str)
    if a is None or b is None:
        return False
    # identify winner and loser in set
    if a == b:
        return False
    winner = a if a > b else b
    loser = b if a > b else a
    # winner must have at least 6
    if winner < 6:
        return False
    # if winner is 7, accept (tie-break or 7-5)
    if winner == 7:
        return True
    # accept winner>=6 and diff >= 2 (covers 6-4,8-6,9-7,...)
    if (winner - loser) >= 2:
        return True
    return False


# ---------- Helpers used by the new profile-by-set builder ----------
def parse_time_to_seconds(v):
    """
    Accepts:
      - 'HH:MM:SS' or 'MM:SS' strings,
      - numeric seconds as int/float,
      - pandas Timestamp/Timedelta,
      - empty/NaN -> None
    Returns seconds as int or None.
    """
    try:
        if v is None:
            return None
        if isinstance(v, (int,float)) and not pd.isna(v):
            return int(v)
        s = str(v).strip()
        if s == '':
            return None
        # try HH:MM:SS or MM:SS
        if ':' in s:
            parts = s.split(':')
            parts = [p.strip() for p in parts if p.strip()!='']
            # right-align: if len==3 -> h:m:s, if len==2 -> m:s
            if len(parts) == 3:
                h,m,se = parts
                return int(h)*3600 + int(m)*60 + int(float(se))
            if len(parts) == 2:
                m,se = parts
                return int(m)*60 + int(float(se))
        # try numeric string (seconds)
        if re.match(r'^\d+(\.\d+)?$', s):
            return int(float(s))
    except Exception:
        pass
    return None

def safe_div(numer, denom):
    try:
        if denom is None or denom == 0:
            return None
        return float(numer) / float(denom)
    except Exception:
        return None


# ---------- Helpers used by scenarios builder ----------
def parse_time_to_seconds(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)) and not pd.isna(v):
            return int(v)
        s = str(v).strip()
        if s == '':
            return None
        if ':' in s:
            parts = s.split(':')
            parts = [p.strip() for p in parts if p.strip() != '']
            if len(parts) == 3:
                h, m, se = parts
                return int(h) * 3600 + int(m) * 60 + int(float(se))
            if len(parts) == 2:
                m, se = parts
                return int(m) * 60 + int(float(se))
        if re.match(r'^\d+(\.\d+)?$', s):
            return int(float(s))
    except Exception:
        pass
    return None

def safe_div(n, d):
    try:
        if d is None or d == 0:
            return None
        return float(n) / float(d)
    except Exception:
        return None

# ---------- New/Fixed build function ----------
def build_scenarios_for_player(matches_df, player_id, sample_limit=6):
    """
    Builds:
      - legacy-like non_gs metrics (for backward compatibility with the UI)
      - new profile_by_set aggregations (sets 1..3)
      - retirements summary
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # select player's matches
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and hasattr(cond_w, 'any') and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and hasattr(cond_l, 'any') and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {'meta': {'player_id': pid, 'player_name': '', 'generated_at': datetime.utcnow().isoformat()+'Z', 'version': 'v2-profile-by-set', 'matches': 0},
                'non_gs': {}, 'profile_by_set': {}, 'retirements': {'count':0,'by_set':{},'examples':[]}}

    df = pd.concat(frames, ignore_index=True, sort=False)

    # --- Prepare accumulators for profile_by_set ---
    sets_acc = {}
    for i in (1,2,3):
        sets_acc[i] = {
            'sum_set_time_sec': 0, 'count_set_time': 0,
            # breaks suffered = opponent breakptsconv
            'sum_breaks_suffered': 0.0, 'count_breaks_suffered': 0,
            # breaks obtained = player's breakptsconv
            'sum_breaks_obtained': 0.0, 'count_breaks_obtained': 0,
            # first serve: numerator = ptsplayed1stserv (played), denominator = totservplayed
            'sum_firstserve_played': 0.0, 'sum_firstserve_total': 0.0,
            # first-serve won: numerator = ptswon1stserv, denominator = ptsplayed1stserv
            'sum_firstserve_won': 0.0, 'sum_firstserve_won_total': 0.0,
            # aces and double faults
            'sum_aces': 0.0, 'count_aces': 0,
            'sum_doublefaults': 0.0, 'count_doublefaults': 0,
            'matches_with_set': 0
        }

    # legacy non_gs counters (keep previous semantics)
    non_gs = defaultdict(lambda: {'count': 0})
    den = {
        'non_gs_matches_2_sets': 0,
        'non_gs_matches_3_sets': 0,
        'non_gs_lost_first_set': 0,
        'non_gs_won_first_set': 0
    }

    # retirements
    retire_count = 0
    retire_by_set = Counter()
    retire_examples = []

    # patterns
    patterns = {'VV':0,'PP':0,'PVP':0,'PVV':0,'VPV':0,'VPP':0}
    matches_2_sets = 0
    matches_3_sets = 0

    total_matches = 0
    for idx, r in df.iterrows():
        total_matches += 1
        # determine if player is winner in this row
        player_is_winner = None
        try:
            if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
                player_is_winner = True
            elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
                player_is_winner = False
        except Exception:
            player_is_winner = None

        # parse sets present (up to 5)
        parsed_sets = []
        complete_sets_count = 0
        # helper to read winner/loser int score columns
        for i in range(1,6):
            ws_col = f"winner_score_set{i}"
            ls_col = f"loser_score_set{i}"
            gen_col = f"set{i}_score"
            a = None; b = None
            if ws_col in r.index and r.get(ws_col) not in (None, ''):
                a = _parse_int_safe(r.get(ws_col))
            if ls_col in r.index and r.get(ls_col) not in (None, ''):
                b = _parse_int_safe(r.get(ls_col))
            if a is None and b is None and gen_col in r.index and str_safe(r.get(gen_col)).strip() != '':
                raw = str_safe(r.get(gen_col))
                if '-' in raw:
                    left, right = raw.split('-',1)
                    a = _parse_int_safe(left); b = _parse_int_safe(right)
            if a is not None and b is not None and a != b:
                parsed_sets.append((a,b))
                complete_sets_count += 1
            else:
                parsed_sets.append((None,None))

        # compute winner_sets/loser_sets in row (row refers to winner/loser sides)
        winner_sets = sum(1 for a,b in parsed_sets[:5] if a is not None and b is not None and a > b)
        loser_sets  = sum(1 for a,b in parsed_sets[:5] if a is not None and b is not None and a < b)
        # map to player perspective if possible
        if player_is_winner is True:
            player_set_wins = winner_sets
            opp_set_wins = loser_sets
        elif player_is_winner is False:
            player_set_wins = loser_sets
            opp_set_wins = winner_sets
        else:
            player_set_wins = None
            opp_set_wins = None

        # retirement detection (as before): if player didn't reach 2 and there is set data or explicit RET
        req_to_win = 2
        explicit_ret = False
        match_msg = (r.get('match_message') or '') or ''
        match_status = (r.get('match_status') or '') or ''
        if isinstance(match_msg, str) and match_msg.strip().lower().startswith('ret'): explicit_ret = True
        if isinstance(match_status, str) and 'ret' in match_status.strip().lower(): explicit_ret = True

        is_retire = False
        retire_set_number = None
        if player_set_wins is not None and player_set_wins < req_to_win:
            if complete_sets_count > 0 or explicit_ret:
                is_retire = True
                # retirement set heuristics: first incomplete set index with a raw set present else next set after complete
                found_incomplete = False
                for i in range(1,6):
                    a,b = parsed_sets[i-1]
                    if a is None or b is None:
                        gen = f"set{i}_score"
                        if gen in r.index and str_safe(r.get(gen)).strip() != '':
                            retire_set_number = i
                            found_incomplete = True
                            break
                if not found_incomplete:
                    retire_set_number = complete_sets_count + 1

        if is_retire:
            retire_count += 1
            retire_by_set[str(retire_set_number)] += 1
            retire_examples.append({
                'match_id': str(r.get('match_id') or ''),
                'event_id': str(r.get('event_id') or ''),
                'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                'score': str(r.get('score_string') or r.get('score') or ''),
                'opponent': str(r.get('winner_player_name') or r.get('loser_player_name') or ''),
                'retire_set': retire_set_number
            })

        # collect per-set stats for sets 1..3
        for i in (1,2,3):
            # decide prefixes: my_pref = 'winner' if player_is_winner True else 'loser'
            if player_is_winner is True:
                my_pref = 'winner'; opp_pref = 'loser'
            elif player_is_winner is False:
                my_pref = 'loser'; opp_pref = 'winner'
            else:
                my_pref = None; opp_pref = None

            # set time
            st_col = f"settime_set{i}"
            st_sec = None
            if st_col in r.index:
                st_sec = parse_time_to_seconds(r.get(st_col))
            if st_sec is not None:
                sets_acc[i]['sum_set_time_sec'] += st_sec
                sets_acc[i]['count_set_time'] += 1

            # breaks suffered: opponent's breakptsconv_set{i}
            br_s_col = f"{opp_pref}_breakptsconv_set{i}" if opp_pref else None
            if br_s_col and br_s_col in r.index and str_safe(r.get(br_s_col)).strip() != '':
                try:
                    v = float(str_safe(r.get(br_s_col)))
                    sets_acc[i]['sum_breaks_suffered'] += v
                    sets_acc[i]['count_breaks_suffered'] += 1
                except Exception:
                    pass


            # breaks obtained: player's breakptsconv_set{i}
            br_g_col = f"{my_pref}_breakptsconv_set{i}" if my_pref else None
            if br_g_col and br_g_col in r.index and str_safe(r.get(br_g_col)).strip() != '':
                try:
                    v = float(str_safe(r.get(br_g_col)))
                    sets_acc[i]['sum_breaks_obtained'] += v
                    sets_acc[i]['count_breaks_obtained'] += 1
                except Exception:
                    pass


            # first serve: ptsplayed1stserv / totservplayed (player side)
            fs_played_col = f"{my_pref}_ptsplayed1stserv_set{i}" if my_pref else None
            fs_total_col  = f"{my_pref}_totservplayed_set{i}" if my_pref else None
            fs_played = None; fs_total = None
            if fs_played_col and fs_played_col in r.index:
                fs_played = _parse_int_safe(r.get(fs_played_col))
            if fs_total_col and fs_total_col in r.index:
                fs_total = _parse_int_safe(r.get(fs_total_col))
            # We accumulate sums to compute weighted ratio later: total_played / total_servplayed
            if fs_played is not None and fs_total is not None and fs_total > 0:
                # accumulate totals for first-serve played / total (to compute weighted pct)
                sets_acc[i]['sum_firstserve_played'] += (fs_played or 0)
                sets_acc[i]['sum_firstserve_total'] += (fs_total or 0)


            # first serve won: ptswon1stserv / ptsplayed1stserv
            fs_won_col = f"{my_pref}_ptswon1stserv_set{i}" if my_pref else None
            fs_won = None
            if fs_won_col and fs_won_col in r.index:
                fs_won = _parse_int_safe(r.get(fs_won_col))
            if fs_won is not None and fs_played is not None and fs_played > 0:
                sets_acc[i]['sum_firstserve_won'] += fs_won
                sets_acc[i]['sum_firstserve_won_total'] += fs_played

            # aces
            ac_col = f"{my_pref}_aces_set{i}" if my_pref else None
            if ac_col and ac_col in r.index and str_safe(r.get(ac_col)).strip() != '':
                try:
                    v = float(str_safe(r.get(ac_col)))
                    sets_acc[i]['sum_aces'] += v
                    sets_acc[i]['count_aces'] += 1
                except Exception:
                    pass


            # double faults
            df_col = f"{my_pref}_dblflt_set{i}" if my_pref else None
            if df_col and df_col in r.index and str_safe(r.get(df_col)).strip() != '':
                try:
                    v = float(str_safe(r.get(df_col)))
                    sets_acc[i]['sum_doublefaults'] += v
                    sets_acc[i]['count_doublefaults'] += 1
                except Exception:
                    pass


            # mark matches_with_set if present
            present = False
            if (f"winner_score_set{i}" in r.index and str_safe(r.get(f"winner_score_set{i}")).strip() != '') or \
               (f"loser_score_set{i}" in r.index and str_safe(r.get(f"loser_score_set{i}")).strip() != '') or \
               (f"set{i}_score" in r.index and str_safe(r.get(f"set{i}_score")).strip() != ''):
                present = True

            if present:
                sets_acc[i]['matches_with_set'] += 1

        # patterns (two-set and three-set)
        # build per-set results from player's perspective
        per_set_results = []
        complete_count_for_pattern = 0
        for i in (1,2,3):
            a,b = parsed_sets[i-1]
            if a is not None and b is not None:
                # a = winner games, b = loser games
                if player_is_winner is True:
                    res = 'V' if a > b else 'P'
                elif player_is_winner is False:
                    res = 'V' if b > a else 'P'
                else:
                    res = None
                if res:
                    per_set_results.append(res)
                    complete_count_for_pattern += 1
        if complete_count_for_pattern == 2:
            matches_2_sets += 1
            pat = ''.join(per_set_results[:2])
            if pat in patterns:
                patterns[pat] += 1
            # legacy non_gs counts (partially keep previous keys)
            # evaluate first-set lost/won logic for non-gs
            # check first set win/loss
            a1,b1 = parsed_sets[0]
            if a1 is not None and b1 is not None:
                # lost first?
                if player_is_winner is True:
                    lost_first = (a1 <= b1)
                elif player_is_winner is False:
                    lost_first = (b1 <= a1)
                else:
                    lost_first = None
                if lost_first is True:
                    den['non_gs_lost_first_set'] += 1
                    # if player then wins the match (player_set_wins > opp_set_wins)
                    if player_set_wins is not None and player_set_wins > opp_set_wins:
                        non_gs['wins_after_losing_first_set']['count'] += 1
                if lost_first is False:
                    den['non_gs_won_first_set'] += 1
                    if player_set_wins is not None and player_set_wins < opp_set_wins:
                        non_gs['losses_after_winning_first_set']['count'] += 1

            # wins/losses in 2-set matches
            den['non_gs_matches_2_sets'] += 1
            if player_set_wins is not None and player_set_wins > opp_set_wins:
                non_gs['wins_in_2_sets']['count'] += 1
            else:
                non_gs['losses_in_2_sets']['count'] += 1

        elif complete_count_for_pattern == 3:
            matches_3_sets += 1
            pat = ''.join(per_set_results[:3])
            if pat in patterns:
                patterns[pat] += 1
            # wins/losses in 3-set matches
            den['non_gs_matches_3_sets'] += 1
            if player_set_wins is not None and player_set_wins > opp_set_wins:
                non_gs['wins_in_3_sets']['count'] += 1
            else:
                non_gs['losses_in_3_sets']['count'] += 1

    # finalize profile_by_set
    profile_sets_out = {}
    for i in (1,2,3):
        s = sets_acc[i]
        avg_time_sec = int(s['sum_set_time_sec']/s['count_set_time']) if s['count_set_time']>0 else None
        avg_time_hms = None
        if avg_time_sec is not None:
            h = avg_time_sec // 3600
            m = (avg_time_sec % 3600) // 60
            sec = avg_time_sec % 60
            avg_time_hms = f"{h:02d}:{m:02d}:{sec:02d}" if h>0 else f"{m:02d}:{sec:02d}"

        avg_breaks_suffered = round(s['sum_breaks_suffered']/s['count_breaks_suffered'],3) if s['count_breaks_suffered']>0 else None
        avg_breaks_obtained  = round(s['sum_breaks_obtained']/s['count_breaks_obtained'],3) if s['count_breaks_obtained']>0 else None

        # first serve % : weighted sum of played / sum of totservplayed
        firstserve_pct = None
        if s['sum_firstserve_total'] and s['sum_firstserve_total'] > 0:
            firstserve_pct = round((s['sum_firstserve_played'] / s['sum_firstserve_total']) * 100.0, 2)
        firstserve_win_pct = None
        if s['sum_firstserve_won_total'] and s['sum_firstserve_won_total'] > 0:
            firstserve_win_pct = round((s['sum_firstserve_won'] / s['sum_firstserve_won_total']) * 100.0, 2)

        avg_aces = round(s['sum_aces']/s['count_aces'],3) if s['count_aces']>0 else None
        avg_dbl = round(s['sum_doublefaults']/s['count_doublefaults'],3) if s['count_doublefaults']>0 else None

        profile_sets_out[str(i)] = {
            'avg_set_time_sec': avg_time_sec,
            'avg_set_time_hms': avg_time_hms,
            'avg_breaks_suffered': avg_breaks_suffered,
            'avg_breaks_obtained': avg_breaks_obtained,
            'first_serve_pct': firstserve_pct,
            'first_serve_win_pct': firstserve_win_pct,
            'avg_aces': avg_aces,
            'avg_doublefaults': avg_dbl,
            'matches_with_set': int(s['matches_with_set'])
        }

    patterns_out = {}
    patterns_out['VV'] = {'count': int(patterns['VV']), 'denominator': int(matches_2_sets)}
    patterns_out['PP'] = {'count': int(patterns['PP']), 'denominator': int(matches_2_sets)}
    for k in ('PVP','PVV','VPV','VPP'):
        patterns_out[k] = {'count': int(patterns[k]), 'denominator': int(matches_3_sets)}

    # finalize legacy non_gs denominators
    non_gs_out = {}
    non_gs_out['wins_after_losing_first_set'] = {'count': int(non_gs['wins_after_losing_first_set']['count']), 'denominator': int(den['non_gs_lost_first_set'])}
    non_gs_out['losses_after_winning_first_set'] = {'count': int(non_gs['losses_after_winning_first_set']['count']), 'denominator': int(den['non_gs_won_first_set'])}
    non_gs_out['wins_in_2_sets'] = {'count': int(non_gs['wins_in_2_sets']['count']), 'denominator': int(den['non_gs_matches_2_sets'])}
    non_gs_out['losses_in_2_sets'] = {'count': int(non_gs['losses_in_2_sets']['count']), 'denominator': int(den['non_gs_matches_2_sets'])}
    non_gs_out['wins_in_3_sets'] = {'count': int(non_gs['wins_in_3_sets']['count']), 'denominator': int(den['non_gs_matches_3_sets'])}
    non_gs_out['losses_in_3_sets'] = {'count': int(non_gs['losses_in_3_sets']['count']), 'denominator': int(den['non_gs_matches_3_sets'])}

    retire_out = {'count': int(retire_count), 'by_set': dict((k,int(v)) for k,v in sorted(retire_by_set.items(), key=lambda x:int(x[0]))), 'examples': retire_examples[:sample_limit]}

    out = {
        'meta': {'player_id': pid, 'player_name': '', 'generated_at': datetime.utcnow().isoformat()+'Z', 'version': 'v2-profile-by-set', 'matches': len(df)},
        'non_gs': non_gs_out,
        'profile_by_set': {'sets': profile_sets_out, 'patterns': patterns_out, 'matches_2_sets': int(matches_2_sets), 'matches_3_sets': int(matches_3_sets)},
        'retirements': retire_out
    }
    return out


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
        try:
            obj = build_scenarios_for_player(matches, pid)
            out_path = os.path.join(players_dir, f"{pid}.scenarios.json")
            with open(out_path, 'w', encoding='utf8') as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[scenarios] ERROR building scenarios for {pid}: {e}")
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
