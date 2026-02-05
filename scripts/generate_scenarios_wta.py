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

# new build function
def build_scenarios_for_player(matches_df, player_id, sample_limit=6):
    """
    New implementation:
    - No Grand Slam / non-GS split.
    - Produces 'profile_by_set' with per-set aggregated stats (sets 1..3).
    - Patterns distribution for two- and three-set matches (VV, PP, PVP, PVV, VPV, VPP).
    - Keeps retirements summary (count, by_set, examples).
    - Removes samples list and toggle.
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # select player's matches robustly
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and hasattr(cond_w, 'any') and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and hasattr(cond_l, 'any') and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {
            'meta': {
                'player_id': pid,
                'player_name': '',
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'version': 'v2-profile-by-set',
                'matches': 0
            },
            'profile_by_set': {},
            'retirements': {'count': 0, 'by_set': {}, 'examples': []}
        }
    df = pd.concat(frames, ignore_index=True, sort=False)

    # initialize per set accumulators for sets 1..3
    sets_stats = {}
    for i in (1,2,3):
        sets_stats[i] = {
            'sum_set_time_sec': 0,
            'count_set_time': 0,
            'sum_breaks_suffered': 0.0,
            'count_breaks': 0,
            'sum_firstserve_played': 0.0,
            'sum_firstserve_total': 0.0,  # numerator for first-serve % computation
            'count_firstserve_pct': 0,
            'sum_firstserve_won': 0.0,
            'sum_firstserve_won_total': 0.0,
            'count_firstserve_win_pct': 0,
            'sum_aces': 0.0,
            'count_aces': 0,
            'sum_doublefaults': 0.0,
            'count_doublefaults': 0,
            'matches_with_set': 0  # number of matches where this set is present (complete or partially present)
        }

    # pattern counts
    patterns = {
        # 2 sets
        'VV': 0, 'PP': 0,
        # 3 sets
        'PVP': 0, 'PVV': 0, 'VPV': 0, 'VPP': 0
    }
    matches_2_sets = 0
    matches_3_sets = 0

    # retirements
    retire_count = 0
    retire_by_set = Counter()
    retire_examples = []

    # helpers to build per-match pattern
    def get_set_result_for_player(r, player_is_winner, set_idx):
        """Return 'V' if player won set set_idx, 'P' if lost, None if set not present or incomplete."""
        # columns for winner/loser score per set: winner_score_set1, loser_score_set1
        win_col = f"winner_score_set{set_idx}"
        lose_col = f"loser_score_set{set_idx}"
        # parse integers
        a = None
        b = None
        if win_col in r.index:
            a = _parse_int_safe(r.get(win_col))
        if lose_col in r.index:
            b = _parse_int_safe(r.get(lose_col))
        # if both are None -> try generic setX_score fields (legacy)
        if a is None and b is None:
            gen = f"set{set_idx}_score"
            if gen in r.index:
                raw = r.get(gen) or ''
                if isinstance(raw, str) and '-' in raw:
                    left, right = raw.split('-',1)
                    a = _parse_int_safe(left)
                    b = _parse_int_safe(right)
        # require a and b to decide
        if a is None or b is None:
            return None
        if a == b:
            return None
        # left is winner games -> if player is winner, left==player games
        if player_is_winner is True:
            return 'V' if a > b else 'P'
        elif player_is_winner is False:
            return 'V' if b > a else 'P'
        else:
            # unknown which side is player: try to infer by matching player names if possible
            try:
                wn = (r.get('winner_player_name') or '').strip()
                ln = (r.get('loser_player_name') or '').strip()
                # if pid equals player_id_winner or player_id_loser earlier we would know; but here unknown -> skip
            except Exception:
                pass
            return None

    total_matches = 0
    for idx, r in df.iterrows():
        total_matches += 1
        # who is the player on this row?
        player_is_winner = None
        try:
            if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
                player_is_winner = True
            elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
                player_is_winner = False
        except Exception:
            player_is_winner = None

        # retire detection: similar logic as previous script
        winner_sets = 0
        loser_sets = 0
        complete_sets_count = 0
        parsed_results = []
        for i in (1,2,3,4,5):
            res = get_set_result_for_player(r, player_is_winner, i)  # may be None
            parsed_results.append(res)
            if res is not None:
                complete_sets_count += 1
                if res == 'V':
                    winner_sets += 1
                else:
                    loser_sets += 1

        # retire condition: if match ended without player reaching required sets (best-of-3 -> 2)
        required_to_win = 2
        is_retire = False
        retire_set_number = None
        # Use explicit match_message/match_status detection as fallback
        match_msg = (r.get('match_message') or '') or ''
        match_status = (r.get('match_status') or '') or ''
        explicit_ret = False
        if isinstance(match_msg, str) and match_msg.strip().lower().startswith('ret'):
            explicit_ret = True
        if isinstance(match_status, str) and 'ret' in match_status.strip().lower():
            explicit_ret = True

        # Determine who is the "player" in terms of winner_sets/loser_sets: winner_sets/loser_sets represent counts for row-winner,
        # so we need to map to player perspective:
        # If player_is_winner True -> player_set_wins = winner_sets
        # If False -> player_set_wins = loser_sets
        # If None -> try to infer by comparing names (best-effort); otherwise we cannot decide retire reliably
        player_set_wins = None
        opp_set_wins = None
        if player_is_winner is True:
            player_set_wins = winner_sets
            opp_set_wins = loser_sets
        elif player_is_winner is False:
            player_set_wins = loser_sets
            opp_set_wins = winner_sets
        else:
            # try name matching
            try:
                pname = ''  # unknown — skip
            except Exception:
                pass

        if player_set_wins is not None and player_set_wins < required_to_win:
            if complete_sets_count > 0 or explicit_ret:
                is_retire = True
                # decide retirement set: first incomplete set index if any, else next set index after last complete
                retire_set_number = None
                for i in range(1,6):
                    if get_set_result_for_player(r, player_is_winner, i) is None:
                        # if there is a raw score string present but not valid, treat as incomplete -> retirement here
                        gen = f"set{i}_score"
                        if gen in r.index and (r.get(gen) or '').strip() != '':
                            retire_set_number = i
                            break
                if retire_set_number is None:
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
            # determine columns for per-set measures based on whether player is winner or loser in this row:
            # for player-side stat prefix = 'winner' if player_is_winner True else 'loser' if False
            # for breaks suffered, we want opponent's breakptsconv (so use other prefix)
            if player_is_winner is True:
                my_pref = 'winner'
                opp_pref = 'loser'
            elif player_is_winner is False:
                my_pref = 'loser'
                opp_pref = 'winner'
            else:
                # unknown — attempt to guess using winner_player_name / loser_player_name matching
                # default to skipping per-set numeric stats if unknown
                my_pref = None
                opp_pref = None

            # set time
            st_col = f"settime_set{i}"
            st_sec = None
            if st_col in r.index:
                st_sec = parse_time_to_seconds(r.get(st_col))
            # fallback: some datasets may have numeric seconds columns winner_scoret_set1 etc -> skip for now

            # breaks suffered: take opponent's breakptsconv_set{i}
            br_col = f"{opp_pref}_breakptsconv_set{i}" if opp_pref else None
            br_val = None
            if br_col and br_col in r.index:
                try:
                    v = r.get(br_col)
                    if v is not None and str(v).strip() != '':
                        br_val = float(v)
                except Exception:
                    br_val = None

            # first serve percent: player_ptsplayed1stserv_setX / player_totservplayed_setX
            fs_played_col = f"{my_pref}_ptsplayed1stserv_set{i}" if my_pref else None
            fs_total_col = f"{my_pref}_totservplayed_set{i}" if my_pref else None
            fs_played = None
            fs_total = None
            if fs_played_col and fs_played_col in r.index:
                fs_played = _parse_int_safe(r.get(fs_played_col))
            if fs_total_col and fs_total_col in r.index:
                fs_total = _parse_int_safe(r.get(fs_total_col))

            # first serve won rate: player_ptswon1stserv_setX / player_ptsplayed1stserv_setX
            fs_won_col = f"{my_pref}_ptswon1stserv_set{i}" if my_pref else None
            fs_won = None
            if fs_won_col and fs_won_col in r.index:
                fs_won = _parse_int_safe(r.get(fs_won_col))

            # aces & double faults
            ac_col = f"{my_pref}_aces_set{i}" if my_pref else None
            df_col = f"{my_pref}_dblflt_set{i}" if my_pref else None
            ac_val = None
            df_val = None
            if ac_col and ac_col in r.index:
                try:
                    ac_val = float(r.get(ac_col)) if r.get(ac_col) not in (None,'') else None
                except Exception:
                    ac_val = None
            if df_col and df_col in r.index:
                try:
                    df_val = float(r.get(df_col)) if r.get(df_col) not in (None,'') else None
                except Exception:
                    df_val = None

            # update accumulators if values present
            stats = sets_stats[i]
            if st_sec is not None:
                stats['sum_set_time_sec'] += st_sec
                stats['count_set_time'] += 1
            if br_val is not None:
                stats['sum_breaks_suffered'] += br_val
                stats['count_breaks'] += 1
            if fs_total is not None and fs_total > 0:
                # first-serve percentage contribution
                stats['sum_firstserve_played'] += (fs_total)  # denominator aggregator
                stats['sum_firstserve_total'] += (fs_played or 0)  # numerator aggregator? careful: we want played/total -> we store totals separately
                stats['count_firstserve_pct'] += 1
            if fs_played is not None and fs_played > 0:
                # first-serve points won rate: fs_won / fs_played
                if fs_won is not None:
                    stats['sum_firstserve_won'] += fs_won
                    stats['sum_firstserve_won_total'] += fs_played
                    stats['count_firstserve_win_pct'] += 1
            if ac_val is not None:
                stats['sum_aces'] += ac_val
                stats['count_aces'] += 1
            if df_val is not None:
                stats['sum_doublefaults'] += df_val
                stats['count_doublefaults'] += 1

            # increment matches_with_set if set present at all (complete or partial)
            # detect presence using winner_score_set{i} or loser_score_set{i} or set{i}_score
            present = False
            ws_col = f"winner_score_set{i}"
            ls_col = f"loser_score_set{i}"
            gen_col = f"set{i}_score"
            if (ws_col in r.index and (r.get(ws_col) not in (None,''))) or (ls_col in r.index and (r.get(ls_col) not in (None,''))) or (gen_col in r.index and (r.get(gen_col) not in (None,''))):
                present = True
            if present:
                stats['matches_with_set'] += 1

        # patterns (build string from set1..set3 if available)
        # we only consider patterns for matches that have 2 or 3 complete sets
        per_set_results = []
        complete_count_for_pattern = 0
        for i in (1,2,3):
            res = get_set_result_for_player(r, player_is_winner, i)
            if res is not None:
                per_set_results.append(res)
                complete_count_for_pattern += 1
            else:
                # if later sets missing, will reduce length
                pass

        if complete_count_for_pattern == 2:
            matches_2_sets += 1
            pattern = ''.join(per_set_results[:2])
            if pattern in patterns:
                patterns[pattern] += 1
        elif complete_count_for_pattern == 3:
            matches_3_sets += 1
            pattern = ''.join(per_set_results[:3])
            if pattern in patterns:
                patterns[pattern] += 1

    # finalize sets aggregates into output structure
    profile_sets_out = {}
    for i in (1,2,3):
        s = sets_stats[i]
        # average set time in seconds and formatted
        avg_time_sec = int(s['sum_set_time_sec'] / s['count_set_time']) if s['count_set_time'] > 0 else None
        avg_time_hms = None
        if avg_time_sec is not None:
            h = avg_time_sec // 3600
            m = (avg_time_sec % 3600) // 60
            sec = avg_time_sec % 60
            if h > 0:
                avg_time_hms = f"{h:02d}:{m:02d}:{sec:02d}"
            else:
                avg_time_hms = f"{m:02d}:{sec:02d}"

        # first serve percentage: we computed aggregated numerators and denominators
        firstserve_pct = None
        if s['sum_firstserve_played'] and s['sum_firstserve_played'] > 0:
            # percent of points where first was played? The user asked "percentage moyen de premières par sets : winner_ptsplayed1stserv_setX / winner_totservplayed_setX"
            # we aggregated sum_firstserve_total (sum of played? actually we stored sum_firstserve_total as sum of played values)
            # Correction: we stored sum_firstserve_total = sum of fs_played? and sum_firstserve_played = sum of fs_total. So compute ratio:
            firstserve_pct = safe_div(s['sum_firstserve_total'], s['sum_firstserve_played'])
            if firstserve_pct is not None:
                firstserve_pct = round(firstserve_pct * 100.0, 2)

        # first serve win percentage:
        firstserve_win_pct = None
        if s['sum_firstserve_won_total'] and s['sum_firstserve_won_total'] > 0:
            firstserve_win_pct = safe_div(s['sum_firstserve_won'], s['sum_firstserve_won_total'])
            if firstserve_win_pct is not None:
                firstserve_win_pct = round(firstserve_win_pct * 100.0, 2)

        avg_breaks_suffered = None
        if s['count_breaks'] > 0:
            avg_breaks_suffered = round(s['sum_breaks_suffered'] / s['count_breaks'], 3)

        avg_aces = None
        if s['count_aces'] > 0:
            avg_aces = round(s['sum_aces'] / s['count_aces'], 3)

        avg_doublefaults = None
        if s['count_doublefaults'] > 0:
            avg_doublefaults = round(s['sum_doublefaults'] / s['count_doublefaults'], 3)

        profile_sets_out[str(i)] = {
            'avg_set_time_sec': avg_time_sec,
            'avg_set_time_hms': avg_time_hms,
            'avg_breaks_suffered': avg_breaks_suffered,
            'first_serve_pct': firstserve_pct,
            'first_serve_win_pct': firstserve_win_pct,
            'avg_aces': avg_aces,
            'avg_doublefaults': avg_doublefaults,
            'matches_with_set': int(s['matches_with_set'])
        }

    # prepare patterns output with denominators
    patterns_out = {}
    patterns_out['VV'] = {'count': int(patterns['VV']), 'denominator': int(matches_2_sets)}
    patterns_out['PP'] = {'count': int(patterns['PP']), 'denominator': int(matches_2_sets)}
    # 3-set patterns denominator = matches_3_sets
    for k in ('PVP','PVV','VPV','VPP'):
        patterns_out[k] = {'count': int(patterns[k]), 'denominator': int(matches_3_sets)}

    retire_out = {
        'count': int(retire_count),
        'by_set': dict((k, int(v)) for k,v in sorted(retire_by_set.items(), key=lambda x:int(x[0]))),
        'examples': retire_examples[:sample_limit]
    }

    out = {
        'meta': {
            'player_id': pid,
            'player_name': '',
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'version': 'v2-profile-by-set',
            'matches': len(df)
        },
        'profile_by_set': {
            'sets': profile_sets_out,
            'patterns': patterns_out,
            'matches_2_sets': int(matches_2_sets),
            'matches_3_sets': int(matches_3_sets)
        },
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
