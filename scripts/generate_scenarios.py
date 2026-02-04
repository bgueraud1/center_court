#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_scenarios.py - Module 3: generate scenario datasets per player

Usage:
  python generate_scenarios.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200

Produces per-player JSON files: out_dir/players_atp/{PLAYER_ID}.scenarios.json

Output structure (top-level):
{
  meta: { player_id, player_name, matches, generated_at, version },
  scenarios: {
    non_gs: { ... },
    gs: { ... },
    retirements: { count, by_set: {...}, examples: [...] }
  }
}

Percentages are given as fractions (0..1). Denominators & descriptions are provided so front-end can render % easily.
"""
import argparse
import os
import glob
import json
import re
from collections import defaultdict
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
        dt = pd.to_datetime(val, errors='coerce')
        if pd.isna(dt):
            return ''
        return dt.date().isoformat()
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

# parse set scores: returns list of tuples (left_int, right_int, raw_string)
def parse_set_scores_from_row(row):
    arr = []
    for i in range(1, 6):
        k = f"set{i}_score"
        if k in row.index:
            v = row.get(k)
            if v is None:
                continue
            if isinstance(v, float) and pd.isna(v):
                continue
            sval = str(v).strip()
            if sval == '':
                continue
            # some CSVs encode "6-3" or "7-6(7)" or "RET" inside sets
            if '-' in sval:
                left, right = sval.split('-', 1)
                left_n = re.sub(r'[^0-9]', '', left) or None
                right_n = re.sub(r'[^0-9]', '', right) or None
                try:
                    a = int(left_n) if left_n is not None else None
                    b = int(right_n) if right_n is not None else None
                except Exception:
                    a = b = None
                arr.append((a, b, sval))
            else:
                # sometimes it contains "RET" or "w/o" or punctuated; skip numeric parse
                left_n = re.sub(r'[^0-9]', '', sval)
                if left_n:
                    # if single number, we can't attribute to left/right reliably -> skip
                    pass
                else:
                    # non-standard, keep raw
                    arr.append((None, None, sval))
    return arr

def player_won_row(row, player_id):
    pid = normalize_player_id(player_id)
    if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
        return True
    if 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
        return False
    # fallback by name columns (best-effort)
    # winner_player_name, player_winner
    winner_names = []
    if 'winner_player_name' in row.index and row.get('winner_player_name'):
        winner_names.append(str(row.get('winner_player_name')))
    if 'player_winner' in row.index and row.get('player_winner'):
        winner_names.append(str(row.get('player_winner')))
    loser_names = []
    if 'loser_player_name' in row.index and row.get('loser_player_name'):
        loser_names.append(str(row.get('loser_player_name')))
    if 'player_loser' in row.index and row.get('player_loser'):
        loser_names.append(str(row.get('player_loser')))
    # try to see if pid equals a name (unlikely) -> return None otherwise
    return None

def is_grand_slam_row(row):
    # prefer explicit 'level' column
    if 'level' in row.index and row.get('level'):
        if str(row.get('level')).strip().upper() == 'GS':
            return True
    # fallback: tourney_name contains known GS strings
    tn = ''
    if 'tourney_name' in row.index and row.get('tourney_name'):
        tn = str(row.get('tourney_name')).lower()
    gs_keywords = ['australian', 'wimbledon', 'roland', 'french open', 'french', 'us open', 'australian open', 'roland-garros', 'usopen']
    for kw in gs_keywords:
        if kw in tn:
            return True
    return False

def detect_retirement(row):
    # heuristics: winner_flag == 'A' or match_status/message contains 'ret' or 'walk' or score_string contains 'RET'
    wf = None
    for col in ('winner_flag', 'winner_flag_raw'):
        if col in row.index and row.get(col):
            wf = str(row.get(col)).strip().upper()
            break
    if wf and ('A' in wf or 'RET' in wf or 'W/O' in wf or 'WO' in wf):
        return True
    mstatus = ''
    if 'match_status' in row.index and row.get('match_status'):
        mstatus = str(row.get('match_status')).lower()
    if 'match_message' in row.index and row.get('match_message'):
        mstatus = (mstatus + ' ' + str(row.get('match_message')).lower()).strip()
    if mstatus and ('ret' in mstatus or 'walk' in mstatus or 'w/o' in mstatus):
        return True
    s = ''
    if 'score_string' in row.index and row.get('score_string'):
        s = str(row.get('score_string')).lower()
        if 'ret' in s or 'walk' in s or 'w/o' in s:
            return True
    # fallback false
    return False

# ---------- Core scenario builder ----------
def build_scenarios_for_player(matches_df, player_id, sample_limit=6):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # select player's matches (winner or loser)
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
    frames = []
    if cond_w is not False and cond_w.any():
        frames.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        frames.append(matches_df[cond_l])
    if not frames:
        return {'meta': {'player_id': pid, 'player_name': '', 'matches': 0, 'generated_at': datetime.utcnow().isoformat()+'Z', 'version':'v1'}, 'scenarios': {}}
    df = pd.concat(frames, ignore_index=True, sort=False)

    # canonical name
    player_name = ''
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            vals = [v for v in df[col].dropna().astype(str).tolist() if v]
            if vals:
                player_name = vals[0]
                break

    total_matches = len(df)
    # initialize counters & samples
    non_gs = defaultdict(int)
    gs = defaultdict(int)

    # denominators for percentages
    denom = {
        'lost_first_set': 0,
        'won_first_set': 0,
        'matches_in_2_sets': 0,
        'matches_in_3_sets': 0,
        'matches_in_4_sets': 0,
        'matches_in_5_sets': 0,
        'gs_matches_total': 0,
        'non_gs_matches_total': 0
    }

    # samples for lists
    samples = defaultdict(list)

    # retirement tracking
    retire_count = 0
    retire_by_set = defaultdict(int)
    retire_examples = []

    for idx, r in df.iterrows():
        is_win = player_won_row(r, pid)
        # parse set scores left/right (left = CSV left)
        sets_raw = parse_set_scores_from_row(r)
        # produce player's per-set numeric view: list of (player_games, opp_games, raw)
        player_sets = []
        for i, (a, b, raw) in enumerate(sets_raw):
            if a is None or b is None:
                # can't decide numeric - store Nones but keep raw for retire detection
                player_sets.append((None, None, raw))
            else:
                # IMPORTANT: CSV set left/right correspond to winner/loser of match row,
                # but we want player's games. Determine:
                # If row indicates player is winner => left = player's games, else right = player's games.
                if is_win is True:
                    pg = a; og = b
                elif is_win is False:
                    pg = b; og = a
                else:
                    # unknown winner side: default assume left is player if player's name matches winner name
                    # fallback: keep left as player
                    pg = a; og = b
                player_sets.append((pg, og, raw))

        # determine best_of for this match: prefer 'level' or 'num_sets' or GS detection
        best_of = 3
        if is_grand_slam_row(r):
            best_of = 5
            denom['gs_matches_total'] += 1
        else:
            denom['non_gs_matches_total'] += 1
        # if num_sets column exists and is integer 5 -> treat as best_of=5
        if 'num_sets' in r.index:
            try:
                ns = int(r.get('num_sets'))
                if ns >= 5:
                    best_of = 5
            except Exception:
                pass

        # count non-empty numeric sets
        numeric_sets = [t for t in player_sets if (t[0] is not None and t[1] is not None)]
        total_played_sets = len(numeric_sets)
        # total_played_sets may be 0..5

        # classify matches by number of sets (for denominators)
        if total_played_sets == 2:
            denom['matches_in_2_sets'] += 1
        if total_played_sets == 3:
            denom['matches_in_3_sets'] += 1
        if total_played_sets == 4:
            denom['matches_in_4_sets'] += 1
        if total_played_sets == 5:
            denom['matches_in_5_sets'] += 1

        # FIRST-SET analysis (if set1 numeric available)
        if len(player_sets) >= 1 and (player_sets[0][0] is not None and player_sets[0][1] is not None):
            first_player_games, first_opp_games, _ = player_sets[0]
            if first_player_games < first_opp_games:
                denom['lost_first_set'] += 1
                if is_win:
                    # player won match after losing first set
                    if best_of == 5:
                        gs['wins_after_losing_first_set'] += 1
                    else:
                        non_gs['wins_after_losing_first_set'] += 1
                    if len(samples['wins_after_losing_first_set']) < sample_limit:
                        samples['wins_after_losing_first_set'].append({
                            'match_id': str(r.get('match_id') or ''),
                            'event_id': str(r.get('event_id') or ''),
                            'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                            'score': r.get('score_string') or r.get('score') or ''
                        })
            elif first_player_games > first_opp_games:
                denom['won_first_set'] += 1
                if not is_win:
                    # lost after winning first set
                    if best_of == 5:
                        gs['losses_after_winning_first_set'] += 1
                    else:
                        non_gs['losses_after_winning_first_set'] += 1
                    if len(samples['losses_after_winning_first_set']) < sample_limit:
                        samples['losses_after_winning_first_set'].append({
                            'match_id': str(r.get('match_id') or ''),
                            'event_id': str(r.get('event_id') or ''),
                            'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                            'score': r.get('score_string') or r.get('score') or ''
                        })
        # Wins after losing two first sets (0-2 comeback) — only meaningful for best_of >= 5 (GS)
        if best_of >= 5:
            if len(player_sets) >= 2:
                s1 = player_sets[0]; s2 = player_sets[1]
                if (s1[0] is not None and s1[1] is not None and s2[0] is not None and s2[1] is not None):
                    if s1[0] < s1[1] and s2[0] < s2[1] and is_win:
                        gs['wins_after_losing_first_two_sets'] += 1
                        if len(samples['wins_after_losing_first_two_sets']) < sample_limit:
                            samples['wins_after_losing_first_two_sets'].append({
                                'match_id': str(r.get('match_id') or ''),
                                'event_id': str(r.get('event_id') or ''),
                                'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                                'score': r.get('score_string') or r.get('score') or ''
                            })
            # victories after being down 2-1: after 3 sets player sets count is 1 and opp sets count is 2, but player ultimately wins
            if len(player_sets) >= 3:
                p_first3 = sum(1 for x in player_sets[:3] if x[0] is not None and x[1] is not None and x[0] > x[1])
                o_first3 = sum(1 for x in player_sets[:3] if x[0] is not None and x[1] is not None and x[0] < x[1])
                if (p_first3 == 1 and o_first3 == 2) and is_win:
                    gs['wins_after_down_2_1'] += 1
                    if len(samples['wins_after_down_2_1']) < sample_limit:
                        samples['wins_after_down_2_1'].append({
                            'match_id': str(r.get('match_id') or ''),
                            'event_id': str(r.get('event_id') or ''),
                            'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                            'score': r.get('score_string') or r.get('score') or ''
                        })
            # defeats after winning first two sets (lost after being 2-0 up)
            if len(player_sets) >= 2:
                s1 = player_sets[0]; s2 = player_sets[1]
                if (s1[0] is not None and s1[1] is not None and s2[0] is not None and s2[1] is not None):
                    if s1[0] > s1[1] and s2[0] > s2[1] and not is_win:
                        gs['losses_after_winning_first_two_sets'] += 1
                        if len(samples['losses_after_winning_first_two_sets']) < sample_limit:
                            samples['losses_after_winning_first_two_sets'].append({
                                'match_id': str(r.get('match_id') or ''),
                                'event_id': str(r.get('event_id') or ''),
                                'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                                'score': r.get('score_string') or r.get('score') or ''
                            })
            # defeats after leading 2-1: after three sets player leads 2-1 but ultimately loses
            if len(player_sets) >= 3:
                p_first3 = sum(1 for x in player_sets[:3] if x[0] is not None and x[1] is not None and x[0] > x[1])
                o_first3 = sum(1 for x in player_sets[:3] if x[0] is not None and x[1] is not None and x[0] < x[1])
                if (p_first3 == 2 and o_first3 == 1) and not is_win:
                    gs['losses_after_leading_2_1'] += 1
                    if len(samples['losses_after_leading_2_1']) < sample_limit:
                        samples['losses_after_leading_2_1'].append({
                            'match_id': str(r.get('match_id') or ''),
                            'event_id': str(r.get('event_id') or ''),
                            'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                            'score': r.get('score_string') or r.get('score') or ''
                        })

        # Non-GS counts: wins after losing first set already handled above for non-gs vs gs via best_of check
        # Wins/losses in 2/3 sets
        if total_played_sets >= 1:
            if is_win:
                if total_played_sets == 2:
                    non_gs['wins_in_2_sets'] += 1 if best_of < 5 else 0
                    gs['wins_in_2_sets'] += 1 if best_of >=5 else 0
                if total_played_sets == 3:
                    non_gs['wins_in_3_sets'] += 1 if best_of < 5 else 0
                    gs['wins_in_3_sets'] += 1 if best_of >=5 else 0
                if total_played_sets == 4:
                    gs['wins_in_4_sets'] += 1
                if total_played_sets == 5:
                    gs['wins_in_5_sets'] += 1
            else:
                if total_played_sets == 2:
                    non_gs['losses_in_2_sets'] += 1 if best_of < 5 else 0
                    gs['losses_in_2_sets'] += 1 if best_of >=5 else 0
                if total_played_sets == 3:
                    non_gs['losses_in_3_sets'] += 1 if best_of < 5 else 0
                    gs['losses_in_3_sets'] += 1 if best_of >=5 else 0
                if total_played_sets == 4:
                    gs['losses_in_4_sets'] += 1
                if total_played_sets == 5:
                    gs['losses_in_5_sets'] += 1

        # Additional GS-specific stats: defeats after winning first set (already handled earlier for non-gs too)
        if best_of >= 5:
            # defeats after winning first set already counted in losses_after_winning_first_set above (best_of check)
            pass

        # retirements detection
        if detect_retirement(r):
            retire_count += 1
            # find last non-empty numeric set index
            last_nonempty = 0
            for i in range(1, 6):
                k = f"set{i}_score"
                if k in r.index:
                    v = r.get(k)
                    if v is None:
                        continue
                    sval = str(v).strip()
                    if sval != '':
                        last_nonempty = i
            # if last_nonempty==0 assume retirement before any set => set 1
            retire_set = last_nonempty + 1 if last_nonempty > 0 else 1
            # but if last_nonempty >= best_of then retire at last_nonempty (fallback)
            if retire_set > 5:
                retire_set = last_nonempty
            retire_by_set[str(retire_set)] += 1
            if len(retire_examples) < sample_limit:
                # capture opponent & basic info
                opponent = ''
                if is_win is True:
                    opponent = r.get('player_loser') or r.get('loser_player_name') or ''
                elif is_win is False:
                    opponent = r.get('player_winner') or r.get('winner_player_name') or ''
                retire_examples.append({
                    'match_id': str(r.get('match_id') or ''),
                    'event_id': str(r.get('event_id') or ''),
                    'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                    'opponent': opponent,
                    'retire_set': retire_set,
                    'score': r.get('score_string') or r.get('score') or ''
                })

    # build result objects with percents
    def pct(count, denom_count):
        if denom_count and denom_count > 0:
            return float(count) / float(denom_count)
        return None

    non_gs_out = {}
    # wins after losing first set (non-GS)
    non_gs_out['matches_total'] = denom['non_gs_matches_total']
    non_gs_out['lost_first_set_matches'] = denom['lost_first_set']  # note: denom is global; approximation across all matches (could refine per-surface/year)
    non_gs_out['wins_after_losing_first_set'] = {
        'count': int(non_gs.get('wins_after_losing_first_set', 0)),
        'denominator': int(denom['lost_first_set']),
        'denominator_desc': 'matches where player lost first set',
        'pct': pct(non_gs.get('wins_after_losing_first_set', 0), denom['lost_first_set'])
    }
    non_gs_out['losses_after_winning_first_set'] = {
        'count': int(non_gs.get('losses_after_winning_first_set', 0)),
        'denominator': int(denom['won_first_set']),
        'denominator_desc': 'matches where player won first set',
        'pct': pct(non_gs.get('losses_after_winning_first_set', 0), denom['won_first_set'])
    }

    # wins/losses by 2/3 sets for non-GS (best_of < 5)
    non_gs_out['wins_in_2_sets'] = {
        'count': int(non_gs.get('wins_in_2_sets', 0)),
        'denominator': int(denom['matches_in_2_sets']),
        'denominator_desc': 'matches decided in 2 sets',
        'pct': pct(non_gs.get('wins_in_2_sets', 0), denom['matches_in_2_sets'])
    }
    non_gs_out['wins_in_3_sets'] = {
        'count': int(non_gs.get('wins_in_3_sets', 0)),
        'denominator': int(denom['matches_in_3_sets']),
        'denominator_desc': 'matches decided in 3 sets',
        'pct': pct(non_gs.get('wins_in_3_sets', 0), denom['matches_in_3_sets'])
    }
    non_gs_out['losses_in_2_sets'] = {
        'count': int(non_gs.get('losses_in_2_sets', 0)),
        'denominator': int(denom['matches_in_2_sets']),
        'denominator_desc': 'matches decided in 2 sets',
        'pct': pct(non_gs.get('losses_in_2_sets', 0), denom['matches_in_2_sets'])
    }
    non_gs_out['losses_in_3_sets'] = {
        'count': int(non_gs.get('losses_in_3_sets', 0)),
        'denominator': int(denom['matches_in_3_sets']),
        'denominator_desc': 'matches decided in 3 sets',
        'pct': pct(non_gs.get('losses_in_3_sets', 0), denom['matches_in_3_sets'])
    }

    # GS block
    gs_out = {}
    gs_out['matches_total'] = denom['gs_matches_total']
    gs_out['wins_after_losing_first_set'] = {
        'count': int(gs.get('wins_after_losing_first_set', 0)),
        'denominator': int(denom['lost_first_set']),
        'denominator_desc': 'matches where player lost first set (across all matches)',
        'pct': pct(gs.get('wins_after_losing_first_set', 0), denom['lost_first_set'])
    }
    gs_out['wins_after_losing_first_two_sets'] = {
        'count': int(gs.get('wins_after_losing_first_two_sets', 0)),
        'denominator': None,
        'denominator_desc': 'matches where player lost first two sets',
        'pct': None
    }
    gs_out['wins_after_down_2_1'] = {
        'count': int(gs.get('wins_after_down_2_1', 0)),
        'denominator': None,
        'denominator_desc': 'matches where player was down 2-1 after three sets',
        'pct': None
    }
    gs_out['losses_after_winning_first_two_sets'] = {
        'count': int(gs.get('losses_after_winning_first_two_sets', 0)),
        'denominator': None,
        'denominator_desc': 'matches where player won first two sets',
        'pct': None
    }
    gs_out['losses_after_winning_first_set'] = {
        'count': int(gs.get('losses_after_winning_first_set', 0)),
        'denominator': int(denom['won_first_set']),
        'denominator_desc': 'matches where player won first set',
        'pct': pct(gs.get('losses_after_winning_first_set', 0), denom['won_first_set'])
    }
    gs_out['losses_after_leading_2_1'] = {
        'count': int(gs.get('losses_after_leading_2_1', 0)),
        'denominator': None,
        'denominator_desc': 'matches where player led 2-1 after three sets',
        'pct': None
    }
    # wins by 3/4/5 sets & losses by 3/4/5 sets
    gs_out['wins_in_3_sets'] = {
        'count': int(gs.get('wins_in_3_sets', 0)),
        'denominator': int(denom['matches_in_3_sets']),
        'denominator_desc': 'matches decided in 3 sets',
        'pct': pct(gs.get('wins_in_3_sets', 0), denom['matches_in_3_sets'])
    }
    gs_out['wins_in_4_sets'] = {
        'count': int(gs.get('wins_in_4_sets', 0)),
        'denominator': int(denom['matches_in_4_sets']),
        'denominator_desc': 'matches decided in 4 sets',
        'pct': pct(gs.get('wins_in_4_sets', 0), denom['matches_in_4_sets'])
    }
    gs_out['wins_in_5_sets'] = {
        'count': int(gs.get('wins_in_5_sets', 0)),
        'denominator': int(denom['matches_in_5_sets']),
        'denominator_desc': 'matches decided in 5 sets',
        'pct': pct(gs.get('wins_in_5_sets', 0), denom['matches_in_5_sets'])
    }
    gs_out['losses_in_3_sets'] = {
        'count': int(gs.get('losses_in_3_sets', 0)),
        'denominator': int(denom['matches_in_3_sets']),
        'denominator_desc': 'matches decided in 3 sets',
        'pct': pct(gs.get('losses_in_3_sets', 0), denom['matches_in_3_sets'])
    }
    gs_out['losses_in_4_sets'] = {
        'count': int(gs.get('losses_in_4_sets', 0)),
        'denominator': int(denom['matches_in_4_sets']),
        'denominator_desc': 'matches decided in 4 sets',
        'pct': pct(gs.get('losses_in_4_sets', 0), denom['matches_in_4_sets'])
    }
    gs_out['losses_in_5_sets'] = {
        'count': int(gs.get('losses_in_5_sets', 0)),
        'denominator': int(denom['matches_in_5_sets']),
        'denominator_desc': 'matches decided in 5 sets',
        'pct': pct(gs.get('losses_in_5_sets', 0), denom['matches_in_5_sets'])
    }

    retire_out = {
        'count': int(retire_count),
        'by_set': dict(retire_by_set),
        'examples': retire_examples
    }

    # prepare compact samples object (trim)
    samples_out = {}
    for k, arr in samples.items():
        samples_out[k] = arr[:sample_limit]

    result = {
        'meta': {
            'player_id': pid,
            'player_name': player_name,
            'matches': int(total_matches),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'version': 'v1'
        },
        'scenarios': {
            'non_gs': non_gs_out,
            'gs': gs_out,
            'retirements': retire_out,
            'samples': samples_out
        }
    }
    return result

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
    # fallback on name columns if no ids present
    if not player_ids:
        for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
            if col in matches.columns:
                player_ids.update([normalize_player_id(x) for x in matches[col].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    if player_list:
        player_ids = [p for p in player_ids if p in set(player_list)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, start=1):
        print(f"[scenarios] [{i}/{len(player_ids)}] building scenarios for {pid}")
        try:
            obj = build_scenarios_for_player(matches, pid)
            out_path = os.path.join(players_dir, f"{pid}.scenarios.json")
            with open(out_path, 'w', encoding='utf8') as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[scenarios] ERROR building for {pid}: {e}")

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
