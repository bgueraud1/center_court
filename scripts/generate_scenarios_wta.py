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

    # prepare counters / denominators
    non_gs = defaultdict(lambda: {'count': 0})
    gs = defaultdict(lambda: {'count': 0})
    samples = defaultdict(list)

    # denominators we will compute
    den = {
        'non_gs_matches_2_sets': 0,
        'non_gs_matches_3_sets': 0,
        'non_gs_lost_first_set': 0,
        'non_gs_won_first_set': 0,
        'gs_matches_by_sets': Counter(),   # keys 3,4,5
        'gs_lost_first_two': 0,
        'gs_player_down_2_1': 0,
        'gs_player_up_first_two': 0,
        'gs_player_up_2_1': 0
    }

    retire_count = 0
    retire_by_set = Counter()
    retire_examples = []

    total_matches = 0

    for idx, r in df.iterrows():
        total_matches += 1
        # basic metadata
        match_id = str(r.get('match_id') or '')
        event_id = str(r.get('event_id') or '')
        level = (r.get('level') or '') or ''
        is_gs = False
        try:
            if isinstance(level, str) and 'gs' in level.lower():
                is_gs = True
        except Exception:
            is_gs = False
        required_sets_to_win = 3 if is_gs else 2

        player_is_winner = player_won_row(r, pid)
        # opponent name
        if player_is_winner is True:
            opponent = str(r.get('loser_player_name') or r.get('player_loser') or '')
        elif player_is_winner is False:
            opponent = str(r.get('winner_player_name') or r.get('player_winner') or '')
        else:
            # fallback: choose other name if available
            opponent = str(r.get('winner_player_name') or r.get('loser_player_name') or r.get('player_winner') or r.get('player_loser') or '')

        # sets
        sets = parse_set_scores(r)
        # compute for each set whether complete, and numeric left/right
        complete_flags = []
        parsed_sets = []
        for left, right, raw in sets:
            a = _parse_int_safe(left)
            b = _parse_int_safe(right)
            parsed_sets.append((a, b, raw))
            complete_flags.append(is_complete_set_score(left, right))

        # compute winner-side set wins (left=winner games in CSV)
        winner_sets = 0
        loser_sets = 0
        complete_sets_count = 0
        for i, (a,b,raw) in enumerate(parsed_sets):
            if a is None or b is None:
                # incomplete set representation (e.g. "4-1" or "6-4" partial?) -> check complete_flags
                if complete_flags[i]:
                    complete_sets_count += 1
                    if a > b:
                        winner_sets += 1
                    else:
                        loser_sets += 1
                else:
                    # not a complete set
                    pass
            else:
                if complete_flags[i]:
                    complete_sets_count += 1
                    if a > b:
                        winner_sets += 1
                    else:
                        loser_sets += 1
                else:
                    # incomplete set: count as present but not complete
                    pass

        # retirement detection: per your rule: if winner_sets < required => retirement happened
        is_retire = False
        retire_set_number = None
        # but also guard: if no set scores and match_message indicates 'WO' or 'walkover', skip
        match_msg = (r.get('match_message') or '') or ''
        match_status = (r.get('match_status') or '') or ''
        # detect explicit RET markers in CSV if present
        explicit_ret = False
        if isinstance(match_msg, str) and match_msg.strip().lower().startswith('ret'):
            explicit_ret = True
        if isinstance(match_status, str) and 'ret' in match_status.strip().lower():
            explicit_ret = True

        if winner_sets < required_sets_to_win:
            # only mark retire if there is at least one set recorded (or explicit ret)
            if complete_sets_count > 0 or len(parsed_sets) > 0 or explicit_ret:
                is_retire = True
                # decide set where retirement occurred:
                # - if there is an incomplete set (i.e. a parsed set where complete_flags False but raw present),
                #   retirement happened in that set (index+1)
                found_incomplete = False
                for i, flag in enumerate(complete_flags):
                    if not flag:
                        retire_set_number = i+1
                        found_incomplete = True
                        break
                if not found_incomplete:
                    # retirement must have happened after last complete set -> next set index
                    retire_set_number = complete_sets_count + 1
        # record retire info
        if is_retire:
            retire_count += 1
            retire_by_set[str(retire_set_number)] += 1
            retire_examples.append({
                'match_id': match_id,
                'event_id': event_id,
                'start_date': parse_date(r.get('start_date') or r.get('match_date')),
                'score': str(r.get('score_string') or r.get('score') or ''),
                'opponent': opponent,
                'retire_set': retire_set_number
            })

        # common derived booleans (player-centric)
        # compute player's per-set games & set wins using knowledge that in CSV left=winner games
        player_set_wins = 0
        opp_set_wins = 0
        player_first_set_won = None
        player_second_set_won = None
        # for first two sets
        for i, (a,b,raw) in enumerate(parsed_sets):
            # skip if a or b none
            if a is None or b is None:
                # if incomplete, we treat as 'no decisive result for this set'
                if i == 0:
                    player_first_set_won = None
                if i == 1:
                    player_second_set_won = None
                continue
            # left = winner games, right = loser games
            # determine whether player is winner in the row: if player_is_winner True => left is player's games, else right is player's games
            if player_is_winner is True:
                player_games = a
                opp_games = b
            elif player_is_winner is False:
                player_games = b
                opp_games = a
            else:
                # unknown player side -- attempt to guess by matching player name to winner_player_name
                # fallback: assume player is winner if their name appears in winner_player_name
                player_games = None
                opp_games = None
                try:
                    wn = (r.get('winner_player_name') or '').strip()
                    lp = (r.get('loser_player_name') or '').strip()
                    # simplest guess: if pid present in winner/loser ids matched earlier, but we don't have that here; skip
                except Exception:
                    pass
                # in unknown case, skip
                continue

            # only consider the set if it's complete
            complete_flag = is_complete_set_score(a, b)
            if not complete_flag:
                # mark first/second set as None if incomplete
                if i == 0: player_first_set_won = None
                if i == 1: player_second_set_won = None
                continue

            if player_games > opp_games:
                player_set_wins += 1
                if i == 0: player_first_set_won = True
                if i == 1: player_second_set_won = True
            else:
                opp_set_wins += 1
                if i == 0: player_first_set_won = False
                if i == 1: player_second_set_won = False

        # now compute scenario conditions and denominators
        if is_gs:
            # set-length denominators
            if complete_sets_count in (3,4,5):
                den['gs_matches_by_sets'][complete_sets_count] += 1

            # lost first two sets?
            if len(parsed_sets) >= 2:
                # require both sets complete for this test
                a1,b1,_ = parsed_sets[0]
                a2,b2,_ = parsed_sets[1]
                if is_complete_set_score(a1,b1) and is_complete_set_score(a2,b2):
                    # determine player's result in set1 and set2
                    # player's games as earlier
                    if player_is_winner is True:
                        p1 = (a1 > b1)
                        p2 = (a2 > b2)
                    elif player_is_winner is False:
                        p1 = (b1 > a1)
                        p2 = (b2 > a2)
                    else:
                        p1 = p2 = None
                    if p1 is False and p2 is False:
                        den['gs_lost_first_two'] += 1
                        # if player then wins match, it's a comeback from 0-2
                        if player_set_wins > opp_set_wins:
                            gs['wins_after_losing_first_two_sets']['count'] += 1
                            samples['wins_after_losing_first_two_sets'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})
                        else:
                            # player lost after losing first two -> contributes to denominator but not to wins
                            pass

            # down 2-1 detection: requires at least 3 complete sets
            if complete_sets_count >= 3:
                # after 3 complete sets, compute sets for player
                # compute sets_won_by_player_after3
                player_after3 = 0
                opp_after3 = 0
                for i in range(3):
                    a,b,raw = parsed_sets[i]
                    if not is_complete_set_score(a,b):
                        # incomplete: skip this match for down_2_1 detection
                        player_after3 = None
                        break
                    if player_is_winner is True:
                        if a > b: player_after3 += 1
                        else: opp_after3 += 1
                    elif player_is_winner is False:
                        if b > a: player_after3 += 1
                        else: opp_after3 += 1
                    else:
                        player_after3 = None
                        break
                if player_after3 is not None:
                    # player down 2-1 means opp_after3 == 2 and player_after3 == 1
                    if opp_after3 == 2 and player_after3 == 1:
                        den['gs_player_down_2_1'] += 1
                        if player_set_wins > opp_set_wins:
                            gs['wins_after_down_2_1']['count'] += 1
                            samples['wins_after_down_2_1'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})
                        else:
                            # loss after being down 2-1 -> counts to denominator but not to wins
                            pass
                    # player leading 2-1
                    if player_after3 == 2 and opp_after3 == 1:
                        den['gs_player_up_2_1'] += 1
                        if player_set_wins < opp_set_wins:
                            gs['losses_after_leading_2_1']['count'] += 1
                            samples['losses_after_leading_2_1'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})

            # player won first two sets?
            if player_first_set_won is not None and player_second_set_won is not None:
                if player_first_set_won is True and player_second_set_won is True:
                    den['gs_player_up_first_two'] += 1
                    if player_set_wins < opp_set_wins:
                        gs['losses_after_winning_first_two_sets']['count'] += 1
                        samples['losses_after_winning_first_two_sets'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})
                if player_first_set_won is False and player_second_set_won is False:
                    # already handled above as lost_first_two
                    pass

            # set-length wins/losses in GS: wins_in_3_sets etc.
            if complete_sets_count in (3,4,5):
                if player_set_wins > opp_set_wins:
                    key = f'wins_in_{complete_sets_count}_sets'
                    gs[key]['count'] += 1
                else:
                    key = f'losses_in_{complete_sets_count}_sets'
                    gs[key]['count'] += 1

        else:
            # non GS (best of 3)
            # first-set denominators
            if len(parsed_sets) >= 1:
                a1,b1,_ = parsed_sets[0]
                if is_complete_set_score(a1,b1):
                    # did player lose first set?
                    if player_is_winner is True:
                        lost_first = (a1 <= b1)
                    elif player_is_winner is False:
                        lost_first = (b1 <= a1)  # when player is loser in row, left is winner games
                    else:
                        lost_first = None
                    if lost_first is True:
                        den['non_gs_lost_first_set'] += 1
                        if player_set_wins > opp_set_wins:
                            non_gs['wins_after_losing_first_set']['count'] += 1
                            samples['wins_after_losing_first_set'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})
                    if lost_first is False:
                        den['non_gs_won_first_set'] += 1
                        if player_set_wins < opp_set_wins:
                            non_gs['losses_after_winning_first_set']['count'] += 1
                            samples['losses_after_winning_first_set'].append({'match_id': match_id, 'score': str(r.get('score_string') or ''), 'start_date': parse_date(r.get('start_date') or r.get('match_date'))})

            # count matches ending in 2 or 3 sets (complete)
            if complete_sets_count == 2:
                den['non_gs_matches_2_sets'] += 1
                if player_set_wins > opp_set_wins:
                    non_gs['wins_in_2_sets']['count'] += 1
                else:
                    non_gs['losses_in_2_sets']['count'] += 1
            elif complete_sets_count == 3:
                den['non_gs_matches_3_sets'] += 1
                if player_set_wins > opp_set_wins:
                    non_gs['wins_in_3_sets']['count'] += 1
                else:
                    non_gs['losses_in_3_sets']['count'] += 1

    # finalize denominators into dictionary shape expected by client
    # For non-GS: attach denominator to each relevant metric
    non_gs_out = {}
    # wins/losses after first set
    non_gs_out['wins_after_losing_first_set'] = {
        'count': int(non_gs['wins_after_losing_first_set']['count']),
        'denominator': int(den['non_gs_lost_first_set'])
    }
    non_gs_out['losses_after_winning_first_set'] = {
        'count': int(non_gs['losses_after_winning_first_set']['count']),
        'denominator': int(den['non_gs_won_first_set'])
    }
    # wins/losses by match length
    non_gs_out['wins_in_2_sets'] = {
        'count': int(non_gs['wins_in_2_sets']['count']),
        'denominator': int(den['non_gs_matches_2_sets'])
    }
    non_gs_out['losses_in_2_sets'] = {
        'count': int(non_gs['losses_in_2_sets']['count']),
        'denominator': int(den['non_gs_matches_2_sets'])
    }
    non_gs_out['wins_in_3_sets'] = {
        'count': int(non_gs['wins_in_3_sets']['count']),
        'denominator': int(den['non_gs_matches_3_sets'])
    }
    non_gs_out['losses_in_3_sets'] = {
        'count': int(non_gs['losses_in_3_sets']['count']),
        'denominator': int(den['non_gs_matches_3_sets'])
    }

    # finalize GS metrics
    gs_out = {}
    # comeback from 0-2
    gs_out['wins_after_losing_first_two_sets'] = {
        'count': int(gs['wins_after_losing_first_two_sets']['count']),
        'denominator': int(den['gs_lost_first_two'])
    }
    # wins after down 2-1
    gs_out['wins_after_down_2_1'] = {
        'count': int(gs['wins_after_down_2_1']['count']),
        'denominator': int(den['gs_player_down_2_1'])
    }
    # losses after being up 2-1
    gs_out['losses_after_leading_2_1'] = {
        'count': int(gs['losses_after_leading_2_1']['count']),
        'denominator': int(den['gs_player_up_2_1'])
    }
    # losses after winning first two sets
    gs_out['losses_after_winning_first_two_sets'] = {
        'count': int(gs['losses_after_winning_first_two_sets']['count']),
        'denominator': int(den['gs_player_up_first_two'])
    }
    # also provide wins/losses by 3/4/5 sets with denominators equal to total GS matches ending in that set length
    for k in (3,4,5):
        wins_key = f'wins_in_{k}_sets'
        losses_key = f'losses_in_{k}_sets'
        gs_out[wins_key] = {
            'count': int(gs[wins_key]['count']),
            'denominator': int(den['gs_matches_by_sets'].get(k, 0))
        }
        gs_out[losses_key] = {
            'count': int(gs[losses_key]['count']),
            'denominator': int(den['gs_matches_by_sets'].get(k, 0))
        }

    retire_out = {
        'count': int(retire_count),
        'by_set': dict((k, int(v)) for k,v in sorted(retire_by_set.items(), key=lambda x:int(x[0]))),
        'examples': retire_examples[:sample_limit]
    }

    # samples trim
    samples_trimmed = {}
    for k, arr in samples.items():
        samples_trimmed[k] = arr[:sample_limit]

    out = {
        'meta': {
            'player_id': pid,
            'player_name': '',
            'generated_at': datetime.utcnow().isoformat()+'Z',
            'version': 'v2',
            'matches': len(df)
        },
        'scenarios': {
            'non_gs': non_gs_out,
            'gs': gs_out,
            'retirements': retire_out,
            'samples': samples_trimmed
        }
    }
    return out

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
