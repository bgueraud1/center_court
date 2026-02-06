#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv2bracket_json.py
Normalize various ATP/WTA match CSVs into a canonical JSON for the HTML renderer.

Usage:
  python csv2bracket_json.py input.csv output.json
"""
import sys
import json
import re
import pandas as pd
from pathlib import Path

# -------------------------
# Configuration / mapping
# -------------------------
# canonical round codes we'll output: F, SF, QF, R4, R3, R2, R1, R16, R32, R64, R128
# We'll prefer codes: F, SF, QF, R16, R32, R64, R128, R1..R4 (for WTA '1','2' etc)
ATP_ORDER = ["R128","R64","R32","R16","QF","SF","F"]
WTA_ORDER = ["R128","R64","R32","R16","QF","SF","F"]  # numeric rounds map to R1..R4; but ordering preserved via rank logic

# helper maps for common textual variants -> canonical
ROUND_MAP = {
    # finals
    "F": "F", "FINAL": "F", "FINALS": "F",
    # semis
    "SF": "SF", "S": "SF", "SEMIFINAL": "SF", "SEMIFINALS": "SF",
    # quarters
    "QF": "QF", "Q": "QF", "QUARTER": "QF", "QUARTERFINAL": "QF", "QUARTERFINALS": "QF",
    # numeric (WTA often has 1..4 meaning R1..R4)
    "1": "R1", "2": "R2", "3": "R3", "4": "R4",
    # other common labels
    "R128": "R128", "R64": "R64", "R32": "R32", "R16": "R16", "R8": "QF", "ROUND OF 16": "R16",
    "ROUND-OF-16": "R16", "ROUND 16": "R16"
}

# rank for sorting (lower index -> earlier round -> leftmost). Smaller number -> earlier round.
CANON_RANK = {
    # earliest (leftmost) first: R128..R1..R16..QF..SF..F
    "R128": 0, "R64": 1, "R32": 2, "R16": 3,
    "R4": 4, "R3": 5, "R2": 6, "R1": 7,
    "QF": 8, "SF": 9, "F": 10
}

# list of candidate names for winner/loser columns (common variations)
WINNER_COLS = [
    'winner', 'winner_player_name', 'player_winner', 'player_a', 'player_a_name', 'playerA','player_a_full'
]
LOSER_COLS = [
    'loser', 'loser_player_name', 'player_loser', 'player_b', 'player_b_name', 'playerB','player_b_full'
]

WINNER_ID_COLS = ['player_id_winner', 'player_winner_id', 'playerida', 'winner_id']
LOSER_ID_COLS = ['player_id_loser', 'player_loser_id', 'playeridb', 'loser_id']

WINNER_COUNTRY_COLS = ['winner_country', 'country_winner', 'country_a', 'country_a_name', 'winner_country_code']
LOSER_COUNTRY_COLS = ['loser_country', 'country_loser', 'country_b', 'country_b_name', 'loser_country_code']

SEED_WIN_COLS = ['winner_seed', 'seed_winner', 'seed_a', 'seed_a']
SEED_LOS_COLS = ['loser_seed', 'seed_loser', 'seed_b', 'seed_b']

ROUND_COL_CANDIDATES = ['round', 'match_round', 'round_name']

MATCH_ID_COLS = ['match_id', 'id', 'matchid']

SCORE_COLS = ['score_string','score','set_scores','set1_score','set2_score','set3_score']

# -------------------------
# Helpers
# -------------------------
def first_existing_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def pick(row, candidates):
    for c in candidates:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() != '':
            return row[c]
    return None

def canonical_round(raw_round):
    if raw_round is None: return ''
    s = str(raw_round).strip()
    if s == '':
        return ''
    u = re.sub(r'\s+','', s.upper())
    # direct map
    if u in ROUND_MAP:
        return ROUND_MAP[u]
    # numeric plain (e.g. "1" or "2")
    m = re.match(r'^(\d+)$', u)
    if m:
        n = int(m.group(1))
        return f"R{n}"
    # patterns like "R16", "R32"
    m2 = re.match(r'^R(\d+)$', u)
    if m2:
        return u
    # try common words
    if 'FINAL' in u:
        return 'F'
    if 'SEMIF' in u:
        return 'SF'
    if 'QUART' in u:
        return 'QF'
    # fallback: return original uppercase compact
    return u

def is_walkover_or_retired(score_string):
    if not score_string: return False
    s = str(score_string).upper()
    return ('W/O' in s) or ('W O' in s) or ('RET' in s) or ('RET.' in s)

def safe_str(x):
    return '' if x is None or (isinstance(x, float) and pd.isna(x)) else str(x)

# -------------------------
# Main conversion
# -------------------------
def csv_to_json(in_path, out_path):
    df = pd.read_csv(in_path, dtype=str, low_memory=False).fillna('')

    # try to detect tour (WTA/ATP) from level or tourney_name
    level_col = None
    for c in ['level','tour_level','tour_level_desc','tourney_level']:
        if c in df.columns:
            level_col = c
            break
    level_sample = ''
    if level_col:
        level_sample = ' '.join(df[level_col].astype(str).head(4).tolist()).upper()

    is_wta = 'WTA' in level_sample or 'WOMEN' in level_sample or 'WTA' in str(in_path).upper()

    # pick columns
    winner_col = first_existing_col(df, WINNER_COLS) or first_existing_col(df, ['player_a','player_a_name','player_a_full'])
    loser_col  = first_existing_col(df, LOSER_COLS) or first_existing_col(df, ['player_b','player_b_name','player_b_full'])
    winner_id_col = first_existing_col(df, WINNER_ID_COLS)
    loser_id_col = first_existing_col(df, LOSER_ID_COLS)
    winner_country_col = first_existing_col(df, WINNER_COUNTRY_COLS)
    loser_country_col = first_existing_col(df, LOSER_COUNTRY_COLS)
    seed_win_col = first_existing_col(df, SEED_WIN_COLS)
    seed_los_col = first_existing_col(df, SEED_LOS_COLS)
    round_col = first_existing_col(df, ROUND_COL_CANDIDATES) or first_existing_col(df, ['round'])
    matchid_col = first_existing_col(df, MATCH_ID_COLS)
    score_col = first_existing_col(df, SCORE_COLS) or 'score_string'

    matches = []
    for idx, row in df.iterrows():
        # extract names robustly: prefer explicit winner/loser; if not, try player_a/b and player_winner flag
        winner_name = pick(row, [winner_col, 'player_winner', 'player_a', 'player_a_name', 'playerA']) or ''
        loser_name  = pick(row, [loser_col,  'player_loser', 'player_b', 'player_b_name', 'playerB']) or ''

        # sometimes CSV includes 'player_winner' but also columns player_a/player_b — keep as is.
        # ensure we don't throw away names accidentally:
        winner_name = safe_str(winner_name).strip()
        loser_name  = safe_str(loser_name).strip()

        # ids, countries, seeds
        winner_id = pick(row, [winner_id_col, 'player_id_winner','PlayerIDA','PlayerIDA2']) or ''
        loser_id  = pick(row, [loser_id_col, 'player_id_loser','PlayerIDB','PlayerIDB2']) or ''
        winner_country = pick(row, [winner_country_col, 'country_winner','country_a', 'winner_country_code']) or ''
        loser_country  = pick(row, [loser_country_col, 'country_loser','country_b', 'loser_country_code']) or ''
        winner_seed = pick(row, [seed_win_col, 'seed_winner','seed_a']) or ''
        loser_seed  = pick(row, [seed_los_col, 'seed_loser','seed_b']) or ''

        raw_round = pick(row, [round_col, 'round', 'match_round']) or ''
        canonical = canonical_round(raw_round)

        match_id = pick(row, [matchid_col, 'match_id', 'id']) or f"row{idx+1}"

        score_string = pick(row, [score_col]) or pick(row, ['set1_score','set2_score','set3_score']) or ''

        # if dataset has winner/loser swapped with "player_a/player_b" we try to keep the original mapping:
        # (we already took winner_name/loser_name from winner/loser columns if present)

        # produce canonical match object (keep many fields available for debug)
        match_obj = {
            "match_id": str(match_id),
            "round": canonical,
            "winner_player_name": winner_name,
            "loser_player_name": loser_name,
            "winner_country": winner_country,
            "loser_country": loser_country,
            "winner_seed": winner_seed,
            "loser_seed": loser_seed,
            "player_id_winner": str(winner_id),
            "player_id_loser": str(loser_id),
            "score_string": safe_str(score_string),
            # keep original raw row for debugging if needed (optional)
            # "raw_row_index": int(idx)
        }

        matches.append(match_obj)

    # Build meta
    meta = {}
    for k in ['tourney_name','tourney_id','tourney_year','level','tournament_name','tournament_title']:
        if k in df.columns:
            meta[k] = df[k].astype(str).dropna().unique().tolist()[0] if len(df[k].astype(str).dropna().unique())>0 else ''
    # fallback title
    if not meta.get('tourney_name'):
        meta['tourney_name'] = Path(in_path).stem

    out = {"meta": meta, "matches": matches}

    # save
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(matches)} matches to {out_path} (is_wta={is_wta})")

# -------------------------
# CLI
# -------------------------
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python csv2bracket_json.py input.csv output.json")
        sys.exit(1)
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    csv_to_json(in_file, out_file)
