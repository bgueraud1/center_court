#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_player_meta.py
Module 1 - Génération non-statistique (métadonnées & index léger de matches)

Usage:
    python generate_player_meta.py --matches-dir /path/to/matches_csv_dir --out-dir ./dist

Sorties (par défaut sous ./dist):
  - dist/index/players_index.json
  - dist/players/{PLAYER_ID}.meta.json
  - dist/players/{PLAYER_ID}.matches.json
"""

import argparse
import os
import json
import glob
import re
from collections import Counter, defaultdict
from datetime import datetime
import pandas as pd

# ------------ Helpers ------------

def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def slugify(name: str) -> str:
    if not name:
        return ''
    s = name.strip().lower()
    # replace accents, non-ascii (simple approach)
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s

def normalize_player_id(pid: str) -> str:
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date_only(val):
    """Return ISO date string YYYY-MM-DD if possible, else return original / empty string."""
    if val is None:
        return ''
    if isinstance(val, str):
        v = val.strip()
        if v == '':
            return ''
        # common patterns: 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS', 'HH:MM:SS' etc.
        try:
            # try iso first
            dt = datetime.fromisoformat(v)
            return dt.date().isoformat()
        except Exception:
            pass
        # try pandas parser (lenient)
        try:
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
        except Exception:
            pass
        # fallback: extract YYYY-MM-DD using regex
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
        return v
    # if it's a timestamp
    try:
        dt = pd.to_datetime(val, errors='coerce')
        if not pd.isna(dt):
            return dt.date().isoformat()
    except Exception:
        pass
    return ''

def choose_most_likely_name(name_candidates):
    """
    name_candidates: iterable of strings (may contain None)
    Return the most frequent non-empty candidate, else empty string.
    """
    counts = Counter([str(n).strip() for n in name_candidates if n and str(n).strip()])
    if not counts:
        return ''
    return counts.most_common(1)[0][0]

def read_matches_from_dir(matches_dir):
    """
    Reads all CSV files in directory (non-recursive) and concat into DataFrame.
    Accepts typical CSVs exported previously (comma separated).
    """
    pattern = os.path.join(matches_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {matches_dir} matching *.csv")
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files could be read.")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

# ------------ Core functions ------------

def build_matches_index_for_player(matches_df: pd.DataFrame, player_id: str, max_matches: int = None):
    """
    Create a lightweight index (list) of matches for the player.
    Each entry includes match_id, date, opponent, opponent_id (if any), is_win, score, round, surface, event_year, event_id.
    """
    pid = normalize_player_id(player_id)
    if pid == '':
        return []

    # Identify rows where player appears as winner or loser
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)

    # fallback to matching names if id columns absent
    relevant = pd.DataFrame()
    if cond_w is not False and cond_w.any():
        relevant = pd.concat([relevant, matches_df[cond_w]], ignore_index=True, sort=False)
    if cond_l is not False and cond_l.any():
        relevant = pd.concat([relevant, matches_df[cond_l]], ignore_index=True, sort=False)

    # If no rows found via id, try looking at name columns (safer fallback)
    if relevant.empty:
        # no ID matches - return empty
        return []

    # Normalize certain columns existance
    cols = relevant.columns
    out = []
    for idx, r in relevant.iterrows():
        # determine is_win
        is_win = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_win = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_win = False
        else:
            # fallback by names (best-effort)
            # If player_id present in row but casing different, above handled; else fallback to player_winner/player_loser text match is expensive and fragile; skip
            is_win = None

        match_id = r.get('match_id') if 'match_id' in r.index else r.get('id') if 'id' in r.index else None
        event_id = r.get('event_id') if 'event_id' in r.index else None
        event_year = str(r.get('event_year') or '')
        match_date = parse_date_only(r.get('start_date') or r.get('match_date') or '')
        score = r.get('score_string') if 'score_string' in r.index else r.get('score') if 'score' in r.index else ''
        round_tok = r.get('round') if 'round' in r.index else ''
        surface = (r.get('surface') or '') if 'surface' in r.index else ''
        # opponent fields
        opp_name = ''
        opp_id = None
        if is_win is True:
            opp_name = r.get('player_loser') or r.get('loser_player_name') or ''
            opp_id = r.get('player_id_loser') if 'player_id_loser' in r.index else None
        elif is_win is False:
            opp_name = r.get('player_winner') or r.get('winner_player_name') or ''
            opp_id = r.get('player_id_winner') if 'player_id_winner' in r.index else None
        else:
            # unknown who is winner; attempt best guess
            opp_name = r.get('player_winner') or r.get('winner_player_name') or r.get('player_loser') or r.get('loser_player_name') or ''
            opp_id = r.get('player_id_winner') or r.get('player_id_loser') or None

        entry = {
            'match_id': str(match_id) if match_id is not None else '',
            'event_id': str(event_id) if event_id is not None else '',
            'event_year': str(event_year) if event_year is not None else '',
            'match_date': match_date,
            'opponent': str(opp_name) if opp_name is not None else '',
            'opponent_id': str(opp_id).strip().upper() if opp_id not in (None, '') else None,
            'is_win': bool(is_win) if is_win is not None else None,
            'score': str(score) if score is not None else '',
            'round': str(round_tok) if round_tok is not None else '',
            'surface': str(surface) if surface is not None else ''
        }
        out.append(entry)

    # optional: sort by date ascending
    def date_key(x):
        d = x.get('match_date') or ''
        try:
            return datetime.fromisoformat(d)
        except Exception:
            return datetime.min
    out_sorted = sorted(out, key=date_key)
    if max_matches:
        out_sorted = out_sorted[-int(max_matches):] if len(out_sorted) > int(max_matches) else out_sorted
    return out_sorted

def build_player_meta(matches_df: pd.DataFrame, player_id: str):
    """
    Build a metadata dict for player_id.
    This function intentionally does NOT compute statistics.
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
        return None
    df = pd.concat(frames, ignore_index=True, sort=False)

    # deduce canonical player name: choose most frequent occurrence in player_winner/player_loser
    names = []
    if 'player_winner' in df.columns:
        names.extend(df['player_winner'].dropna().astype(str).tolist())
    if 'player_loser' in df.columns:
        names.extend(df['player_loser'].dropna().astype(str).tolist())
    # also check winner/loser_player_name
    if 'winner_player_name' in df.columns:
        names.extend(df['winner_player_name'].dropna().astype(str).tolist())
    if 'loser_player_name' in df.columns:
        names.extend(df['loser_player_name'].dropna().astype(str).tolist())

    player_name = choose_most_likely_name(names) or ''

    # try to find a country (most frequent non-empty)
    countries = []
    for col in ('country_winner', 'winner_country', 'country_loser', 'loser_country', 'country_winner_1', 'country_loser_1'):
        if col in df.columns:
            countries.extend([str(x).strip().upper() for x in df[col].dropna().astype(str).tolist() if str(x).strip() != ''])
    country = Counter(countries).most_common(1)[0][0] if countries else None

    # basic summary counts
    matches_played = len(df)
    matches_won = 0
    if 'player_id_winner' in df.columns:
        matches_won = int((df['player_id_winner'].astype(str).str.strip().str.upper() == pid).sum())
    else:
        # fallback: try player_winner string comparisons to deduce wins (less reliable)
        if 'player_winner' in df.columns:
            matches_won = int((df['player_winner'].astype(str).str.strip().str.lower() == player_name.strip().lower()).sum())

    matches_lost = matches_played - matches_won

    # small summary object
    summary = {
        'matches_played': int(matches_played),
        'matches_won': int(matches_won),
        'matches_lost': int(matches_lost)
    }

    # build list of match summaries (lightweight)
    matches_index = build_matches_index_for_player(matches_df, pid)

    meta = {
        'player_id': pid,
        'name': player_name,
        'slug': slugify(player_name) or pid.lower(),
        'country': country,
        'summary': summary,
        'matches_count': len(matches_index),
        'matches_index_path': f"players/{pid}.matches.json",  # relative path in output dir
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'version': 'v1'
    }

    return meta, matches_index

# ------------ CLI / Main ------------

def main(matches_dir: str, out_dir: str, limit_players: int = None):
    print("=> Reading matches CSVs from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("=> Read matches: rows =", len(matches), "columns =", len(matches.columns))

    # ensure some standard columns exist lower-case mapping (we'll keep original names)
    # find unique player ids from winner/loser columns
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])

    # remove empty ids
    player_ids = sorted([p for p in player_ids if p])

    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    print(f"=> Found {len(player_ids)} player ids to process")

    # prepare output directories
    idx_dir = os.path.join(out_dir, "index")
    players_dir = os.path.join(out_dir, "players")
    safe_mkdir(idx_dir)
    safe_mkdir(players_dir)

    players_index = []

    # iterate players
    for i, pid in enumerate(player_ids, start=1):
        print(f"[{i}/{len(player_ids)}] Processing player {pid} ...", end=' ')
        try:
            result = build_player_meta(matches, pid)
            if not result:
                print("skip (no rows)")
                continue
            meta, matches_index = result
            # write files
            meta_path = os.path.join(players_dir, f"{pid}.meta.json")
            matches_path = os.path.join(players_dir, f"{pid}.matches.json")
            with open(meta_path, 'w', encoding='utf8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            with open(matches_path, 'w', encoding='utf8') as f:
                json.dump({'matches': matches_index, 'generated_at': datetime.utcnow().isoformat() + 'Z'}, f, ensure_ascii=False, indent=2)

            players_index.append({
                'player_id': pid,
                'name': meta.get('name'),
                'slug': meta.get('slug'),
                'country': meta.get('country'),
                'matches_count': meta.get('matches_count'),
                'meta_path': f"players/{pid}.meta.json",
                'matches_path': f"players/{pid}.matches.json"
            })
            print("done (matches:", len(matches_index), ")")
        except Exception as e:
            print("ERROR processing", pid, ":", e)

    # write global index
    players_index_path = os.path.join(idx_dir, "players_index.json")
    with open(players_index_path, 'w', encoding='utf8') as f:
        json.dump({'players': players_index, 'generated_at': datetime.utcnow().isoformat() + 'Z'}, f, ensure_ascii=False, indent=2)

    print("=> Done. players_index written to", players_index_path)
    print("=> Per-player files written to", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate player metadata and lightweight matches index from matches CSVs.")
    ap.add_argument("--matches-dir", required=True, help="Directory containing matches CSV files (glob *.csv).")
    ap.add_argument("--out-dir", default="./dist", help="Output directory (default ./dist).")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process (for testing).")
    args = ap.parse_args()
    main(args.matches_dir, args.out_dir, args.limit_players)
