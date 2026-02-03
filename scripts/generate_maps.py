#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Module: generation des cartes (geo aggregates)
Usage:
  python generate_maps.py --matches-dir /path/to/matches --out-dir ./dist --host-event-map ./host_event_map.json --limit-players 200
Sorties:
  - <out_dir>/players_atp/{PLAYER_ID}.maps.json
Notes:
 - Produit deux objets principaux dans le JSON de sortie:
    map_opponent_stats : { 'FRA': {wins, losses, matches, win_rate, sample_matches:[..]}, ... }
    map_host_stats     : { 'FRA': {wins, losses, matches, titles, win_rate, sample_matches:[..]}, ... }
 - Lecture robuste des CSV, gestion des colonnes manquantes, fallback par nom si nécessaire.
"""
from __future__ import annotations
import argparse
import os
import json
import glob
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

# ----------------- Helpers -----------------
def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid: Optional[str]) -> str:
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date_only(val: Optional[str]) -> str:
    if val is None:
        return ''
    try:
        v = str(val).strip()
        if not v:
            return ''
        # try iso parse
        try:
            dt = datetime.fromisoformat(v)
            return dt.date().isoformat()
        except Exception:
            pass
        # pandas fallback
        dt = pd.to_datetime(v, errors='coerce')
        if not pd.isna(dt):
            return dt.date().isoformat()
        # regex fallback find yyyy-mm-dd
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
        return v
    except Exception:
        return ''

def read_matches_from_dir(matches_dir: str) -> pd.DataFrame:
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
            print(f"[generate_maps] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files could be read.")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

def load_host_event_map(path: Optional[str]) -> Dict[str, Dict[str, str]]:
    if not path:
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            d = json.load(f)
            # expecting mapping { event_id: { year: ISO_CODE, ... }, ... }
            return d if isinstance(d, dict) else {}
    except Exception as e:
        print(f"[generate_maps] Could not load host_event_map.json at {path}: {e}")
        return {}

# ----------------- Core builder -----------------

def build_maps_for_player(matches_df: pd.DataFrame,
                          player_id: str,
                          sample_limit: int = 6,
                          host_event_map: Optional[Dict[str, Dict[str, str]]] = None) -> Optional[Dict[str, Any]]:
    """
    Build maps (country aggregates) for one player given full matches_df.
    Returns a dict suitable for JSON dumping (or None if player_id invalid).
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # determine rows where this player participates
    # be defensive: columns may be absent
    winner_col = 'player_id_winner'
    loser_col = 'player_id_loser'
    frames = []
    if winner_col in matches_df.columns:
        try:
            mask_w = matches_df[winner_col].astype(str).str.strip().str.upper() == pid
            if mask_w.any():
                frames.append(matches_df.loc[mask_w])
        except Exception:
            pass
    if loser_col in matches_df.columns:
        try:
            mask_l = matches_df[loser_col].astype(str).str.strip().str.upper() == pid
            if mask_l.any():
                frames.append(matches_df.loc[mask_l])
        except Exception:
            pass

    # if nothing from exact id, fallback to substring/name heuristics (defensive)
    if not frames:
        # try substring on id columns
        mask_any = pd.Series(False, index=matches_df.index)
        for cid in (winner_col, loser_col):
            if cid in matches_df.columns:
                try:
                    mask_any = mask_any | matches_df[cid].astype(str).str.upper().str.contains(pid, na=False)
                except Exception:
                    pass
        # try name columns if we have any occurrence of pid like 'N. DJOKOVIC' may not help; skip name fallback here
        if mask_any.any():
            frames.append(matches_df.loc[mask_any])

    if not frames:
        # nothing found => return empty map structure
        return {
            'meta': {'player_id': pid, 'player_name': '', 'matches': 0, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v1'},
            'map_opponent_stats': {},
            'map_host_stats': {}
        }

    df = pd.concat(frames, ignore_index=True, sort=False)

    # pick canonical player name if possible
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            name_candidates.extend([str(x) for x in df[col].dropna().astype(str).tolist()])
    player_name = ''
    if name_candidates:
        player_name = Counter(name_candidates).most_common(1)[0][0]

    # We'll produce a normalized list of matches with minimal fields used by maps
    matches_out: List[Dict[str, Any]] = []
    title_round_tokens = set(['W','WIN','F'])  # which rounds count as a title

    for idx, row in df.iterrows():
        # determine if player won this row
        is_win = None
        try:
            if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
                is_win = True
            elif 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
                is_win = False
            else:
                # fallback by comparing winner/loser name fields against most frequent name if available
                if player_name:
                    pname = player_name.strip().lower()
                    if 'player_winner' in row.index and pname in str(row.get('player_winner','')).lower():
                        is_win = True
                    elif 'player_loser' in row.index and pname in str(row.get('player_loser','')).lower():
                        is_win = False
        except Exception:
            is_win = None

        # opponent country detection
        opp_country = ''
        try:
            if is_win is True:
                # opponent is loser
                for col in ('country_loser','loser_country','country_loser_1','loser_country_1','country_loser1'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_country = str(row.get(col, '')).strip().upper()
                        break
            elif is_win is False:
                for col in ('country_winner','winner_country','country_winner_1','winner_country_1','country_winner1'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_country = str(row.get(col, '')).strip().upper()
                        break
            # if still empty, try general columns
            if not opp_country:
                for col in ('country_winner','country_loser','winner_country','loser_country'):
                    if col in row.index and str(row.get(col, '')).strip():
                        # choose the other player's country if possible
                        val = str(row.get(col, '')).strip().upper()
                        # determine whether 'col' refers to opponent or not
                        # simple heuristic: if col contains 'winner' and player is loser, then it's opponent
                        if ('winner' in col and is_win is False) or ('loser' in col and is_win is True):
                            opp_country = val
                            break
        except Exception:
            opp_country = ''

        # host country detection
        host_country = ''
        try:
            # direct host columns
            for col in ('host_country','event_country','country_event','host_country_code'):
                if col in row.index and str(row.get(col, '')).strip():
                    host_country = str(row.get(col, '')).strip().upper()
                    break

            # fallback: use winner/loser country as best-effort
            if not host_country:
                for col in ('country_winner','winner_country','country_loser','loser_country'):
                    if col in row.index and str(row.get(col, '')).strip():
                        host_country = str(row.get(col, '')).strip().upper()
                        break

            # final fallback: use event_id + event_year mapping if available
            if not host_country:
                event_id = str(row.get('event_id') or '').strip()
                event_year = str(row.get('event_year') or '').strip()
                if event_id and host_event_map:
                    ev_map = host_event_map.get(event_id) or host_event_map.get(str(event_id))
                    if isinstance(ev_map, dict):
                        # prefer exact year match
                        host_country = ev_map.get(event_year) or ev_map.get('default') or (next(iter(ev_map.values()), '') if ev_map else '')
                        if isinstance(host_country, str):
                            host_country = host_country.strip().upper()
        except Exception:
            host_country = ''

        match_entry = {
            'match_id': str(row.get('match_id') or ''),
            'event_id': str(row.get('event_id') or ''),
            'event_year': str(row.get('event_year') or ''),
            'match_date': parse_date_only(row.get('start_date') or row.get('match_date') or ''),
            'tourney_name': str(row.get('tourney_name') or '')[:250],
            'opponent_country': opp_country,
            'host_country': host_country,
            'is_win': True if is_win is True else (False if is_win is False else None),
            'score': str(row.get('score_string') or row.get('score') or '')
        }
        matches_out.append(match_entry)

    # Build aggregates
    opp_map: Dict[str, Dict[str, Any]] = {}
    host_map: Dict[str, Dict[str, Any]] = {}

    for m in matches_out:
        oc = (m.get('opponent_country') or '').strip().upper()
        if oc:
            o = opp_map.get(oc, {'wins': 0, 'losses': 0, 'matches': 0, 'sample_matches': []})
            if m.get('is_win') is True:
                o['wins'] += 1
            elif m.get('is_win') is False:
                o['losses'] += 1
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append({
                    'event_id': m.get('event_id'),
                    'event_year': m.get('event_year'),
                    'tourney_name': m.get('tourney_name'),
                    'match_date': m.get('match_date'),
                    'is_win': bool(m.get('is_win')) if m.get('is_win') is not None else None,
                    'score': m.get('score')
                })
            opp_map[oc] = o

        hc = (m.get('host_country') or '').strip().upper()
        if hc:
            h = host_map.get(hc, {'wins': 0, 'losses': 0, 'matches': 0, 'titles': 0, 'sample_matches': []})
            if m.get('is_win') is True:
                h['wins'] += 1
            elif m.get('is_win') is False:
                h['losses'] += 1
            h['matches'] += 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append({
                    'event_id': m.get('event_id'),
                    'event_year': m.get('event_year'),
                    'tourney_name': m.get('tourney_name'),
                    'match_date': m.get('match_date'),
                    'is_win': bool(m.get('is_win')) if m.get('is_win') is not None else None,
                    'score': m.get('score')
                })
            host_map[hc] = h

    # Titles: detect unique event_id+year where player won final/title
    title_event_ids = set()
    for m in matches_out:
        try:
            if m.get('is_win') and str(m.get('round') or '').upper() in ('W','WIN','F'):
                key = f"{m.get('event_id')}_{m.get('event_year')}"
                title_event_ids.add(key)
        except Exception:
            # our minimal matches_out likely doesn't have 'round'; so try to detect titles using score=='' + indicator -- skip if not available
            pass

    # Another attempt: if CSV contains a 'round' column we didn't copy to matches_out, try to inspect original df rows
    if 'round' in df.columns:
        for idx, row in df.iterrows():
            try:
                if normalize_player_id(row.get('player_id_winner')) == pid and str(row.get('round') or '').strip().upper() in ('W','WIN','F'):
                    key = f"{row.get('event_id')}_{row.get('event_year')}"
                    title_event_ids.add(key)
            except Exception:
                pass

    # Map titles to host country via host_event_map if possible (increment country titles)
    for tkey in title_event_ids:
        parts = tkey.split('_', 1)
        ev_id = parts[0] if parts else ''
        ev_year = parts[1] if len(parts) > 1 else ''
        host_iso = None
        if host_event_map:
            ev_map = host_event_map.get(ev_id) or host_event_map.get(str(ev_id))
            if isinstance(ev_map, dict):
                host_iso = ev_map.get(ev_year) or ev_map.get('default')
                # fallback to any available value
                if not host_iso:
                    for v in ev_map.values():
                        if v:
                            host_iso = v
                            break
        if host_iso:
            host_iso = str(host_iso).strip().upper()
            if host_iso:
                if host_iso not in host_map:
                    host_map[host_iso] = {'wins': 0, 'losses': 0, 'matches': 0, 'titles': 0, 'sample_matches': []}
                host_map[host_iso]['titles'] = host_map[host_iso].get('titles', 0) + 1

    # compute win_rate fields
    map_opponent_stats: Dict[str, Any] = {}
    for c, s in opp_map.items():
        matches_n = int(s.get('matches', 0))
        wins_n = int(s.get('wins', 0))
        win_rate = (wins_n / matches_n) if matches_n else None
        map_opponent_stats[c] = {
            'wins': wins_n,
            'losses': int(s.get('losses', 0)),
            'matches': matches_n,
            'win_rate': win_rate,
            'sample_matches': s.get('sample_matches', [])
        }

    map_host_stats: Dict[str, Any] = {}
    for c, s in host_map.items():
        matches_n = int(s.get('matches', 0))
        wins_n = int(s.get('wins', 0))
        win_rate = (wins_n / matches_n) if matches_n else None
        map_host_stats[c] = {
            'wins': wins_n,
            'losses': int(s.get('losses', 0)),
            'matches': matches_n,
            'win_rate': win_rate,
            'titles': int(s.get('titles', 0)),
            'sample_matches': s.get('sample_matches', [])
        }

    result = {
        'meta': {
            'player_id': pid,
            'player_name': player_name,
            'matches': int(len(df)),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'version': 'v1'
        },
        'map_opponent_stats': map_opponent_stats,
        'map_host_stats': map_host_stats
    }
    return result

# ----------------- CLI Main -----------------

def main(matches_dir: str, out_dir: str, host_event_map_path: Optional[str] = None,
         limit_players: Optional[int] = None, specific_player: Optional[str] = None):
    print("[generate_maps] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[generate_maps] matches rows:", len(matches), "columns:", len(matches.columns))

    host_event_map = load_host_event_map(host_event_map_path)

    # discover player ids from winner/loser columns
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    print(f"[generate_maps] discovered {len(player_ids)} player ids")

    if specific_player:
        player_ids = [normalize_player_id(specific_player)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, start=1):
        try:
            print(f"[generate_maps] [{i}/{len(player_ids)}] building maps for {pid} ...")
            maps_obj = build_maps_for_player(matches, pid, sample_limit=6, host_event_map=host_event_map)
            out_path = os.path.join(players_dir, f"{pid}.maps.json")
            with open(out_path, 'w', encoding='utf8') as f:
                json.dump(maps_obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[generate_maps] ERROR building maps for {pid}: {e}")

    print("[generate_maps] Done. Maps written to", players_dir)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate per-player maps (opponent/host country aggregates) from matches CSVs.")
    ap.add_argument("--matches-dir", required=True, help="Directory containing matches CSV files")
    ap.add_argument("--out-dir", default="./dist", help="Output directory")
    ap.add_argument("--host-event-map", default=None, help="Path to host_event_map.json (optional)")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process")
    ap.add_argument("--player", help="Process a single player id (e.g. S0AG)")
    args = ap.parse_args()
    main(args.matches_dir, args.out_dir, host_event_map_path=args.host_event_map, limit_players=args.limit_players, specific_player=args.player)
