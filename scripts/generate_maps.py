#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Module 5: generation des cartes (geo aggregates)
Usage:
  python generate_maps.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200
  python generate_maps.py --matches-dir matches/atp_matches --out-dir dist --player S0AG --host-event-map host_event_map.json

Sorties:
  - <out_dir>/players_atp/{PLAYER_ID}.maps.json
Behavior:
  - Follows the map-aggregation logic used by generate_players_atp_origin.py:
    produces map_opponent_stats and map_host_stats objects suitable for Plotly maps.
"""
from pathlib import Path
import argparse
import os
import json
import glob
import re
from collections import Counter
from datetime import datetime
import pandas as pd

# ----------------- Helpers -----------------

def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid):
    if pid is None:
        return ''
    return str(pid).strip().upper()

def parse_date_only(val):
    """Return YYYY-MM-DD where possible, otherwise a best-effort string (compatible with origin)."""
    if val is None:
        return ''
    try:
        if isinstance(val, str):
            v = val.strip()
            if v == '':
                return ''
            try:
                # Try strict ISO
                dt = datetime.fromisoformat(v)
                return dt.date().isoformat()
            except Exception:
                pass
            # pandas fallback
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
            # regex fallback: first YYYY-MM-DD
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

def read_matches_from_dir(matches_dir):
    """
    Read all CSV files (sorted) inside matches_dir and concat to a single DataFrame.
    """
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

def load_host_event_map(path=None):
    """
    Try to load mapping from event_id -> { year: IOC }.
    If path provided, use it; else try common filenames in cwd.
    """
    if path:
        p = Path(path)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding='utf8'))
            except Exception:
                print(f"[generate_maps] Warning: failed to read host-event map at {path}")
    # try common filenames
    for fname in ("host_event_map.json", "HOST_COUNTRY_TO_EVENT_IDS.json", "host_event_mapping.json"):
        p = Path(fname)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding='utf8'))
            except Exception:
                pass
    return None

# ----------------- Core maps builder -----------------

def build_maps_for_player(matches_df: pd.DataFrame, player_id: str, sample_limit=6, host_event_map=None):
    """
    Build maps (country aggregates) for one player given a full matches_df.
    Returns a dict suitable for JSON dumping, matching the format produced by generate_players_atp_origin.py.
    """
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # Detect rows where player participates
    cond_w = False
    cond_l = False
    if 'player_id_winner' in matches_df.columns:
        try:
            cond_w = matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid
        except Exception:
            cond_w = False
    if 'player_id_loser' in matches_df.columns:
        try:
            cond_l = matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid
        except Exception:
            cond_l = False

    frames = []
    if isinstance(cond_w, pd.Series) and cond_w.any():
        frames.append(matches_df[cond_w])
    if isinstance(cond_l, pd.Series) and cond_l.any():
        frames.append(matches_df[cond_l])

    if not frames:
        # nothing found
        result = {
            'meta': {'player_id': pid, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'matches': 0, 'version': 'v1'},
            'opponent_countries': {},
            'host_countries': {},
            'map_opponent_stats': {},
            'map_host_stats': {}
        }
        return result

    df = pd.concat(frames, ignore_index=True, sort=False)

    # determine canonical player name if available
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            name_candidates.extend([str(x) for x in df[col].dropna().astype(str).tolist()])
    player_name = ''
    if name_candidates:
        player_name = Counter(name_candidates).most_common(1)[0][0]

    # aggregates
    opp_agg = {}   # country -> {wins, losses, matches, sample_matches}
    host_agg = {}  # country -> {wins, losses, matches, titles, sample_matches}

    title_rounds = set(['W','WIN','F'])

    for idx, r in df.iterrows():
        # determine if player won
        is_win = None
        try:
            if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
                is_win = True
            elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
                is_win = False
            else:
                # fallback: compare by name columns if available (less reliable)
                # try winner_player_name / loser_player_name
                wn = r.get('winner_player_name') or r.get('player_winner') or ''
                ln = r.get('loser_player_name') or r.get('player_loser') or ''
                try:
                    wn_s = str(wn).strip()
                    ln_s = str(ln).strip()
                    if wn_s and pid and wn_s.upper().find(pid) != -1:
                        is_win = True
                    elif ln_s and pid and ln_s.upper().find(pid) != -1:
                        is_win = False
                except Exception:
                    is_win = None
        except Exception:
            is_win = None

        # opponent country detection
        opp_country = ''
        if is_win is True:
            for col in ('country_loser', 'loser_country', 'country_loser_1', 'loser_country_1'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    opp_country = str(r.get(col, '')).strip().upper()
                    break
        elif is_win is False:
            for col in ('country_winner', 'winner_country', 'country_winner_1', 'winner_country_1'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    opp_country = str(r.get(col, '')).strip().upper()
                    break

        # host country detection: first try explicit host columns
        host_country = ''
        for col in ('host_country','event_country','country_event'):
            if col in r.index and str(r.get(col, '')).strip() != '':
                host_country = str(r.get(col, '')).strip().upper()
                break

        # fallback: winner/loser country if explicit host not found
        if not host_country:
            for col in ('country_winner', 'winner_country', 'country_loser', 'loser_country'):
                if col in r.index and str(r.get(col, '')).strip() != '':
                    host_country = str(r.get(col, '')).strip().upper()
                    break

        # event->host mapping via host_event_map if available (preferred)
        ev = str(r.get('event_id') or '')
        ev_year = str(r.get('event_year') or '')
        if (not host_country) and ev:
            try:
                ev_map = None
                if host_event_map:
                    ev_map = host_event_map.get(ev) or host_event_map.get(str(ev))
                if ev_map and isinstance(ev_map, dict):
                    if ev_year and ev_year in ev_map:
                        host_country = ev_map.get(ev_year)
                    elif 'default' in ev_map:
                        host_country = ev_map.get('default')
                    else:
                        # pick first mapping
                        for v in ev_map.values():
                            host_country = v
                            break
                    if isinstance(host_country, str):
                        host_country = host_country.strip().upper()
            except Exception:
                pass

        match_entry = {
            'match_id': str(r.get('match_id') or ''),
            'event_id': str(r.get('event_id') or ''),
            'event_year': str(r.get('event_year') or ''),
            'tourney_name': str(r.get('tourney_name') or '') if 'tourney_name' in r.index else '',
            'match_date': parse_date_only(r.get('start_date') or r.get('match_date') or ''),
            'opponent_country': opp_country,
            'host_country': host_country,
            'is_win': bool(is_win) if is_win is not None else None,
            'score': str(r.get('score_string') or r.get('score') or '')
        }

        # update opponent map
        if opp_country:
            o = opp_agg.get(opp_country, {'wins':0,'losses':0,'matches':0,'sample_matches':[]})
            if is_win is True:
                o['wins'] += 1
            elif is_win is False:
                o['losses'] += 1
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append(match_entry)
            opp_agg[opp_country] = o

        # update host map
        if host_country:
            h = host_agg.get(host_country, {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]})
            if is_win is True:
                h['wins'] += 1
            elif is_win is False:
                h['losses'] += 1
            h['matches'] += 1
            # titles detection by round token
            rnd = str(r.get('round') or '')
            if is_win is True and rnd and any(tok in rnd.upper() for tok in title_rounds):
                h['titles'] = h.get('titles', 0) + 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append(match_entry)
            host_agg[host_country] = h

    # mark additional titles by unique tournament event (safer approach)
    title_event_ids = set()
    # Build set of unique event_id_event_year where player won the tournament
    for idx, row in df.iterrows():
        try:
            is_win = None
            if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
                is_win = True
            elif 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
                is_win = False
            else:
                is_win = None
        except Exception:
            is_win = None
        roundv = str(row.get('round') or '')
        if is_win and roundv and any(tok in roundv.upper() for tok in title_rounds):
            eid = str(row.get('event_id') or '')
            eyear = str(row.get('event_year') or '')
            if eid:
                title_event_ids.add(f"{eid}_{eyear}")

    # Use host_event_map to increment titles per host country for unique tournament wins
    for tkey in title_event_ids:
        ev_id, ev_year = (tkey.split('_') + [''])[:2]
        host_iso = None
        try:
            if host_event_map:
                ev_map = host_event_map.get(ev_id) or host_event_map.get(str(ev_id))
                if isinstance(ev_map, dict):
                    if ev_year and ev_year in ev_map:
                        host_iso = ev_map.get(ev_year)
                    elif 'default' in ev_map:
                        host_iso = ev_map.get('default')
                    else:
                        for v in ev_map.values():
                            host_iso = v
                            break
        except Exception:
            host_iso = None
        if host_iso:
            host_iso = str(host_iso).strip().upper()
            if host_iso not in host_agg:
                host_agg[host_iso] = {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]}
            host_agg[host_iso]['titles'] = host_agg[host_iso].get('titles', 0) + 1

    # compute win_rate floats and prepare map objects
    map_opponent_stats = {}
    for c, s in opp_agg.items():
        matches_n = s.get('matches', 0)
        wins_n = s.get('wins', 0)
        win_rate = (wins_n / matches_n) if matches_n else None
        map_opponent_stats[c] = {
            'wins': wins_n,
            'losses': s.get('losses', 0),
            'matches': matches_n,
            'win_rate': win_rate,
            'sample_matches': s.get('sample_matches', [])
        }

    map_host_stats = {}
    for c, s in host_agg.items():
        matches_n = s.get('matches', 0)
        wins_n = s.get('wins', 0)
        win_rate = (wins_n / matches_n) if matches_n else None
        map_host_stats[c] = {
            'wins': wins_n,
            'losses': s.get('losses', 0),
            'matches': matches_n,
            'win_rate': win_rate,
            'titles': s.get('titles', 0),
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
        # legacy/alternate keys (kept for compatibility)
        'opponent_countries': opp_agg,
        'host_countries': host_agg,
        # canonical keys used by the player HTML/JS
        'map_opponent_stats': map_opponent_stats,
        'map_host_stats': map_host_stats
    }
    return result

def build_maps_for_player_from_matches_dir(matches_dir, player_id, sample_limit=6, host_event_map=None):
    matches = read_matches_from_dir(matches_dir)
    return build_maps_for_player(matches, player_id, sample_limit=sample_limit, host_event_map=host_event_map)

# ----------------- CLI Main -----------------

def main(matches_dir, out_dir, player_list=None, limit_players=None, host_event_map_path=None):
    print("[generate_maps] Reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[generate_maps] matches rows:", len(matches), "columns:", len(matches.columns))

    # try to load host_event_map (if provided)
    host_event_map = load_host_event_map(host_event_map_path)

    # discover players from winner/loser columns
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    print(f"[generate_maps] discovered {len(player_ids)} player ids")

    if player_list:
        player_ids = [p for p in player_ids if p in set(player_list)]
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
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process")
    ap.add_argument("--player", help="Process a single player id (e.g. S0AG)", action='append')
    ap.add_argument("--host-event-map", help="Path to host_event_map.json (optional)")
    args = ap.parse_args()
    plist = args.player if args.player else None
    main(args.matches_dir, args.out_dir, player_list=plist, limit_players=args.limit_players, host_event_map_path=args.host_event_map)
