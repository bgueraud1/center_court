#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Module 5: generation des cartes (geo aggregates)
Usage:
  python generate_maps.py --matches-dir /path/to/matches --out-dir ./dist --limit-players 200

Sorties:
  - <out_dir>/players_atp/{PLAYER_ID}.maps.json
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
    try:
        s = str(pid)
    except Exception:
        return ''
    return s.strip().upper()

def normalize_name(n):
    if n is None:
        return ''
    try:
        s = str(n)
    except Exception:
        return ''
    return s.strip()

def parse_date_only(val):
    """Return YYYY-MM-DD where possible, otherwise a best-effort string."""
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
            print(f"[generate_maps] Warning: failed to read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files could be read.")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

def load_host_event_map(path=None):
    if not path:
        # try common names
        for fname in ("host_event_map.json", "HOST_COUNTRY_TO_EVENT_IDS.json", "host_event_mapping.json"):
            p = Path(fname)
            if p.exists():
                try:
                    return json.loads(p.read_text(encoding='utf8'))
                except Exception:
                    pass
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding='utf8'))
    except Exception:
        return None

# ----------------- Core maps builder -----------------

def build_maps_for_player(matches_df: pd.DataFrame, player_id: str, sample_limit=6, host_event_map=None):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # Build mask of rows where player participates (try both id columns and name columns)
    masks = []
    if 'player_id_winner' in matches_df.columns:
        try:
            masks.append(matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
        except Exception:
            pass
    if 'player_id_loser' in matches_df.columns:
        try:
            masks.append(matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)
        except Exception:
            pass

    # If no id masks, try matching names roughly (rare)
    # We'll build a weak name candidate from the dataframe for this pid by scanning candidate name columns
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in matches_df.columns:
            try:
                name_candidates.extend([normalize_name(x) for x in matches_df[col].dropna().astype(str).tolist()])
            except Exception:
                pass
    # find a most common name that includes a token of player_id (heuristic)
    player_name_guess = ''
    if name_candidates:
        c = Counter(name_candidates).most_common(1)
        if c:
            player_name_guess = c[0][0]

    if not masks:
        # try fallback: match player_name_guess presence in winner/loser name columns
        try:
            m1 = matches_df['player_winner'].astype(str).str.strip().str.upper().str.contains(pid, na=False)
            m2 = matches_df['player_loser'].astype(str).str.strip().str.upper().str.contains(pid, na=False)
            masks.extend([m1, m2])
        except Exception:
            pass

    if masks:
        full_mask = masks[0]
        for m in masks[1:]:
            try:
                full_mask = full_mask | m
            except Exception:
                pass
        df_part = matches_df[full_mask].copy()
    else:
        # nothing matched; return empty structure
        return {
            'meta': {'player_id': pid, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'matches': 0, 'version': 'v1'},
            'opponent_countries': {},
            'host_countries': {},
            'map_opponent_stats': {},
            'map_host_stats': {}
        }

    # Determine canonical player name from rows where this id appears (if possible)
    name_candidates_rows = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df_part.columns:
            name_candidates_rows.extend([normalize_name(x) for x in df_part[col].dropna().astype(str).tolist()])
    player_name = ''
    if name_candidates_rows:
        player_name = Counter(name_candidates_rows).most_common(1)[0][0]

    # Prepare aggregates
    opp_map = {}
    host_map = {}
    title_rounds = set(['W','WIN','F'])

    # iterate rows - robustly read columns by name
    for _, row in df_part.iterrows():
        # detect win/lose robustly:
        is_win = None
        # check player id columns first
        try:
            pid_w = normalize_player_id(row.get('player_id_winner') if 'player_id_winner' in row.index else None)
            pid_l = normalize_player_id(row.get('player_id_loser') if 'player_id_loser' in row.index else None)
            if pid_w == pid:
                is_win = True
            elif pid_l == pid:
                is_win = False
        except Exception:
            is_win = None

        # fallback to name-based detection using available name columns and the player_name guess
        if is_win is None:
            try:
                wn = normalize_name(row.get('winner_player_name') or row.get('player_winner') or '')
                ln = normalize_name(row.get('loser_player_name') or row.get('player_loser') or '')
                # exact match against discovered canonical name
                if player_name and wn and player_name == wn:
                    is_win = True
                elif player_name and ln and player_name == ln:
                    is_win = False
                else:
                    # partial heuristic: if winner contains PID token (rare) or equal to player_name_guess
                    if wn and pid and pid in wn.upper():
                        is_win = True
                    elif ln and pid and pid in ln.upper():
                        is_win = False
            except Exception:
                is_win = None

        # Opponent country detection depends on is_win
        opp_country = ''
        if is_win is True:
            for col in ('country_loser', 'loser_country', 'country_loser_1', 'loser_country_1'):
                if col in row.index and str(row.get(col, '')).strip() != '':
                    opp_country = str(row.get(col, '')).strip().upper()
                    break
        elif is_win is False:
            for col in ('country_winner', 'winner_country', 'country_winner_1', 'winner_country_1'):
                if col in row.index and str(row.get(col, '')).strip() != '':
                    opp_country = str(row.get(col, '')).strip().upper()
                    break
        else:
            # if unknown, try to infer opponent country by looking at both winner/loser country and comparing to player's country
            # try country_winner/country_loser columns
            cw = (str(row.get('country_winner') or '')).strip().upper() if 'country_winner' in row.index else ''
            cl = (str(row.get('country_loser') or '')).strip().upper() if 'country_loser' in row.index else ''
            # if one of these equals player's country we can guess opponent is the other; otherwise leave empty
            player_country_guess = None
            # try to get player country from winner/loser columns if pid matched winner/loser earlier (but we don't know)
            if cw and cl:
                # can't decide, leave empty
                opp_country = ''
            else:
                opp_country = cw or cl or ''

        # Host country detection
        host_country = ''
        for col in ('host_country','event_country','country_event'):
            if col in row.index and str(row.get(col, '')).strip() != '':
                host_country = str(row.get(col, '')).strip().upper()
                break
        if not host_country:
            for col in ('country_winner', 'winner_country', 'country_loser', 'loser_country'):
                if col in row.index and str(row.get(col, '')).strip() != '':
                    host_country = str(row.get(col, '')).strip().upper()
                    break

        # use host_event_map if available and host_country still empty
        ev = str(row.get('event_id') or '')
        ev_year = str(row.get('event_year') or '')
        if (not host_country) and ev and host_event_map:
            try:
                ev_map = host_event_map.get(ev) or host_event_map.get(str(ev))
                if isinstance(ev_map, dict):
                    if ev_year and ev_year in ev_map:
                        host_country = ev_map.get(ev_year)
                    elif 'default' in ev_map:
                        host_country = ev_map.get('default')
                    else:
                        # pick first value
                        for v in ev_map.values():
                            host_country = v
                            break
                    if isinstance(host_country, str):
                        host_country = host_country.strip().upper()
            except Exception:
                pass

        # Build match entry
        match_entry = {
            'match_id': str(row.get('match_id') or ''),
            'event_id': str(row.get('event_id') or ''),
            'event_year': ev_year,
            'match_date': parse_date_only(row.get('start_date') or row.get('match_date') or ''),
            'opponent_country': opp_country,
            'host_country': host_country,
            'is_win': bool(is_win) if is_win is not None else None,
            'score': str(row.get('score_string') or row.get('score') or '')
        }

        # update opponent map
        if opp_country:
            o = opp_map.get(opp_country, {'wins':0,'losses':0,'matches':0,'sample_matches':[]})
            if is_win is True:
                o['wins'] += 1
            elif is_win is False:
                o['losses'] += 1
            else:
                # unknown - count as match but don't increment win/loss
                pass
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append(match_entry)
            opp_map[opp_country] = o

        # update host map
        if host_country:
            h = host_map.get(host_country, {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]})
            if is_win is True:
                h['wins'] += 1
            elif is_win is False:
                h['losses'] += 1
            else:
                pass
            h['matches'] += 1
            rnd = str(row.get('round') or '')
            if is_win is True and rnd and any(tok in rnd.upper() for tok in title_rounds):
                h['titles'] = h.get('titles', 0) + 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append(match_entry)
            host_map[host_country] = h

    # compute additional titles by unique event wins (dedupe by event_id+year)
    title_event_ids = set()
    for _, row in df_part.iterrows():
        # robust detection again
        is_win = None
        try:
            if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
                is_win = True
            elif 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
                is_win = False
        except Exception:
            is_win = None
        rnd = str(row.get('round') or '')
        if is_win and rnd and any(tok in rnd.upper() for tok in title_rounds):
            eid = str(row.get('event_id') or '')
            eyear = str(row.get('event_year') or '')
            if eid:
                title_event_ids.add(f"{eid}__{eyear}")

    for tkey in title_event_ids:
        eid, eyear = tkey.split('__') if '__' in tkey else (tkey, '')
        host_iso = None
        if host_event_map:
            try:
                em = host_event_map.get(eid) or host_event_map.get(str(eid))
                if isinstance(em, dict):
                    if eyear and eyear in em:
                        host_iso = em.get(eyear)
                    elif 'default' in em:
                        host_iso = em.get('default')
                    else:
                        for v in em.values():
                            host_iso = v
                            break
            except Exception:
                host_iso = None
        if host_iso:
            host_iso = str(host_iso).strip().upper()
            if host_iso not in host_map:
                host_map[host_iso] = {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]}
            host_map[host_iso]['titles'] = host_map[host_iso].get('titles', 0) + 1

    # compute win_rate and prepare canonical maps
    map_opponent_stats = {}
    for c, o in opp_map.items():
        m = o.get('matches', 0) or 0
        w = o.get('wins', 0) or 0
        win_rate = (w / m) if m else None
        map_opponent_stats[c] = {
            'wins': o.get('wins', 0),
            'losses': o.get('losses', 0),
            'matches': int(m),
            'win_rate': float(win_rate) if win_rate is not None else None,
            'sample_matches': o.get('sample_matches', [])
        }

    map_host_stats = {}
    for c, h in host_map.items():
        m = h.get('matches', 0) or 0
        w = h.get('wins', 0) or 0
        win_rate = (w / m) if m else None
        map_host_stats[c] = {
            'wins': h.get('wins', 0),
            'losses': h.get('losses', 0),
            'matches': int(m),
            'win_rate': float(win_rate) if win_rate is not None else None,
            'titles': int(h.get('titles', 0) or 0),
            'sample_matches': h.get('sample_matches', [])
        }

    result = {
        'meta': {
            'player_id': pid,
            'player_name': player_name,
            'matches': int(len(df_part)),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'version': 'v1'
        },
        # legacy keys
        'opponent_countries': opp_map,
        'host_countries': host_map,
        # canonical keys used by player.html
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

    host_event_map = load_host_event_map(host_event_map_path)

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
