#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Generate per-player maps (opponent/host country aggregates)

Usage:
  python generate_maps.py --matches-dir /path/to/csvs --out-dir ./dist --player S0AG

Output:
  <out_dir>/players_atp/{PLAYER_ID}.maps.json

Notes:
- Prioritize detection by player_id_winner / player_id_loser (strict).
- Expect country codes in CSV to be ISO-3 (e.g. ITA, ESP). If CSV contains ISO-2, the script will try a small builtin mapping.
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

def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

def norm_pid(x):
    if x is None: return ''
    return str(x).strip().upper()

def norm_str(x):
    if x is None: return ''
    return str(x).strip()

def parse_date_only(v):
    if v is None: return ''
    try:
        if isinstance(v, str):
            v = v.strip()
            if not v: return ''
            try:
                return datetime.fromisoformat(v).date().isoformat()
            except Exception:
                pass
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
            m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
            if m: return m.group(1)
            return v
        else:
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt): return dt.date().isoformat()
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
    return pd.concat(frames, ignore_index=True, sort=False)

# small mapping for common ISO2 -> ISO3 if input uses 2-letter codes
ISO2_TO_ISO3 = {
    'IT':'ITA','ES':'ESP','FR':'FRA','DE':'DEU','GB':'GBR','US':'USA','AR':'ARG','BR':'BRA','CH':'CHE','CA':'CAN','AU':'AUS','RS':'SRB','IT':'ITA'
}

def ensure_iso3(code):
    if not code: return ''
    s = str(code).strip().upper()
    if len(s) == 3:
        return s
    if len(s) == 2:
        return ISO2_TO_ISO3.get(s, '')
    return ''

def build_maps_for_player(matches_df: pd.DataFrame, player_id: str, sample_limit=6, host_event_map=None):
    pid = norm_pid(player_id)
    if not pid: return None

    # try strict selection by player id columns
    has_pid_w = 'player_id_winner' in matches_df.columns
    has_pid_l = 'player_id_loser' in matches_df.columns

    rows = []
    if has_pid_w:
        try:
            sel = matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid
            rows.append(matches_df[sel])
        except Exception:
            pass
    if has_pid_l:
        try:
            sel = matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid
            rows.append(matches_df[sel])
        except Exception:
            pass

    # if nothing matched and there are name columns, fallback to name match (very rare)
    if not rows:
        name_cols = [c for c in ('player_winner','player_loser','winner_player_name','loser_player_name') if c in matches_df.columns]
        if name_cols:
            # attempt to find rows where any of those columns contains pid token (not ideal)
            mask = pd.Series(False, index=matches_df.index)
            for c in name_cols:
                try:
                    mask = mask | matches_df[c].astype(str).str.strip().str.upper().str.contains(pid, na=False)
                except Exception:
                    pass
            if mask.any():
                rows.append(matches_df[mask])

    if not rows:
        return {
            'meta': {'player_id': pid, 'player_name':'', 'matches':0, 'generated_at': datetime.utcnow().isoformat()+'Z', 'version':'v1'},
            'opponent_countries': {}, 'host_countries': {}, 'map_opponent_stats': {}, 'map_host_stats': {}
        }

    df = pd.concat(rows, ignore_index=True, sort=False)

    # canonical player name if possible
    name_cands = []
    for c in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if c in df.columns:
            name_cands.extend([norm_str(x) for x in df[c].dropna().astype(str).tolist()])
    player_name = Counter(name_cands).most_common(1)[0][0] if name_cands else ''

    opp_map = {}
    host_map = {}
    title_rounds = set(['W','WIN','F'])

    for _, r in df.iterrows():
        # determine is_win strictly from id columns if present
        is_win = None
        try:
            if has_pid_w and norm_pid(r.get('player_id_winner')) == pid:
                is_win = True
            elif has_pid_l and norm_pid(r.get('player_id_loser')) == pid:
                is_win = False
        except Exception:
            is_win = None

        # If still unknown, try heuristic by matching names
        if is_win is None:
            try:
                wn = norm_str(r.get('winner_player_name') or r.get('player_winner') or '')
                ln = norm_str(r.get('loser_player_name') or r.get('player_loser') or '')
                if player_name:
                    if wn and player_name == wn: is_win = True
                    elif ln and player_name == ln: is_win = False
                # last resort: if winner contains pid token
                if is_win is None:
                    if wn and pid in wn.upper(): is_win = True
                    elif ln and pid in ln.upper(): is_win = False
            except Exception:
                is_win = None

        # opponent country
        opp_country = ''
        if is_win is True:
            for col in ('country_loser','loser_country','country_loser_1','loser_country_1'):
                if col in r.index and norm_str(r.get(col, '')) != '':
                    opp_country = norm_str(r.get(col)).upper(); break
        elif is_win is False:
            for col in ('country_winner','winner_country','country_winner_1','winner_country_1'):
                if col in r.index and norm_str(r.get(col, '')) != '':
                    opp_country = norm_str(r.get(col)).upper(); break
        else:
            # unknown -> attempt to use loser_country/winner_country but avoid mixing
            if 'loser_country' in r.index and norm_str(r.get('loser_country','')) != '':
                opp_country = norm_str(r.get('loser_country')).upper()
            elif 'winner_country' in r.index and norm_str(r.get('winner_country','')) != '':
                opp_country = norm_str(r.get('winner_country')).upper()

        # host country detection
        host_country = ''
        for col in ('host_country','event_country','country_event'):
            if col in r.index and norm_str(r.get(col,'')) != '':
                host_country = norm_str(r.get(col)).upper(); break
        if not host_country:
            # fallback to event mapping or winner/loser country as last resort
            ev = str(r.get('event_id') or '')
            evy = str(r.get('event_year') or '')
            if ev and host_event_map:
                try:
                    em = host_event_map.get(ev) or host_event_map.get(str(ev))
                    if isinstance(em, dict):
                        if evy and evy in em:
                            host_country = norm_str(em.get(evy,'')).upper()
                        elif 'default' in em:
                            host_country = norm_str(em.get('default','')).upper()
                        else:
                            for v in em.values():
                                host_country = norm_str(v).upper(); break
                except Exception:
                    pass
        if not host_country:
            for col in ('country_winner','winner_country','country_loser','loser_country'):
                if col in r.index and norm_str(r.get(col,'')) != '':
                    host_country = norm_str(r.get(col)).upper(); break

        # package a match entry (include opponent/player info if available)
        match_entry = {
            'match_id': norm_str(r.get('match_id') or ''),
            'event_id': norm_str(r.get('event_id') or ''),
            'event_year': norm_str(r.get('event_year') or ''),
            'match_date': parse_date_only(r.get('start_date') or r.get('match_date') or ''),
            'opponent_country': opp_country,
            'host_country': host_country,
            'is_win': True if is_win is True else (False if is_win is False else None),
            'score': norm_str(r.get('score_string') or r.get('score') or ''),
            'opponent': norm_str(r.get('loser_player_name') if is_win else r.get('winner_player_name') or ''),
            'opponent_id': norm_str(r.get('player_id_loser') if is_win else r.get('player_id_winner') or '')
        }

        # update opponent map
        if opp_country:
            iso3 = ensure_iso3(opp_country) or opp_country
            o = opp_map.get(iso3, {'wins':0,'losses':0,'matches':0,'sample_matches':[]})
            if is_win is True:
                o['wins'] += 1
            elif is_win is False:
                o['losses'] += 1
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append(match_entry)
            opp_map[iso3] = o

        # update host map
        if host_country:
            iso3 = ensure_iso3(host_country) or host_country
            h = host_map.get(iso3, {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]})
            if is_win is True:
                h['wins'] += 1
            elif is_win is False:
                h['losses'] += 1
            h['matches'] += 1
            rnd = norm_str(r.get('round') or '')
            if is_win is True and rnd and any(tok in rnd.upper() for tok in title_rounds):
                h['titles'] = h.get('titles', 0) + 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append(match_entry)
            host_map[iso3] = h

    # dedupe titles by unique event (ensure not double-counted)
    title_event_ids = set()
    for _, r in df.iterrows():
        try:
            win = (has_pid_w and norm_pid(r.get('player_id_winner')) == pid) or False
            rnd = norm_str(r.get('round') or '')
            if win and rnd and any(tok in rnd.upper() for tok in title_rounds):
                eid = norm_str(r.get('event_id') or '')
                eyr = norm_str(r.get('event_year') or '')
                if eid:
                    title_event_ids.add((eid,eyr))
        except Exception:
            pass

    for eid, eyr in title_event_ids:
        # attempt to find host via host_event_map
        host_iso = ''
        if host_event_map:
            try:
                em = host_event_map.get(eid) or host_event_map.get(str(eid))
                if isinstance(em, dict):
                    if eyr and eyr in em:
                        host_iso = norm_str(em.get(eyr,'')).upper()
                    elif 'default' in em:
                        host_iso = norm_str(em.get('default','')).upper()
                    else:
                        for v in em.values():
                            host_iso = norm_str(v).upper(); break
            except Exception:
                pass
        if not host_iso:
            continue
        iso3 = ensure_iso3(host_iso) or host_iso
        if iso3 not in host_map:
            host_map[iso3] = {'wins':0,'losses':0,'matches':0,'titles':0,'sample_matches':[]}
        host_map[iso3]['titles'] = host_map[iso3].get('titles', 0) + 1

    # compute win_rate floats and canonical maps
    map_opp = {}
    for c, o in opp_map.items():
        m = o.get('matches',0) or 0
        w = o.get('wins',0) or 0
        map_opp[c] = {
            'wins': int(w),
            'losses': int(o.get('losses',0) or 0),
            'matches': int(m),
            'win_rate': (float(w)/m) if m else None,
            'sample_matches': o.get('sample_matches', [])
        }

    map_host = {}
    for c, h in host_map.items():
        m = h.get('matches',0) or 0
        w = h.get('wins',0) or 0
        map_host[c] = {
            'wins': int(w),
            'losses': int(h.get('losses',0) or 0),
            'matches': int(m),
            'win_rate': (float(w)/m) if m else None,
            'titles': int(h.get('titles',0) or 0),
            'sample_matches': h.get('sample_matches', [])
        }

    return {
        'meta': {'player_id': pid, 'player_name': player_name, 'matches': int(len(df)), 'generated_at': datetime.utcnow().isoformat()+'Z', 'version':'v1'},
        'opponent_countries': opp_map,
        'host_countries': host_map,
        'map_opponent_stats': map_opp,
        'map_host_stats': map_host
    }

def main(matches_dir, out_dir, player_list=None, limit_players=None, host_event_map_path=None):
    print("[generate_maps] reading matches from", matches_dir)
    matches = read_matches_from_dir(matches_dir)
    print("[generate_maps] rows:", len(matches), "columns:", len(matches.columns))
    host_event_map = None
    if host_event_map_path:
        try:
            host_event_map = json.loads(Path(host_event_map_path).read_text(encoding='utf8'))
        except Exception:
            host_event_map = None

    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([norm_pid(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([norm_pid(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])
    print("[generate_maps] discovered players:", len(player_ids))

    if player_list:
        player_ids = [p for p in player_ids if p in set(player_list)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, 1):
        try:
            print(f"[generate_maps] [{i}/{len(player_ids)}] building maps for {pid}")
            obj = build_maps_for_player(matches, pid, sample_limit=6, host_event_map=host_event_map)
            out_path = os.path.join(players_dir, f"{pid}.maps.json")
            with open(out_path, 'w', encoding='utf8') as fh:
                json.dump(obj, fh, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[generate_maps] ERROR {pid}: {e}")

    print("[generate_maps] done. maps at", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matches-dir", required=True)
    ap.add_argument("--out-dir", default="./dist")
    ap.add_argument("--limit-players", type=int, default=None)
    ap.add_argument("--player", action='append', help="player id to process (repeatable)")
    ap.add_argument("--host-event-map", help="json mapping event->host country")
    args = ap.parse_args()
    plist = args.player if args.player else None
    main(args.matches_dir, args.out_dir, player_list=plist, limit_players=args.limit_players, host_event_map_path=args.host_event_map)
