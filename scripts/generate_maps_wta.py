#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_maps.py - Module: generation des cartes (geo aggregates)

Fixed: ensure player IDs are normalized (remove trailing .0), comparisons use normalized ids,
and output filenames never contain the ".0" suffix.

Usage:
  python generate_maps.py --matches-dir /path/to/matches --out-dir ./dist --host-event-map ./host_event_map.json --limit-players 200

Sorties:
  - <out_dir>/players/{PLAYER_ID}.maps.json
"""
from __future__ import annotations
import argparse
import os
import json
import glob
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd
import numbers


# ----------------- Helpers -----------------
def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)


def sanitize_filename(name: Optional[str]) -> str:
    if name is None:
        return ''
    s = str(name)
    # keep only allowed chars (letters, digits, dot, underscore, dash)
    s = re.sub(r'[^A-Za-z0-9._-]', '_', s)
    return s


def normalize_player_id(pid: Optional[Any]) -> str:
    """
    Robust normalization of player ids so that values like 331809.0 become '331809'.
    - If pid is a float and integer-valued -> 'int' string
    - If pid is numeric-like string with trailing .0 -> 'int' string
    - Otherwise -> stripped upper-case string
    """
    if pid is None:
        return ''
    try:
        # pandas NA / nan
        if isinstance(pid, float) and pd.isna(pid):
            return ''
    except Exception:
        pass

    # If it's an actual numeric integral type
    if isinstance(pid, numbers.Integral):
        return str(int(pid))

    # If it's a float and integral-valued
    if isinstance(pid, float):
        try:
            if pid.is_integer():
                return str(int(pid))
            return str(pid)
        except Exception:
            return str(pid)

    s = str(pid).strip()
    if not s:
        return ''

    # common case: '331809.0' or '12345.000' -> convert to int string
    m = re.match(r'^([+-]?\d+)\.0+$', s)
    if m:
        return m.group(1)

    # try parse float and convert if integer-valued
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass

    # fallback: uppercase trimmed string
    return s.upper()


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


# ----------------- IOC -> ISO3 mapping -----------------
IOC_TO_ISO3 = {
    "RSA":"ZAF", "GER":"DEU", "NED":"NLD", "INA":"IDN",
    "PHI":"PHL", "POR":"PRT", "GRE":"GRC", "BUL":"BGR",
    "LAT":"LVA", "MAD":"MDG", "ALG":"DZA", "CHI":"CHL",
    "GUA":"GTM", "ESA":"SLV", "SUI":"CHE", "SLO":"SVN",
    "CRO":"HRV", "URU":"URY", "PAR":"PRY", "NGR":"NGA",
    "DEN":"DNK", "GBR":"GBR", "USA":"USA", "ARG":"ARG",
    "ESP":"ESP", "FRA":"FRA", "ITA":"ITA", "BRA":"BRA",
}

ISO2_TO_ISO3_COMMON = {
    "DE":"DEU", "CH":"CHE", "NL":"NLD", "ES":"ESP", "FR":"FRA",
    "IT":"ITA", "GB":"GBR", "US":"USA", "AR":"ARG", "BR":"BRA",
}


def to_iso3(code: Optional[str]) -> str:
    if not code:
        return ''
    s = str(code).strip().upper()
    if not s:
        return ''
    if s in IOC_TO_ISO3:
        return IOC_TO_ISO3[s]
    if len(s) == 2 and s.isalpha():
        return ISO2_TO_ISO3_COMMON.get(s, '')
    if len(s) == 3 and s.isalpha():
        return s
    s2 = re.sub(r'[^A-Z]', '', s)
    if s2 in IOC_TO_ISO3:
        return IOC_TO_ISO3[s2]
    if len(s2) == 2:
        return ISO2_TO_ISO3_COMMON.get(s2, '')
    if len(s2) == 3 and s2.isalpha():
        return s2
    return ''


def normalize_event_token(val: Optional[Any]) -> str:
    if val is None:
        return ''
    try:
        if isinstance(val, float) and pd.isna(val):
            return ''
    except Exception:
        pass
    if isinstance(val, numbers.Integral):
        return str(int(val))
    try:
        fv = float(val)
        if fv.is_integer():
            return str(int(fv))
    except Exception:
        pass
    return str(val).strip()


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
            return d if isinstance(d, dict) else {}
    except Exception as e:
        print(f"[generate_maps] Could not load host_event_map at {path}: {e}")
        return {}


# ----------------- Core builder -----------------

def build_maps_for_player(matches_df: pd.DataFrame,
                          player_id: str,
                          sample_limit: int = 6,
                          host_event_map: Optional[Dict[str, Dict[str, str]]] = None) -> Optional[Dict[str, Any]]:
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    winner_col = 'player_id_winner'
    loser_col = 'player_id_loser'
    frames = []

    # use normalized comparison on the columns to handle values like 331809.0
    if winner_col in matches_df.columns:
        try:
            mask_w = matches_df[winner_col].map(normalize_player_id) == pid
            if mask_w.any():
                frames.append(matches_df.loc[mask_w])
        except Exception:
            pass
    if loser_col in matches_df.columns:
        try:
            mask_l = matches_df[loser_col].map(normalize_player_id) == pid
            if mask_l.any():
                frames.append(matches_df.loc[mask_l])
        except Exception:
            pass

    # fallback substring on normalized id columns
    if not frames:
        mask_any = pd.Series(False, index=matches_df.index)
        for cid in (winner_col, loser_col):
            if cid in matches_df.columns:
                try:
                    norm_col = matches_df[cid].map(normalize_player_id)
                    mask_any = mask_any | norm_col.str.contains(re.escape(pid), na=False)
                except Exception:
                    pass
        if mask_any.any():
            frames.append(matches_df.loc[mask_any])

    if not frames:
        return {
            'meta': {'player_id': pid, 'player_name': '', 'matches': 0, 'generated_at': datetime.utcnow().isoformat() + 'Z', 'version': 'v1'},
            'map_opponent_stats': {},
            'map_host_stats': {}
        }

    df = pd.concat(frames, ignore_index=True, sort=False)

    # canonical player name
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name'):
        if col in df.columns:
            name_candidates.extend([str(x) for x in df[col].dropna().astype(str).tolist()])
    player_name = ''
    if name_candidates:
        player_name = Counter(name_candidates).most_common(1)[0][0]

    matches_out: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        is_win = None
        try:
            if 'player_id_winner' in row.index and normalize_player_id(row.get('player_id_winner')) == pid:
                is_win = True
            elif 'player_id_loser' in row.index and normalize_player_id(row.get('player_id_loser')) == pid:
                is_win = False
            else:
                if player_name:
                    pname = player_name.strip().lower()
                    if 'player_winner' in row.index and pname in str(row.get('player_winner','')).lower():
                        is_win = True
                    elif 'player_loser' in row.index and pname in str(row.get('player_loser','')).lower():
                        is_win = False
        except Exception:
            is_win = None

        opp_country = ''
        try:
            if is_win is True:
                for col in ('loser_country','country_loser_1','loser_country_1','country_loser1'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_country = str(row.get(col, '')).strip().upper()
                        break
            elif is_win is False:
                for col in ('winner_country','country_winner_1','winner_country_1','country_winner1'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_country = str(row.get(col, '')).strip().upper()
                        break
            if not opp_country:
                for col in ('country_winner','country_loser','winner_country','loser_country'):
                    if col in row.index and str(row.get(col, '')).strip():
                        val = str(row.get(col, '')).strip().upper()
                        if ('winner' in col and is_win is False) or ('loser' in col and is_win is True):
                            opp_country = val
                            break
        except Exception:
            opp_country = ''

        host_country = ''
        try:
            for col in ('host_country','event_country','country_event','host_country_code'):
                if col in row.index and str(row.get(col, '')).strip():
                    host_country = str(row.get(col, '')).strip().upper()
                    break

            if not host_country:
                raw_event_id = row.get('event_id') if 'event_id' in row.index else None
                raw_event_year = row.get('event_year') if 'event_year' in row.index else None
                event_id = normalize_event_token(raw_event_id)
                event_year = normalize_event_token(raw_event_year)
                if event_id and host_event_map:
                    ev_map = host_event_map.get(event_id) or host_event_map.get(str(event_id))
                    if isinstance(ev_map, dict):
                        host_iso = None
                        if event_year:
                            host_iso = ev_map.get(event_year)
                        if not host_iso:
                            host_iso = ev_map.get('default')
                        if not host_iso:
                            for v in ev_map.values():
                                if v:
                                    host_iso = v
                                    break
                        if host_iso:
                            host_country = str(host_iso).strip().upper()

            if not host_country:
                for col in ('country_winner','winner_country','country_loser','loser_country'):
                    if col in row.index and str(row.get(col, '')).strip():
                        host_country = str(row.get(col, '')).strip().upper()
                        break
        except Exception:
            host_country = ''

        stored_event_id = normalize_event_token(row.get('event_id') if 'event_id' in row.index else None)
        stored_event_year = normalize_event_token(row.get('event_year') if 'event_year' in row.index else None)

        opp_iso3 = to_iso3(opp_country)
        host_iso3 = to_iso3(host_country)

        opp_name = ''
        try:
            if is_win is True:
                for col in ('player_loser','loser_player_name','loser_name'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_name = str(row.get(col, '')).strip()
                        break
            elif is_win is False:
                for col in ('player_winner','winner_player_name','winner_name'):
                    if col in row.index and str(row.get(col, '')).strip():
                        opp_name = str(row.get(col, '')).strip()
                        break
            if not opp_name:
                for col in ('winner_player_name','loser_player_name','player_winner','player_loser'):
                    if col in row.index and str(row.get(col, '')).strip():
                        val = str(row.get(col, '')).strip()
                        if val and val != player_name:
                            opp_name = val
                            break
        except Exception:
            opp_name = ''

        match_entry = {
            'match_id': str(row.get('match_id') or ''),
            'event_id': stored_event_id,
            'event_year': stored_event_year,
            'match_date': parse_date_only(row.get('start_date') or row.get('match_date') or ''),
            'tourney_name': str(row.get('tourney_name') or '')[:250],
            'opponent_country': opp_country,
            'opponent_country_iso3': opp_iso3,
            'host_country': host_country,
            'host_country_iso3': host_iso3,
            'opponent_name': opp_name,
            'is_win': True if is_win is True else (False if is_win is False else None),
            'score': str(row.get('score_string') or row.get('score') or '')
        }

        matches_out.append(match_entry)

    opp_map: Dict[str, Dict[str, Any]] = {}
    host_map: Dict[str, Dict[str, Any]] = {}

    opp_name_matches = defaultdict(Counter)
    opp_name_wins = defaultdict(Counter)
    host_tourney_matches = defaultdict(Counter)
    host_tourney_wins = defaultdict(Counter)

    for m in matches_out:
        oc = (m.get('opponent_country_iso3') or '').strip().upper()
        if oc:
            o = opp_map.get(oc, {'wins': 0, 'losses': 0, 'matches': 0, 'sample_matches': []})
            if m.get('is_win') is True:
                o['wins'] += 1
            elif m.get('is_win') is False:
                o['losses'] += 1
            o['matches'] += 1
            if len(o['sample_matches']) < sample_limit:
                o['sample_matches'].append({
                    'match_id': m.get('match_id'),
                    'event_id': m.get('event_id'),
                    'event_year': m.get('event_year'),
                    'tourney_name': m.get('tourney_name'),
                    'match_date': m.get('match_date'),
                    'opponent_country': m.get('opponent_country'),
                    'opponent_country_iso3': m.get('opponent_country_iso3'),
                    'host_country': m.get('host_country'),
                    'host_country_iso3': m.get('host_country_iso3'),
                    'is_win': bool(m.get('is_win')) if m.get('is_win') is not None else None,
                    'score': m.get('score')
                })
            opp_map[oc] = o

        opp_name = (m.get('opponent_name') or '').strip()
        try:
            if opp_name:
                opp_name_matches[oc][opp_name] += 1
                if m.get('is_win') is True:
                    opp_name_wins[oc][opp_name] += 1
        except Exception:
            pass

        hc = (m.get('host_country_iso3') or '').strip().upper()
        if hc:
            h = host_map.get(hc, {'wins': 0, 'losses': 0, 'matches': 0, 'titles': 0, 'sample_matches': []})
            if m.get('is_win') is True:
                h['wins'] += 1
            elif m.get('is_win') is False:
                h['losses'] += 1
            h['matches'] += 1
            if len(h['sample_matches']) < sample_limit:
                h['sample_matches'].append({
                    'match_id': m.get('match_id'),
                    'event_id': m.get('event_id'),
                    'event_year': m.get('event_year'),
                    'tourney_name': m.get('tourney_name'),
                    'match_date': m.get('match_date'),
                    'opponent_country': m.get('opponent_country'),
                    'opponent_country_iso3': m.get('opponent_country_iso3'),
                    'host_country': m.get('host_country'),
                    'host_country_iso3': m.get('host_country_iso3'),
                    'is_win': bool(m.get('is_win')) if m.get('is_win') is not None else None,
                    'score': m.get('score')
                })
            host_map[hc] = h
            try:
                if hc:
                    tname = (m.get('tourney_name') or '').strip()
                    if tname:
                        host_tourney_matches[hc][tname] += 1
                        if m.get('is_win') is True:
                            host_tourney_wins[hc][tname] += 1
            except Exception:
                pass

    title_event_ids = set()
    for m in matches_out:
        try:
            if m.get('is_win') and str(m.get('round') or '').upper() in ('W','WIN','F'):
                key = f"{m.get('event_id')}_{m.get('event_year')}"
                title_event_ids.add(key)
        except Exception:
            pass

    if 'round' in df.columns:
        for idx, row in df.iterrows():
            try:
                if normalize_player_id(row.get('player_id_winner')) == pid and str(row.get('round') or '').strip().upper() in ('W','WIN','F'):
                    key = f"{normalize_event_token(row.get('event_id'))}_{normalize_event_token(row.get('event_year'))}"
                    title_event_ids.add(key)
            except Exception:
                pass

    for tkey in title_event_ids:
        parts = tkey.split('_', 1)
        ev_id = parts[0] if parts else ''
        ev_year = parts[1] if len(parts) > 1 else ''
        host_iso = None
        if host_event_map:
            ev_map = host_event_map.get(ev_id) or host_event_map.get(str(ev_id))
            if isinstance(ev_map, dict):
                host_iso = ev_map.get(ev_year) or ev_map.get('default')
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

    top_opponent_per_country = {}
    for c, counter in opp_name_matches.items():
        if not counter:
            continue
        name, matches_cnt = counter.most_common(1)[0]
        wins_cnt = int(opp_name_wins[c].get(name, 0))
        top_opponent_per_country[c] = {'name': name, 'wins': wins_cnt, 'matches': int(matches_cnt)}

    top_tourney_per_country = {}
    for c, counter in host_tourney_matches.items():
        if not counter:
            continue
        tname, matches_cnt = counter.most_common(1)[0]
        wins_cnt = int(host_tourney_wins[c].get(tname, 0))
        top_tourney_per_country[c] = {'tourney_name': tname, 'wins': wins_cnt, 'matches': int(matches_cnt)}

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
            'sample_matches': s.get('sample_matches', []),
            'top_opponent': top_opponent_per_country.get(c)
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
            'sample_matches': s.get('sample_matches', []),
            'top_tourney': top_tourney_per_country.get(c)
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
        'map_host_stats': map_host_stats,
        'opponent_countries': map_opponent_stats,
        'host_countries': map_host_stats
    }
    return result


# ----------------- CLI Main -----------------

def main(matches_dir: str, out_dir: str, host_event_map_path: Optional[str] = None,
         limit_players: Optional[int] = None, specific_player: Optional[str] = None):
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

    if specific_player:
        player_ids = [normalize_player_id(specific_player)]
    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    players_dir = os.path.join(out_dir, "players")
    safe_mkdir(players_dir)

    for i, pid in enumerate(player_ids, start=1):
        try:
            print(f"[generate_maps] [{i}/{len(player_ids)}] building maps for {pid} ...")
            maps_obj = build_maps_for_player(matches, pid, sample_limit=6, host_event_map=host_event_map)
            safe_pid = sanitize_filename(pid)
            out_path = os.path.join(players_dir, f"{safe_pid}.maps.json")
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
