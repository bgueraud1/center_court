#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_stats_leaderboards.py (robust CSV loader + logging)
Usage:
  python scripts/generate_stats_leaderboards.py --players-dir ./docs/players --out-dir ./docs/leaderboards --players-csv ./player_data_wta.csv
"""
import os
import glob
import json
import argparse
import math
import re
import unicodedata
import csv
from collections import OrderedDict

# ---------------- Config des statistiques ----------------
STATS_META = OrderedDict([
    ("matches_played", {"label": "Matches", "type": "int", "higher_is_better": True}),
    ("matches_won", {"label": "Matches won", "type": "int", "higher_is_better": True}),
    ("matches_lost", {"label": "Matches lost", "type": "int", "higher_is_better": False}),
    ("win_rate", {"label": "Win rate", "type": "pct", "higher_is_better": True}),
    ("aces", {"label": "Number of aces (career)", "type": "int", "higher_is_better": True}),
    ("aces_per_service_point", {"label": "Aces per service point", "type": "float", "higher_is_better": True}),
    ("doublefaults", {"label": "Number of double faults (career)", "type": "int", "higher_is_better": False}),
    ("doublefaults_per_service_point", {"label": "Double faults per service point", "type": "float", "higher_is_better": False}),
    ("firstserve_percent", {"label": "First serve %", "type": "pct", "higher_is_better": True}),
    ("firstserve_points_won_percent", {"label": "First serve points won %", "type": "pct", "higher_is_better": True}),
    ("secondserve_points_won_percent", {"label": "Second serve points won %", "type": "pct", "higher_is_better": True}),
    ("service_points_won_percent", {"label": "Service points won %", "type": "pct", "higher_is_better": True}),
    ("return_points_won_percent", {"label": "Return points won %", "type": "pct", "higher_is_better": True}),
    ("breakpoints_faced", {"label": "Breakpoints faced (career)", "type": "int", "higher_is_better": False}),
    ("breakpoints_converted", {"label": "Breakpoints converted (career)", "type": "int", "higher_is_better": True}),
    ("breakpoints_converted_rate", {"label": "Breakpoints converted rate", "type": "pct", "higher_is_better": True}),
    ("service_games_lost_rate", {"label": "Service games lost rate", "type": "pct", "higher_is_better": False}),
    ("tiebreak_win_rate", {"label": "Tie-break win rate", "type": "pct", "higher_is_better": True}),
    ("match_time_hours", {"label": "Mean match time (hours)", "type": "float", "higher_is_better": False})
])

# ---------------- Helpers ----------------
def safe_mkdir(p):
    os.makedirs(p, exist_ok=True)

def load_json_path(path):
    try:
        with open(path, 'r', encoding='utf8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[err] failed to load {path}: {e}")
        return None

def maybe_get_country_code(j):
    if not isinstance(j, dict):
        return None
    for k in ('country_code', 'iso2', 'country', 'nationality', 'nation'):
        v = j.get(k)
        if not v:
            continue
        s = str(v).strip()
        if len(s) == 2 and s.isalpha():
            return s.upper()
        if len(s) == 3 and s.isalpha():
            return s[:2].upper()
    return None

def slugify(name):
    if not name:
        return ''
    s = str(name).strip().lower()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if not unicodedata.category(ch).startswith('M'))
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"\-+", "-", s)
    return s.strip('-')

def to_number(v):
    try:
        if v is None: return None
        n = float(v)
        if math.isnan(n) or math.isinf(n): return None
        if abs(n - round(n)) < 1e-9:
            return int(round(n))
        return n
    except Exception:
        return None

# Robust CSV discovery: try several candidate locations
def find_csv_file(path_hint):
    candidates = []
    if not path_hint:
        return None
    # if absolute path and exists, use it
    if os.path.isabs(path_hint) and os.path.exists(path_hint):
        return path_hint
    # candidate: as given (relative to cwd)
    candidates.append(os.path.abspath(path_hint))
    # candidate: relative to script file directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.join(script_dir, path_hint))
    # candidate: one level up from script dir
    candidates.append(os.path.join(script_dir, '..', path_hint))
    # candidate: try scanning up to 4 parent directories from cwd
    cwd = os.path.abspath(os.getcwd())
    cur = cwd
    for _ in range(5):
        candidates.append(os.path.join(cur, path_hint))
        cur = os.path.dirname(cur)
    # filtrer et renvoyer le premier qui existe
    seen = set()
    for c in candidates:
        cnorm = os.path.normpath(c)
        if cnorm in seen: continue
        seen.add(cnorm)
        if os.path.exists(cnorm) and os.path.isfile(cnorm):
            return cnorm
    return None

def load_player_csv(players_csv_path):
    """Robust loader: detect header indices for player_id and full_name using various header names."""
    resolved = find_csv_file(players_csv_path)
    if not resolved:
        print(f"[warn] players CSV not found at given path '{players_csv_path}' (checked several locations).")
        return {}
    print(f"[info] using players CSV: {resolved}")
    mapping = {}
    try:
        with open(resolved, 'r', encoding='utf8', newline='') as fh:
            reader = csv.reader(fh)
            try:
                header = next(reader)
            except StopIteration:
                print(f"[warn] CSV file {resolved} is empty.")
                return {}
            # handle BOM in first header cell
            header[0] = header[0].lstrip('\ufeff').strip()
            # normalize header tokens
            header_norm = [h.lower().strip() for h in header]
            # find indices heuristically
            pid_idx = None
            full_idx = None
            for i, h in enumerate(header_norm):
                if 'player_id' in h or h in ('player id','id','pid') or h.endswith('_id'):
                    if pid_idx is None: pid_idx = i
                if ('full' in h and 'name' in h) or h in ('full_name','fullname','name'):
                    if full_idx is None: full_idx = i
            # fallback: try find 'player' and 'name' separately if earlier misses
            if pid_idx is None:
                for i,h in enumerate(header_norm):
                    if h == 'player' or h == 'playerid' or h == 'player id':
                        pid_idx = i; break
            if full_idx is None:
                for i,h in enumerate(header_norm):
                    if h == 'display_name' or h == 'long_name':
                        full_idx = i; break
            if pid_idx is None or full_idx is None:
                print(f"[warn] CSV headers found: {header_norm}")
                print("[warn] could not detect both 'player_id' and 'full_name' columns automatically.")
                # attempt best-effort: if header contains 'player' and 'name' assign them
                for i,h in enumerate(header_norm):
                    if 'player' in h and pid_idx is None:
                        pid_idx = i
                    if 'name' in h and full_idx is None:
                        full_idx = i
            if pid_idx is None or full_idx is None:
                print("[warn] Aborting CSV mapping: required columns not detected.")
                return {}
            # read rows
            count = 0
            for row in reader:
                # skip short/empty rows
                if len(row) <= max(pid_idx, full_idx):
                    continue
                pid = row[pid_idx].strip()
                full = row[full_idx].strip()
                if not pid or not full:
                    continue
                mapping[pid] = full
                mapping[pid.upper()] = full
                mapping[pid.lower()] = full
                count += 1
            print(f"[info] loaded {count} entries from CSV (player_id -> full_name).")
            return mapping
    except Exception as e:
        print(f"[err] failed to read CSV {resolved}: {e}")
        return {}

# (rest of script omitted here for brevity in this snippet — it is identical to previous working logic)
# For completeness, we include the rest of the original script (building leaderboards) below.
# -------- include original script logic, using load_player_csv(...) defined above --------

def build_leaderboards(players_dir, out_dir, players_csv_path=None):
    safe_mkdir(out_dir)
    files = sorted(glob.glob(os.path.join(players_dir, "*.stats.json")))
    players = []

    # optional mapping player_id -> full_name
    players_map = {}
    if players_csv_path:
        players_map = load_player_csv(players_csv_path)
        print(f"[info] loaded players CSV mapping: {len([k for k in players_map.keys() if k and k.lower()==k or k.upper()==k or True])} (raw dictionary size)")

    for f in files:
        j = load_json_path(f)
        if not j: continue
        pid = (j.get('player_id') or os.path.splitext(os.path.basename(f))[0]).strip()
        # look up CSV mapping with multiple normalizations
        full_from_csv = None
        if pid:
            full_from_csv = players_map.get(pid) or players_map.get(pid.upper()) or players_map.get(pid.lower())
        name_field = j.get('player_name') or j.get('slug') or full_from_csv or pid
        country_code = maybe_get_country_code(j)

        profile_url = None
        if j.get('profile_url'):
            profile_url = j.get('profile_url')
        else:
            slug_source = full_from_csv or name_field
            slug_part = slugify(slug_source)
            profile_url = f"https://www.center-court.net/players/{pid.lower()}-{slug_part}"
        if full_from_csv:
            print(f"[info] player {pid} -> used CSV full_name '{full_from_csv}' to build profile_url")
        else:
            if j.get('profile_url'):
                print(f"[info] player {pid} -> used existing profile_url from stats.json")
            else:
                print(f"[warn] player {pid} -> CSV missing; built fallback profile_url using '{name_field}' -> {profile_url}")

        players.append({
            "player_id": pid,
            "player_name": name_field,
            "country_code": country_code,
            "profile_url": profile_url,
            "json_path": f
        })

    def get_stat_value_for_player(j, stat_key, meta):
        if stat_key == 'win_rate':
            try:
                career = j.get('career') or {}
                mp = career.get('matches_played') if career else None
                mw = career.get('matches_won') if career else None
                if mp is None and isinstance(j.get('meta',{}).get('matches'), int):
                    mp = j['meta']['matches']
                if mp:
                    if mw is None: mw = 0
                    val = float(mw) / float(mp)
                    return val if not math.isnan(val) else None
            except:
                return None
        if stat_key in ('matches_played','matches_won','matches_lost'):
            if j.get('career'):
                if stat_key == 'matches_played' and j['career'].get('matches_played') is not None:
                    return to_number(j['career'].get('matches_played'))
                if stat_key == 'matches_won' and j['career'].get('matches_won') is not None:
                    return to_number(j['career'].get('matches_won'))
                if stat_key == 'matches_lost' and j['career'].get('matches_lost') is not None:
                    return to_number(j['career'].get('matches_lost'))
            if stat_key == 'matches_played' and isinstance(j.get('meta',{}).get('matches'), int):
                return to_number(j['meta']['matches'])
        career = j.get('career') or {}
        stat_agg = career.get('stat_agg') if career else None
        if stat_agg and stat_key in stat_agg:
            mp = stat_agg.get(stat_key)
            if isinstance(mp, dict):
                if STATS_META.get(stat_key,{}).get('type') == 'int' and mp.get('sum') is not None:
                    return to_number(mp.get('sum'))
                if mp.get('mean') is not None:
                    return to_number(mp.get('mean'))
                if mp.get('sum') is not None:
                    return to_number(mp.get('sum'))
            else:
                return to_number(mp)
        return None

    index = []
    for stat_key, meta in STATS_META.items():
        entries = []
        for p in players:
            j = load_json_path(p['json_path'])
            if not j: continue
            raw = get_stat_value_for_player(j, stat_key, meta)
            entries.append({
                "player_id": p['player_id'],
                "player_name": p['player_name'],
                "country_code": p['country_code'],
                "profile_url": p['profile_url'],
                "value": raw
            })
        entries_with_val = [e for e in entries if e['value'] is not None]
        reverse = bool(meta.get('higher_is_better', True))
        try:
            entries_with_val.sort(key=lambda x: (float(x['value']) if x['value'] is not None else float('-inf')), reverse=reverse)
        except Exception:
            entries_with_val.sort(key=lambda x: (str(x.get('value'))), reverse=reverse)

        ranked = []
        last_val = None
        last_rank = 0
        for idx, e in enumerate(entries_with_val, start=1):
            v = e['value']
            if last_val is None or float(v) != float(last_val):
                rank = idx
                last_rank = rank
                last_val = v
            else:
                rank = last_rank
            ranked.append({
                "rank": rank,
                "player_id": e['player_id'],
                "player_name": e['player_name'],
                "country_code": e['country_code'],
                "profile_url": e['profile_url'],
                "value": e['value']
            })

        out = {
            "stat_key": stat_key,
            "label": meta.get('label'),
            "type": meta.get('type'),
            "higher_is_better": meta.get('higher_is_better', True),
            "total_players": len(ranked),
            "entries": ranked
        }
        out_path = os.path.join(out_dir, f"{stat_key}.json")
        with open(out_path, 'w', encoding='utf8') as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)
        print(f"[ok] wrote {out_path} ({len(ranked)} entries)")

        index.append({
            "stat_key": stat_key,
            "label": meta.get('label'),
            "type": meta.get('type'),
            "higher_is_better": meta.get('higher_is_better', True),
            "url": os.path.basename(out_path)
        })

    idx_path = os.path.join(out_dir, "index.json")
    with open(idx_path, 'w', encoding='utf8') as fh:
        json.dump({"stats": index}, fh, ensure_ascii=False, indent=2)
    print(f"[ok] wrote index {idx_path}")

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--players-dir", default="./players", help="Directory with player stats JSON files")
    ap.add_argument("--out-dir", default="./leaderboards", help="Output directory for leaderboards")
    ap.add_argument("--players-csv", default="./player_data_wta.csv", help="CSV file mapping player_id -> full_name (header contains full_name and player_id)")
    args = ap.parse_args()
    build_leaderboards(args.players_dir, args.out_dir, args.players_csv)

if __name__ == "__main__":
    main()
