#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_stats_leaderboards.py
Parcourt players_dir/*.stats.json (générés par generate_detailed_stats.py)
et produit des leaderboards JSON triés par statistique dans out_dir.

Usage:
  python generate_stats_leaderboards.py --players-dir ./dist/players_atp --out-dir ./dist/leaderboards --players-csv ./player_data_atp.csv
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
    """Robust slugification: remove accents, keep a-z0-9 and '-', collapse duplicates."""
    if not name:
        return ''
    s = str(name).strip().lower()
    # normalize unicode and remove diacritics
    s = unicodedata.normalize('NFD', s)
    s = ''.join(ch for ch in s if not unicodedata.category(ch).startswith('M'))
    # remove undesired characters, keep letters/numbers/space/dash
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"\-+", "-", s)
    return s.strip('-')

def is_number(v):
    try:
        if v is None: return False
        if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))): return True
        float(v)
        return True
    except Exception:
        return False

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

def format_value_for_type(v, typ):
    if v is None:
        return ''
    if typ == 'int':
        try:
            return str(int(round(v)))
        except:
            return str(v)
    if typ == 'pct':
        try:
            val = float(v)
            if abs(val) <= 1.0:
                val = val * 100.0
            return f"{round(val,2)}%"
        except:
            return str(v)
    if typ == 'float':
        try:
            return str(round(float(v), 3))
        except:
            return str(v)
    return str(v)

def load_player_csv(players_csv_path):
    """Load CSV mapping player_id -> full_name (full_name header expected)."""
    mapping = {}
    try:
        with open(players_csv_path, 'r', encoding='utf8') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                pid = (row.get('player_id') or '').strip()
                full_name = (row.get('full_name') or '').strip()
                if pid and full_name:
                    mapping[pid] = full_name
    except Exception as e:
        print(f"[warn] could not load players CSV '{players_csv_path}': {e}")
    return mapping

# ---------------- Build leaderboards ----------------
def build_leaderboards(players_dir, out_dir, players_csv_path=None):
    safe_mkdir(out_dir)
    files = sorted(glob.glob(os.path.join(players_dir, "*.stats.json")))
    players = []

    # optional mapping player_id -> full_name
    players_map = {}
    if players_csv_path:
        players_map = load_player_csv(players_csv_path)

    # read all player stats JSONs to get base info and json path
    for f in files:
        j = load_json_path(f)
        if not j:
            continue
        pid = (j.get('player_id') or os.path.splitext(os.path.basename(f))[0]).strip()
        # prefer full_name from CSV mapping (if available) to avoid short/inital names
        full_from_csv = players_map.get(pid)
        # prefer explicit slug field if present in stats json
        name_field = j.get('player_name') or j.get('slug') or full_from_csv or pid
        country_code = maybe_get_country_code(j)
        # slug_part computed from full_from_csv if available else from name_field
        slug_source = full_from_csv or name_field
        slug_part = slugify(slug_source)
        # profile_url: prefer existing profile_url in stats json (if present and non-empty)
        profile_url = None
        if j.get('profile_url'):
            profile_url = j.get('profile_url')
        else:
            # create canonical profile url
            # use pid lower-case prefix then slug_part
            profile_url = f"https://www.center-court.net/players_atp/{pid.lower()}-{slug_part}"
        players.append({
            "player_id": pid,
            "player_name": name_field,
            "country_code": country_code,
            "profile_url": profile_url,
            "json_path": f
        })

    # helper to extract stat values from player's JSON
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

    # build per-stat leaderboards
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

        # dense ranking
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
    ap.add_argument("--players-dir", default="./players_atp", help="Directory with player stats JSON files")
    ap.add_argument("--out-dir", default="./leaderboards", help="Output directory for leaderboards")
    ap.add_argument("--players-csv", default="./player_data_atp.csv", help="CSV file mapping player_id -> full_name (full_name header)")
    args = ap.parse_args()
    build_leaderboards(args.players_dir, args.out_dir, args.players_csv)

if __name__ == "__main__":
    main()
