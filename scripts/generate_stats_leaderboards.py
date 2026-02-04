#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_stats_leaderboards.py
Parcourt dist/players_atp/*.stats.json (générés par generate_detailed_stats.py)
et produit des leaderboards JSON triés par statistique dans dist/leaderboards/.

Usage:
  python generate_stats_leaderboards.py --players-dir ./dist/players_atp --out-dir ./dist/leaderboards
"""
import os
import glob
import json
import argparse
import math
import re
from collections import OrderedDict

# ---------------- Config des statistiques ----------------
# key -> metadata: label, type, higher_is_better (bool)
STATS_META = OrderedDict([
    # 4 principales
    ("matches_played", {"label": "Matches", "type": "int", "higher_is_better": True}),
    ("matches_won", {"label": "Matches won", "type": "int", "higher_is_better": True}),
    ("matches_lost", {"label": "Matches lost", "type": "int", "higher_is_better": False}),  # lower better
    ("win_rate", {"label": "Win rate", "type": "pct", "higher_is_better": True}),

    # 15 career stats (use career.stat_agg.mean or career.stat_agg.sum for counts if preferable)
    ("aces", {"label": "Number of aces (career)", "type": "int", "higher_is_better": True}),
    ("aces_per_service_point", {"label": "Aces per service point", "type": "float", "higher_is_better": True}),
    ("doublefaults", {"label": "Number of double faults (career)", "type": "int", "higher_is_better": False}),  # negative per user
    ("doublefaults_per_service_point", {"label": "Double faults per service point", "type": "float", "higher_is_better": False}),
    ("firstserve_percent", {"label": "First serve %", "type": "pct", "higher_is_better": True}),
    ("firstserve_points_won_percent", {"label": "First serve points won %", "type": "pct", "higher_is_better": True}),
    ("secondserve_points_won_percent", {"label": "Second serve points won %", "type": "pct", "higher_is_better": True}),
    ("service_points_won_percent", {"label": "Service points won %", "type": "pct", "higher_is_better": True}),
    ("return_points_won_percent", {"label": "Return points won %", "type": "pct", "higher_is_better": True}),
    ("breakpoints_faced", {"label": "Breakpoints faced (career)", "type": "int", "higher_is_better": False}),  # negative
    ("breakpoints_converted", {"label": "Breakpoints converted (career)", "type": "int", "higher_is_better": True}),
    ("breakpoints_converted_rate", {"label": "Breakpoints converted rate", "type": "pct", "higher_is_better": True}),
    ("service_games_lost_rate", {"label": "Service games lost rate", "type": "pct", "higher_is_better": False}),  # negative
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
    # try multiple candidate keys, return uppercase 2-letter if plausible else None
    if not isinstance(j, dict):
        return None
    for k in ('country_code', 'iso2', 'country', 'nationality', 'nation'):
        v = j.get(k)
        if not v:
            continue
        s = str(v).strip()
        # if it's like "US" or "GB", or "USA" -> try first 2 letters
        if len(s) == 2 and s.isalpha():
            return s.upper()
        if len(s) == 3 and s.isalpha():
            return s[:2].upper()
        # sometimes "United States" -> we cannot reliably map without a lookup; skip
    return None

def slugify(name):
    if not name:
        return ''
    s = str(name).strip().lower()
    s = re.sub(r'[^a-z0-9\s\-]', '', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'\-+', '-', s)
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
        # if integer-like
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
        # value should be 0..100
        try:
            val = float(v)
            # if looks like 0..1 -> multiply
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

# ---------------- Build leaderboards ----------------
def build_leaderboards(players_dir, out_dir):
    safe_mkdir(out_dir)
    files = sorted(glob.glob(os.path.join(players_dir, "*.stats.json")))
    players = []
    # read all JSONs
    for f in files:
        j = load_json_path(f)
        if not j:
            continue
        pid = (j.get('player_id') or os.path.splitext(os.path.basename(f))[0]).strip()
        name = j.get('player_name') or j.get('slug') or pid
        country_code = maybe_get_country_code(j)
        slug_part = slugify(name)
        profile_url = f"https://www.center-court.net/players_atp/{pid.lower()}-{slug_part}"
        players.append({
            "player_id": pid,
            "player_name": name,
            "country_code": country_code,
            "profile_url": profile_url,
            "json_path": f
        })

    # function to extract stat value from a player's stats json
    def get_stat_value_for_player(j, stat_key, meta):
        # special computed stats
        if stat_key == 'win_rate':
            # compute from career matches
            try:
                mp = j.get('career', {}).get('matches_played') if j.get('career') else None
                mw = j.get('career', {}).get('matches_won') if j.get('career') else None
                if mp is None and isinstance(j.get('meta',{}).get('matches'), int):
                    mp = j['meta']['matches']
                if mp:
                    if mw is None: mw = 0
                    val = float(mw) / float(mp)
                    return val if not math.isnan(val) else None
            except:
                return None
        # matches_played / won / lost come from career top-level keys OR meta.matches or career.stat_agg counts
        if stat_key in ('matches_played','matches_won','matches_lost'):
            # try top-level
            if j.get('career'):
                if stat_key == 'matches_played' and j['career'].get('matches_played') is not None:
                    return to_number(j['career'].get('matches_played'))
                if stat_key == 'matches_won' and j['career'].get('matches_won') is not None:
                    return to_number(j['career'].get('matches_won'))
                if stat_key == 'matches_lost' and j['career'].get('matches_lost') is not None:
                    return to_number(j['career'].get('matches_lost'))
            # fallback meta.matches
            if stat_key == 'matches_played' and isinstance(j.get('meta',{}).get('matches'), int):
                return to_number(j['meta']['matches'])
        # general career.stat_agg mean or sum
        career = j.get('career') or {}
        stat_agg = career.get('stat_agg') if career else None
        if stat_agg and stat_key in stat_agg:
            # prefer sum for counts if available (aces, doublefaults)
            # detect int-like by meta type in STATS_META
            mp = stat_agg.get(stat_key)
            if isinstance(mp, dict):
                # prefer 'sum' for counts
                if STATS_META.get(stat_key,{}).get('type') == 'int' and mp.get('sum') is not None:
                    return to_number(mp.get('sum'))
                # otherwise prefer 'mean'
                if mp.get('mean') is not None:
                    return to_number(mp.get('mean'))
                if mp.get('sum') is not None:
                    return to_number(mp.get('sum'))
            else:
                return to_number(mp)
        # fallback None
        return None

    # build per-stat leaderboards
    index = []
    for stat_key, meta in STATS_META.items():
        entries = []
        for p in players:
            j = load_json_path(p['json_path'])
            if not j: continue
            raw = get_stat_value_for_player(j, stat_key, meta)
            # For percent fields, career.stat_agg.mean may be 0..1 -> we keep raw numeric (0..1) and format later
            entries.append({
                "player_id": p['player_id'],
                "player_name": p['player_name'],
                "country_code": p['country_code'],
                "profile_url": p['profile_url'],
                "value": raw
            })
        # filter out players with None values
        entries_with_val = [e for e in entries if e['value'] is not None]
        # sorting
        reverse = bool(meta.get('higher_is_better', True))
        # if sorting by value numeric; treat None as lowest
        try:
            entries_with_val.sort(key=lambda x: (float(x['value']) if x['value'] is not None else float('-inf')), reverse=reverse)
        except Exception:
            # fallback to string
            entries_with_val.sort(key=lambda x: (str(x.get('value'))), reverse=reverse)

        # generate ranks (dense ranking by value)
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

        # write leaderboard file
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
        # add to index
        index.append({
            "stat_key": stat_key,
            "label": meta.get('label'),
            "type": meta.get('type'),
            "higher_is_better": meta.get('higher_is_better', True),
            "url": os.path.basename(out_path)
        })

    # write index
    idx_path = os.path.join(out_dir, "index.json")
    with open(idx_path, 'w', encoding='utf8') as fh:
        json.dump({"stats": index}, fh, ensure_ascii=False, indent=2)
    print(f"[ok] wrote index {idx_path}")

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--players-dir", default="./players_atp", help="Directory with player stats JSON files")
    ap.add_argument("--out-dir", default="./leaderboards", help="Output directory for leaderboards")
    args = ap.parse_args()
    build_leaderboards(args.players_dir, args.out_dir)

if __name__ == "__main__":
    main()
