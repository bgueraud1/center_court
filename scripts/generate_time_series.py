#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_timeseries_from_stats.py
Génère des fichiers <player>.timeseries.json à partir des JSON "detailed statistics".

Usage:
  python generate_timeseries_from_stats.py --stats-dir /path/to/players_atp --out-dir ./dist --limit-players 200 --player rafael-nadal
"""
import argparse
import os
import glob
import json
from datetime import datetime
import math
import numpy as np
import pandas as pd
import re

# ---------- Configuration / définition des statistiques ----------
RATE_KEYS = set([
    'aces_per_service_point','doublefaults_per_service_point',
    'firstserve_percent','firstserve_points_won_percent','secondserve_points_won_percent',
    'service_points_won_percent','return_points_won_percent',
    'breakpoints_converted_rate','service_games_lost_rate','tiebreak_win_rate'
])

STATS_DEFINITION = [
    { 'label': 'Number of aces', 'key': 'aces', 'type': 'count' },
    { 'label': 'Aces per service point', 'key': 'aces_per_service_point', 'type': 'rate' },
    { 'label': 'Number of double faults', 'key': 'doublefaults', 'type': 'count' },
    { 'label': 'Double faults per service point', 'key': 'doublefaults_per_service_point', 'type': 'rate' },

    { 'label': 'First serve %', 'key': 'firstserve_percent', 'type': 'pct' },
    { 'label': 'First serve points won %', 'key': 'firstserve_points_won_percent', 'type': 'pct' },
    { 'label': 'Second serve points won %', 'key': 'secondserve_points_won_percent', 'type': 'pct' },
    { 'label': 'Service points won %', 'key': 'service_points_won_percent', 'type': 'pct' },
    { 'label': 'Return points won %', 'key': 'return_points_won_percent', 'type': 'pct' },

    { 'label': 'Breakpoints faced', 'key': 'breakpoints_faced', 'type': 'count' },
    { 'label': 'Breakpoints converted (count)', 'key': 'breakpoints_converted', 'type': 'count' },
    { 'label': 'Breakpoints converted rate', 'key': 'breakpoints_converted_rate', 'type': 'rate' },

    { 'label': 'Service games lost rate', 'key': 'service_games_lost_rate', 'type': 'rate' },
    { 'label': 'Tie-breaks win rate', 'key': 'tiebreak_win_rate', 'type': 'rate' },

    { 'label': 'Mean match time (hours)', 'key': 'match_time_hours', 'type': 'hours' }
]

# surface -> couleurs demandées
SURFACE_COLORS = {
    'ALL': '#000000',       # noir
    'grass': '#2ea24e',     # vert
    'hard': '#2b63c6',      # bleu
    'clay': '#d2483b',      # rouge
    'carpet': '#8a3fae',    # violet
    'other': '#999999'      # gris pour les autres/unknown
}

# ---------- Helpers ----------
def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_surface(s):
    if s is None: return 'other'
    s2 = str(s).strip().lower()
    if s2 in ('hard', 'hardcourt', 'hard-court', 'h'): return 'hard'
    if s2 in ('clay', 'claycourt', 'clay-court', 'c'): return 'clay'
    if s2 in ('grass', 'g'): return 'grass'
    if s2 in ('carpet', 'carpetcourt', 'carpet-court'): return 'carpet'
    if s2 in ('all','total','overall'): return 'ALL'
    if s2 == '': return 'other'
    return s2

def maybe_number(v):
    if v is None: return None
    try:
        if isinstance(v, dict):
            # prefer 'mean' if present
            if 'mean' in v: return float(v['mean'])
            if 'value' in v: return float(v['value'])
            # fallback: count/denominator pairs
            if 'count' in v and 'denominator' in v and v['denominator']:
                return float(v['count']) / float(v['denominator'])
            # else can't parse
            return None
        if isinstance(v, (int,float,np.number)): return float(v)
        s = str(v).strip()
        if s == '': return None
        # remove percent sign if any
        if s.endswith('%'):
            s = s[:-1].strip()
        # replace comma decimal
        s = s.replace(',','.')
        return float(s)
    except Exception:
        return None

def normalize_pct_value(v, key=None):
    """Retourne valeur en % (0..100) si applicable, sinon retourne la valeur brute."""
    if v is None: return None
    # keys in RATE_KEYS or types 'pct' should be percent-like
    try:
        fv = float(v)
    except Exception:
        return None
    # If likely a fraction 0..1 -> multiply
    if abs(fv) <= 1.0:
        return fv * 100.0
    # if already 0..100 assume percent
    return fv

# ---------- Natural cubic spline implementation ----------
def natural_cubic_spline_interpolate(xs, ys, x_eval):
    """
    Natural cubic spline interpolation.
    xs, ys: 1D arrays, xs strictly increasing, no NaNs, len >= 1
    x_eval: array-like of x where to evaluate
    returns: numpy array of interpolated y values (same shape as x_eval)
    """
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    x_eval = np.asarray(x_eval, dtype=float)
    n = len(xs)
    if n == 1:
        return np.full_like(x_eval, ys[0], dtype=float)
    # h
    h = np.diff(xs)
    # guard against zero spacing
    if np.any(h <= 0):
        raise ValueError("xs must be strictly increasing for spline")
    # alpha
    alpha = np.zeros(n)
    for i in range(1, n-1):
        alpha[i] = (3.0/h[i])*(ys[i+1]-ys[i]) - (3.0/h[i-1])*(ys[i]-ys[i-1])
    # solve tridiagonal for c
    l = np.ones(n)
    mu = np.zeros(n)
    z = np.zeros(n)
    for i in range(1, n-1):
        l[i] = 2.0*(xs[i+1] - xs[i-1]) - h[i-1]*mu[i-1]
        mu[i] = h[i] / l[i]
        z[i] = (alpha[i] - h[i-1]*z[i-1]) / l[i]
    c = np.zeros(n)
    b = np.zeros(n-1)
    d = np.zeros(n-1)
    for j in range(n-2, -1, -1):
        c[j] = z[j] - mu[j]*c[j+1]
        b[j] = (ys[j+1] - ys[j])/h[j] - h[j]*(c[j+1] + 2.0*c[j])/3.0
        d[j] = (c[j+1] - c[j]) / (3.0*h[j])
    a = ys[:-1]
    # evaluate
    y_out = np.zeros_like(x_eval, dtype=float)
    # for each evaluation point, find interval
    idxs = np.searchsorted(xs, x_eval) - 1
    idxs = np.clip(idxs, 0, n-2)
    for k, x0 in enumerate(x_eval):
        i = idxs[k]
        dx = x0 - xs[i]
        y_out[k] = a[i] + b[i]*dx + c[i]*dx*dx + d[i]*dx*dx*dx
    return y_out

# ---------- Core processing ----------
def extract_years_and_surfaces(stats_json):
    # years from stats_by_year keys or available_years
    by_year = stats_json.get('stats_by_year', {}) if isinstance(stats_json, dict) else {}
    years = []
    if isinstance(stats_json, dict):
        ay = stats_json.get('available_years')
        if ay and isinstance(ay, (list,tuple)) and len(ay):
            years = sorted([int(str(y).strip()) for y in ay if str(y).strip().isdigit()])
        else:
            years = sorted([int(y) for y in by_year.keys() if str(y).strip().isdigit()])
    years = sorted(set(years))
    # surfaces
    career_by_surface = stats_json.get('career_by_surface') or {}
    surfaces = set()
    for s in career_by_surface.keys():
        surfaces.add(normalize_surface(s))
    # also detect surfaces within stats_by_year -> by_surface
    for ydata in by_year.values():
        by_s = ydata.get('by_surface') or {}
        for s in by_s.keys():
            surfaces.add(normalize_surface(s))
    # include canonical ones
    surfaces = set(surfaces)
    # ensure main ones exist
    default_surfaces = ['hard','clay','grass','carpet']
    for ds in default_surfaces:
        if ds in surfaces: pass
    # always include ALL
    surfaces.add('ALL')
    # ensure 'other' present if no surface
    if len(surfaces) == 1:
        surfaces.add('other')
    return years, sorted(surfaces)

def get_stat_value_for(stats_json, year, surface, key):
    """
    Attempt to extract a numeric value for stat key for given year & surface
    return None if missing.
    """
    # prefer stats_by_year[year] if exists
    sy = None
    if isinstance(stats_json, dict):
        by_year = stats_json.get('stats_by_year') or {}
        sy = by_year.get(str(year)) if by_year.get(str(year)) is not None else None
    # if sy exists and surface != ALL, try sy.by_surface[surface].stat_agg
    val = None
    if sy:
        if surface and surface != 'ALL':
            by_s = sy.get('by_surface') or {}
            # keys in by_s may be different casing; try several variants
            candidate = by_s.get(surface) or by_s.get(surface.lower()) or by_s.get(surface.upper())
            if candidate:
                stat_agg = candidate.get('stat_agg') or candidate
                val = stat_agg.get(key) if isinstance(stat_agg, dict) else None
        # fallback to sy.stat_agg
        if val is None:
            stat_agg = sy.get('stat_agg') or sy
            val = stat_agg.get(key) if isinstance(stat_agg, dict) else None
    # fallback: career_by_surface (if year missing or surface-specific)
    if val is None and surface and surface != 'ALL':
        career_by_surface = stats_json.get('career_by_surface') or {}
        candidate = career_by_surface.get(surface) or career_by_surface.get(surface.lower()) or career_by_surface.get(surface.upper())
        if candidate:
            stat_agg = candidate.get('stat_agg') or candidate
            val = stat_agg.get(key) if isinstance(stat_agg, dict) else None
    # fallback: career/stat_agg top-level
    if val is None and isinstance(stats_json, dict):
        career = stats_json.get('career') or {}
        stat_agg = career.get('stat_agg') or stats_json.get('stat_agg') or {}
        if isinstance(stat_agg, dict):
            val = stat_agg.get(key)
    # numeric conversion
    num = maybe_number(val)
    return num

def build_timeseries_from_stats_file(path, out_dir, rolling_window=20):
    try:
        with open(path, 'r', encoding='utf8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ts] failed to load {path}: {e}")
        return None
    # determine player id / slug heuristics
    player_id = data.get('player_id') or data.get('pid') or os.path.splitext(os.path.basename(path))[0]
    slug = data.get('slug') or data.get('name') or os.path.splitext(os.path.basename(path))[0]
    years, surfaces = extract_years_and_surfaces(data)
    if not years:
        # try derive years from matches list if present
        if isinstance(data, dict) and isinstance(data.get('matches'), list):
            ys = set()
            for m in data.get('matches',[]):
                y = m.get('event_year') or (m.get('start_date')[:4] if isinstance(m.get('start_date'), str) and len(m.get('start_date'))>=4 else None)
                if y:
                    try:
                        ys.add(int(y))
                    except Exception:
                        pass
            years = sorted(ys)
    if not years:
        # nothing to produce
        print(f"[ts] no years found for {player_id} ({path}) — skipping")
        return None

    # canonical surfaces to include always
    canonical = ['ALL', 'grass', 'hard', 'clay', 'carpet', 'other']
    # ensure surfaces list contains canonical
    for c in canonical:
        if c not in surfaces:
            surfaces.append(c)
    surfaces = sorted(list(set(surfaces)), key=lambda s: (0 if s=='ALL' else 1, s))

    # Build structure
    stats_out = {}
    for sdef in STATS_DEFINITION:
        key = sdef['key']
        label = sdef['label']
        s_type = sdef.get('type','count')
        stat_entry = {'label': label, 'key': key, 'type': s_type, 'per_surface': {}}
        for surf in surfaces:
            pts = []
            for y in years:
                raw = get_stat_value_for(data, y, surf, key)
                if raw is None:
                    v = None
                else:
                    # if pct or rate keys -> normalize to 0..100
                    if s_type in ('pct','rate') or key in RATE_KEYS:
                        v = normalize_pct_value(raw, key=key)
                    else:
                        v = float(raw)
                # round floats for compactness
                if v is not None and isinstance(v, float) and (not math.isfinite(v)):
                    v = None
                if v is not None:
                    # round to 4 decimals for percentages, 3 for others
                    if s_type in ('pct','rate'):
                        v = round(float(v), 4)
                    else:
                        v = round(float(v), 4)
                pts.append({'year': int(y), 'value': v})
            # Compose smoothed (spline) series: select known points (non-null)
            xs = [p['year'] for p in pts if p['value'] is not None]
            ys = [p['value'] for p in pts if p['value'] is not None]
            smoothed = []
            if len(xs) == 0:
                smoothed = []
            elif len(xs) == 1:
                # replicate single point as single sample
                smoothed = [{'x': float(xs[0]), 'y': float(ys[0])}]
            else:
                # sample 100 points between min and max year inclusive
                x_min, x_max = min(xs), max(xs)
                x_eval = np.linspace(float(x_min), float(x_max), 100)
                try:
                    y_eval = natural_cubic_spline_interpolate(xs, ys, x_eval)
                    smoothed = [{'x': float(x_), 'y': None if (y_ is None or (isinstance(y_, float) and not math.isfinite(y_))) else float(round(float(y_), 4))} for x_, y_ in zip(x_eval.tolist(), y_eval.tolist())]
                except Exception as e:
                    # fallback linear interpolation
                    x_eval = np.linspace(float(x_min), float(x_max), 100)
                    y_eval = np.interp(x_eval, xs, ys)
                    smoothed = [{'x': float(x_), 'y': float(round(float(y_), 4))} for x_, y_ in zip(x_eval.tolist(), y_eval.tolist())]
            stat_entry['per_surface'][surf] = {
                'color': SURFACE_COLORS.get(surf, SURFACE_COLORS['other']),
                'points': pts,
                'smoothed': smoothed
            }
        stats_out[key] = stat_entry

    # Build flattened 'series' compatible with simple client view
    flat_series = []
    for key, ent in stats_out.items():
        for surf, obj in ent['per_surface'].items():
            label = f"{ent['label']} — {surf}"
            points_for_series = []
            for p in obj['points']:
                # date as year string for backward compat
                points_for_series.append({'date': str(p['year']), 'value': p['value']})
            flat_series.append({'stat_key': key, 'surface': surf, 'label': label, 'points': points_for_series, 'color': obj['color']})

    out_obj = {
        'meta': {
            'player_id': player_id,
            'slug': slug,
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'years': years,
            'surfaces': surfaces
        },
        'stats': stats_out,
        'series': flat_series
    }

    # write out
    players_dir = os.path.join(out_dir, "players_atp")
    safe_mkdir(players_dir)
    # filename: prefer slug lower or player_id
    fname = str(slug).lower() if slug else str(player_id).lower()
    # sanitize filename
    fname = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', fname)
    out_path = os.path.join(players_dir, f"{fname}.timeseries.json")
    try:
        with open(out_path, 'w', encoding='utf8') as f:
            json.dump(out_obj, f, ensure_ascii=False, indent=2)
        print(f"[ts] wrote {out_path}")
        return out_path
    except Exception as e:
        print(f"[ts] failed to write {out_path}: {e}")
        return None

# ---------- CLI ----------
def main(stats_dir, out_dir, limit_players=None, player=None):
    print("[ts] scanning stats dir:", stats_dir)
    # search for candidate json files
    patterns = [
        os.path.join(stats_dir, "*.stats.json"),
        os.path.join(stats_dir, "*.json"),
    ]
    files = []
    for p in patterns:
        files.extend(sorted(glob.glob(p)))
    # filter: keep JSON that likely contain stat_agg / stats_by_year
    candidates = []
    for f in files:
        try:
            with open(f, 'r', encoding='utf8') as fh:
                j = json.load(fh)
            if isinstance(j, dict) and (j.get('stat_agg') or j.get('stats_by_year') or j.get('career') or j.get('career_by_surface')):
                candidates.append(f)
        except Exception:
            continue
    if not candidates:
        print("[ts] No stats JSON files found in", stats_dir)
        return
    # apply player filter
    if player:
        player_lower = str(player).lower()
        candidates = [c for c in candidates if player_lower in os.path.splitext(os.path.basename(c))[0].lower() or (lambda jj: (isinstance(jj, dict) and ((str(jj.get('slug') or '').lower()==player_lower) or (str(jj.get('player_id') or '').lower()==player_lower))))(json.load(open(c,'r',encoding='utf8')))]
        if not candidates:
            print("[ts] No file matching player", player)
            return
    if limit_players:
        candidates = candidates[:int(limit_players)]
    print(f"[ts] {len(candidates)} stats files will be processed")
    for i, f in enumerate(candidates, start=1):
        print(f"[ts] [{i}/{len(candidates)}] processing {os.path.basename(f)}")
        build_timeseries_from_stats_file(f, out_dir)
    print("[ts] done.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate time series per player from detailed stats JSON")
    ap.add_argument("--stats-dir", default="./players_atp", help="Directory with player stats JSON files")
    ap.add_argument("--out-dir", default="./dist", help="Output directory")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players")
    ap.add_argument("--player", default=None, help="Process single player (slug or id)")
    args = ap.parse_args()
    main(args.stats_dir, args.out_dir, limit_players=args.limit_players, player=args.player)
