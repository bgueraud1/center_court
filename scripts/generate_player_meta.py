#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_player_meta.py (révisé)
Module 1 - Génération non-statistique (métadonnées & index léger de matches)

Usage:
    python scripts/generate_player_meta.py --matches-dir ./matches/atp_matches --out-dir ./docs

Outputs:
  - OUT_DIR/index/players_index.json
  - OUT_DIR/players_atp/data/{player_slug}.json   <- file used by player.html (lazy load)
  - OUT_DIR/players_atp/{PLAYER_ID}.meta.json     <- kept for backward compatibility
  - OUT_DIR/players_atp/{PLAYER_ID}.matches.json  <- lightweight matches list (kept)
"""
from pathlib import Path
import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
import pandas as pd

# -------- helpers --------
def safe_mkdir(path):
    os.makedirs(path, exist_ok=True)

def normalize_player_id(pid):
    if pid is None:
        return ''
    return str(pid).strip().upper()

def slugify(name: str) -> str:
    if not name:
        return ''
    s = name.strip().lower()
    # remove accents roughly, keep ascii/words
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_]+", "-", s)
    s = s.strip("-")
    return s or ''

def parse_date_only(val):
    if val is None:
        return ''
    if isinstance(val, str):
        v = val.strip()
        if v == '':
            return ''
        try:
            dt = datetime.fromisoformat(v)
            return dt.date().isoformat()
        except Exception:
            pass
        try:
            dt = pd.to_datetime(v, errors='coerce')
            if not pd.isna(dt):
                return dt.date().isoformat()
        except Exception:
            pass
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
        # fallback: try YYYY
        m2 = re.search(r"(\d{4})", v)
        if m2:
            return m2.group(1)
        return v
    try:
        dt = pd.to_datetime(val, errors='coerce')
        if not pd.isna(dt):
            return dt.date().isoformat()
    except Exception:
        pass
    return ''

def read_matches_from_dir(matches_dir):
    matches_dir = Path(matches_dir)
    files = sorted(matches_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {matches_dir}")
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"Warning: cannot read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files loaded")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

# lightweight match index builder (fields used by the client)
def build_matches_index_for_player(matches_df: pd.DataFrame, player_id: str, max_matches: int = None):
    pid = normalize_player_id(player_id)
    if not pid:
        return []
    # candidate conditions
    cond_w = ('player_id_winner' in matches_df.columns) and (matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid)
    cond_l = ('player_id_loser' in matches_df.columns) and (matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid)

    rows = []
    if cond_w is not False and cond_w.any():
        rows.append(matches_df[cond_w])
    if cond_l is not False and cond_l.any():
        rows.append(matches_df[cond_l])
    if not rows:
        # fallback: look for name columns? expensive, skip
        return []

    rel = pd.concat(rows, ignore_index=True, sort=False)

    out = []
    for idx, r in rel.iterrows():
        # try to determine is_win
        is_win = None
        if 'player_id_winner' in r.index and normalize_player_id(r.get('player_id_winner')) == pid:
            is_win = True
        elif 'player_id_loser' in r.index and normalize_player_id(r.get('player_id_loser')) == pid:
            is_win = False
        else:
            is_win = None

        match_id = r.get('match_id') or r.get('id') or ''
        event_id = r.get('event_id') or ''
        event_year = str(r.get('event_year') or '')
        match_date = parse_date_only(r.get('start_date') or r.get('match_date') or '')
        score = r.get('score_string') if 'score_string' in r.index else r.get('score') if 'score' in r.index else ''
        round_tok = r.get('round') if 'round' in r.index else ''
        surface = (r.get('surface') or '')

        opp_name = ''
        opp_id = None
        if is_win is True:
            opp_name = r.get('player_loser') or r.get('loser_player_name') or ''
            opp_id = r.get('player_id_loser') if 'player_id_loser' in r.index else None
        elif is_win is False:
            opp_name = r.get('player_winner') or r.get('winner_player_name') or ''
            opp_id = r.get('player_id_winner') if 'player_id_winner' in r.index else None
        else:
            opp_name = r.get('player_winner') or r.get('player_loser') or r.get('winner_player_name') or r.get('loser_player_name') or ''
            opp_id = r.get('player_id_winner') or r.get('player_id_loser') or None

        entry = {
            'match_id': str(match_id),
            'event_id': str(event_id),
            'event_year': str(event_year),
            'match_date': match_date,
            'opponent': str(opp_name) if opp_name is not None else '',
            'opponent_id': str(opp_id).strip().upper() if opp_id not in (None, '') else None,
            'is_win': bool(is_win) if is_win is not None else None,
            'score': str(score) if score is not None else '',
            'round': str(round_tok) if round_tok is not None else '',
            'surface': str(surface) if surface is not None else ''
        }
        out.append(entry)

    # sort ascending by date (client sorts/filters), but keep stable order
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

def choose_most_likely_name(cands):
    counts = Counter([str(x).strip() for x in cands if x and str(x).strip()])
    if not counts:
        return ''
    return counts.most_common(1)[0][0]

# build per-player combined object used by player.html (lazy)
def build_player_combined(matches_df: pd.DataFrame, player_id: str, player_data_df: pd.DataFrame = None):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    # find all rows for this player
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

    # name
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name','player_winner_name','player_loser_name'):
        if col in df.columns:
            name_candidates.extend(df[col].dropna().astype(str).tolist())
    name = choose_most_likely_name(name_candidates) or pid

    # enrich from player_data_df if present
    birthdate = ''
    birthplace = ''
    height_cm = None
    hand = ''
    backhand = ''
    best_rank = None
    first_appearance = ''
    last_appearance = ''
    image = None
    country = None

    if isinstance(player_data_df, pd.DataFrame) and 'player_id' in player_data_df.columns:
        # try to match row (normalize case)
        row = player_data_df[player_data_df['player_id'].astype(str).str.strip().str.upper() == pid]
        if not row.empty:
            row = row.iloc[0].to_dict()
            birthdate = parse_date_only(row.get('birth_date') or row.get('dob') or row.get('date_of_birth') or '')
            birthplace = row.get('birth_place') or row.get('birthplace') or ''
            # height attempts
            try:
                if 'height_cm' in row and row.get('height_cm'):
                    height_cm = float(row.get('height_cm'))
            except Exception:
                height_cm = None
            hand = row.get('hand') or row.get('plays') or ''
            backhand = row.get('backhand') or ''
            best_rank = row.get('best_rank') or row.get('career_high_rank') or None
            image = row.get('image') or row.get('photo') or None
            country = row.get('country') or row.get('nationality') or row.get('country_code') or country

    # fallback: try to deduce country from matches columns
    if not country:
        country_cols = []
        for c in ('winner_country','loser_country','country_winner','country_loser','player_country'):
            if c in df.columns:
                # extend by values
                vals = df[c].dropna().astype(str).str.strip().str.upper().tolist()
                country_cols.extend(vals)
        if country_cols:
            country = Counter([c for c in country_cols if c]).most_common(1)[0][0]

    # fallback: try deducing first/last appearance from matches data
    years = sorted([y for y in df['event_year'].dropna().astype(str).unique() if str(y).strip()!=''])
    if years:
        first_appearance = years[0]
        last_appearance = years[-1]

    # basic summary
    matches_index = build_matches_index_for_player(matches_df, pid)
    matches_played = len(matches_index)
    matches_won = sum(1 for m in matches_index if m.get('is_win') is True)
    matches_lost = sum(1 for m in matches_index if m.get('is_win') is False)

    summary = {
        'matches_played': int(matches_played),
        'matches_won': int(matches_won),
        'matches_lost': int(matches_lost)
    }

    # build matches_by_year (group matches_index by event_year descending)
    matches_by_year = defaultdict(list)
    for m in matches_index:
        y = m.get('event_year') or ''
        matches_by_year[y].append(m)
    # sort years descending and within each year sort by date desc
    matches_by_year_sorted = {}
    for y in sorted([k for k in matches_by_year.keys() if k], reverse=True):
        arr = matches_by_year[y]
        def date_key(x):
            d = x.get('match_date') or ''
            try:
                return datetime.fromisoformat(d)
            except Exception:
                return datetime.min
        arr_sorted = sorted(arr, key=date_key, reverse=True)
        matches_by_year_sorted[y] = arr_sorted
    # include empty year '' if present
    if '' in matches_by_year and '' not in matches_by_year_sorted:
        matches_by_year_sorted[''] = matches_by_year['']

    # --- trophies and best_by_year calculation ---
    trophies_map = {}
    best_by_year = {}
    # We also attempt to get 'points_for_result' if present in original matches df
    # Build dict of matches by match_id for lookup of columns not in index
    match_lookup = {}
    # build mapping from match_id -> original row (first occurence)
    for idx, r in df.iterrows():
        mid = r.get('match_id') or r.get('id') or ''
        if mid and str(mid) not in match_lookup:
            match_lookup[str(mid)] = r

    for m in matches_index:
        mid = m.get('match_id') or ''
        y = m.get('event_year') or ''
        key = f"{m.get('event_id','')}_{y}"
        round_tok = (m.get('round') or '').upper()
        cat = None
        rr = match_lookup.get(str(mid))
        if rr is not None:
            cat = rr.get('category') or rr.get('level') or rr.get('tourney_level') or rr.get('category_name') or None

        # best_by_year logic: pick best event (by points) per event_id+year
        if y:
            if y not in best_by_year:
                best_by_year[y] = {}
            candidate = 0
            pts = None
            if rr is not None:
                for c in ('points_for_result','points','ranking_points','points_won'):
                    try:
                        if c in rr.index and rr.get(c) not in (None, ''):
                            pts = int(float(rr.get(c)))
                            break
                    except Exception:
                        pts = None
            if pts is not None:
                candidate = pts
            cur = best_by_year[y].get(key)
            if cur is None or candidate > cur.get('points', 0):
                best_by_year[y][key] = {
                    'event_id': m.get('event_id'),
                    'event_year': y,
                    'tourney_name': (rr.get('tourney_name') if rr is not None else ''),
                    'category': cat or 'Other',
                    'surface': (rr.get('surface') if rr is not None else ''),
                    'points': candidate
                }

        # trophies detection: a win in final or round token W/F/WIN
        if m.get('is_win') and round_tok in ('W','WIN','F'):
            trophies_map[key] = {
                'event_id': m.get('event_id'),
                'event_year': y,
                'tourney_name': (rr.get('tourney_name') if rr is not None else ''),
                'category': cat or 'Other',
                'surface': (rr.get('surface') if rr is not None else '')
            }

    # convert best_by_year maps to lists and compute totals
    best_by_year_lists = {}
    for y, d in best_by_year.items():
        arr = list(d.values())
        arr_sorted = sorted(arr, key=lambda t: (-t.get('points', 0), t.get('tourney_name','')))
        best_by_year_lists[y] = arr_sorted
    total_points_by_year = { y: sum(item.get('points',0) for item in arr) for y, arr in best_by_year_lists.items() }

    trophies_list = list(trophies_map.values())
    trophies_sorted = sorted(trophies_list, key=lambda t: (-int(t.get('event_year') or 0), t.get('tourney_name','')))

    # build combined object that player.html / JS expects
    slug_name = slugify(name) or pid.lower()
    player_slug = f"{pid.lower()}-{slug_name}"

    combined = {
        'player_id': pid,
        'name': name,
        'slug': player_slug,
        # country kept as ISO / code if available
        'country': country,
        'birthdate': birthdate,
        'birthplace': birthplace,
        'height_cm': height_cm,
        'hand': hand,
        'backhand': backhand,
        'best_rank': best_rank,
        'first_appearance': first_appearance,
        'last_appearance': last_appearance,
        'image': image,
        'summary': summary,
        'matches': matches_index,
        'matches_by_year': matches_by_year_sorted,
        'trophies': trophies_sorted,
        'best_by_year': best_by_year_lists,
        'total_points_by_year': total_points_by_year,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'version': 'meta_v2'
    }

    return combined

# -------- main CLI --------
def main(matches_dir: str, out_dir: str, limit_players: int = None, player_data_csv: str = None):
    matches = read_matches_from_dir(matches_dir)
    print(f"Read matches: rows={len(matches)}, cols={len(matches.columns)}")

    # optional player data CSV (for richer fields)
    player_data_df = None
    if player_data_csv:
        try:
            player_data_df = pd.read_csv(player_data_csv, low_memory=False)
            print("Loaded player data CSV:", player_data_csv, "rows:", len(player_data_df))
        except Exception as e:
            print("Warning: could not read player_data CSV:", e)
            player_data_df = None

    # collect player ids
    player_ids = set()
    if 'player_id_winner' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
    if 'player_id_loser' in matches.columns:
        player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
    player_ids = sorted([p for p in player_ids if p])

    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    print("Players to process:", len(player_ids))

    out_dir = Path(out_dir)
    idx_dir = out_dir / "index"
    players_dir = out_dir / "players_atp"
    players_data_dir = players_dir / "data"
    safe_mkdir(idx_dir)
    safe_mkdir(players_dir)
    safe_mkdir(players_data_dir)

    players_index = []
    for i, pid in enumerate(player_ids, start=1):
        try:
            print(f"[{i}/{len(player_ids)}] {pid} ...", end=' ')
            combined = build_player_combined(matches, pid, player_data_df)
            if combined is None:
                print("skip (no data)")
                continue
            # write combined data file used by lazy JS
            json_slug = combined['slug']
            data_path = players_data_dir / f"{json_slug}.json"
            with open(data_path, 'w', encoding='utf8') as f:
                json.dump(combined, f, ensure_ascii=False, indent=2)

            # also write legacy meta and matches for backward compatibility
            meta_path = players_dir / f"{pid}.meta.json"
            matches_path = players_dir / f"{pid}.matches.json"
            legacy_meta = {
                'player_id': pid,
                'name': combined['name'],
                'slug': combined['slug'],
                'country': combined.get('country'),
                'summary': combined.get('summary'),
                'matches_count': len(combined.get('matches', [])),
                'matches_index_path': f"players_atp/{pid}.matches.json",
                'generated_at': combined['generated_at'],
                'version': 'meta_v2'
            }
            with open(meta_path, 'w', encoding='utf8') as f:
                json.dump(legacy_meta, f, ensure_ascii=False, indent=2)
            with open(matches_path, 'w', encoding='utf8') as f:
                json.dump({'matches': combined.get('matches', []), 'generated_at': combined['generated_at']}, f, ensure_ascii=False, indent=2)

            # index entry (used by players index page)
            players_index.append({
                'player_id': pid,
                'name': combined['name'],
                'slug': combined['slug'],   # ex: s0ag-jannik-sinner
                'page_href': f"players_atp/{combined['slug']}",   # link target used by index page -> _redirects rule will rewrite path to player.html
                'data_path': f"players_atp/data/{combined['slug']}.json",
                'country': combined.get('country'),
                'matches_count': len(combined.get('matches', []))
            })
            print("done")
        except Exception as e:
            print("ERROR", e)

    # write index
    players_index_path = idx_dir / "players_index.json"
    with open(players_index_path, 'w', encoding='utf8') as f:
        json.dump({'players': players_index, 'generated_at': datetime.utcnow().isoformat()+'Z'}, f, ensure_ascii=False, indent=2)

    print("Wrote players_index:", players_index_path)
    print("Wrote player data to:", players_data_dir)
    print("Legacy meta/matches written to:", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate player meta + lightweight matches index for ATP players.")
    ap.add_argument("--matches-dir", required=True, help="Dir with matches CSVs (glob *.csv).")
    ap.add_argument("--out-dir", default="./docs", help="Output directory (publish dir, default ./docs).")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit processed players (testing).")
    ap.add_argument("--player-data-csv", default=None, help="Optional CSV with extra player fields (player_id,...).")
    args = ap.parse_args()
    main(args.matches_dir, args.out_dir, args.limit_players, args.player_data_csv)
