

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_player_meta.py (strict start_date) — updated
"""
from pathlib import Path
import argparse
import json
import os
import re
import traceback
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
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_]+", "-", s)
    s = s.strip("-")
    return s or ''

def parse_date_only(val):
    """Return ISO date 'YYYY-MM-DD' if possible, else ''."""
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
        return ''
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
            df = pd.read_csv(f, low_memory=False, dtype=str)
            # normaliser les valeurs 'nan' importées comme chaînes vers de vrais NA
            df = df.where(pd.notnull(df), None)
            frames.append(df)
        except Exception as e:
            print(f"Warning: cannot read {f}: {e}")
    if not frames:
        raise RuntimeError("No CSV files loaded")
    matches = pd.concat(frames, ignore_index=True, sort=False)
    return matches

# Points mapping (updated using supplied table)
POINTS_TABLE = {
    'grand_slam': {'W':2000,'F':1300,'SF':800,'QF':400,'R16':200,'R32':100,'R64':50,'R128':10},
    'wta_1000': {'W':1000,'F':650,'SF':400,'QF':200,'R16':100,'R32':50,'R64':30,'R128':10},
    'wta_500': {'W':500,'F':330,'SF':200,'QF':100,'R16':50,'R32':25,'R64':0},
    'wta_250': {'W':250,'F':165,'SF':100,'QF':50,'R16':25,'R32':13,'R64':0},
    'wta_finals': {'W':500,'F':400,'SF':400,'RR_WIN':200},
    'default': {'W':0,'F':0,'SF':0,'QF':0,'R16':0,'R32':0,'R64':0,'R128':0}
}


def detect_category_key(cat_str):
    if not cat_str:
        return 'default'
    s = str(cat_str).lower()
    s = s.replace('-', ' ')
    if 'grand' in s or 'slam' in s or 'major' in s or '\bgs\b' in s:
        return 'grand_slam'
    if 'final' in s and ('wta' in s or 'finals' in s):
        return 'wta_finals'
    if 'masters' in s or '1000' in s or 'wta 1000' in s:
        return 'wta_1000'
    if '500' in s and 'wta' in s:
        return 'wta_500'
    if '250' in s and 'wta' in s:
        return 'wta_250'
    # catch some abbreviations
    if s.strip() in ('gs','grand slam'):
        return 'grand_slam'
    return 'default'


def normalize_round_token(round_tok):
    if round_tok is None:
        return ''
    r = str(round_tok).strip().upper()
    if r in ('W','WIN'):
        return 'W'
    if r in ('F','FINAL'):
        return 'F'
    if r in ('SF','SEMI','SEMI-FINAL','S'):
        return 'SF'
    if r in ('QF','QUARTER','QUARTER-FINAL','Q'):
        return 'QF'
    if r.startswith('R') and r[1:].isdigit():
        return r
    if 'ROUND' in r and '16' in r:
        return 'R16'
    if 'ROUND' in r and '32' in r:
        return 'R32'
    if 'ROUND' in r and '64' in r:
        return 'R64'
    if r in ('RR','RR1','RR2'):
        return 'RR'
    return r


def points_for_match_row(rr):
    if rr is None:
        return 0
    # prefer explicit numeric columns
    for c in ('points_for_result','points','ranking_points','points_won'):
        try:
            if c in rr.index and rr.get(c) not in (None, ''):
                val = rr.get(c)
                try:
                    return int(float(val))
                except Exception:
                    pass
        except Exception:
            pass
    # fallback to calculation by category + round
    cat = None
    for c in ('category','level','tourney_level','category_name','tourney_name'):
        try:
            if c in rr.index and rr.get(c) not in (None, ''):
                cat = rr.get(c)
                break
        except Exception:
            pass
    cat_key = detect_category_key(cat)
    round_tok = normalize_round_token(rr.get('round') if 'round' in rr.index else '')
    table = POINTS_TABLE.get(cat_key, POINTS_TABLE['default'])
    # If the row represents a winner but round token is 'F', prefer winner points
    is_win = False
    try:
        if 'player_id_winner' in rr.index and str(rr.get('player_id_winner')).strip() != '':
            # this function doesn't know the player of interest; keep generic behavior
            pass
    except Exception:
        pass
    if round_tok in table and table.get(round_tok) is not None:
        return int(table.get(round_tok) or 0)
    if round_tok.startswith('R'):
        if round_tok in table:
            return int(table.get(round_tok) or 0)
        if 'R32' in table and 'R16' not in table:
            return int(table.get('R32') or 0)
    return int(table.get('W') or 0) if round_tok == 'W' else int(table.get('F') or 0) if round_tok == 'F' else 0

# build_matches_index_for_player: **only** use start_date for dates
def build_matches_index_for_player(matches_df: pd.DataFrame, player_id: str, max_matches: int = None):
    pid = normalize_player_id(player_id)
    if not pid:
        return []
    cond_w_series = None
    cond_l_series = None
    try:
        if 'player_id_winner' in matches_df.columns:
            cond_w_series = matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid
        if 'player_id_loser' in matches_df.columns:
            cond_l_series = matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid
    except Exception:
        cond_w_series = None
        cond_l_series = None

    rows = []
    if isinstance(cond_w_series, pd.Series) and cond_w_series.any():
        rows.append(matches_df[cond_w_series])
    if isinstance(cond_l_series, pd.Series) and cond_l_series.any():
        rows.append(matches_df[cond_l_series])
    if not rows:
        return []

    rel = pd.concat(rows, ignore_index=True, sort=False)

    out = []
    for idx, r in rel.iterrows():
        try:
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
            # STRICT: only read start_date column
            start_date = parse_date_only(r.get('start_date') or '')
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

            tourney_name = r.get('tourney_name') or r.get('event_name') or ''
            category = r.get('category') or r.get('level') or r.get('tourney_level') or r.get('category_name') or ''
            pts = points_for_match_row(r)

            entry = {
                'match_id': str(match_id),
                'event_id': str(event_id),
                'event_year': str(event_year),
                # strict field name
                'start_date': start_date,
                'opponent': str(opp_name) if opp_name is not None else '',
                'opponent_id': str(opp_id).strip().upper() if opp_id not in (None, '') else None,
                'is_win': bool(is_win) if is_win is not None else None,
                'score': str(score) if score is not None else '',
                'round': str(round_tok) if round_tok is not None else '',
                'surface': str(surface) if surface is not None else '',
                'tourney_name': str(tourney_name) if tourney_name is not None else '',
                'category': str(category) if category is not None else '',
                'points': int(pts or 0)
            }
            out.append(entry)
        except Exception:
            print("Warning: failed to process a match row for player", pid)
            traceback.print_exc()

    # sort by start_date ascending (older -> newer) keep stable
    def date_key(x):
        d = x.get('start_date') or ''
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

# Round ordering helper (F/W > SF > QF > R16 > R32 > R64 > R128 > others)
ROUND_ORDER = {
    'W': 0, 'WIN': 0, 'F': 0,
    'SF': 1, 'S': 1,
    'QF': 2,
    'R16': 3, '4R': 3,
    'R32': 4,
    'R64': 5,
    'R128': 6,
    'RR': 7
}
def round_sort_index(tok):
    t = normalize_round_token(tok)
    return ROUND_ORDER.get(t, 99)

# build_player_combined: use start_date everywhere (no fallback)
def build_player_combined(matches_df: pd.DataFrame, player_id: str, player_data_df: pd.DataFrame = None):
    pid = normalize_player_id(player_id)
    if not pid:
        return None

    cond_w_series = None
    cond_l_series = None
    try:
        if 'player_id_winner' in matches_df.columns:
            cond_w_series = matches_df['player_id_winner'].astype(str).str.strip().str.upper() == pid
        if 'player_id_loser' in matches_df.columns:
            cond_l_series = matches_df['player_id_loser'].astype(str).str.strip().str.upper() == pid
    except Exception:
        cond_w_series = None
        cond_l_series = None

    frames = []
    if isinstance(cond_w_series, pd.Series) and cond_w_series.any():
        frames.append(matches_df[cond_w_series])
    if isinstance(cond_l_series, pd.Series) and cond_l_series.any():
        frames.append(matches_df[cond_l_series])
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True, sort=False)

    # name
    name_candidates = []
    for col in ('player_winner','player_loser','winner_player_name','loser_player_name','player_winner_name','player_loser_name','full_name','name'):
        if col in df.columns:
            try:
                name_candidates.extend(df[col].dropna().astype(str).tolist())
            except Exception:
                pass
    name = choose_most_likely_name(name_candidates) or pid

    # enrich from player_data_df
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
    full_name = name

    if isinstance(player_data_df, pd.DataFrame) and 'player_id' in player_data_df.columns:
        row = None
        try:
            # Normaliser les deux côtés pour éviter les problèmes du type float '123.0'
            col_str = player_data_df['player_id'].astype(str).str.strip().str.upper().str.replace(r'\.0$','', regex=True)
            pid_key = str(pid).strip().upper()
            pid_key = re.sub(r'\.0$', '', pid_key)  # retire éventuel ".0"

            mask = col_str == pid_key
            sub = player_data_df[mask]
            if not sub.empty:
                row = sub.iloc[0].to_dict()
        except Exception:
            row = None

        # row est soit None soit un dict — on peut tester proprement
        if row:
            full_name = row.get('full_name') or name
            birthdate = parse_date_only(row.get('birth_date') or row.get('dob') or '')
            birthplace = row.get('birthplace') or row.get('birth_place') or ''
            try:
                if 'height_cm' in row and row.get('height_cm'):
                    height_cm = float(str(row.get('height_cm')).replace('m','').strip())
                else:
                    h = row.get('height_cm') or row.get('height') or row.get('height_inches')
                    if h and isinstance(h, str) and 'm' in h:
                        try:
                            height_cm = float(h.replace('m','').strip())
                        except Exception:
                            height_cm = None
            except Exception:
                height_cm = None
            hand = row.get('plays') or row.get('hand') or ''
            backhand = row.get('backhand') or ''
            best_rank = row.get('highest_ranking') or row.get('best_rank') or row.get('career_high_rank') or None
            image = row.get('image') or row.get('photo') or None
            country = row.get('represented_country') or row.get('country') or row.get('country_code') or None
            first_appearance = parse_date_only(row.get('first_appearance') or '') or ''
            last_appearance = parse_date_only(row.get('last_appearance') or '') or ''


    # fallback deduce first/last from df event_year only (dates not used)
    try:
        years = sorted([y for y in df['event_year'].dropna().astype(str).unique() if str(y).strip()!=''])
        if years and not first_appearance:
            first_appearance = years[0]
        if years and not last_appearance:
            last_appearance = years[-1]
    except Exception:
        pass

    # build matches index (entries include start_date)
    matches_index = build_matches_index_for_player(matches_df, pid)
    matches_played = len(matches_index)
    matches_won = sum(1 for m in matches_index if m.get('is_win') is True)
    matches_lost = sum(1 for m in matches_index if m.get('is_win') is False)

    summary = {
        'matches_played': int(matches_played),
        'matches_won': int(matches_won),
        'matches_lost': int(matches_lost)
    }

    # build match_lookup: map match_id -> original row (so we can read start_date if needed)
    match_lookup = {}
    try:
        for idx, r in df.iterrows():
            mid = r.get('match_id') or r.get('id') or ''
            if mid and str(mid) not in match_lookup:
                match_lookup[str(mid)] = r
    except Exception:
        traceback.print_exc()

    # GROUP TOURNAMENTS: display name ALWAYS from tourney_name column. start_date ONLY from start_date col.
    tournaments_by_year = defaultdict(lambda: {})
    for m in matches_index:
        try:
            y = m.get('event_year') or ''
            tourney_name = (m.get('tourney_name') or '').strip()
            ev = m.get('event_id') or ''
            # get start_date strictly from match entry's start_date (already parsed)
            sd = m.get('start_date') or ''
            # Key uses event_id + tourney_name + start_date to avoid accidental duplicates,
            # but displayed name = tourney_name (no fallback)
            event_key = f"{ev or 'NOID'}||{tourney_name}||{sd}"
            tb = tournaments_by_year[y]
            if event_key not in tb:
                # category & surface from match entry (prefer)
                category = m.get('category') or ''
                surface = m.get('surface') or ''
                tb[event_key] = {
                    'event_id': ev,
                    'tourney_name': tourney_name,
                    'category': category,
                    'surface': surface,
                    'start_date': sd,   # strict
                    'matches': []
                }
            tb[event_key]['matches'].append(m)
        except Exception:
            print("Warning: failed grouping match into tournament for player", pid)
            traceback.print_exc()

    # convert and sort tournaments_by_year: tournaments sorted by start_date descending then name
    matches_by_year_structured = {}
    for y, d in tournaments_by_year.items():
        arr = []
        for evk, info in d.items():
            try:
                # inside each tournament, sort matches: round order then start_date desc
                info['matches'] = sorted(
                    info['matches'],
                    key=lambda mm: (round_sort_index(mm.get('round')),
                                    -int(pd.to_datetime(mm.get('start_date') or '1970-01-01', errors='coerce', utc=True).timestamp() or 0))
                )
            except Exception:
                try:
                    info['matches'] = sorted(info['matches'], key=lambda mm: mm.get('start_date') or '', reverse=True)
                except Exception:
                    pass
            arr.append(info)
        def tkey(t):
            sd = t.get('start_date') or ''
            try:
                ts = pd.to_datetime(sd, errors='coerce')
                if pd.isna(ts):
                    return (datetime.min, t.get('tourney_name') or '')
                return (ts.to_pydatetime(), t.get('tourney_name') or '')
            except Exception:
                return (datetime.min, t.get('tourney_name') or '')
        arr_sorted = sorted(arr, key=lambda t: (tkey(t)[0], tkey(t)[1]), reverse=True)
        matches_by_year_structured[y] = arr_sorted

    # trophies & best_by_year based on matches_index (points present or computed)
    trophies_map = {}
    best_by_year = {}
    for m in matches_index:
        try:
            y = m.get('event_year') or ''
            event_key = f"{m.get('event_id','')}_{y}"
            rtok = normalize_round_token(m.get('round') or '')
            is_win = bool(m.get('is_win') is True)

            # Determine points for trophy / best: if player won the match and round is final ('F'), treat as winner 'W' to give winner points
            # Compute points from m.get('points') if present, otherwise derive from table but prefer winner points when appropriate
            pts = int(m.get('points') or 0)
            if pts == 0:
                # derive
                cat_key = detect_category_key(m.get('category') or '')
                table = POINTS_TABLE.get(cat_key, POINTS_TABLE['default'])
                # if winner and final, give W points
                if is_win and rtok in ('F',''):
                    pts = int(table.get('W') or 0)
                else:
                    # if round present in table use it, otherwise fallback
                    pts = int(table.get(rtok) or table.get('W') or 0)

            # Only consider *actual winners* as trophies. A finalist who lost shouldn't be recorded as winner.
            if is_win and rtok in ('W','WIN','F'):
                # store winner points (ensure winner points used)
                # if rtok == 'F' but is_win True, we already ensured pts is winner points
                trophies_map[event_key] = {
                    'event_id': m.get('event_id'),
                    'event_year': y,
                    'tourney_name': m.get('tourney_name') or '',
                    'category': m.get('category') or '',
                    'surface': m.get('surface') or '',
                    'points': int(pts or 0)
                }

            # best_by_year: keep the best single performance per event/year for the player and include final round
            if y:
                if y not in best_by_year:
                    best_by_year[y] = {}
                key = f"{m.get('event_id','')}"
                cur = best_by_year[y].get(key)
                if cur is None or int(pts or 0) > int(cur.get('points', 0)):
                    best_by_year[y][key] = {
                        'event_id': m.get('event_id'),
                        'event_year': y,
                        'tourney_name': m.get('tourney_name') or '',
                        'category': m.get('category') or '',
                        'surface': m.get('surface') or '',
                        'points': int(pts or 0),
                        'round': normalize_round_token(m.get('round') or '')
                    }
        except Exception:
            print("Warning: failed computing trophies/best for player", pid)
            traceback.print_exc()

    best_by_year_lists = {}
    for y, d in best_by_year.items():
        arr = list(d.values())
        arr_sorted = sorted(arr, key=lambda t: (-t.get('points', 0), t.get('tourney_name','')))
        best_by_year_lists[y] = arr_sorted
    total_points_by_year = { y: sum(item.get('points',0) for item in arr) for y, arr in best_by_year_lists.items() }

    trophies_list = list(trophies_map.values())
    trophies_sorted = sorted(trophies_list, key=lambda t: (-int(t.get('points') or 0), -int(t.get('event_year') or 0)))

    slug_name = slugify(full_name or name) or pid.lower()
    player_slug = f"{pid.lower()}-{slug_name}"

    combined = {
        'player_id': pid,
        'name': full_name or name,
        'slug': player_slug,
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
        'matches_by_year': matches_by_year_structured,
        'trophies': trophies_sorted,
        'best_by_year': best_by_year_lists,
        'total_points_by_year': total_points_by_year,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'version': 'meta_v4-strict-startdate-updated'
    }

    return combined

# -------- main CLI --------
def main(matches_dir: str, out_dir: str, limit_players: int = None, player_data_csv: str = None):
    matches = read_matches_from_dir(matches_dir)
    print(f"Read matches: rows={len(matches)}, cols={len(matches.columns)}")

    player_data_df = None
    if player_data_csv:
        try:
            player_data_df = pd.read_csv(player_data_csv, low_memory=False)
            print("Loaded player data CSV:", player_data_csv, "rows:", len(player_data_df))
        except Exception as e:
            print("Warning: could not read player_data CSV:", e)
            player_data_df = None

    player_ids = set()
    if 'player_id_winner' in matches.columns:
        try:
            player_ids.update([normalize_player_id(x) for x in matches['player_id_winner'].dropna().unique()])
        except Exception:
            traceback.print_exc()
    if 'player_id_loser' in matches.columns:
        try:
            player_ids.update([normalize_player_id(x) for x in matches['player_id_loser'].dropna().unique()])
        except Exception:
            traceback.print_exc()
    player_ids = sorted([p for p in player_ids if p])

    if limit_players:
        player_ids = player_ids[:int(limit_players)]

    print("Players to process:", len(player_ids))

    out_dir = Path(out_dir)
    idx_dir = out_dir / "index"
    players_dir = out_dir / "players"
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
            json_slug = combined['slug']
            data_path = players_data_dir / f"{json_slug}.json"
            with open(data_path, 'w', encoding='utf8') as f:
                json.dump(combined, f, ensure_ascii=False, indent=2)
            meta_path = players_dir / f"{pid}.meta.json"
            matches_path = players_dir / f"{pid}.matches.json"
            legacy_meta = {
                'player_id': pid,
                'name': combined['name'],
                'slug': combined['slug'],
                'country': combined.get('country'),
                'summary': combined.get('summary'),
                'matches_count': len(combined.get('matches', [])),
                'matches_index_path': f"players/{pid}.matches.json",
                'generated_at': combined['generated_at'],
                'version': combined.get('version','meta_v4-strict-startdate-updated')
            }
            with open(meta_path, 'w', encoding='utf8') as f:
                json.dump(legacy_meta, f, ensure_ascii=False, indent=2)
            with open(matches_path, 'w', encoding='utf8') as f:
                json.dump({'matches': combined.get('matches', []), 'generated_at': combined['generated_at']}, f, ensure_ascii=False, indent=2)

            players_index.append({
                'player_id': pid,
                'name': combined['name'],
                'slug': combined['slug'],
                'page_href': f"players/{combined['slug']}",
                'data_path': f"players/data/{combined['slug']}.json",
                'country': combined.get('country'),
                'matches_count': len(combined.get('matches', []))
            })
            print("done")
        except Exception as e:
            print("ERROR", e)
            traceback.print_exc()

    players_index_path = idx_dir / "players_wta_index.json"
    with open(players_index_path, 'w', encoding='utf8') as f:
        json.dump({'players': players_index, 'generated_at': datetime.utcnow().isoformat()+'Z'}, f, ensure_ascii=False, indent=2)

    print("Wrote players_wta_index:", players_index_path)
    print("Wrote player data to:", players_data_dir)
    print("Legacy meta/matches written to:", players_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate player meta + lightweight matches index for WTA players.")
    ap.add_argument("--matches-dir", required=True, help="Dir with matches CSVs (glob *.csv).")
    ap.add_argument("--out-dir", default="./docs", help="Output directory (publish dir, default ./docs).")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit processed players (testing).")
    ap.add_argument("--player-data-csv", default="player_data_wta.csv", help="Optional CSV with extra player fields (player_id,...).")
    args = ap.parse_args()
    main(args.matches_dir, args.out_dir, args.limit_players, args.player_data_csv)
