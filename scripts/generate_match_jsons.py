#!/usr/bin/env python3
"""
scripts/generate_match_jsons.py

Génère des JSONs de match à partir des CSV ATP / WTA.

Usage:
  python3 scripts/generate_match_jsons.py --src matches --out docs/matches
  python3 scripts/generate_match_jsons.py --src matches --out docs/matches --only 73_1997,609_2021
"""

import csv
import json
import os
import argparse
import re
from pathlib import Path

def safe_float(val):
    if val is None or val == '':
        return None
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val).strip())
        except Exception:
            return None

def get_first_exists(row, keys):
    for k in keys:
        if k in row and row[k] not in (None, ''):
            return row[k]
    return None

def normalize_sets_from_row(row):
    sets = []
    for i in range(1, 6):
        k = f"set{i}_score"
        if k in row and row[k] not in (None, ''):
            sets.append(row[k])
    return sets

def extract_common_stats(row):
    def pick(*keys):
        return get_first_exists(row, keys)

    ws = {
        'aces': safe_float(pick('aces_tot_winner','winner_aces_set1','aces_set1')),
        'doublefaults': safe_float(pick('doublefaults_tot_winner','winner_dblflt_set1','dblflt_set1')),
        'firstserve_percent': safe_float(pick('firstserve_percent_tot_winner','firstservepercent_tot_winner')),
        'totalpointswon_percent': safe_float(pick('totalpointswon_percent_tot_winner','totalpointswon_percent_tot_winner')),
    }
    ls = {
        'aces': safe_float(pick('aces_tot_loser','loser_aces_set1','aces_set1')),
        'doublefaults': safe_float(pick('doublefaults_tot_loser','loser_dblflt_set1','dblflt_set1')),
        'firstserve_percent': safe_float(pick('firstserve_percent_tot_loser','firstservepercent_tot_loser')),
        'totalpointswon_percent': safe_float(pick('totalpointswon_percent_tot_loser','totalpointswon_percent_tot_loser')),
    }

    # meta ids/names
    meta_winner = {
        "name": pick('winner_player_name','winner','player_winner'),
        "player_id": pick('player_id_winner','player_id_winner'),
        "country": pick('winner_country','country_winner'),
        "seed": pick('winner_seed','seed_winner'),
    }
    meta_loser = {
        "name": pick('loser_player_name','loser','player_loser'),
        "player_id": pick('player_id_loser','player_id_loser'),
        "country": pick('loser_country','country_loser'),
        "seed": pick('loser_seed','seed_loser'),
    }

    return {'winner': ws, 'loser': ls, 'meta_winner': meta_winner, 'meta_loser': meta_loser}

def normalize_pair(t, y):
    return f"{str(t).strip()}_{str(y).strip()}"

def extract_tourney_year_from_filename(fname):
    m = re.search(r'(\d+)[_-](\d{4})', fname)
    if m:
        return m.group(1), m.group(2)
    return None, None

def process_csv_file(path_csv, out_base, kind='atp', allowed_pairs=None):
    path_csv = Path(path_csv)
    fname = path_csv.name
    tourney, year = extract_tourney_year_from_filename(fname)

    with open(path_csv, newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    if not rows:
        print(f"[WARN] {path_csv} empty -> skipped")
        return

    if not tourney or not year:
        first = rows[0]
        tourney = tourney or get_first_exists(first, ('event_id','tourney_id','tourneyid','tourney_id'))
        year = year or get_first_exists(first, ('event_year','tourney_year','year','tourney_year'))

    if not tourney or not year:
        print(f"[WARN] Impossible de déterminer tourney/year pour {path_csv} -> skipping")
        return

    pair_key = normalize_pair(tourney, year)
    if allowed_pairs is not None and pair_key not in allowed_pairs:
        print(f"[SKIP] {pair_key} non demandé -> skipped file {path_csv.name}")
        return

    # output directory (per kind)
    out_dir = Path(out_base) / (f"{tourney}_{year}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # collect index entries for this tournament
    index_matches = []

    for i, row in enumerate(rows):
        match_id = get_first_exists(row, ('match_id','matchid','Match_ID','match_id_winner','match_id_loser'))
        if not match_id:
            r = get_first_exists(row, ('round','Round','round_name')) or f"r{i+1}"
            w = get_first_exists(row, ('winner_player_name','winner','player_winner')) or ""
            l = get_first_exists(row, ('loser_player_name','loser','player_loser')) or ""
            match_id = f"{r}_{(w[:10] if w else 'W')}_{(l[:10] if l else 'L')}"
            match_id = re.sub(r'\s+','', match_id)
        match_id = str(match_id).strip()

        payload = {}
        for k in ('tourney_name','event_id','event_year','tourney_id','tourney_year','level','start_date','end_date','surface','match_date','match_time_total','round','match_status'):
            if k in row and row[k] not in (None, ''):
                payload[k] = row[k]

        payload['tourney'] = tourney
        payload['year'] = year
        payload['match_id'] = match_id
        payload['score_string'] = get_first_exists(row, ('score_string','score','score')) or ''
        payload['num_sets'] = get_first_exists(row, ('num_sets','num_sets')) or None
        payload['sets'] = normalize_sets_from_row(row)
        payload['players'] = {
            'winner': {
                'name': get_first_exists(row,('winner_player_name','winner','player_winner','winner_player')),
                'country': get_first_exists(row,('winner_country','country_winner')),
                'seed': get_first_exists(row,('winner_seed','seed_winner')),
            },
            'loser': {
                'name': get_first_exists(row,('loser_player_name','loser','player_loser','loser_player')),
                'country': get_first_exists(row,('loser_country','country_loser')),
                'seed': get_first_exists(row,('loser_seed','seed_loser')),
            }
        }
        payload['stats'] = extract_common_stats(row)
        payload['raw'] = row

        out_file = out_dir / f"{match_id}.json"
        with open(out_file, 'w', encoding='utf-8') as of:
            json.dump(payload, of, ensure_ascii=False, indent=2)

        # build the link to the match page (client templates expect these paths)
        if kind == 'atp':
            link = f"/atp_matches/match_atp.html?t={tourney}&y={year}&m={match_id}"
        else:
            link = f"/wta_matches/match_wta.html?t={tourney}&y={year}&m={match_id}"

        index_matches.append({
            "match_id": match_id,
            "score": payload.get('score_string',''),
            "winner": payload['players']['winner'].get('name'),
            "loser": payload['players']['loser'].get('name'),
            "round": payload.get('round'),
            "link": link
        })

        print(f"[OK] {kind.upper()} wrote {out_file}")

    # write index.json for this tournament
    index_payload = {
        "tourney": tourney,
        "year": year,
        "kind": kind,
        "matches": index_matches
    }
    index_file = 'docs' / 'index.json'
    with open(index_file, 'w', encoding='utf-8') as ii:
        json.dump(index_payload, ii, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote index {index_file} (contains {len(index_matches)} matches)")

def parse_only_arg(arg_values):
    if not arg_values:
        return None
    s = set()
    for v in arg_values:
        parts = v.split(',')
        for p in parts:
            p = p.strip()
            if not p:
                continue
            pp = re.sub(r'[\s\-]+', '_', p)
            if re.match(r'^\d+_\d{4}$', pp):
                s.add(pp)
            else:
                s.add(pp)
    return s

def main():
    parser = argparse.ArgumentParser(description="Génère JSONs de matches ATP/WTA depuis CSVs.")
    parser.add_argument('--src', default='matches', help='Dossier source contenant atp_matches et wta_matches (default: matches)')
    parser.add_argument('--out', default='docs/matches', help='Dossier base de sortie (default: docs/matches)')
    parser.add_argument('--only', '-o', action='append', help='Liste (ou plusieurs options) de couples id_year à générer, ex: --only 73_1997')
    args = parser.parse_args()

    src = Path(args.src)
    out_base = Path(args.out)

    allowed_pairs = parse_only_arg(args.only)
    if allowed_pairs:
        print(f"[INFO] Génération restreinte aux couples: {sorted(allowed_pairs)}")

    atp_out_root = out_base / 'atp_matches_json'
    wta_out_root = out_base / 'wta_matches_json'
    atp_out_root.mkdir(parents=True, exist_ok=True)
    wta_out_root.mkdir(parents=True, exist_ok=True)

    atp_dir = src / 'atp_matches'
    if atp_dir.exists():
        atp_files = list(atp_dir.glob('*.csv')) + list(atp_dir.glob('*.CSV'))
        for f in atp_files:
            process_csv_file(f, atp_out_root, kind='atp', allowed_pairs=allowed_pairs)
    else:
        print("[WARN] dossier", atp_dir, "introuvable")

    wta_dir = src / 'wta_matches'
    if wta_dir.exists():
        wta_files = list(wta_dir.glob('*.csv')) + list(wta_dir.glob('*.CSV'))
        for f in wta_files:
            process_csv_file(f, wta_out_root, kind='wta', allowed_pairs=allowed_pairs)
    else:
        print("[WARN] dossier", wta_dir, "introuvable")

if __name__ == '__main__':
    main()
