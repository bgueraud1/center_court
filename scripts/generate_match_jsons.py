#!/usr/bin/env python3
"""
Générateur de JSONs pour les matches ATP et WTA.

Usage:
    python3 scripts/generate_match_jsons.py --input matches --output docs/matches_json
    python3 scripts/generate_match_jsons.py --input-list created_files.txt --output docs/matches_json
"""

import csv
import json
import argparse
import os
import re
from pathlib import Path


def make_safe_filename(s: str) -> str:
    # garde que lettres, chiffres, _, - et remplace espaces par _
    s = (s or "").strip()
    s = s.replace(' ', '_')
    s = re.sub(r"[^A-Za-z0-9_\-\.]+", '', s)
    return s


def coerce_value(v: str):
    if v is None:
        return None
    v = v.strip()
    if v == "":
        return None
    # try int
    try:
        if re.match(r"^[+-]?\d+$", v):
            return int(v)
        # float with decimal or scientific
        if re.match(r"^[+-]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][+-]?\d+)?$", v):
            return float(v)
    except Exception:
        pass
    return v


def process_csv_file(csv_path: Path, gender: str, out_dir: Path):
    print(f"Processing {gender} CSV: {csv_path}")
    with csv_path.open(newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        total = 0
        for row in reader:
            total += 1
            # detect tournament id + year + match id
            # ATP: event_id,event_year,match_id
            # WTA: tourney_id,tourney_year,match_id (or date)
            tourney_id = row.get('event_id') or row.get('tourney_id') or row.get('eventId')
            year = row.get('event_year') or row.get('tourney_year') or row.get('year')
            match_id = row.get('match_id') or row.get('matchId') or row.get('match_id')
            # fallback: if match_id is missing, try to compose one from row number
            if not match_id:
                match_id = f"row{total:04d}"

            # safe filename
            tid = make_safe_filename(tourney_id or 'unknown')
            yr = make_safe_filename(year or 'unknown')
            mid = make_safe_filename(match_id or f"m{total}")

            fname = f"{gender}_{tid}_{yr}_{mid}.json"
            out_path = out_dir / fname
            # coerce numeric-like fields
            clean = {}
            for k, v in row.items():
                clean_k = k.strip()
                clean[clean_k] = coerce_value(v)

            # also add some convenience fields if not present
            # common: winner_player_name / loser_player_name / score_string
            if 'winner_player_name' not in clean and 'winner' in clean:
                clean['winner_player_name'] = clean.get('winner')
            if 'loser_player_name' not in clean and 'loser' in clean:
                clean['loser_player_name'] = clean.get('loser')

            # write json
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open('w', encoding='utf-8') as outfh:
                json.dump(clean, outfh, ensure_ascii=False, indent=2)

        print(f"  -> {total} rows processed, output dir: {out_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', '-i', default='matches', help='dossier racine contenant atp_matches et wta_matches')
    p.add_argument('--output', '-o', default='docs/matches_json', help='dossier de sortie (statique)')
    p.add_argument('--input-list', help='File containing list of CSV paths to process (one per line). If provided, only these CSVs are processed.', default=None)
    args = p.parse_args()

    input_dir = Path(args.input)
    out_root = Path(args.output)

    csv_files_to_process = []

    if args.input_list:
        li = Path(args.input_list)
        if li.exists():
            for line in li.read_text(encoding='utf-8').splitlines():
                line = line.strip()
                if not line:
                    continue
                pth = Path(line)
                if not pth.is_absolute():
                    pth = Path.cwd() / pth
                csv_files_to_process.append(pth)
        else:
            print("input-list file not found:", args.input_list)
            csv_files_to_process = []
    else:
        # old behavior: collect all CSVs in atp_matches and wta_matches
        atp_dir = input_dir / 'atp_matches'
        wta_dir = input_dir / 'wta_matches'
        if atp_dir.exists():
            for f in sorted(atp_dir.glob('*.csv')):
                csv_files_to_process.append(f)
        if wta_dir.exists():
            for f in sorted(wta_dir.glob('*.csv')):
                csv_files_to_process.append(f)

    if not csv_files_to_process:
        print("No CSV files to process. Exiting.")
        return

    for csv_path in csv_files_to_process:
        # determine gender by parent directory name or filename heuristics:
        parent = csv_path.parent.name.lower()
        gender = 'wta' if 'wta' in parent else ('atp' if 'atp' in parent else 'wta')
        try:
            process_csv_file(csv_path, gender, out_root / gender)
        except Exception as e:
            print(f"Failed processing {csv_path}: {e}")

    # Optionnel : créer un index léger
    index = []
    for g in ('atp', 'wta'):
        gd = out_root / g
        if not gd.exists():
            continue
        for jf in sorted(gd.glob('*.json')):
            try:
                with jf.open(encoding='utf-8') as fh:
                    obj = json.load(fh)
                index.append({'gender': g, 'file': f"/{jf.relative_to(out_root.parent)}", 'filename': jf.name,
                              'meta': {
                                  'winner': obj.get('winner_player_name') or obj.get('winner'),
                                  'loser': obj.get('loser_player_name') or obj.get('loser'),
                                  'score': obj.get('score_string') or obj.get('score'),
                                  'date': obj.get('match_date') or obj.get('date') or obj.get('match_date')
                              }})
            except Exception:
                index.append({'gender': g, 'file': str(jf), 'filename': jf.name, 'meta': {}})

    try:
        (out_root / 'matches_index.json').parent.mkdir(parents=True, exist_ok=True)
        with (out_root / 'matches_index.json').open('w', encoding='utf-8') as fh:
            json.dump(index, fh, ensure_ascii=False, indent=2)
        print(f"Index généré: {out_root / 'matches_index.json'} ({len(index)} entrées)")
    except Exception as e:
        print("Impossible d'écrire l'index:", e)


if __name__ == '__main__':
    main()