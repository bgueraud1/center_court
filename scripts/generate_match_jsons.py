#!/usr/bin/env python3
"""
Générateur de JSONs pour les matches ATP et WTA.
Usage:
    python3 scripts/generate_json.py --input matches --output docs/matches_json

Fonctionnement:
- Parcourt matches/atp_matches et matches/wta_matches
- Pour chaque CSV, lit chaque ligne et écrit un JSON unique par match
  dans <output>/atp/ ou <output>/wta/.
- Nom de fichier JSON: {gender}_{tourneyid}_{year}_{matchid}.json
  (ex: atp_73_1997_MS001.json ou wta_0609_2021_LS001.json)

Le script essaie de convertir les colonnes numériques au format number quand c'est possible.
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
    args = p.parse_args()

    input_dir = Path(args.input)
    out_root = Path(args.output)

    atp_dir = input_dir / 'atp_matches'
    wta_dir = input_dir / 'wta_matches'

    if atp_dir.exists():
        for f in sorted(atp_dir.glob('*.csv')):
            process_csv_file(f, 'atp', out_root / 'atp')
    else:
        print(f"Warning: {atp_dir} not found")

    if wta_dir.exists():
        for f in sorted(wta_dir.glob('*.csv')):
            process_csv_file(f, 'wta', out_root / 'wta')
    else:
        print(f"Warning: {wta_dir} not found")

    # Optionnel : créer un index léger
    # on parcourt out_root et liste les fichiers
    index = []
    for g in ('atp', 'wta'):
        gd = out_root / g
        if not gd.exists():
            continue
        for jf in sorted(gd.glob('*.json')):
            # try to open and extract small meta
            try:
                with jf.open(encoding='utf-8') as fh:
                    obj = json.load(fh)
                index.append({'gender': g, 'file': f"/ {jf.relative_to(out_root.parent)}", 'filename': jf.name,
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
