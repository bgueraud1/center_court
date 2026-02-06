#!/usr/bin/env python3
"""
inject_missing_matches.py

Script complet : parcourt les dossiers ./matches/atp_matches et ./matches/wta_matches,
construit les matches, détecte les interruptions dans les identifiants du type
LSxxx / MSxxx (ou toute paire préfixe+numérique similaire) et insère des matchs
synthétiques (BYE) dans le round précédent quand cela est possible.

Usage: placer les CSVs dans ./matches/atp_matches/*.csv et ./matches/wta_matches/*.csv
puis lancer : python inject_missing_matches.py

Le comportement suit exactement ta demande : si un identifiant attendu est manquant
(par ex. MS016 absent alors que MS015 et MS017 existent conformément à la numérotation),
on regarde le match "parent" dans le round suivant (floor(parent_num/2) relation)
et on crée un faux match BYE dans le round précédent si on détecte qu'un des deux
joueurs du parent n'a pas de match au round précédent.

Les placeholders pour l'adversaire manquant sont : player_id_loser='XXXX', loser_country='XXX'.

"""

from pathlib import Path
from collections import defaultdict
import csv
import json
import re
import random
from datetime import datetime

# --- Configuration ---
MATCHES_DIR = Path('./matches')
OUTPUT_DIR = Path('./out')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Helpers ---

def normalize_name(n):
    return (n or '').strip().lower()


def read_csv_rows(path):
    """Lit un CSV et renvoie une liste de dicts (empty list si erreur)."""
    try:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]
    except Exception as e:
        print(f"Erreur lecture CSV {path}: {e}")
        return []


def parse_match_id(mid):
    """Retourne (prefix, num, width) pour un identifiant comme 'MS016' ou 'LS31'.
    width est la largeur des chiffres trouvée (pour zero-padding preservation).
    Si mid invalide, retourne (None, None, 3).
    """
    if not mid:
        return None, None, 3
    s = str(mid).strip()
    m = re.match(r'^([^0-9]*?)(0*)(\d+)$', s)
    if not m:
        return None, None, 3
    prefix = m.group(1) or ''
    leading = m.group(2) or ''
    digits = m.group(3)
    width = max(len(leading + digits), 3)
    try:
        num = int(digits)
    except Exception:
        num = None
    return prefix, num, width


def format_match_id(prefix, num, width=3):
    if prefix is None:
        prefix = ''
    return f"{prefix}{str(num).zfill(width)}"


def player_present_in_matches(pid, pname, matches):
    """Vérifie présence par id (si fourni) ou par nom normalisé."""
    if not matches:
        return False
    pn = normalize_name(pname) if pname else ''
    for m in matches:
        # check ids
        for k in ('player_id_winner','player_id_loser','player_winner_id','player_loser_id','winner_id','loser_id'):
            if pid and m.get(k) and str(m.get(k)) == str(pid):
                return True
        # check names
        for k in ('winner_player_name','loser_player_name','winner_name','loser_name','player_winner','player_loser'):
            if pn and m.get(k) and normalize_name(m.get(k)) == pn:
                return True
    return False


def find_match_by_id_num(num, matches):
    for m in matches:
        _, n, _ = parse_match_id(m.get('match_id') or m.get('match') or '')
        if n is not None and n == num:
            return m
    return None


# --- Core: injection des synthétiques (BYE) ---

def inject_missing_ms_ls(groups):
    """
    Pour chaque colonne (prev = round avec plus de matchs, curr = round suivant),
    on vérifie pour chaque match 'parent' du round curr que les enfants (2*p, 2*p+1)
    existent dans prev. Si l'un des enfants est absent et qu'exactement l'un des deux
    joueurs du parent n'a pas de match dans prev, on crée un match synthétique BYE
    dans prev pour ce joueur.
    """
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    for col_index in range(1, len(rounds)):
        prev_key = rounds[col_index - 1]
        curr_key = rounds[col_index]
        prev_matches = groups.get(prev_key, [])
        curr_matches = groups.get(curr_key, [])

        # compute digit width from existing ids
        digit_width = 3
        for m in (prev_matches + curr_matches):
            _, _, w = parse_match_id(m.get('match_id') or m.get('match') or '')
            if w and w > digit_width:
                digit_width = w

        # set of existing numeric ids in prev
        prev_nums = set()
        for pm in prev_matches:
            _, num, _ = parse_match_id(pm.get('match_id') or pm.get('match') or '')
            if num is not None:
                prev_nums.add(num)

        synthetics = []

        for parent in curr_matches:
            pmid = parent.get('match_id') or parent.get('match') or ''
            prefix, pnum, _ = parse_match_id(pmid)
            if pnum is None:
                continue
            children = [2 * pnum, 2 * pnum + 1]

            for child_num in children:
                if child_num in prev_nums:
                    continue
                if find_match_by_id_num(child_num, prev_matches):
                    prev_nums.add(child_num)
                    continue

                # Determine parent players
                winner_name = parent.get('winner_player_name') or parent.get('player_winner') or parent.get('winner') or ''
                loser_name = parent.get('loser_player_name') or parent.get('player_loser') or parent.get('loser') or ''
                winner_id = parent.get('player_id_winner') or parent.get('player_winner_id') or parent.get('winner_id') or ''
                loser_id = parent.get('player_id_loser') or parent.get('player_loser_id') or parent.get('loser_id') or ''

                winner_has_prev = player_present_in_matches(winner_id, winner_name, prev_matches)
                loser_has_prev = player_present_in_matches(loser_id, loser_name, prev_matches)

                missing_player = None
                if not winner_has_prev and loser_has_prev:
                    missing_player = {
                        'id': winner_id or '',
                        'name': winner_name or '',
                        'country': parent.get('winner_country') or parent.get('winner_nationality') or '',
                        'seed': parent.get('winner_seed') or ''
                    }
                elif not loser_has_prev and winner_has_prev:
                    missing_player = {
                        'id': loser_id or '',
                        'name': loser_name or '',
                        'country': parent.get('loser_country') or parent.get('loser_nationality') or '',
                        'seed': parent.get('loser_seed') or ''
                    }
                else:
                    # si les deux sont présents ou les deux absents, on skip (trop ambigu)
                    continue

                synth_mid = format_match_id(prefix or 'MS', child_num, digit_width)
                synth = {
                    'match_id': synth_mid,
                    'round': prev_key,
                    'winner_player_name': missing_player['name'] or '',
                    'loser_player_name': 'BYE',
                    'score_string': '',
                    'player_id_winner': missing_player['id'] or '',
                    'player_id_loser': 'XXXX',
                    'winner_country': missing_player.get('country') or '',
                    'loser_country': 'XXX',
                    'winner_seed': missing_player.get('seed') or '',
                    'loser_seed': ''
                }

                synthetics.append((child_num, synth))
                prev_nums.add(child_num)

        # Insérer synthétiques dans groups[prev_key]
        if synthetics:
            # pour garder un ordre raisonnable, on insère triés par child_num
            synthetics.sort(key=lambda t: t[0])
            for _, s in synthetics:
                groups[prev_key].append(s)


# --- Flatten helper ---

def flatten_groups_to_matches(groups):
    rounds = sorted(groups.keys(), key=lambda k: len(groups[k]), reverse=True)
    out = []
    for r in rounds:
        out.extend(groups[r])
    return out


# --- Main processing loop (lit CSVs et écrit JSONs) ---

def main():
    index = defaultdict(list)
    calendar_list = []

    for kind in ('atp_matches', 'wta_matches'):
        d = MATCHES_DIR / kind
        if not d.exists():
            print(f"Folder not found: {d} -> skipping")
            continue

        for csvfile in sorted(d.glob('*.csv')):
            rows = read_csv_rows(csvfile)
            if not rows:
                continue

            name_parts = csvfile.stem.split('_')
            if len(name_parts) >= 3:
                _, tourney_id_str, year_str = name_parts[:3]
            else:
                tourney_id_str = rows[0].get('tourney_id') or rows[0].get('event_id') or 'unknown'
                year_str = rows[0].get('tourney_year') or rows[0].get('event_year') or 'unknown'

            tourney_id = tourney_id_str
            year = year_str

            first = rows[0]
            raw_start = first.get('start_date') or first.get('tourney_start_date') or ''
            start_date = ''
            if raw_start:
                try:
                    dt = datetime.fromisoformat(raw_start)
                    start_date = dt.date().isoformat()
                except Exception:
                    try:
                        start_date = datetime.strptime(raw_start[:10], '%Y-%m-%d').date().isoformat()
                    except Exception:
                        start_date = raw_start

            meta = {
                'source': 'ATP' if kind.startswith('atp') else 'WTA',
                'tourney_id': tourney_id,
                'year': int(year) if str(year).isdigit() else year,
                'tourney_name': first.get('tourney_name') or first.get('tournament_name') or '',
                'tourney_title': first.get('tournament_title') or first.get('tourney_title') or '',
                'surface': (first.get('surface') or '').title(),
                'level': first.get('level') or '',
                'prize_money': first.get('prize_money') or '',
                'prize_money_currency': first.get('prize_money_currency') or '',
                'singles_draw_size': int(first.get('singles_draw_size')) if first.get('singles_draw_size') and str(first.get('singles_draw_size')).isdigit() else None,
                'city': first.get('city') or '',
                'country': first.get('country') or '',
                'start_date': start_date
            }

            # build matches array with normalized field names
            matches = []
            for r in rows:
                m = {
                    'match_id': r.get('match_id') or r.get('match') or r.get('event_id') or '',
                    'round': (r.get('round') or r.get('round_name') or '').strip(),
                    'winner_player_name': r.get('winner_player_name') or r.get('player_winner') or r.get('winner') or '',
                    'loser_player_name': r.get('loser_player_name') or r.get('player_loser') or r.get('loser') or '',
                    'score_string': r.get('score_string') or r.get('score') or r.get('score_str') or '',
                    'player_id_winner': r.get('player_id_winner') or r.get('player_winner_id') or r.get('winner_id') or r.get('PlayerIDA') or r.get('PlayerIDA2') or '',
                    'player_id_loser': r.get('player_id_loser') or r.get('player_loser_id') or r.get('loser_id') or r.get('PlayerIDB') or r.get('PlayerIDB2') or '',
                    'winner_country': r.get('winner_country') or r.get('country_winner') or r.get('winner_country_code') or r.get('country_a') or r.get('country_b') or '',
                    'loser_country': r.get('loser_country') or r.get('country_loser') or r.get('loser_country_code') or '',
                    'winner_seed': r.get('winner_seed') or r.get('seed_winner') or r.get('seed_a') or r.get('seed_winner') or '',
                    'loser_seed': r.get('loser_seed') or r.get('seed_loser') or r.get('seed_b') or r.get('seed_loser') or ''
                }
                matches.append(m)

            # group by round
            groups = defaultdict(list)
            for m in matches:
                rk = m.get('round') or ''
                groups[rk].append(m)

            # --- INJECTION : insérer BYE synthétiques si nécessaire ---
            inject_missing_ms_ls(groups)

            # flatten groups into final matches in order left->right
            final_matches = flatten_groups_to_matches(groups)

            # deterministic sort fallback: by round name then numeric match id
            def match_sort_key(m):
                r = m.get('round') or ''
                _, num, _ = parse_match_id(m.get('match_id') or m.get('match') or '')
                return (r, num if num is not None else 0)

            final_matches_sorted = sorted(final_matches, key=match_sort_key)

            out = {'meta': meta, 'matches': final_matches_sorted}
            out_name = f"{meta['source'].lower()}_{tourney_id}_{year}.json"
            out_path = OUTPUT_DIR / out_name
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(out, f, ensure_ascii=False, indent=2)

            index_key = f"{meta['source'].lower()}_{tourney_id}"
            index[index_key].append(int(year) if str(year).isdigit() else year)
            calendar_list.append({
                'source': meta['source'].lower(),
                'tourney_id': tourney_id,
                'year': int(year) if str(year).isdigit() else year,
                'tourney_name': meta['tourney_name'],
                'start_date': meta['start_date'] or '',
                'surface': meta['surface'],
                'level': meta['level']
            })

    # write index
    index_out = {k: sorted(v) for k, v in index.items()}
    with open(OUTPUT_DIR / 'tournaments_index.json', 'w', encoding='utf-8') as f:
        json.dump(index_out, f, ensure_ascii=False, indent=2)

    # write calendar
    def sort_key(item):
        d = item.get('start_date')
        try:
            if not d:
                return datetime.max
            return datetime.fromisoformat(d)
        except Exception:
            try:
                return datetime.strptime(d[:10], '%Y-%m-%d')
            except Exception:
                return datetime.max

    calendar_sorted = sorted(calendar_list, key=sort_key)
    with open(OUTPUT_DIR / 'tournaments_calendar.json', 'w', encoding='utf-8') as f:
        json.dump(calendar_sorted, f, ensure_ascii=False, indent=2)

    print('Done — JSON files written to', OUTPUT_DIR)


if __name__ == '__main__':
    main()
