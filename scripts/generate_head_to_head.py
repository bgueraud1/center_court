#!/usr/bin/env python3
"""
generate_head_to_head.py

Génère des JSON "head-to-head" à partir des CSVs de matches et des fichiers player_data_atp.csv / player_data_wta.csv.

Usage examples:
  # générer le h2h pour une paire (p1 p2)
  python generate_head_to_head.py --pair DH58 F0F1

  # générer tous les fichiers h2h possibles (tous les duos présents dans les matches)
  python generate_head_to_head.py --all

Options:
  --matches-dir    dossier racine contenant atp_matches/ et wta_matches/ (default: ./matches)
  --outdir         dossier de sortie pour JSON (default: ./docs/data/h2h)
  --player-atp     chemin vers player_data_atp.csv (default: ./player_data_atp.csv)
  --player-wta     chemin vers player_data_wta.csv (default: ./player_data_wta.csv)
  --verbose
"""
import os
import csv
import json
import argparse
from glob import glob
from collections import defaultdict
from datetime import datetime

# -------------------------
# Utilitaires
# -------------------------
def norm_key(k: str) -> str:
    return k.strip().lower().replace(' ', '_')

def normalize_id(s):
    if s is None:
        return ''
    s = str(s).strip()
    # strip leading zeros? We keep as-is because IDs may have meaningful leading zeros (e.g. '0300')
    return s.upper()

def load_player_csv(path):
    """
    Charge un CSV player_data en dict player_id -> record (dict).
    Normalise les noms de champ en minuscules _.
    """
    players = {}
    if not os.path.isfile(path):
        return players
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        rows = list(reader)
        if not rows:
            return players
        headers = [norm_key(h) for h in rows[0]]
        for r in rows[1:]:
            # pad row to headers length
            if len(r) < len(headers):
                r = r + ['']*(len(headers)-len(r))
            rec = { headers[i]: (r[i].strip() if r[i] is not None else '') for i in range(len(headers)) }
            # heuristique pour trouver l'ID
            pid = ''
            for candidate in ('player_id', 'playerid', 'player', 'player_id_winner', 'playerid_winner', 'playerid_wta'):
                if candidate in rec and rec[candidate]:
                    pid = rec[candidate]
                    break
            # some wta files put id in column named 'player_id' or 'player id' etc.
            if not pid:
                # try to find any header that looks like id
                for h in headers:
                    if 'id' in h and rec.get(h):
                        pid = rec[h]; break
            pid = normalize_id(pid)
            if pid:
                players[pid] = rec
            else:
                # fallback: try full_name to create synthetic id
                name = rec.get('full_name') or rec.get('fullname') or rec.get('full name') or ''
                if name:
                    synthetic = name.strip().upper().replace(' ', '_')
                    players[synthetic] = rec
    return players

def iter_match_csv_files(matches_dir_root):
    # look for atp_matches/ and wta_matches/
    patterns = [
        os.path.join(matches_dir_root, 'atp_matches', '*.csv'),
        os.path.join(matches_dir_root, 'wta_matches', '*.csv'),
        os.path.join(matches_dir_root, '*.csv')  # fallback: root csv files
    ]
    seen = []
    for pat in patterns:
        for f in glob(pat):
            if f not in seen:
                seen.append(f)
    return sorted(seen)

def normalize_row_dict(row_dict):
    """Lowercase keys, strip whitespace from values"""
    return { norm_key(k): (v.strip() if isinstance(v, str) else v) for k,v in row_dict.items() }

def get_field(d, *candidates):
    for c in candidates:
        if c in d and d[c] != '':
            return d[c]
    return ''

def parse_matches(matches_files, verbose=False):
    """
    Parse les fichiers matches CSV et renvoie une liste de lignes (dict normalisées).
    On garde toutes les colonnes possibles mais normalise les noms en minuscules underscore.
    """
    matches = []
    for f in matches_files:
        try:
            with open(f, newline='', encoding='utf-8') as fh:
                # Détecte l'en-tête et lit via csv.DictReader
                reader = csv.DictReader(fh)
                for raw in reader:
                    row = normalize_row_dict(raw)
                    # add source file for trace
                    row['_source_file'] = os.path.basename(f)
                    matches.append(row)
        except Exception as e:
            if verbose:
                print(f"[WARN] impossible de lire {f}: {e}")
    return matches

# -------------------------
# Extraction logique
# -------------------------
def extract_basic_match_info(r):
    """
    Récupère un dict réduit contenant uniquement les champs utiles pour H2H.
    On essaye plusieurs noms de colonnes pour être tolerant.
    """
    m = {}
    # ids
    m['player_id_winner'] = normalize_id(get_field(r, 'player_id_winner','playerid_winner','playerid','player_id_wta','player_winner_id','player_winner'))
    m['player_id_loser']  = normalize_id(get_field(r, 'player_id_loser','playerid_loser','player_loser_id','player_loser'))
    # names
    m['winner_player_name'] = get_field(r, 'winner_player_name','winner','player_winner','player_a','player_a_name','player_a_player')
    m['loser_player_name']  = get_field(r, 'loser_player_name','loser','player_loser','player_b','player_b_name')
    # event info
    m['event_id'] = get_field(r, 'event_id','tourney_id','eventid','event')
    m['event_year'] = get_field(r, 'event_year','tourney_year','year')
    m['tourney_name'] = get_field(r, 'tourney_name','tournament_name','tournament','tourney')
    m['level'] = get_field(r, 'level')
    m['start_date'] = get_field(r, 'start_date')
    m['end_date'] = get_field(r, 'end_date')
    m['surface'] = get_field(r, 'surface')
    m['match_id'] = get_field(r, 'match_id','matchid','msid','match_id_wta','match_id_atp')
    m['round'] = get_field(r, 'round','match_round')
    m['score_string'] = get_field(r, 'score_string','score','set1_score')
    # if score not available but sets present, assemble
    if not m['score_string']:
        sets = []
        for s in ('set1_score','set2_score','set3_score','set4_score','set5_score'):
            v = get_field(r, s)
            if v:
                sets.append(v)
        if sets:
            m['score_string'] = ' '.join(sets)
    m['match_time_total'] = get_field(r, 'match_time_total','match_time','duration')
    m['match_date'] = get_field(r, 'match_date','date')
    # countries
    m['winner_country'] = get_field(r, 'winner_country','country_winner','country_a','represented_country','country')
    m['loser_country']  = get_field(r, 'loser_country','country_loser','country_b','represented_country_b')
    # raw row for debug if needed
    m['_raw'] = r
    return m

def pair_key(a,b):
    # standard key independent of order, used for batching
    k = tuple(sorted([a.upper(), b.upper()]))
    return f"{k[0]}__{k[1]}"

# -------------------------
# Calculs & génération JSON
# -------------------------
def build_pair_json(p1, p2, matches_for_pair, players_index, career_counts):
    """
    Construit un JSON compact pour la paire (p1,p2).
    players_index est un dict contenant 'atp' and 'wta' maps (non strict, on cherchera par id).
    career_counts: dict id -> {wins, losses}
    """
    p1u = normalize_id(p1)
    p2u = normalize_id(p2)
    # try to fetch player data from indices (both atp and wta)
    player1 = players_index.get('atp', {}).get(p1u) or players_index.get('wta', {}).get(p1u) or players_index.get('all', {}).get(p1u) or {}
    player2 = players_index.get('atp', {}).get(p2u) or players_index.get('wta', {}).get(p2u) or players_index.get('all', {}).get(p2u) or {}

    # minimal player object
    def make_player_obj(pid, rec):
        obj = {}
        obj['id'] = pid
        # prefer common fields
        obj['full_name'] = rec.get('full_name') or rec.get('fullname') or rec.get('full name') or rec.get('player_name') or ''
        obj['country'] = rec.get('represented_country') or rec.get('represented') or rec.get('represented_country') or rec.get('represented_country_name') or rec.get('represented_country_code') or rec.get('represented_country_code') or rec.get('represented_country') or rec.get('represented_country') or rec.get('represented_country') or rec.get('country') or ''
        # physicals
        obj['height'] = rec.get('height_inches') or rec.get('height') or rec.get('height_cm') or ''
        obj['weight'] = rec.get('weight') or rec.get('weight_lbs') or ''
        obj['plays'] = rec.get('plays') or ''
        obj['backhand'] = rec.get('backhand') or ''
        obj['birth_date'] = rec.get('birth_date') or rec.get('birthdate') or rec.get('birth_date_local') or ''
        obj['first_appearance'] = rec.get('first_appearance') or rec.get('first_appearance') or ''
        obj['last_appearance'] = rec.get('last_appearance') or rec.get('last_appearance') or ''
        # ranking/prize
        best_rank = rec.get('highest_ranking') or rec.get('best_rank') or rec.get('best rank') or rec.get('best_rank')
        obj['best_rank'] = try_int(best_rank)
        obj['prize_money'] = rec.get('prize_money') or rec.get('career_prize') or rec.get('prize_money_usd') or ''
        obj['turned_pro'] = rec.get('turned_pro') or rec.get('turnedpro') or ''
        obj['raw'] = rec  # keep raw for future needs
        return obj

    p1obj = make_player_obj(p1u, player1)
    p2obj = make_player_obj(p2u, player2)

    # compute h2h counts
    p1wins = 0
    p2wins = 0
    matches_out = []
    for m in matches_for_pair:
        # m already reduced by extract_basic_match_info
        wid = normalize_id(m.get('player_id_winner') or '')
        lid = normalize_id(m.get('player_id_loser') or '')
        # if ids missing, try to detect by names
        winner_name = m.get('winner_player_name') or ''
        loser_name = m.get('loser_player_name') or ''
        # decide who is winner relative to p1/p2
        winner_is_p1 = False
        winner_is_p2 = False
        if wid:
            if wid == p1u: winner_is_p1 = True
            elif wid == p2u: winner_is_p2 = True
        else:
            # fallback by name contains
            if winner_name and p1obj.get('full_name') and p1obj['full_name'].lower() in winner_name.lower():
                winner_is_p1 = True
            if winner_name and p2obj.get('full_name') and p2obj['full_name'].lower() in winner_name.lower():
                winner_is_p2 = True

        if winner_is_p1 and not winner_is_p2:
            p1wins += 1
        elif winner_is_p2 and not winner_is_p1:
            p2wins += 1
        else:
            # ambiguous — try to deduce from loser
            if lid:
                if lid == p1u:
                    p2wins += 1
                elif lid == p2u:
                    p1wins += 1

        # prepare lightweight match object
        mo = {
            'event_id': m.get('event_id') or '',
            'event_year': m.get('event_year') or '',
            'tourney_name': m.get('tourney_name') or '',
            'level': m.get('level') or '',
            'start_date': m.get('start_date') or '',
            'surface': m.get('surface') or '',
            'match_id': m.get('match_id') or '',
            'round': m.get('round') or '',
            'score_string': m.get('score_string') or '',
            'match_time_total': m.get('match_time_total') or '',
            'match_date': m.get('match_date') or '',
            'winner_player_name': winner_name or '',
            'loser_player_name': loser_name or '',
            'player_id_winner': wid or '',
            'player_id_loser': lid or '',
            'winner_country': m.get('winner_country') or '',
            'loser_country': m.get('loser_country') or '',
        }
        # heuristic link if event_id and match_id present
        if mo['event_id'] and mo['match_id']:
            # sanitize - some match_id contain letters; we leave as is
            mo['match_link'] = f"/en/scores/match-stats/archive/{mo['event_id']}/{mo['match_id']}"
        else:
            mo['match_link'] = ''
        matches_out.append(mo)

    # career counts from career_counts map (computed from all matches dataset)
    career_p1 = career_counts.get(p1u, {'wins':0,'losses':0})
    career_p2 = career_counts.get(p2u, {'wins':0,'losses':0})

    # build final payload
    payload = {
        'meta': {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'pair': [p1u, p2u],
            'total_matches': len(matches_out)
        },
        'player1': p1obj,
        'player2': p2obj,
        'aggregates': {
            'player1': {
                'h2h_wins': p1wins,
                'h2h_losses': p2wins,
                'career_wins': career_p1.get('wins', 0),
                'career_losses': career_p1.get('losses', 0),
                'prize_money': p1obj.get('prize_money', '')
            },
            'player2': {
                'h2h_wins': p2wins,
                'h2h_losses': p1wins,
                'career_wins': career_p2.get('wins', 0),
                'career_losses': career_p2.get('losses', 0),
                'prize_money': p2obj.get('prize_money', '')
            },
            'total_matches': len(matches_out)
        },
        'matches': sorted(matches_out, key=lambda x: (x.get('event_year') or '', x.get('match_date') or ''), reverse=True)
    }
    return payload

def try_int(v):
    try:
        return int(float(v))
    except Exception:
        return None

# -------------------------
# Algorithme principal
# -------------------------
def compute_career_counts(all_matches, verbose=False):
    """
    Calcule nombre de wins/losses pour chaque player_id disponible dans dataset.
    On utilise player_id_winner / player_id_loser si présentes, sinon on tente par nom.
    """
    counts = defaultdict(lambda: {'wins': 0, 'losses': 0})
    for r in all_matches:
        wid = normalize_id(get_field(r, 'player_id_winner','playerid_winner','playerid'))
        lid = normalize_id(get_field(r, 'player_id_loser','playerid_loser','playerid_loser','playerid_loser'))
        # if present, increment easily
        if wid:
            counts[wid]['wins'] += 1
        if lid:
            counts[lid]['losses'] += 1
        # Note: some csvs may have winner/loser but not ids; we skip those for career totals (can't reliably attribute)
    return counts

def find_matches_for_pair(all_matches, p1, p2):
    """
    Retourne une liste d'objets matches réduits (extract_basic_match_info) qui concernent p1 et p2.
    p1/p2 sont normalisés (uppercase).
    On teste par ids puis par noms.
    """
    p1u = normalize_id(p1)
    p2u = normalize_id(p2)
    out = []
    for r in all_matches:
        m = extract_basic_match_info(r)
        wid = normalize_id(m.get('player_id_winner') or '')
        lid = normalize_id(m.get('player_id_loser') or '')
        # if both ids present: quick check
        if wid and lid:
            if ( (wid == p1u and lid == p2u) or (wid == p2u and lid == p1u) ):
                out.append(m); continue
        # fallback with names
        wn = (m.get('winner_player_name') or '').lower()
        ln = (m.get('loser_player_name') or '').lower()
        # candidate names from player_data might be in full_name; but we don't pass it here, so check id substrings too
        if p1u and (p1u.lower() in wn or p1u.lower() in ln or p1u.lower() in json_serialize_row(r := r.get('_raw', r) if isinstance(r, dict) else r)):
            # might match; but we only add if p2 also matches in same match
            if (p2u and (p2u.lower() in wn or p2u.lower() in ln or p2u.lower() in json_serialize_row(r))):
                out.append(m)
                continue
        # last fallback: check if winner or loser names contain both players' textual fragments (we can't be perfect)
        # If either name contains both p1 or p2 substrings (not robust) -> skip
        # Use simple name substring matching: if both p1/p2 appear anywhere in the row text
        textrow = json_serialize_row(r)
        if p1u.lower() in textrow and p2u.lower() in textrow:
            out.append(m)
    return out

def json_serialize_row(r):
    try:
        return json.dumps(r).lower()
    except Exception:
        return str(r).lower()

# -------------------------
# CLI
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="Génère JSON Head-to-Head depuis CSVs matches + player data.")
    ap.add_argument('--matches-dir', default='./matches', help='dossier contenant atp_matches/ et wta_matches/')
    ap.add_argument('--outdir', default='./docs/data/h2h', help='dossier de sortie pour JSON')
    ap.add_argument('--player-atp', default='./player_data_atp.csv', help='fichier player_data_atp.csv')
    ap.add_argument('--player-wta', default='./player_data_wta.csv', help='fichier player_data_wta.csv')
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument('--pair', nargs=2, metavar=('P1','P2'), help='générer JSON pour une paire (id1 id2)')
    group.add_argument('--all', action='store_true', help='générer tous les JSONs pour toutes les paires détectées')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    # load players
    atp = load_player_csv(args.player_atp)  # dict
    wta = load_player_csv(args.player_wta)
    # combine a small 'all' index for convenience
    all_players = {}
    all_players.update(atp)
    for k,v in wta.items():
        if k not in all_players: all_players[k] = v
    players_index = {'atp': atp, 'wta': wta, 'all': all_players}

    # load matches
    match_files = iter_match_csv_files(args.matches_dir)
    if args.verbose:
        print(f"[INFO] fichiers matches détectés: {len(match_files)}")
        for mf in match_files:
            print("  -", mf)
    all_matches = parse_matches(match_files, verbose=args.verbose)

    # compute career totals across dataset
    career_counts = compute_career_counts(all_matches, verbose=args.verbose)
    if args.verbose:
        print(f"[INFO] career counts pour {len(career_counts)} players calculés.")

    if args.pair:
        p1, p2 = args.pair
        matches_for_pair = find_matches_for_pair(all_matches, p1, p2)
        if args.verbose:
            print(f"[INFO] found {len(matches_for_pair)} H2H matches between {p1} and {p2}")
        payload = build_pair_json(p1, p2, matches_for_pair, players_index, career_counts)
        # write both orientations for convenience
        out1 = os.path.join(args.outdir, f"{normalize_id(p1)}_{normalize_id(p2)}.json")
        out2 = os.path.join(args.outdir, f"{normalize_id(p2)}_{normalize_id(p1)}.json")
        with open(out1, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        # also produce swapped payload (swap player1/player2 and reverse matches order)
        swapped = payload.copy()
        swapped['player1'], swapped['player2'] = payload['player2'], payload['player1']
        # swap aggregates
        swapped['aggregates'] = {
            'player1': payload['aggregates']['player2'],
            'player2': payload['aggregates']['player1'],
            'total_matches': payload['aggregates'].get('total_matches', len(payload.get('matches',[])))
        }
        # invert matches: change winner/loser fields so that ordering still makes sense for other side
        swapped_matches = []
        for m in payload['matches']:
            nm = dict(m)
            # swap winner/loser fields
            nm['winner_player_name'], nm['loser_player_name'] = m.get('loser_player_name',''), m.get('winner_player_name','')
            nm['player_id_winner'], nm['player_id_loser'] = m.get('player_id_loser',''), m.get('player_id_winner','')
            nm['winner_country'], nm['loser_country'] = m.get('loser_country',''), m.get('winner_country','')
            swapped_matches.append(nm)
        swapped['matches'] = swapped_matches
        with open(out2, 'w', encoding='utf-8') as fh:
            json.dump(swapped, fh, ensure_ascii=False, indent=2)
        print(f"[OK] écrit: {out1} ({len(payload['matches'])} matches)")
        print(f"[OK] écrit: {out2} ({len(swapped['matches'])} matches)")
        return

    # else --all
    # Build map of pairs from matches
    pairs = defaultdict(list)
    # We'll use ids when present; otherwise attempt to derive 'names' key
    for r in all_matches:
        m = extract_basic_match_info(r)
        a = normalize_id(m.get('player_id_winner') or '')
        b = normalize_id(m.get('player_id_loser') or '')
        if a and b:
            k = pair_key(a, b)
            pairs[k].append(m)
        else:
            # fallback to name-based key (non-ideal): use normalized lower-case names
            wn = (m.get('winner_player_name') or '').strip()
            ln = (m.get('loser_player_name') or '').strip()
            if wn and ln:
                # create synthetic ids from names
                ida = wn.upper().replace(' ', '_')
                idb = ln.upper().replace(' ', '_')
                k = pair_key(ida, idb)
                pairs[k].append(m)
    if args.verbose:
        print(f"[INFO] found {len(pairs)} distinct pairs to generate")

    n = 0
    for k, matchlist in pairs.items():
        # k looks like 'A__B' where A <= B lexicographically
        a,b = k.split('__', 1)
        # But a/b might be synthetic name keys; we preserve as file names
        outname = f"{a}_{b}.json"
        outpath = os.path.join(args.outdir, outname)
        payload = build_pair_json(a, b, matchlist, players_index, career_counts)
        with open(outpath, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False)
        # also output reverse orientation
        revpath = os.path.join(args.outdir, f"{b}_{a}.json")
        swapped = payload.copy()
        swapped['player1'], swapped['player2'] = payload['player2'], payload['player1']
        swapped['aggregates'] = {
            'player1': payload['aggregates']['player2'],
            'player2': payload['aggregates']['player1'],
            'total_matches': payload['aggregates'].get('total_matches', len(payload.get('matches',[])))
        }
        swapped_matches = []
        for m in payload['matches']:
            nm = dict(m)
            nm['winner_player_name'], nm['loser_player_name'] = m.get('loser_player_name',''), m.get('winner_player_name','')
            nm['player_id_winner'], nm['player_id_loser'] = m.get('player_id_loser',''), m.get('player_id_winner','')
            nm['winner_country'], nm['loser_country'] = m.get('loser_country',''), m.get('winner_country','')
            swapped_matches.append(nm)
        swapped['matches'] = swapped_matches
        with open(revpath, 'w', encoding='utf-8') as fh:
            json.dump(swapped, fh, ensure_ascii=False)
        n += 1
        if args.verbose and n % 100 == 0:
            print(f"[INFO] générés {n} pairs...")
    print(f"[OK] générés {n} paires (JSONs) dans {args.outdir}")

if __name__ == '__main__':
    main()
