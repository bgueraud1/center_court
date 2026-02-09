#!/usr/bin/env python3
"""
scripts/generate_daily_guess.py (fix birth-year extraction + stricter selection)

Usage (exemple):
 python scripts/generate_daily_guess.py \
   --atp player_data_atp.csv \
   --wta player_data_wta.csv \
   --geocodes-dir maps_html \
   --out-daily docs/tools/daily_guess.json \
   --out-players docs/tools/players_catalog.json \
   --out-geocodes docs/tools/geocodes_combined.json \
   [--seed 2026-02-09]
"""
import csv, json, argparse, os, re, random, sys
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

NAME_CANDIDATES = [
    'full_name','full name','Full Name','Full_Name','Full_name','fullname','fullName',
    'name','player_name','player name','display_name','display name'
]

# Fields that must be present (non-empty / non-None) for a player to be eligible for selection.
REQUIRED_FIELDS_FULL = (
    'full_name',
    'player_id',
    'represented_country',
    'rank',
    'birth_date',
    'birth_year',
    'birthplace',
    'height_cm',
    'plays',
    'backhand'
)
# A minimal required set (more permissive) that fits datasets where some optional
# fields are frequently missing (for instance many WTA rows have missing backhand
# or height_cm). By default we use the minimal set so both ATP and WTA produce
# selections; pass --require-all to enforce the full set.
REQUIRED_FIELDS_MINIMAL = (
    'full_name',
    'player_id',
    'represented_country',
    'rank',
    'birth_date',
    'birth_year'
)

# runtime flag (set from CLI)
REQUIRE_ALL = False


def norm_key(k):
    return k.strip() if isinstance(k, str) else k

def parse_rank(val):
    if val is None: return None
    s = str(val).strip()
    if s == '' or s == '-' or s.lower() == 'nan': return None
    m = re.search(r'(\d+)', s)
    return int(m.group(1)) if m else None

def parse_height_from_row(row):
    for key in ('height_cm','height_cm_raw','height','height_cm '):
        if key in row and row[key]:
            s = str(row[key]).strip()
            m = re.search(r'([\d.,]+)', s)
            if m:
                try:
                    val = float(m.group(1).replace(',','.'))
                    if val < 5: return int(round(val*100))
                    if 50 < val < 300: return int(round(val))
                except: pass
    for key in ('height_inches','height_inches '):
        if key in row and row[key]:
            s = str(row[key])
            m = re.search(r"(\d+)\s*'\s*(\d+)", s)
            if m:
                feet = int(m.group(1)); inches = int(m.group(2))
                total = feet*12 + inches
                return int(round(total * 2.54))
    return None

def load_csv(path, source):
    out = []
    if not os.path.isfile(path):
        print(f"[WARN] CSV introuvable: {path}", file=sys.stderr)
        return out
    with open(path, newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh)
        reader.fieldnames = [norm_key(fn) for fn in (reader.fieldnames or [])]
        for idx, row in enumerate(reader):
            row2 = {norm_key(k): (v.strip() if isinstance(v, str) else v) for k,v in row.items()}
            row2['_source'] = source
            row2['_csv_row_index'] = idx + 1
            out.append(row2)
    return out


def find_name_in_row(row):
    for k in NAME_CANDIDATES:
        if k in row and row.get(k):
            v = row.get(k)
            if isinstance(v, str) and v.strip(): return v.strip()
    # heuristic fallback
    for k,v in row.items():
        if not v or k.startswith('_'): continue
        if isinstance(v, str):
            s = v.strip()
            if len(s) > 3 and re.search(r'[A-Za-zÀ-ÖØ-öø-ÿ]', s) and ' ' in s:
                return s
    return None


def normalize_bool_play(s):
    if not s: return None
    s2 = str(s).lower()
    if 'right' in s2: return True
    if 'left' in s2: return False
    return None


def normalize_twohand(s):
    if not s: return None
    s2 = str(s).lower()
    if 'two' in s2: return True
    if 'one' in s2: return False
    return None


def extract_year_from_string(s):
    """Try multiple strategies to extract a plausible 4-digit year from string.
       Returns int year or None. Discards years > current year."""
    if not s or not isinstance(s, str): return None
    s0 = s.strip()
    # try common formats via datetime
    formats = ['%Y-%m-%d','%Y-%m-%dT%H:%M:%S','%Y','%b %d %Y','%B %d %Y','%d %b %Y','%d %B %Y']
    for fmt in formats:
        try:
            d = datetime.strptime(s0, fmt)
            y = d.year
            if 1800 <= y <= datetime.now().year: return y
        except:
            pass
    # try fromisoformat
    try:
        d = datetime.fromisoformat(s0)
        y = d.year
        if 1800 <= y <= datetime.now().year: return y
    except:
        pass
    # regex fallback: find first 4-digit year 1800-2099
    m = re.search(r'\b(18|19|20)\d{2}\b', s0)
    if m:
        try:
            y = int(m.group(0))
            if 1800 <= y <= datetime.now().year: return y
        except:
            pass
    return None


def build_player_record(row):
    full_name = find_name_in_row(row) or ''
    player_id = row.get('player_id') or row.get('id') or row.get('playerid') or row.get('player id') or ''
    country = (row.get('represented_country') or row.get('represented') or row.get('country') or '').strip()
    rank = parse_rank(row.get('highest_ranking') or row.get('best_rank') or row.get('bestRank') or row.get('ranking') or row.get('best_rank'))
    birth_date = row.get('birth_date') or row.get('birthdate') or row.get('birth_date ')
    birthplace = row.get('birthplace') or row.get('birth_place') or row.get('birth place') or row.get('birthplace ')
    height_cm = parse_height_from_row(row)
    plays = row.get('plays') or row.get('play')
    backhand = row.get('backhand') or ''
    birth_year = extract_year_from_string(birth_date) if birth_date else None
    age = None
    if birth_year:
        try:
            age = datetime.now().year - birth_year
        except:
            age = None
    return {
        'full_name': full_name,
        'player_id': player_id,
        'represented_country': country,
        'rank': rank,
        'birth_date': birth_date,
        'birth_year': birth_year,
        'birthplace': birthplace,
        'height_cm': height_cm,
        'plays': plays,
        'backhand': backhand,
        'right_handed': normalize_bool_play(plays),
        'two_handed': normalize_twohand(backhand),
        'age': age,
        'source': row.get('_source',''),
        '_raw_row': row
    }


def load_geocodes(dirpath):
    combined = {}
    if not os.path.isdir(dirpath): return combined
    for fname in os.listdir(dirpath):
        if not fname.lower().startswith('geocode'): continue
        path = os.path.join(dirpath, fname)
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                if 'geocode' in data and isinstance(data['geocode'], dict):
                    for k,v in data['geocode'].items(): combined[k] = v
                else:
                    for k,v in data.items(): combined[k] = v
        except Exception as e:
            print(f"[WARN] lecture geocode {path} failed: {e}", file=sys.stderr)
    return combined


def is_player_complete(p):
    """Return True if player p has all required fields non-empty/non-None.
       Uses the minimal or full REQUIRED_FIELDS depending on the runtime flag
       REQUIRE_ALL (set by CLI option --require-all).
       Note: rank and birth_year and height_cm (when required) must be not None (numeric)."""
    if not isinstance(p, dict):
        return False
    fields = REQUIRED_FIELDS_FULL if REQUIRE_ALL else REQUIRED_FIELDS_MINIMAL
    for k in fields:
        if k not in p:
            return False
        v = p.get(k)
        # For numeric fields, require not None
        if k in ('rank','birth_year','height_cm'):
            if v is None:
                return False
        else:
            # require non-empty string (strip)
            if v is None: return False
            if isinstance(v, str) and v.strip() == '':
                return False
    return True


def filter_valid_players(players, require_birth_after_year=1980):
    out = []
    for p in players:
        if not p.get('full_name'): continue
        # require a non-empty birth_date and a successfully extracted birth_year
        by = p.get('birth_year')
        bd = p.get('birth_date')
        if bd is None or (isinstance(bd, str) and bd.strip() == ''):
            # exclude players without birth_date
            continue
        if by is None:
            # exclude players where we couldn't extract birth_year
            continue
        try:
            if by < require_birth_after_year:
                continue
        except:
            continue
        # require all other fields to be present and non-empty
        if not is_player_complete(p):
            continue
        out.append(p)
    return out


def choose_for_level_with_country(players, topN, rng, exclude_ids=None):
    exclude_ids = set(str(x) for x in (exclude_ids or []))
    pool = [p for p in players if p.get('rank') is not None and p.get('rank') <= topN and p.get('represented_country') and str(p.get('player_id')) not in exclude_ids and is_player_complete(p)]
    if not pool:
        pool = [p for p in players if p.get('represented_country') and str(p.get('player_id')) not in exclude_ids and is_player_complete(p)]
    if not pool:
        pool = [p for p in players if str(p.get('player_id')) not in exclude_ids and is_player_complete(p)]
    if not pool:
        return None, None, 0
    countries = sorted(list({p['represented_country'] for p in pool if p.get('represented_country')}))
    country = rng.choice(countries) if countries else None
    country_players = [p for p in pool if p.get('represented_country') == country] if country else pool
    chosen = rng.choice(country_players) if country_players else rng.choice(pool)
    pool_size = len([p for p in players if p.get('rank') is not None and p.get('rank') <= topN and p.get('represented_country') and is_player_complete(p)])
    return chosen, country, pool_size


def choose_random_by_country(players, rng, exclude_ids=None):
    exclude_ids = set(str(x) for x in (exclude_ids or []))
    pool = [p for p in players if p.get('represented_country') and str(p.get('player_id')) not in exclude_ids and is_player_complete(p)]
    if not pool: return None, None
    countries = sorted(list({p['represented_country'] for p in pool}))
    country = rng.choice(countries)
    candidates = [p for p in pool if p.get('represented_country') == country]
    if not candidates: return None, country
    chosen = rng.choice(candidates)
    return chosen, country


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--atp', required=True)
    ap.add_argument('--wta', required=True)
    ap.add_argument('--geocodes-dir', default='maps_html')
    ap.add_argument('--out-daily', required=True)
    ap.add_argument('--out-players', required=True)
    ap.add_argument('--out-geocodes', required=True)
    ap.add_argument('--seed', required=False)
    ap.add_argument('--require-all', action='store_true', help='Require all fields (full set) to be present for a player to be eligible')
    args = ap.parse_args()

    # set runtime option for completeness checks
    global REQUIRE_ALL
    REQUIRE_ALL = bool(args.require_all)

    try:
        if ZoneInfo:
            tz = ZoneInfo('Europe/Paris')
            now = datetime.now(tz)
        else:
            now = datetime.now(timezone.utc)
    except:
        now = datetime.now(timezone.utc)

    seed_value = args.seed
    if seed_value is None:
        rng = random.Random()
        randomness_mode = 'random'
    else:
        rng = random.Random(seed_value)
        randomness_mode = f'deterministic(seed={seed_value})'

    atp_rows = load_csv(args.atp, 'ATP')
    wta_rows = load_csv(args.wta, 'WTA')

    atp_players = [build_player_record(r) for r in atp_rows]
    wta_players = [build_player_record(r) for r in wta_rows]
    all_players = atp_players + wta_players

    missing_name = [p for p in all_players if not p.get('full_name')]
    if missing_name:
        print(f"[WARN] {len(missing_name)} lignes sans full_name (exclues des sélections).", file=sys.stderr)
        for p in missing_name[:30]:
            ridx = p['_raw_row'].get('_csv_row_index') if isinstance(p.get('_raw_row'), dict) else '?'
            print(f"  - idx={ridx} player_id={p.get('player_id')} country={p.get('represented_country')} birth={p.get('birth_date')}", file=sys.stderr)

    # apply birth_year filter (exclude < require_birth_after_year) and completeness check
    atp_valid = filter_valid_players(atp_players, require_birth_after_year=1980)
    wta_valid = filter_valid_players(wta_players, require_birth_after_year=1980)

    os.makedirs(os.path.dirname(args.out_players), exist_ok=True)
    with open(args.out_players, 'w', encoding='utf-8') as fh:
        json.dump({'generated_at': now.isoformat(), 'count': len(all_players), 'players': all_players}, fh, ensure_ascii=False, indent=2)

    geos = load_geocodes(args.geocodes_dir)
    os.makedirs(os.path.dirname(args.out_geocodes), exist_ok=True)
    with open(args.out_geocodes, 'w', encoding='utf-8') as fh:
        json.dump({'geocode': geos}, fh, ensure_ascii=False, indent=2)

    out = {
        'date': now.strftime('%Y-%m-%d'),
        'generated_at_iso': now.isoformat(),
        'randomness': randomness_mode,
        'seed': seed_value,
        'ATP': {},
        'WTA': {}
    }

    def pick_levels_for_gender(valid_players, fixed_by_country=None):
        """Pick top20/top100/top300 and a 'by_country' entry for the provided valid_players.
           If fixed_by_country is provided, the 'by_country' pick will be taken from that country
           (if possible); otherwise a random country present in valid_players will be chosen.
        """
        chosen_ids = set()
        res = {}
        for topN, key in ((20,'top20'), (100,'top100'), (300,'top300')):
            chosen, country, pool_size = choose_for_level_with_country(valid_players, topN, rng, exclude_ids=chosen_ids)
            if chosen is None:
                res[key] = {'chosen': None, 'country': None, 'pool_size': 0}
            else:
                # safety check: if chosen has birth_year and it's <1980, log error (shouldn't happen)
                by = chosen.get('birth_year')
                if by is not None and by < 1980:
                    print(f"[ERROR] choix invalide (birth_year<{1980}) pour top{topN}: {chosen.get('player_id')} / {chosen.get('full_name')} / {by}", file=sys.stderr)
                    # skip and try again up to a few times
                    attempts = 0
                    ok = False
                    while attempts < 5 and not ok:
                        chosen, country, pool_size = choose_for_level_with_country(valid_players, topN, rng, exclude_ids=chosen_ids)
                        attempts += 1
                        if not chosen: break
                        by = chosen.get('birth_year')
                        if by is None or by >= 1980:
                            ok = True
                    if not ok:
                        # fallback to None
                        res[key] = {'chosen': None, 'country': None, 'pool_size': pool_size}
                        continue
                res[key] = {
                    'chosen': {
                        'player_id': chosen.get('player_id'),
                        'full_name': chosen.get('full_name'),
                        'source': chosen.get('source'),
                        'represented_country': chosen.get('represented_country'),
                        'rank': chosen.get('rank'),
                        'height_cm': chosen.get('height_cm'),
                        'birth_date': chosen.get('birth_date'),
                        'birthplace': chosen.get('birthplace'),
                        'birth_year': chosen.get('birth_year')
                    },
                    'country': country,
                    'pool_size': pool_size
                }
                if chosen.get('player_id'): chosen_ids.add(str(chosen.get('player_id')))
        # by_country selection: prefer fixed_by_country if provided
        if fixed_by_country:
            # attempt to pick from fixed_by_country; if no candidates there, fall back to normal random selection
            candidates = [p for p in valid_players if p.get('represented_country') == fixed_by_country and is_player_complete(p) and str(p.get('player_id')) not in chosen_ids]
            if candidates:
                chosen = rng.choice(candidates)
                chosen_country = fixed_by_country
                pool_size = len([p for p in valid_players if p.get('represented_country') == fixed_by_country and is_player_complete(p)])
            else:
                chosen, chosen_country = choose_random_by_country(valid_players, rng, exclude_ids=chosen_ids)
                pool_size = len([p for p in valid_players if p.get('represented_country') == chosen_country and is_player_complete(p)]) if chosen_country else 0
        else:
            chosen, chosen_country = choose_random_by_country(valid_players, rng, exclude_ids=chosen_ids)
            pool_size = len([p for p in valid_players if p.get('represented_country') == chosen_country and is_player_complete(p)]) if chosen_country else 0

        if chosen:
            res['by_country'] = {
                'chosen': {
                    'player_id': chosen.get('player_id'),
                    'full_name': chosen.get('full_name'),
                    'source': chosen.get('source'),
                    'represented_country': chosen.get('represented_country'),
                    'rank': chosen.get('rank'),
                    'height_cm': chosen.get('height_cm'),
                    'birth_date': chosen.get('birth_date'),
                    'birthplace': chosen.get('birthplace'),
                    'birth_year': chosen.get('birth_year')
                },
                'country': chosen_country,
                'pool_size': pool_size
            }
        else:
            res['by_country'] = {'chosen': None, 'country': None, 'pool_size': 0}
        return res

    # choose a random country that has at least one eligible ATP and one eligible WTA player
    atp_countries = {p.get('represented_country') for p in atp_valid if p.get('represented_country')}
    wta_countries = {p.get('represented_country') for p in wta_valid if p.get('represented_country')}
    common_countries = sorted(list(atp_countries.intersection(wta_countries)))
    fixed_country = None
    if common_countries:
        fixed_country = rng.choice(common_countries)
        print(f"[INFO] country chosen for both ATP and WTA by-country pick: {fixed_country}")

    out['ATP'] = pick_levels_for_gender(atp_valid, fixed_by_country=fixed_country)
    out['WTA'] = pick_levels_for_gender(wta_valid, fixed_by_country=fixed_country)

    os.makedirs(os.path.dirname(args.out_daily), exist_ok=True)
    with open(args.out_daily, 'w', encoding='utf-8') as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)

    print("Wrote:", args.out_players, args.out_geocodes, args.out_daily)
    print("Randomness mode:", randomness_mode)
    sys.exit(0)

if __name__ == '__main__':
    main()
