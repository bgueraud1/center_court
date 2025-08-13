# map_birth_place_false.py
import json
import random
import math
import re
from datetime import datetime
import pandas as pd

# import your existing normalize_dates_and_heights (used to produce all_pts)
# if your file map_birth_place.py is in PYTHONPATH, this will import the function you already wrote.
try:
    from map_birth_place import normalize_dates_and_heights
except Exception:
    # fallback minimal reimplementation if import fails.
    def normalize_dates_and_heights(df):
        """
        Minimal local fallback that mimics your original function to produce all_pts.
        It expects df rows to have 'lat','lon','birth_date','full_name','player_id','birthplace','best_rank','plays','height_cm'
        """
        all_pts = []
        for _, row in df.iterrows():
            raw = row.get('birth_date')
            if pd.isna(raw) or not isinstance(raw, str) or raw.strip() == "":
                continue
            cleaned = re.sub(r'[^A-Za-z0-9 ]', '', raw.strip())
            try:
                dt = datetime.strptime(cleaned, "%b %d %Y")
            except ValueError:
                try:
                    dt = datetime.strptime(cleaned, "%B %d %Y")
                except Exception:
                    continue
            iso = dt.date().isoformat()

            raw_h = row.get('height_cm', '')
            try:
                height_m = float(raw_h.strip().rstrip('m')) if isinstance(raw_h, str) and raw_h.strip().endswith('m') else None
            except:
                height_m = None

            try:
                pid = int(row.get('player_id'))
            except Exception:
                pid = None

            all_pts.append({
                "lat": row['lat'],
                "lon": row['lon'],
                "birth_date": iso,
                "full_name": row.get('full_name', '') or '',
                "player_id": pid,
                "birthplace": row.get('birthplace', '') or '',
                "best_rank": row.get('best_rank', None),
                "plays": row.get('plays', '') or '',
                "height_m": height_m
            })
        return all_pts


def load_cache_coords(cache_file: str) -> dict:
    """Load geocode cache produced by your geocoding step.
    Expected format: { birthplace_string: [lat, lon] } or (lat, lon)
    """
    try:
        with open(cache_file, 'r') as f:
            c = json.load(f)
    except Exception:
        return {}
    # normalize values to tuples (lat, lon) or (None,None)
    normalized = {}
    for k, v in c.items():
        if v is None:
            normalized[k] = (None, None)
        elif isinstance(v, list) and len(v) >= 2:
            normalized[k] = (v[0], v[1])
        elif isinstance(v, (tuple, list)) and len(v) >= 2:
            normalized[k] = (v[0], v[1])
        else:
            normalized[k] = (None, None)
    return normalized


def apply_cache_coords(df: pd.DataFrame, cache_file: str) -> pd.DataFrame:
    """Fill df['lat']/df['lon'] from cache for birthplace strings that are present in cache.
    Does not perform network calls. Returns a copy.
    """
    cache = load_cache_coords(cache_file)
    df2 = df.copy()
    # ensure birthplace column exists
    if 'birthplace' not in df2.columns:
        df2['birthplace'] = ''
    df2['birthplace'] = df2['birthplace'].fillna('').astype(str)
    # fill lat/lon only if birthplace in cache and cache value not (None,None)
    lat_list = []
    lon_list = []
    for bp in df2['birthplace']:
        if bp in cache:
            lat, lon = cache[bp]
            lat_list.append(lat)
            lon_list.append(lon)
        else:
            lat_list.append(float('nan'))
            lon_list.append(float('nan'))
    df2['lat'] = lat_list
    df2['lon'] = lon_list
    return df2

def can_parse_birthdate(val):
    """Return True if val looks like a parseable birth_date (matches your normalize_dates_and_heights logic)."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    s = str(val).strip()
    if s == "":
        return False
    cleaned = re.sub(r'[^A-Za-z0-9 ]', '', s)
    try:
        datetime.strptime(cleaned, "%b %d %Y")
        return True
    except Exception:
        try:
            datetime.strptime(cleaned, "%B %d %Y")
            return True
        except Exception:
            return False


def create_false_all_pts_from_df(df: pd.DataFrame, seed: int = None, mean_mode: str = 'unweighted',
                                 require_birthdate_for_add=True):
    """
    Improved: When adding players to reach the mean percentage, only choose candidates
    that currently have NO birthplace recorded AND have a parseable birth_date (so
    they will appear on the final map).  Returns (all_pts, stats).
    """
    rnd = random.Random(seed)

    dfc = df.copy().reset_index(drop=True)

    # Ensure represented_country present & normalized
    if 'represented_country' not in dfc.columns:
        raise ValueError("DataFrame must contain 'represented_country' column (ISO3).")
    dfc['represented_country'] = dfc['represented_country'].fillna('').astype(str).str.strip().str.upper()

    # Count per-country totals and current recorded (lat & lon present)
    per_country = {}
    for iso, grp in dfc.groupby('represented_country'):
        total = len(grp)
        have = int(((grp['lat'].notna()) & (grp['lon'].notna())).sum())
        per_country[iso] = {'total': total, 'have': have}

    # Compute mean percentage
    if mean_mode == 'weighted':
        tot_players = sum(v['total'] for v in per_country.values())
        tot_have = sum(v['have'] for v in per_country.values())
        mean_pct = 0.0 if tot_players == 0 else 100.0 * tot_have / tot_players
    else:
        percs = []
        for iso, st in per_country.items():
            if st['total'] > 0:
                percs.append(100.0 * st['have'] / st['total'])
        mean_pct = float(sum(percs) / len(percs)) if percs else 0.0

    # Build list of available birthplace locations per country (unique birthplace strings with coords)
    locs_by_country = {}
    has_coords = dfc[(dfc['lat'].notna()) & (dfc['lon'].notna())]
    for iso, g in has_coords.groupby('represented_country'):
        seen = set()
        items = []
        for _, r in g.iterrows():
            bp = (str(r['birthplace']).strip(), float(r['lat']), float(r['lon']))
            if bp not in seen:
                seen.add(bp)
                items.append({'birthplace': bp[0], 'lat': bp[1], 'lon': bp[2]})
        if items:
            locs_by_country[iso] = items

    dfc2 = dfc.copy()
    per_country_after = {}
    details = {}  # store indices added/removed for debugging

    for iso, st in per_country.items():
        total = st['total']
        current_have = st['have']
        if total == 0:
            per_country_after[iso] = {'before': current_have, 'after': 0}
            details[iso] = {'removed': [], 'added': [], 'reason': 'no players'}
            continue

        desired_have = int(round(mean_pct / 100.0 * total))
        desired_have = max(0, min(total, desired_have))

        idxs = dfc2.index[dfc2['represented_country'] == iso].tolist()
        have_idxs = [i for i in idxs if pd.notna(dfc2.at[i, 'lat']) and pd.notna(dfc2.at[i, 'lon'])]
        miss_idxs_all = [i for i in idxs if not (pd.notna(dfc2.at[i, 'lat']) and pd.notna(dfc2.at[i, 'lon']))]

        removed = []
        added = []
        reason = ''

        if current_have == desired_have:
            per_country_after[iso] = {'before': current_have, 'after': current_have}
            details[iso] = {'removed': removed, 'added': added, 'reason': 'already at desired'}
            continue

        if current_have > desired_have:
            to_remove = current_have - desired_have
            to_remove = min(to_remove, len(have_idxs))
            if to_remove > 0:
                remove_choice = rnd.sample(have_idxs, to_remove)
                for i in remove_choice:
                    dfc2.at[i, 'lat'] = float('nan')
                    dfc2.at[i, 'lon'] = float('nan')
                    dfc2.at[i, 'birthplace'] = ''
                removed = remove_choice
            per_country_after[iso] = {'before': current_have, 'after': current_have - len(removed)}
            details[iso] = {'removed': removed, 'added': added, 'reason': 'removed excess'}
        else:
            need = desired_have - current_have
            # choose candidate miss indices that have parseable birth_date if requested
            if require_birthdate_for_add:
                miss_idxs = [i for i in miss_idxs_all if can_parse_birthdate(dfc2.at[i, 'birth_date'])]
            else:
                miss_idxs = miss_idxs_all[:]

            if not miss_idxs:
                # nothing we can add that would appear on the final map
                reason = 'no eligible missing players with parseable birth_date'
                per_country_after[iso] = {'before': current_have, 'after': current_have}
                details[iso] = {'removed': removed, 'added': added, 'reason': reason}
                continue

            available_locs = locs_by_country.get(iso, [])
            if not available_locs:
                reason = 'no available birthplace locations within country to copy'
                per_country_after[iso] = {'before': current_have, 'after': current_have}
                details[iso] = {'removed': removed, 'added': added, 'reason': reason}
                continue

            pick_n = min(need, len(miss_idxs))
            chosen_idxs = rnd.sample(miss_idxs, pick_n)
            for i in chosen_idxs:
                loc = rnd.choice(available_locs)
                dfc2.at[i, 'birthplace'] = loc['birthplace']
                dfc2.at[i, 'lat'] = float(loc['lat'])
                dfc2.at[i, 'lon'] = float(loc['lon'])
                added.append(i)

            per_country_after[iso] = {'before': current_have, 'after': current_have + len(added)}
            details[iso] = {'removed': removed, 'added': added, 'reason': 'added'}

    # Prepare df subset with coords for normalize_dates_and_heights
    df_with_coords = dfc2[dfc2['lat'].notna() & dfc2['lon'].notna()].copy()

    # Produce all_pts using your normalize function
    all_pts = normalize_dates_and_heights(df_with_coords)

    stats = {
        'mean_pct': mean_pct,
        'per_country_before_after': per_country_after,
        'details': details,
        'total_players_input': len(dfc),
        'total_points_output': len(all_pts)
    }

    return all_pts, stats



# Example helper to be called from main (matching your pipeline style)
def build_false_birthplace_map_from_csv(input_csv, cache_file, out_html, seed=None):
    """
    Convenience function that:
      - loads CSV,
      - applies cache coords (no network calls),
      - creates false all_pts,
      - and returns (all_pts, stats). It DOES NOT save map HTML by itself,
      because you already have build_and_save_map(all_pts, out_html) in map_birth_place.py.
    """
    df = pd.read_csv(input_csv)
    df_with_coords = apply_cache_coords(df, cache_file)
    all_pts, stats = create_false_all_pts_from_df(df_with_coords, seed=seed)
    return all_pts, stats



