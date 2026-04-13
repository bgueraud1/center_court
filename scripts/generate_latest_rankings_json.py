#!/usr/bin/env python3
"""
Optimized generator for latest_{atp,wta}_ranking.json

Usage:
  python3 scripts/generate_latest_rankings_json.py \
    --rankings-dir atp_rankings \
    --players-csv player_data_atp.csv \
    --out docs/tools/latest_atp_ranking.json \
    --latest --circuit ATP [--compact]

This version reads ranking CSV in chunks and uses dict lookups for fast enrichment.
Produces verbose progress logs so it can't appear "stuck".
"""

from pathlib import Path
import argparse
import json
import pandas as pd
import unicodedata
import re
import sys
from datetime import datetime

CHUNKSIZE = 5000  # adjust if you want bigger chunks

ISO3_TO_ALPHA2 = {
    "ARG": "AR", "AUS": "AU", "AUT": "AT", "BEL": "BE", "BGR": "BG", "BRA": "BR", "CAN": "CA", "CHN": "CN",
    "COL": "CO", "CZE": "CZ", "CRO": "HR", "ESP": "ES", "EST": "EE", "FRA": "FR", "GBR": "GB", "GB": "GB",
    "GER": "DE", "DEU": "DE", "ITA": "IT", "JPN": "JP", "KOR": "KR", "KAZ": "KZ", "NED": "NL", "NLD": "NL", "NZL": "NZ",
    "POL": "PL", "PRT": "PT", "ROU": "RO", "RUS": "RU", "SRB": "RS", "SLO": "SI", "SWE": "SE", "SUI": "CH",
    "TPE": "TW", "UKR": "UA", "USA": "US", "URU": "UY", "MEX": "MX", "IND": "IN", "IRL": "IE", "ISR": "IL",
    "ZAF": "ZA", "DNK": "DK", "HUN": "HU", "NOR": "NO", "BLR": "BY", "VEN": "VE", "CHI": "CL", "ECU": "EC",
    "PER": "PE", "DOM": "DO", "PAN": "PA", "CYP": "CY", "GRC": "GR", "GRE": "GR", "LUX": "LU", "LTU": "LT", "LVA": "LV",
    "MYS": "MY", "PHL": "PH", "SGP": "SG", "THA": "TH", "VIE": "VN", "ALG": "DZ", "MAR": "MA", "TUN": "TN", "EGY": "EG",
    "LAT": "LV", "POR": "PT", "NIG": "NG", "KEN": "KE"
}


def iso3_to_alpha2(code: str) -> str:
    if not code:
        return ""
    c = str(code).strip().upper()
    if len(c) == 2 and c.isalpha():
        return c
    if len(c) == 3 and c.isalpha():
        return ISO3_TO_ALPHA2.get(c, "")
    cand = ''.join(ch for ch in c if ch.isalpha())[:2]
    return cand.upper() if len(cand) == 2 else ""


def emoji_from_alpha2(alpha2: str) -> str:
    if not alpha2 or len(alpha2.strip()) != 2:
        return ""
    s = alpha2.strip().upper()
    try:
        return ''.join(chr(ord(ch) + 127397) for ch in s)
    except Exception:
        return ""


_slug_re = re.compile(r'[^a-z0-9\-]+')


def slugify(name: str) -> str:
    if not name:
        return ''
    s = unicodedata.normalize('NFKD', str(name))
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'[-\s]+', '-', s).strip('-')
    s = _slug_re.sub('', s)
    return s[:200]


def normalize_name_key(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize('NFKD', str(name))
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    s = re.sub(r'\s+', ' ', s)
    return s


def parse_rank_value(value):
    try:
        if value is None:
            return None
        s = str(value).strip().replace(",", "")
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def parse_points_value(value):
    try:
        if value is None:
            return None
        s = str(value).strip().replace(",", "")
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def parse_file_date_from_name(p: Path):
    m = re.match(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$", p.name)
    if not m:
        return None
    try:
        return pd.Timestamp(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def list_rank_files_with_dates(rankings_dir: Path):
    files = []
    for p in rankings_dir.glob("data_*.csv"):
        dt = parse_file_date_from_name(p)
        if dt is not None:
            files.append((dt, p))
    files.sort(key=lambda x: x[0])
    return files


def choose_closest_file_to_date(files_with_dates, target_date, same_year=None):
    candidates = files_with_dates
    if same_year is not None:
        same = [(dt, p) for dt, p in files_with_dates if dt.year == same_year]
        if same:
            candidates = same

    if not candidates:
        return None

    target_date = pd.Timestamp(target_date).normalize()
    return min(candidates, key=lambda x: abs(x[0] - target_date))[1]


def detect_latest_file_by_filename(rankings_dir: Path):
    files = list_rank_files_with_dates(rankings_dir)
    return files[-1][1] if files else None


def detect_previous_file_by_filename(rankings_dir: Path, current_file: Path):
    files = list_rank_files_with_dates(rankings_dir)
    if not files:
        return None

    for i, (_, p) in enumerate(files):
        if p.resolve() == current_file.resolve():
            return files[i - 1][1] if i > 0 else None

    current_dt = parse_file_date_from_name(current_file)
    if current_dt is None:
        return None

    prev = None
    for dt, p in files:
        if dt < current_dt:
            prev = p
        elif dt >= current_dt:
            break
    return prev


def detect_latest_file_by_inside_date(rankings_dir: Path):
    best = None
    best_date = None
    for p in rankings_dir.glob("data_*.csv"):
        try:
            df = pd.read_csv(p, usecols=['date'], dtype=str, keep_default_na=False)
            if df.empty:
                continue
            parsed = pd.to_datetime(df['date'].replace('', pd.NaT), errors='coerce')
            maxd = parsed.max()
            if pd.isna(maxd):
                continue
            if best_date is None or maxd > best_date:
                best_date = maxd
                best = p
        except Exception:
            continue
    return best


def get_ranking_date_from_file(rankings_path: Path):
    try:
        it = pd.read_csv(rankings_path, usecols=['date'], dtype=str, keep_default_na=False, chunksize=CHUNKSIZE)
        for chunk in it:
            if 'date' not in chunk.columns:
                continue
            vals = chunk["date"].astype(str).str.strip()
            vals = vals[vals != ""]
            if len(vals) > 0:
                dt = pd.to_datetime(vals.iat[0], errors="coerce")
                if pd.notna(dt):
                    return pd.Timestamp(dt).normalize()
    except Exception:
        pass

    dt = parse_file_date_from_name(rankings_path)
    if dt is not None:
        return dt.normalize()
    return None


def normalize_rank_columns(df):
    cols = {c.strip(): c for c in df.columns}
    mapping = {}
    for c in cols:
        cl = c.lower()
        if cl in ("full_name", "fullname", "name", "player", "player_name"):
            mapping[c] = "full_name"
        if cl in ("player_id", "id"):
            mapping[c] = "player_id"
        if cl in ("ranking", "rank", "position", "#"):
            mapping[c] = "ranking"
        if cl in ("points", "official points", "pts"):
            mapping[c] = "points"
        if cl in ("movement",):
            mapping[c] = "movement"
        if cl in ("date",):
            mapping[c] = "date"

    df = df.rename(columns=mapping)

    for c in ("full_name", "player_id", "ranking", "points", "movement", "date"):
        if c not in df.columns:
            df[c] = ""

    return df[["full_name", "player_id", "ranking", "points", "movement", "date"]]


def build_players_maps(players_csv: Path):
    print("Loading players CSV into memory:", players_csv)
    try:
        players_df = pd.read_csv(players_csv, dtype=str, keep_default_na=False)
    except Exception as e:
        print("ERROR reading players CSV:", e, file=sys.stderr)
        return {}, {}, {}

    players_df.columns = [c.strip() for c in players_df.columns]

    if 'full_name' not in players_df.columns or players_df['full_name'].astype(str).str.strip().eq('').all():
        if 'first_name' in players_df.columns:
            fn = 'first_name'
            ln = 'last_name' if 'last_name' in players_df.columns else ('lastname' if 'lastname' in players_df.columns else None)
            if ln and ln in players_df.columns:
                players_df['full_name'] = (
                    players_df[fn].fillna('').astype(str).str.strip() + ' ' +
                    players_df[ln].fillna('').astype(str).str.strip()
                ).str.strip()
            else:
                players_df['full_name'] = players_df[fn].fillna('').astype(str).str.strip()
        else:
            for alt in ('player', 'name'):
                if alt in players_df.columns:
                    players_df['full_name'] = players_df[alt].fillna('').astype(str).str.strip()
                    break
            if 'full_name' not in players_df.columns:
                players_df['full_name'] = ""

    players_df['__name_key'] = players_df['full_name'].fillna('').astype(str).map(normalize_name_key)

    pid_map = {}
    name_map = {}
    name_to_pid_map = {}

    for _, r in players_df.iterrows():
        pid = str(r.get('player_id') or '').strip()
        k = str(r.get('__name_key') or '').strip()
        if not k:
            continue

        info = {
            'birth_date': r.get('birth_date', '') or '',
            'represented_country': r.get('represented_country', '') or ''
        }

        name_map[k] = info
        if pid:
            pid_map[pid] = info
            name_to_pid_map[k] = pid

    print(
        f"Built players maps: player_id keys={len(pid_map)} "
        f"name keys={len(name_map)} name->pid keys={len(name_to_pid_map)}"
    )
    return pid_map, name_map, name_to_pid_map


def compute_age_vectorized(birth_dates_series, ranking_date):
    bd = pd.to_datetime(birth_dates_series.replace('', pd.NaT), errors='coerce')
    if isinstance(ranking_date, str):
        ranking_date = pd.to_datetime(ranking_date)
    diff_days = (ranking_date - bd).dt.days
    ages = (diff_days / 365.25).floordiv(1).astype('Int64')
    return ages


def scan_rank_file(rankings_path: Path, build_maps: bool = False):
    """
    Scan a ranking CSV and return:
      - pid_map (if build_maps=True)
      - name_map (if build_maps=True)
      - max_rank
      - row_count
    """
    pid_map = {}
    name_map = {}
    max_rank = None
    row_count = 0

    print("Scanning rankings CSV:", rankings_path)
    try:
        it = pd.read_csv(rankings_path, dtype=str, keep_default_na=False, chunksize=CHUNKSIZE)
    except Exception as e:
        print("ERROR reading rankings CSV:", e, file=sys.stderr)
        return pid_map, name_map, None, 0

    for chunk_idx, chunk in enumerate(it, start=1):
        chunk = normalize_rank_columns(chunk)
        if build_maps:
            chunk['__name_key'] = chunk['full_name'].fillna('').astype(str).map(normalize_name_key)

        for _, row in chunk.iterrows():
            row_count += 1
            rank = parse_rank_value(row.get('ranking'))
            if rank is not None:
                max_rank = rank if max_rank is None else max(max_rank, rank)

            if build_maps and rank is not None:
                pid = str(row.get('player_id') or '').strip()
                name_key = str(row.get('__name_key') or "").strip()
                if pid:
                    pid_map[pid] = rank
                if name_key:
                    name_map[name_key] = rank

        print(f"  scan chunk #{chunk_idx} done. rows scanned so far: {row_count}")

    print(f"Scan complete: rows={row_count}, max_rank={max_rank}")
    return pid_map, name_map, max_rank, row_count


def fallback_rank_from_stats(max_rank, row_count):
    return (max_rank + 1) if max_rank is not None else (row_count + 1)


def lookup_rank_from_maps(pid, name_key, pid_map, name_map):
    if pid and pid in pid_map:
        return True, pid_map[pid]
    if name_key and name_key in name_map:
        return True, name_map[name_key]
    return False, None


def build_yearly_sample_maps(files_with_dates, ranking_date, cache):
    """
    Build one sampled ranking map per year, near the same month/day as ranking_date.
    Used for ever_ranked.
    """
    sample_maps = []
    if not files_with_dates:
        return sample_maps

    earliest_year = min(dt.year for dt, _ in files_with_dates)
    current_year = ranking_date.year

    seen_paths = set()

    for year in range(current_year - 1, earliest_year - 1, -1):
        years_back = current_year - year
        target = pd.Timestamp(ranking_date) - pd.DateOffset(years=years_back)
        chosen = choose_closest_file_to_date(files_with_dates, target, same_year=year)
        if chosen is None:
            continue

        resolved = chosen.resolve()
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)

        pid_map, name_map, max_rank, row_count = load_rank_file_maps(chosen, cache)
        sample_maps.append({
            "year": year,
            "path": chosen,
            "pid_map": pid_map,
            "name_map": name_map,
            "max_rank": max_rank,
            "row_count": row_count,
            "fallback_rank": fallback_rank_from_stats(max_rank, row_count),
        })

    return sample_maps


def load_rank_file_maps(rankings_path: Path, cache):
    key = (str(rankings_path.resolve()), True)
    if key in cache:
        return cache[key]
    result = scan_rank_file(rankings_path, build_maps=True)
    cache[key] = result
    return result


def load_rank_file_stats(rankings_path: Path, cache):
    key = (str(rankings_path.resolve()), False)
    if key in cache:
        return cache[key]
    result = scan_rank_file(rankings_path, build_maps=False)
    cache[key] = result
    return result


def resolve_player_id(pid: str, name_key: str, name_to_pid_map: dict) -> str:
    pid = str(pid or "").strip()
    if pid:
        return pid
    if name_key and name_key in name_to_pid_map:
        return str(name_to_pid_map[name_key]).strip()
    return ""


def process_rankings_in_chunks(
    rankings_path: Path,
    pid_map,
    name_map,
    prev_pid_map,
    prev_name_map,
    prev_fallback_rank: int,
    year_pid_map,
    year_name_map,
    year_fallback_rank: int,
    begin_pid_map,
    begin_name_map,
    name_to_pid_map,
    begin_fallback_rank: int,
    yearly_sample_maps,
    current_fallback_rank: int,
    circuit,
    base_url,
    compact,
    chunksize=CHUNKSIZE
):
    out = []
    total = 0
    print("Processing rankings CSV in chunks:", rankings_path)

    try:
        it = pd.read_csv(rankings_path, dtype=str, keep_default_na=False, chunksize=chunksize)
    except Exception as e:
        print("ERROR reading rankings CSV:", e, file=sys.stderr)
        return [], ""

    chunk_idx = 0
    ranking_date_str = None

    for chunk in it:
        chunk_idx += 1
        print(f"  processing chunk #{chunk_idx} (rows {total + 1}..{total + len(chunk)})")

        chunk = normalize_rank_columns(chunk)
        chunk['__name_key'] = chunk['full_name'].fillna('').astype(str).map(normalize_name_key)

        if ranking_date_str is None:
            cand = chunk['date'].loc[chunk['date'].astype(bool)]
            if not cand.empty:
                ranking_date_str = str(cand.iloc[0])

        birth_dates = []
        countries = []
        current_ranks = []
        current_valid_flags = []
        last_week_ranks = []
        last_week_flags = []
        last_year_ranks = []
        last_year_flags = []
        begin_year_ranks = []
        begin_year_flags = []
        ever_ranked_flags = []

        for _, row in chunk.iterrows():
            raw_pid = str(row.get('player_id') or '').strip()
            name_key = str(row.get('__name_key') or "").strip()
            full_name = str(row.get('full_name') or '').strip()

            pid = resolve_player_id(raw_pid, name_key, name_to_pid_map)

            raw_current_rank = parse_rank_value(row.get('ranking'))
            current_valid = raw_current_rank is not None
            current_rank = raw_current_rank if raw_current_rank is not None else current_fallback_rank

            found_prev, prev_rank_raw = lookup_rank_from_maps(pid, name_key, prev_pid_map, prev_name_map)
            prev_rank = prev_rank_raw if found_prev else prev_fallback_rank
            ranked_last_week = bool(found_prev)

            found_year, year_rank_raw = lookup_rank_from_maps(pid, name_key, year_pid_map, year_name_map)
            year_rank = year_rank_raw if found_year else year_fallback_rank
            ranked_last_year = bool(found_year)

            found_begin, begin_rank_raw = lookup_rank_from_maps(pid, name_key, begin_pid_map, begin_name_map)
            begin_rank = begin_rank_raw if found_begin else begin_fallback_rank
            ranked_beginning_year = bool(found_begin)

            ever_ranked = current_valid
            if not ever_ranked:
                for sample in yearly_sample_maps:
                    found_sample, _ = lookup_rank_from_maps(pid, name_key, sample["pid_map"], sample["name_map"])
                    if found_sample:
                        ever_ranked = True
                        break

            current_ranks.append(current_rank)
            current_valid_flags.append(current_valid)
            last_week_ranks.append(prev_rank)
            last_week_flags.append(ranked_last_week)
            last_year_ranks.append(year_rank)
            last_year_flags.append(ranked_last_year)
            begin_year_ranks.append(begin_rank)
            begin_year_flags.append(ranked_beginning_year)
            ever_ranked_flags.append(ever_ranked)

            bd = ''
            country = ''
            if pid and pid in pid_map:
                info = pid_map[pid]
                bd = info.get('birth_date', '') or ''
                country = info.get('represented_country', '') or ''
            elif name_key and name_key in name_map:
                info = name_map[name_key]
                bd = info.get('birth_date', '') or ''
                country = info.get('represented_country', '') or ''

            birth_dates.append(bd)
            countries.append(country)

        if ranking_date_str is None or ranking_date_str == '':
            ranking_date = pd.to_datetime(datetime.utcnow().strftime("%Y-%m-%d"))
        else:
            ranking_date = pd.to_datetime(ranking_date_str)

        ages = compute_age_vectorized(pd.Series(birth_dates), ranking_date)

        rows_list = chunk.to_dict(orient='records')
        for pos, row in enumerate(rows_list):
            total += 1

            full_name = row.get('full_name') or ''
            raw_pid = str(row.get('player_id') or '').strip()
            name_key = str(row.get('__name_key') or "").strip()
            pid = resolve_player_id(raw_pid, name_key, name_to_pid_map)
            slug = slugify(full_name)

            points = parse_points_value(row.get('points'))
            bd = birth_dates[pos] if pos < len(birth_dates) else ''
            country_raw = countries[pos] if pos < len(countries) else ''
            country_code = iso3_to_alpha2(country_raw) if country_raw else ''
            flag = emoji_from_alpha2(country_code) if country_code else ''

            age_val = ages.iloc[pos] if pos < len(ages) else pd.NA
            age = int(age_val) if pd.notna(age_val) else None

            current_rank = current_ranks[pos] if pos < len(current_ranks) else current_fallback_rank
            prev_rank = last_week_ranks[pos] if pos < len(last_week_ranks) else prev_fallback_rank
            year_rank = last_year_ranks[pos] if pos < len(last_year_ranks) else year_fallback_rank
            begin_rank = begin_year_ranks[pos] if pos < len(begin_year_ranks) else begin_fallback_rank

            evolution = prev_rank - current_rank if current_rank is not None and prev_rank is not None else None
            evolution_year = year_rank - current_rank if current_rank is not None and year_rank is not None else None
            evolution_this_year = begin_rank - current_rank if current_rank is not None and begin_rank is not None else None

            ranked_last_week = bool(last_week_flags[pos] if pos < len(last_week_flags) else False)
            ranked_last_year = bool(last_year_flags[pos] if pos < len(last_year_flags) else False)
            ranked_beginning_year = bool(begin_year_flags[pos] if pos < len(begin_year_flags) else False)
            ever_ranked = bool(ever_ranked_flags[pos] if pos < len(ever_ranked_flags) else False)

            if circuit.upper() == "ATP":
                if pid:
                    url = f"/players_atp/{pid}-{slug}"
                else:
                    url = f"/players_atp/{slug}"
            else:
                if pid:
                    url = f"/players/{pid}-{slug}"
                else:
                    url = f"/players/{slug}"

            out.append({
                "ranking": current_rank,
                "evolution": evolution,
                "evolution_year": evolution_year,
                "evolution_this_year": evolution_this_year,
                "ranked_last_week": ranked_last_week,
                "ranked_last_year": ranked_last_year,
                "ranked_beginning_year": ranked_beginning_year,
                "ever_ranked": ever_ranked,
                "full_name": full_name,
                "player_id": pid,
                "player_slug": slug,
                "player_url": (base_url.rstrip('/') + url) if base_url else url,
                "points": points,
                "birth_date": bd,
                "age": age,
                "country_code": country_code,
                "country_name": country_raw,
                "flag_emoji": flag,
                "date": ranking_date_str if ranking_date_str else ranking_date.strftime("%Y-%m-%d"),
                "circuit": circuit.upper()
            })

        print(f"  chunk #{chunk_idx} done. cumulative rows output: {len(out)}")

    print("All chunks processed. total rows:", len(out))
    return out, (ranking_date_str if ranking_date_str else ranking_date.strftime("%Y-%m-%d"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rankings-dir", required=True)
    p.add_argument("--players-csv", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--latest", action="store_true")
    p.add_argument("--date", required=False)
    p.add_argument("--circuit", choices=['ATP', 'WTA'], required=True)
    p.add_argument("--base-url", default="")
    p.add_argument("--compact", action="store_true", help="write compact JSON (no indent)")
    args = p.parse_args()

    rankings_dir = Path(args.rankings_dir)
    if not rankings_dir.exists():
        print("ERROR: rankings dir does not exist:", rankings_dir, file=sys.stderr)
        sys.exit(2)

    chosen = None
    if args.date:
        fn = f"data_{args.date.replace('-', '_')}.csv"
        cand = rankings_dir / fn
        if cand.exists():
            chosen = cand
        else:
            print("Requested date file not found:", cand, file=sys.stderr)
            sys.exit(3)
    elif args.latest:
        chosen = detect_latest_file_by_filename(rankings_dir)
        if not chosen:
            chosen = detect_latest_file_by_inside_date(rankings_dir)
        if not chosen:
            files = sorted(rankings_dir.glob("data_*.csv"))
            chosen = files[-1] if files else None

    if not chosen:
        print("No ranking file found in", rankings_dir, file=sys.stderr)
        sys.exit(4)

    print("Using ranking source:", chosen)

    ranking_date = get_ranking_date_from_file(chosen)
    if ranking_date is None:
        ranking_date = parse_file_date_from_name(chosen)
    if ranking_date is None:
        ranking_date = pd.Timestamp.utcnow().normalize()

    print("Ranking date:", ranking_date.strftime("%Y-%m-%d"))

    cache = {}

    pid_map, name_map, name_to_pid_map = build_players_maps(Path(args.players_csv))

    _, _, current_max_rank, current_row_count = load_rank_file_stats(chosen, cache)
    current_fallback_rank = fallback_rank_from_stats(current_max_rank, current_row_count)

    prev_pid_map, prev_name_map = {}, {}
    prev_fallback_rank = current_fallback_rank
    previous_file = detect_previous_file_by_filename(rankings_dir, chosen)
    if previous_file:
        print("Using previous ranking source:", previous_file)
        prev_pid_map, prev_name_map, prev_max_rank, prev_row_count = load_rank_file_maps(previous_file, cache)
        prev_fallback_rank = fallback_rank_from_stats(prev_max_rank, prev_row_count)
    else:
        print("No previous ranking file found, ranked_last_week will be False for everyone")

    files_with_dates = list_rank_files_with_dates(rankings_dir)

    year_target = ranking_date - pd.DateOffset(years=1)
    year_file = choose_closest_file_to_date(files_with_dates, year_target, same_year=year_target.year)
    year_pid_map, year_name_map = {}, {}
    year_fallback_rank = current_fallback_rank
    if year_file:
        print("Using one-year-ago ranking source:", year_file)
        year_pid_map, year_name_map, year_max_rank, year_row_count = load_rank_file_maps(year_file, cache)
        year_fallback_rank = fallback_rank_from_stats(year_max_rank, year_row_count)
    else:
        print("No one-year-ago ranking file found, evolution_year will use fallback")

    begin_target = pd.Timestamp(year=ranking_date.year, month=1, day=1)
    begin_file = choose_closest_file_to_date(files_with_dates, begin_target, same_year=ranking_date.year)
    begin_pid_map, begin_name_map = {}, {}
    begin_fallback_rank = current_fallback_rank
    if begin_file:
        print("Using beginning-of-year ranking source:", begin_file)
        begin_pid_map, begin_name_map, begin_max_rank, begin_row_count = load_rank_file_maps(begin_file, cache)
        begin_fallback_rank = fallback_rank_from_stats(begin_max_rank, begin_row_count)
    else:
        print("No beginning-of-year ranking file found, evolution_this_year will use fallback")

    yearly_sample_maps = build_yearly_sample_maps(files_with_dates, ranking_date, cache)
    print("Yearly samples used for ever_ranked:", len(yearly_sample_maps))

    out_rows, ranking_date_str = process_rankings_in_chunks(
        chosen,
        pid_map,
        name_map,
        prev_pid_map,
        prev_name_map,
        prev_fallback_rank,
        year_pid_map,
        year_name_map,
        year_fallback_rank,
        begin_pid_map,
        begin_name_map,
        name_to_pid_map,
        begin_fallback_rank,
        yearly_sample_maps,
        current_fallback_rank,
        args.circuit,
        args.base_url,
        args.compact
    )

    if not out_rows:
        print("ERROR: no rows were produced from the ranking file", file=sys.stderr)
        sys.exit(5)

    outpath = Path(args.out)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    print("Writing output JSON to", outpath)

    try:
        ranking_date_val = pd.to_datetime(ranking_date_str) if ranking_date_str else pd.to_datetime(pd.Timestamp.utcnow().strftime("%Y-%m-%d"))
    except Exception:
        ranking_date_val = pd.to_datetime(pd.Timestamp.utcnow().strftime("%Y-%m-%d"))



    with outpath.open("w", encoding="utf-8") as fh:
        if args.compact:
            json.dump(out_rows, fh, ensure_ascii=False, separators=(',', ':'))
        else:
            json.dump(out_rows, fh, ensure_ascii=False, indent=2)

    print("Wrote", outpath, "rows:", len(out_rows), "ranking_date:", ranking_date_str)
    sys.exit(0)


if __name__ == "__main__":
    main()