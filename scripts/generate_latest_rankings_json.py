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
import math
import sys
from datetime import datetime

CHUNKSIZE = 5000  # adjust if you want bigger chunks

ISO3_TO_ALPHA2 = {
    "ARG":"AR","AUS":"AU","AUT":"AT","BEL":"BE","BGR":"BG","BRA":"BR","CAN":"CA","CHN":"CN",
    "COL":"CO","CZE":"CZ","CRO":"HR","ESP":"ES","EST":"EE","FRA":"FR","GBR":"GB","GB":"GB",
    "GER":"DE","DEU":"DE","ITA":"IT","JPN":"JP","KOR":"KR","KAZ":"KZ","NED":"NL","NLD":"NL","NZL":"NZ",
    "POL":"PL","PRT":"PT","ROU":"RO","RUS":"RU","SRB":"RS","SLO":"SI","SWE":"SE","SUI":"CH",
    "TPE":"TW","UKR":"UA","USA":"US","URU":"UY","MEX":"MX","IND":"IN","IRL":"IE","ISR":"IL",
    "ZAF":"ZA","DNK":"DK","HUN":"HU","NOR":"NO","BLR":"BY","VEN":"VE","CHI":"CL","ECU":"EC",
    "PER":"PE","DOM":"DO","PAN":"PA","CYP":"CY","GRC":"GR","GRE":"GR","LUX":"LU","LTU":"LT","LVA":"LV",
    "MYS":"MY","PHL":"PH","SGP":"SG","THA":"TH","VIE":"VN","ALG":"DZ","MAR":"MA","TUN":"TN","EGY":"EG",
    "LAT":"LV","POR":"PT","NIG":"NG","KEN":"KE"
}

def iso3_to_alpha2(code: str) -> str:
    if not code:
        return ""
    c = str(code).strip().upper()
    if len(c) == 2 and c.isalpha(): return c
    if len(c) == 3 and c.isalpha(): return ISO3_TO_ALPHA2.get(c, "")
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
    if not name: return ''
    s = unicodedata.normalize('NFKD', name)
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'[-\s]+', '-', s).strip('-')
    s = _slug_re.sub('', s)
    return s[:200]

def detect_latest_file_by_filename(rankings_dir: Path):
    candidates = []
    for p in rankings_dir.glob("data_*.csv"):
        m = re.match(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$", p.name)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                candidates.append((dt, p))
            except Exception:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]

def detect_latest_file_by_inside_date(rankings_dir: Path):
    best = None
    best_date = None
    for p in rankings_dir.glob("data_*.csv"):
        try:
            df = pd.read_csv(p, usecols=['date'], parse_dates=['date'], infer_datetime_format=True, keep_default_na=False)
            if df.empty: continue
            maxd = df['date'].max()
            if pd.isna(maxd): continue
            if best_date is None or maxd > best_date:
                best_date = maxd; best = p
        except Exception:
            continue
    return best

def normalize_rank_columns(df):
    # normalize possible column names, return df with columns we need
    cols = {c.strip():c for c in df.columns}
    mapping = {}
    for c in cols:
        cl = c.lower()
        if cl in ("full_name","fullname","name","player","player_name"):
            mapping[c] = "full_name"
        if cl in ("player_id","id"):
            mapping[c] = "player_id"
        if cl in ("ranking","rank","position","#"):
            mapping[c] = "ranking"
        if cl in ("points","official points","pts"):
            mapping[c] = "points"
        if cl in ("movement",):
            mapping[c] = "movement"
        if cl in ("date",):
            mapping[c] = "date"
    df = df.rename(columns=mapping)
    for c in ("full_name","player_id","ranking","points","movement","date"):
        if c not in df.columns:
            df[c] = ""
    return df[["full_name","player_id","ranking","points","movement","date"]]

def build_players_maps(players_csv: Path):
    print("Loading players CSV into memory:", players_csv)
    pcols = None
    try:
        players_df = pd.read_csv(players_csv, dtype=str, keep_default_na=False)
    except Exception as e:
        print("ERROR reading players CSV:", e, file=sys.stderr); return {}, {}
    players_df.columns = [c.strip() for c in players_df.columns]
    # ensure full_name exists
    if 'full_name' not in players_df.columns or players_df['full_name'].isnull().all():
        if 'first_name' in players_df.columns:
            fn = 'first_name' if 'first_name' in players_df.columns else 'firstname'
            ln = 'last_name' if 'last_name' in players_df.columns else ('lastname' if 'lastname' in players_df.columns else None)
            if ln and ln in players_df.columns:
                players_df['full_name'] = (players_df[fn].fillna('').astype(str).str.strip() + ' ' + players_df[ln].fillna('').astype(str).str.strip()).str.strip()
            else:
                players_df['full_name'] = players_df[fn].fillna('').astype(str).str.strip()
        else:
            for alt in ('player','name'):
                if alt in players_df.columns:
                    players_df['full_name'] = players_df[alt].fillna('').astype(str).str.strip()
                    break
    players_df['__name_lc'] = players_df['full_name'].fillna('').astype(str).str.strip().str.lower()
    # build player_id map
    pid_map = {}
    if 'player_id' in players_df.columns:
        for _, r in players_df.iterrows():
            pid = (r.get('player_id') or '')
            if pid is None: pid = ''
            pid = str(pid).strip()
            if pid:
                pid_map[pid] = {
                    'birth_date': r.get('birth_date','') or '',
                    'represented_country': r.get('represented_country','') or ''
                }
    # name map (deduplicate keep last)
    name_map = {}
    for _, r in players_df.iterrows():
        k = (r.get('__name_lc') or '').strip()
        if not k: continue
        name_map[k] = {
            'birth_date': r.get('birth_date','') or '',
            'represented_country': r.get('represented_country','') or ''
        }
    print(f"Built players maps: player_id keys={len(pid_map)} name keys={len(name_map)}")
    return pid_map, name_map

def compute_age_vectorized(birth_dates_series, ranking_date):
    # birth_dates_series: pd.Series of strings
    bd = pd.to_datetime(birth_dates_series.replace('', pd.NaT), errors='coerce')
    if isinstance(ranking_date, str):
        ranking_date = pd.to_datetime(ranking_date)
    diff = (ranking_date - bd).dt.days
    ages = (diff / 365.25).floordiv(1).astype('Int64')  # allows NA
    # convert to native python int or None
    return ages

def process_rankings_in_chunks(rankings_path: Path, pid_map, name_map, circuit, base_url, compact, chunksize=CHUNKSIZE):
    out = []
    total = 0
    print("Processing rankings CSV in chunks:", rankings_path)
    # use iterator
    it = pd.read_csv(rankings_path, dtype=str, keep_default_na=False, chunksize=chunksize)
    chunk_idx = 0
    ranking_date_str = None
    for chunk in it:
        chunk_idx += 1
        print(f"  processing chunk #{chunk_idx} (rows {total+1}..{total+len(chunk)})")
        chunk = normalize_rank_columns(chunk)
        # decide ranking_date_str from first non-empty date if unknown
        if ranking_date_str is None:
            cand = chunk['date'].loc[chunk['date'].astype(bool)]
            if not cand.empty:
                ranking_date_str = cand.iloc[0]
        # lower names for lookup
        chunk['__name_lc'] = chunk['full_name'].fillna('').astype(str).str.strip().str.lower()
        # try to enrich from pid_map quickly
        birth_dates = []
        countries = []
        for idx, row in chunk.iterrows():
            pid = str(row.get('player_id','') or '').strip()
            name_lc = row.get('__name_lc','') or ''
            bd = ''
            country = ''
            if pid and pid in pid_map:
                info = pid_map[pid]
                bd = info.get('birth_date','') or ''
                country = info.get('represented_country','') or ''
            elif name_lc and name_lc in name_map:
                info = name_map[name_lc]
                bd = info.get('birth_date','') or ''
                country = info.get('represented_country','') or ''
            birth_dates.append(bd)
            countries.append(country)
        # vectorized age compute
        if ranking_date_str is None or ranking_date_str == '':
            # fallback to filename -- handled earlier, but ensure we have a value
            ranking_date = pd.to_datetime(datetime.utcnow().strftime("%Y-%m-%d"))
        else:
            ranking_date = pd.to_datetime(ranking_date_str)
        ages = compute_age_vectorized(pd.Series(birth_dates), ranking_date)
        # build output rows
        for i, row in chunk.iterrows():
            total += 1
            rk = None
            try:
                rk = int(float(row.get('ranking') or 0)) if row.get('ranking') else None
            except Exception:
                rk = None
            full_name = row.get('full_name') or ''
            pid = (row.get('player_id') or '').strip()
            slug = slugify(full_name)
            points_raw = row.get('points') or ''
            try:
                points = int(float(str(points_raw).replace(',','').strip())) if points_raw not in (None,"") else None
            except Exception:
                points = None
            bd = birth_dates[i - chunk.index[0]] if isinstance(chunk.index, pd.RangeIndex) else birth_dates[list(chunk.index).index(i)]
            # safer: index into birth_dates by positional order; simpler: use a separate loop with enumerate earlier
            # to avoid complexities, compute positions:
            # but above approach with index is brittle; so reconstruct simpler:
            # We'll instead use enumerate over chunk.itertuples below (see improved block)
        # REWRITE the per-row append to avoid index complexity (iterate by position):
        # convert chunk to list of dicts for deterministic order
        rows_list = chunk.to_dict(orient='records')
        for pos, row in enumerate(rows_list):
            rk = None
            try:
                rk = int(float(row.get('ranking') or 0)) if row.get('ranking') else None
            except Exception:
                rk = None
            full_name = row.get('full_name') or ''
            pid = (row.get('player_id') or '').strip()
            slug = slugify(full_name)
            points_raw = row.get('points') or ''
            try:
                points = int(float(str(points_raw).replace(',','').strip())) if points_raw not in (None,"") else None
            except Exception:
                points = None
            bd = birth_dates[pos] if pos < len(birth_dates) else ''
            country_raw = countries[pos] if pos < len(countries) else ''
            country_code = iso3_to_alpha2(country_raw) if country_raw else ''
            flag = emoji_from_alpha2(country_code) if country_code else ''
            age_val = ages.iloc[pos] if pos < len(ages) else pd.NA
            age = int(age_val) if pd.notna(age_val) else None
            if circuit.upper() == "ATP":
                if pid:
                    url = f"/players_atp/{pid}-{slug}.html"
                else:
                    url = f"/players_atp/{slug}.html"
            else:
                if pid:
                    url = f"/players/{pid}-{slug}.html"
                else:
                    url = f"/players/{slug}.html"
            out.append({
                "ranking": rk,
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
    p.add_argument("--circuit", choices=['ATP','WTA'], required=True)
    p.add_argument("--base-url", default="")
    p.add_argument("--compact", action="store_true", help="write compact JSON (no indent)")
    args = p.parse_args()

    rankings_dir = Path(args.rankings_dir)
    if not rankings_dir.exists():
        print("ERROR: rankings dir does not exist:", rankings_dir, file=sys.stderr); sys.exit(2)

    chosen = None
    if args.date:
        fn = f"data_{args.date.replace('-','_')}.csv"
        cand = rankings_dir / fn
        if cand.exists(): chosen = cand
        else:
            print("Requested date file not found:", cand, file=sys.stderr); sys.exit(3)
    elif args.latest:
        chosen = detect_latest_file_by_filename(rankings_dir)
        if not chosen:
            chosen = detect_latest_file_by_inside_date(rankings_dir)
        if not chosen:
            files = sorted(rankings_dir.glob("data_*.csv"))
            chosen = files[-1] if files else None

    if not chosen:
        print("No ranking file found in", rankings_dir, file=sys.stderr); sys.exit(4)

    print("Using ranking source:", chosen)
    # build player maps
    pid_map, name_map = build_players_maps(Path(args.players_csv))
    # process in chunks
    out_rows, ranking_date_str = process_rankings_in_chunks(chosen, pid_map, name_map, args.circuit, args.base_url, args.compact)
    outpath = Path(args.out)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    print("Writing output JSON to", outpath)
    with outpath.open("w", encoding="utf-8") as fh:
        if args.compact:
            json.dump(out_rows, fh, ensure_ascii=False, separators=(',',':'))
        else:
            json.dump(out_rows, fh, ensure_ascii=False, indent=2)
    print("Wrote", outpath, "rows:", len(out_rows), "ranking_date:", ranking_date_str)
    sys.exit(0)

if __name__ == "__main__":
    main()
