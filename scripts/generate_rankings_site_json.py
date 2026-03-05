#!/usr/bin/env python3
# scripts/generate_rankings_site_json.py
"""
Generate per-date JSON and index/latest JSON for ATP/WTA rankings.

Usage examples:
  python3 scripts/generate_rankings_site_json.py --rankings-dir wta_rankings --players-csv player_data_wta.csv --out-dir docs/tools --prefix wta
  python3 scripts/generate_rankings_site_json.py --rankings-dir atp_rankings --players-csv player_data_atp.csv --out-dir docs/tools --prefix atp

Outputs:
  docs/tools/wta_2026_01_12.json
  docs/tools/wta_index.json
  docs/tools/latest_wta_ranking.json
"""
from pathlib import Path
import argparse
import pandas as pd
import json
from datetime import datetime
import re
import math

# Minimal ISO3 -> ISO2 map; extend as needed
ISO3_TO_ALPHA2 = {
    "ARG":"AR","AUS":"AU","AUT":"AT","BEL":"BE","BGR":"BG","BRA":"BR","CAN":"CA","CHN":"CN",
    "COL":"CO","CZE":"CZ","CRO":"HR","ESP":"ES","EST":"EE","FRA":"FR","GBR":"GB","UK":"GB",
    "GER":"DE","DEU":"DE","ITA":"IT","JPN":"JP","KOR":"KR","KAZ":"KZ","NED":"NL","NLD":"NL","NZL":"NZ",
    "POL":"PL","PRT":"PT","ROU":"RO","RUS":"RU","SRB":"RS","SLO":"SI","SWE":"SE","SUI":"CH","CHE":"CH",
    "TPE":"TW","UKR":"UA","USA":"US","US":"US","URU":"UY","MEX":"MX","IND":"IN","IRL":"IE","ISR":"IL",
    "ZAF":"ZA","DNK":"DK","HUN":"HU","NOR":"NO","BLR":"BY","VEN":"VE","CHI":"CL","ECU":"EC",
    "PER":"PE","DOM":"DO","PAN":"PA","CYP":"CY","GRC":"GR","GRE":"GR","LUX":"LU","LTU":"LT","LVA":"LV",
    "MYS":"MY","PHL":"PH","SGP":"SG","THA":"TH","VIE":"VN","ALG":"DZ","MAR":"MA","TUN":"TN","EGY":"EG",
    "GB":"GB", "ENG":"GB", "SCOT":"GB", "WALES":"GB", "NED":"NL", "ROM":"RO", "MKD":"MK", "POR":"PT",
    # add more if needed...
}

def iso_to_alpha2(code: str) -> str:
    if not code:
        return ""
    c = str(code).strip().upper()
    if len(c) == 2 and c.isalpha():
        return c
    if len(c) == 3 and c.isalpha():
        if c in ISO3_TO_ALPHA2:
            return ISO3_TO_ALPHA2[c]
        # last attempt: take first two letters if plausible
        cand = c[:2]
        if cand.isalpha():
            return cand
    # fallback: letters only
    letters = "".join(ch for ch in c if ch.isalpha())
    if letters in ISO3_TO_ALPHA2:
        return ISO3_TO_ALPHA2[letters]
    return ""

def flag_emoji_from_alpha2(alpha2: str) -> str:
    if not alpha2 or len(alpha2) != 2:
        return ""
    try:
        return ''.join(chr(ord(ch) + 127397) for ch in alpha2.upper())
    except Exception:
        return ""

_slug_re = re.compile(r'[^a-z0-9\-]+')
def slugify(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    # replace non-ascii letters by ascii approximations? keep simple
    s = re.sub(r"['’`]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = _slug_re.sub("-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-")
    return s or "player"

def parse_date_from_filename(fn: str) -> str:
    # expect data_YYYY_MM_DD.csv
    m = re.search(r"data_(\d{4}_\d{2}_\d{2})", fn)
    if m:
        return m.group(1).replace("_","-")
    return ""

def compute_age(birth_date_str: str, ranking_date_str: str):
    if not birth_date_str:
        return None
    try:
        bd = pd.to_datetime(birth_date_str, errors='coerce')
        rd = pd.to_datetime(ranking_date_str, errors='coerce')
        if pd.isna(bd) or pd.isna(rd):
            return None
        days = (rd - bd).days
        return int(math.floor(days / 365.25))
    except Exception:
        return None

def process_one_csv(csv_path: Path, players_df: pd.DataFrame, prefix: str):
    # read ranking CSV (tolerant)
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # normalize column names
    df_cols = {c.lower(): c for c in df.columns}
    # determine canonical columns
    def try_col(*cands):
        for c in cands:
            if c in df_cols:
                return df_cols[c]
        return None
    col_full_name = try_col("full_name","full name","player","player_name","name")
    col_player_id = try_col("player_id","player id","id")
    col_ranking = try_col("ranking","rank")
    col_points = try_col("points","official points")
    col_date = try_col("date")
    out = []
    file_date = parse_date_from_filename(csv_path.name)
    for _, row in df.iterrows():
        full_name = (row[col_full_name] if col_full_name else "") if row is not None else ""
        player_id = (row[col_player_id] if col_player_id else "") if row is not None else ""
        ranking = int(row[col_ranking]) if col_ranking and str(row[col_ranking]).strip().isdigit() else None
        points = row[col_points] if col_points else ""
        date_str = row[col_date] if col_date else file_date
        # attach player info from players_df
        matched = None
        if player_id and player_id != "":
            # try numeric coerce
            try:
                pid = int(player_id)
            except Exception:
                pid = player_id
            # prefer exact match on player_id
            if 'player_id' in players_df.columns:
                found = players_df[players_df['player_id'].astype(str) == str(player_id)]
                if not found.empty:
                    matched = found.iloc[0].to_dict()
        if matched is None and full_name:
            # fallback by case-insensitive full_name
            candidates = players_df[players_df['full_name'].str.strip().str.lower() == full_name.strip().lower()] if 'full_name' in players_df.columns else pd.DataFrame()
            if not candidates.empty:
                matched = candidates.iloc[0].to_dict()
        # extract birth_date and represented country from matched
        birth_date = matched.get('birth_date','') if matched else ""
        represented = matched.get('represented_country','') if matched else ""
        country_alpha2 = iso_to_alpha2(represented) if represented else ""
        flag_emoji = flag_emoji_from_alpha2(country_alpha2) if country_alpha2 else ""
        slug = slugify(full_name or (matched.get('full_name') if matched else "") or player_id or "player")
        # build player_url
        if prefix == "atp":
            if player_id:
                player_url = f"/players_atp/${str(player_id)}-${slug}"
            else:
                player_url = f"/players_atp/{slug}.html"
        else:
            if player_id:
                player_url = f"/players/{str(player_id)}-{slug}"
            else:
                player_url = f"/players/{slug}.html"

        age = compute_age(birth_date, date_str) if birth_date else None

        out.append({
            "ranking": ranking,
            "full_name": full_name,
            "player_id": player_id,
            "country_raw": represented,
            "country_code": country_alpha2,
            "flag_emoji": flag_emoji,
            "birth_date": birth_date,
            "age": age,
            "points": points,
            "date": date_str,
            "player_slug": slug,
            "player_url": player_url
        })
    # sort by ranking if present
    out_sorted = sorted([r for r in out if r.get('ranking') is not None], key=lambda x: int(x['ranking']))
    # for rows lacking ranking, append at end
    out_sorted += [r for r in out if r.get('ranking') is None]
    return out_sorted

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rankings-dir", required=True, help="Path to rankings dir (wta_rankings or atp_rankings)")
    p.add_argument("--players-csv", required=True, help="player_data_wta.csv or player_data_atp.csv")
    p.add_argument("--out-dir", required=True, help="Output directory (docs/tools)")
    p.add_argument("--prefix", required=True, choices=["wta","atp"], help="prefix used in output filenames")
    args = p.parse_args()

    rankings_dir = Path(args.rankings_dir)
    players_csv = Path(args.players_csv)
    out_dir = Path(args.out_dir)
    prefix = args.prefix

    if not rankings_dir.exists() or not rankings_dir.is_dir():
        raise SystemExit(f"Rankings directory not found: {rankings_dir}")
    if not players_csv.exists():
        print("Warning: players CSV not found, proceeding without enrichment:", players_csv)

    out_dir.mkdir(parents=True, exist_ok=True)

    players_df = pd.read_csv(players_csv, keep_default_na=False) if players_csv.exists() else pd.DataFrame(columns=[])

    # scan CSV files
    files = sorted(rankings_dir.glob("data_*.csv"))
    index = []
    for f in files:
        date = parse_date_from_filename(f.name)
        if not date:
            continue
        print("Processing", f)
        rows = process_one_csv(f, players_df, prefix)
        out_file = out_dir / f"{prefix}_{f.name.replace('data_','')[:-4]}.json"  # prefix_YYYY_MM_DD.json
        with out_file.open("w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=2)
        index.append({"date": date, "json": f"{out_file.name}"})
        print(" Wrote", out_file)

    # write index
    index_file = out_dir / f"{prefix}_index.json"
    with index_file.open("w", encoding="utf-8") as fh:
        json.dump(index, fh, ensure_ascii=False, indent=2)

    # write latest pointer (if any files)
    latest_file = out_dir / f"latest_{prefix}_ranking.json"
    if index:
        latest = index[-1]
        with open(out_dir / latest['json'], 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        with latest_file.open("w", encoding="utf-8") as fh:
            json.dump({"date": latest['date'], "data_json": latest['json'], "rows": data}, fh, ensure_ascii=False, indent=2)
        print("Latest written ->", latest_file)
    else:
        print("No ranking CSV found; index empty.")

if __name__ == "__main__":
    main()
