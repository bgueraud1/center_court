#!/usr/bin/env python3
"""
add_missing_players_from_rankings.py

Ajoute au player_data_atp.csv les joueurs présents dans les fichiers
data_YYYY_MM_DD.csv (dossier atp_rankings) mais absents du player_data_atp.csv.

Remplit uniquement : full_name, first_appearance, last_appearance, highest_ranking.
Les autres colonnes restent vides.

Usage:
    python add_missing_players_from_rankings.py
Options:
    --player-file PATH   : chemin vers player_data_atp.csv (défaut: ./player_data_atp.csv)
    --atp-dir PATH       : dossier contenant data_*.csv (défaut: ./atp_rankings)
    --no-backup          : ne pas créer de backup du fichier player_data_atp.csv
"""
from pathlib import Path
from datetime import datetime
import argparse
import pandas as pd
import re
import shutil
import sys

# ---------- Helpers ----------
def find_col(df, candidates):
    cols_l = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_l:
            return cols_l[cand.lower()]
    return None

def extract_int_rank(v):
    """Return int rank if parseable, else None."""
    if pd.isna(v):
        return None
    try:
        if isinstance(v, (int, float)) and not (isinstance(v, float) and pd.isna(v)):
            return int(v)
        s = str(v).strip()
        s = s.replace(",", "")
        m = re.search(r"(\d{1,4})", s)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None

def format_date(ts):
    if pd.isna(ts):
        return ""
    try:
        t = pd.to_datetime(ts, errors="coerce")
        if pd.isna(t):
            return ""
        return t.strftime("%Y-%m-%d")
    except Exception:
        return ""

# ---------- Main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--player-file", type=str, default=None,
                   help="Path to player_data_atp.csv (default: ./player_data_atp.csv)")
    p.add_argument("--atp-dir", type=str, default=None,
                   help="Path to atp_rankings dir (default: ./atp_rankings)")
    p.add_argument("--no-backup", action="store_true", help="Don't create a backup of player_data_atp.csv")
    args = p.parse_args()

    base = Path(__file__).parent.resolve()
    player_file = Path(args.player_file) if args.player_file else base / "player_data_atp.csv"
    atp_dir = Path(args.atp_dir) if args.atp_dir else base / "atp_rankings"

    if not player_file.exists():
        print(f"ERROR: player file not found: {player_file}")
        sys.exit(1)
    if not atp_dir.exists() or not any(atp_dir.glob("data_*.csv")):
        print(f"ERROR: atp_rankings folder not found or contains no data_*.csv: {atp_dir}")
        sys.exit(1)

    # load players
    print("Loading player file:", player_file)
    players_df = pd.read_csv(player_file, dtype=str, keep_default_na=False)

    # ensure the three target columns exist (create if needed)
    fa_col = find_col(players_df, ["first_appearance", "first appearance"]) or "first_appearance"
    la_col = find_col(players_df, ["last_appearance", "last appearance"]) or "last_appearance"
    hr_col = find_col(players_df, ["highest_ranking", "highest ranking"]) or "highest_ranking"
    name_col = find_col(players_df, ["full_name", "player_name", "name"]) or "full_name"

    # create columns if missing
    for c in (fa_col, la_col, hr_col):
        if c not in players_df.columns:
            players_df[c] = ""

    # existing names (exact match, stripped)
    existing_names = set(players_df[name_col].astype(str).str.strip())

    # read ranking files and aggregate per player
    print("Reading ranking CSVs from:", atp_dir)
    ranking_files = sorted(atp_dir.glob("data_*.csv"))
    ranking_chunks = []
    for f in ranking_files:
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            if df.empty:
                continue
            # detect columns
            name_c = find_col(df, ["full_name", "Player", "player", "full name"]) or None
            rank_c = find_col(df, ["ranking", "rank", "ranking_num"]) or None
            date_c = find_col(df, ["date", "Date"]) or None
            if not (name_c and rank_c and date_c):
                # try flexible detection by lower-case
                lc = {c.lower(): c for c in df.columns}
                if not name_c:
                    for cand in ("full_name", "player", "player_name", "full name"):
                        if cand in lc:
                            name_c = lc[cand]; break
                if not rank_c:
                    for cand in ("ranking", "rank"):
                        if cand in lc:
                            rank_c = lc[cand]; break
                if not date_c:
                    for cand in ("date",):
                        if cand in lc:
                            date_c = lc[cand]; break
            if not (name_c and rank_c and date_c):
                print(f"Warning: skipping {f.name} (missing required columns).")
                continue
            subset = df[[name_c, rank_c, date_c]].copy()
            subset.columns = ["full_name", "ranking", "date"]
            ranking_chunks.append(subset)
        except Exception as e:
            print(f"Warning: failed to read {f.name}: {e}")

    if not ranking_chunks:
        print("No ranking data found. Exiting.")
        sys.exit(0)

    all_ranks = pd.concat(ranking_chunks, ignore_index=True)
    all_ranks["full_name"] = all_ranks["full_name"].astype(str).str.strip()
    all_ranks["ranking_num"] = all_ranks["ranking"].apply(extract_int_rank)
    all_ranks["date_parsed"] = pd.to_datetime(all_ranks["date"], errors="coerce")
    # drop rows without valid name or date
    all_ranks = all_ranks[~all_ranks["full_name"].eq("")]
    all_ranks = all_ranks[~all_ranks["date_parsed"].isna()]

    # compute aggregates per full_name
    agg = all_ranks.groupby("full_name").agg(
        first_appearance=("date_parsed", "min"),
        last_appearance=("date_parsed", "max"),
        best_rank=("ranking_num", lambda s: int(pd.Series([x for x in s.dropna().astype(int)]).min()) if any(pd.notna(s)) else None)
    ).reset_index()

    # normalize aggregates
    agg["first_appearance"] = agg["first_appearance"].dt.strftime("%Y-%m-%d")
    agg["last_appearance"] = agg["last_appearance"].dt.strftime("%Y-%m-%d")
    # best_rank may be None
    agg["best_rank"] = agg["best_rank"].apply(lambda x: int(x) if (pd.notna(x) and x != "") else "")

    # find missing players
    agg_names = set(agg["full_name"].tolist())
    missing_names = sorted(list(agg_names - existing_names))

    if not missing_names:
        print("No missing players to add. Exiting.")
        sys.exit(0)

    print(f"Found {len(missing_names)} players in rankings but missing from player file. Sample: {missing_names[:10]}")

    # backup original
    if not args.no_backup:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        bak = player_file.with_suffix(f".bak.{stamp}")
        shutil.copy2(player_file, bak)
        print("Backup created:", bak)

    # Build rows to append
    new_rows = []
    # keep original column order
    cols = list(players_df.columns)
    for name in missing_names:
        row = {c: "" for c in cols}
        row[name_col] = name
        row[fa_col] = agg.loc[agg["full_name"] == name, "first_appearance"].values[0]
        row[la_col] = agg.loc[agg["full_name"] == name, "last_appearance"].values[0]
        br = agg.loc[agg["full_name"] == name, "best_rank"].values[0]
        row[hr_col] = "" if (br is None or (isinstance(br, float) and pd.isna(br))) else str(int(br))
        new_rows.append(row)

    # append to players_df
    if new_rows:
        add_df = pd.DataFrame(new_rows, columns=cols)
        result_df = pd.concat([players_df, add_df], ignore_index=True, sort=False)
    else:
        result_df = players_df.copy()

    # write back
    result_df.to_csv(player_file, index=False, encoding="utf-8-sig")
    print(f"Appended {len(new_rows)} new players to {player_file}")
    print("Done.")

if __name__ == "__main__":
    main()
