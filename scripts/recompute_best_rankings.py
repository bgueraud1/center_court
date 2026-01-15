#!/usr/bin/env python3
"""
Recompute best rankings for active players and update player_data CSV(s).

Usage:
  python3 scripts/recompute_best_rankings.py --rankings-dir wta_rankings --csv player_data_wta.csv
  python3 scripts/recompute_best_rankings.py --rankings-dir atp_rankings --csv player_data_atp.csv

Behavior:
- reads all data_YYYY_MM_DD.csv in rankings-dir, concatenates them
- determines latest ranking date and list of active player_ids on that date
- computes min(rank) per player across all dates (smallest numeric = best)
- updates the CSV column (tries many candidate column names: 'highest_ranking','best_rank','best rank')
- writes CSV back (in-place) unless --dry-run
- options: --dry-run, --sentinel (default 9999999)
"""
import argparse
from pathlib import Path
import pandas as pd
import json
import sys
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RANK_COL_CANDIDATES = ["rank","ranking","position","pos","best_rank","highest_ranking","highest_rank","rank_value"]

def detect_rank_col(df: pd.DataFrame):
    for c in RANK_COL_CANDIDATES:
        if c in df.columns:
            return c
    # fallback: try any column with name containing 'rank' or 'position'
    for c in df.columns:
        if ("rank" in c.lower()) or ("position" in c.lower()):
            return c
    return None

def detect_pid_col(df: pd.DataFrame):
    for c in ("player_id","player id","id","pid"):
        if c in df.columns:
            return c
    # fallback: int-like column with few unique values?
    for c in df.columns:
        if c.lower() in ("playerid","player_id","player"):
            return c
    return None

def find_player_id_col_in_players(df_players: pd.DataFrame):
    for c in ("player_id","player id","id","pid"):
        if c in df_players.columns:
            return c
    for c in df_players.columns:
        if c.lower() in ("playerid","player_id","player"):
            return c
    raise RuntimeError("Couldn't find player_id column in players CSV")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rankings-dir", required=True, help="Directory containing data_YYYY_MM_DD.csv files")
    p.add_argument("--csv", required=True, help="player_data csv to update (e.g. player_data_wta.csv)")
    p.add_argument("--sentinel", type=int, default=9999999, help="sentinel value for missing ranks")
    p.add_argument("--dry-run", action="store_true", help="do not write CSV, just print summary")
    args = p.parse_args()

    rankings_dir = Path(args.rankings_dir)
    if not rankings_dir.exists() or not rankings_dir.is_dir():
        logging.error("rankings_dir %s does not exist or is not a directory", rankings_dir)
        sys.exit(1)

    # read ranking files
    files = sorted(rankings_dir.glob("data_*.csv"))
    if not files:
        logging.warning("No ranking files found in %s", rankings_dir)
        sys.exit(0)

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str).fillna("")
            df["__src_file"] = f.name
            frames.append(df)
        except Exception as e:
            logging.warning("Could not read %s: %s", f, e)
    if not frames:
        logging.warning("No readable ranking frames in %s", rankings_dir)
        sys.exit(0)

    ranks_df = pd.concat(frames, ignore_index=True)
    # try to infer pid and rank column names
    rank_col = detect_rank_col(ranks_df)
    pid_col = detect_pid_col(ranks_df)
    if pid_col is None:
        logging.error("Could not detect player id column in ranking files (tried common names). Columns: %s", list(ranks_df.columns))
        sys.exit(1)
    if rank_col is None:
        logging.error("Could not detect numeric rank column in ranking files. Columns: %s", list(ranks_df.columns))
        sys.exit(1)

    # normalize rank values to numeric; coerce non-numeric to NaN
    ranks_df["__rank_num"] = pd.to_numeric(ranks_df[rank_col].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    # drop NaNs for rank calculations
    valid_ranks = ranks_df.dropna(subset=["__rank_num"]).copy()
    valid_ranks["__rank_num"] = valid_ranks["__rank_num"].astype(int)

    # compute best rank (min) per player
    best = valid_ranks.groupby(pid_col)["__rank_num"].min().rename("best_rank_computed").reset_index()
    logging.info("Computed best ranks for %d players from %d ranking rows", best.shape[0], len(valid_ranks))

    # determine latest ranking date and active ids from that file(s)
    # we assume a 'date' column exists or infer from filename
    if "date" in ranks_df.columns:
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["date"], errors="coerce")
    else:
        # try extract date from filename pattern data_YYYY_MM_DD.csv
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["__src_file"].str.extract(r"data_(\d{4}_\d{2}_\d{2})")[0].str.replace("_","-"), errors="coerce")
    latest_date = ranks_df["__date_parsed"].max()
    if pd.isna(latest_date):
        logging.warning("Could not determine latest ranking date; will treat all players as potentially active")
        active_ids = set(best[pid_col].astype(str))
    else:
        latest_mask = ranks_df["__date_parsed"] == latest_date
        active_ids = set(ranks_df.loc[latest_mask, pid_col].astype(str))
        logging.info("Latest ranking date detected: %s -> active players: %d", latest_date.date(), len(active_ids))

    # Load players CSV
    players_path = Path(args.csv)
    if not players_path.exists():
        logging.error("Players CSV not found: %s", players_path)
        sys.exit(1)
    players_df = pd.read_csv(players_path, dtype=str).fillna("")

    player_pid_col = find_player_id_col_in_players(players_df)
    logging.info("Players CSV player id column: %s", player_pid_col)

    # Try to detect which column stores the best/highest rank in players csv
    candidate_player_rank_cols = ["highest_ranking","best_rank","best rank","bestRank","highest_rank"]
    player_rank_col = None
    for c in candidate_player_rank_cols:
        if c in players_df.columns:
            player_rank_col = c
            break
    if player_rank_col is None:
        # create one
        player_rank_col = "best_rank"
        players_df[player_rank_col] = ""

    # Build lookup dict: pid -> best_rank_computed
    best_dict = {str(r[pid_col]): int(r["best_rank_computed"]) for _, r in best.iterrows()}

    updated = 0
    for idx, row in players_df.iterrows():
        pid = str(row.get(player_pid_col, "")).strip()
        if not pid:
            continue
        # only update active players (present in latest rankings)
        if pid not in active_ids:
            continue
        computed = best_dict.get(pid)
        if computed is None:
            # no ranking info -> set sentinel
            computed = args.sentinel
        # parse current stored value numeric
        cur_raw = str(row.get(player_rank_col, "")).strip()
        cur_digits = re.sub(r"[^\d]", "", cur_raw) if cur_raw else ""
        try:
            cur_val = int(cur_digits) if cur_digits else args.sentinel
        except Exception:
            cur_val = args.sentinel

        if computed != cur_val:
            players_df.at[idx, player_rank_col] = "" if computed >= args.sentinel else str(int(computed))
            updated += 1

    logging.info("Updated %d player rows (active players) in %s (column %s)", updated, players_path.name, player_rank_col)

    if args.dry_run:
        logging.info("Dry-run: not writing CSV (use --dry-run to test)")
        # print few examples
        print(players_df[[player_pid_col, player_rank_col]].head(20).to_string(index=False))
        sys.exit(0)

    # write back (overwrite)
    backup = players_path.with_suffix(players_path.suffix + ".bak")
    try:
        players_path.rename(backup)
        players_df.to_csv(players_path, index=False)
        logging.info("Wrote updated players CSV %s (backup at %s)", players_path, backup)
    except Exception as e:
        logging.error("Failed to write players CSV: %s", e)
        # try to restore backup if exists
        if backup.exists():
            backup.rename(players_path)
        sys.exit(1)


if __name__ == "__main__":
    main()
