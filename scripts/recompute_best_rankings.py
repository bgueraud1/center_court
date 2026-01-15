#!/usr/bin/env python3
"""
Recompute best rankings for active players and update player_data CSV(s).

This version is tolerant when ranking files have only 'full_name' (no player_id).
If player_id column is present we use it. Otherwise we fallback to matching by name.

Usage:
  python3 scripts/recompute_best_rankings.py --rankings-dir atp_rankings --csv player_data_atp.csv
  python3 scripts/recompute_best_rankings.py --rankings-dir wta_rankings --csv player_data_wta.csv

Options:
  --dry-run : do not write CSV (print summary)
  --sentinel N : sentinel value for missing ranks (default 9999999)
"""
import argparse
from pathlib import Path
import pandas as pd
import sys
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RANK_COL_CANDIDATES = ["rank", "ranking", "position", "pos", "best_rank", "highest_ranking", "highest_rank", "rank_value"]
PID_CANDIDATES = ["player_id", "player id", "playerid", "id", "pid"]
NAME_CANDIDATES = ["full_name", "fullname", "full name", "name", "player_name", "player name"]

def detect_rank_col(df: pd.DataFrame):
    for c in RANK_COL_CANDIDATES:
        if c in df.columns:
            return c
    for c in df.columns:
        if "rank" in c.lower() or "position" in c.lower():
            return c
    return None

def detect_pid_or_name_col(df: pd.DataFrame):
    # prefer explicit player_id-like columns
    for c in PID_CANDIDATES:
        if c in df.columns:
            return ("id", c)
    # fallback: name-like columns
    for c in NAME_CANDIDATES:
        if c in df.columns:
            return ("name", c)
    # case-insensitive fallback
    cols_lower = {col.lower(): col for col in df.columns}
    for c in PID_CANDIDATES:
        if c.lower() in cols_lower:
            return ("id", cols_lower[c.lower()])
    for c in NAME_CANDIDATES:
        if c.lower() in cols_lower:
            return ("name", cols_lower[c.lower()])
    return (None, None)

def find_player_id_col_in_players(df_players: pd.DataFrame):
    for c in ("player_id","player id","id","pid"):
        if c in df_players.columns:
            return c
    for c in df_players.columns:
        if c.lower() in ("playerid","player_id","player"):
            return c
    return None

def find_player_name_col_in_players(df_players: pd.DataFrame):
    for c in ("full_name","fullname","name","player_name","player name"):
        if c in df_players.columns:
            return c
    for c in df_players.columns:
        if c.lower() in ("full_name","fullname","name","player_name".lower()):
            return c
    return None

def normalize_text(s):
    if pd.isna(s) or s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip()).casefold()

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
    rank_col = detect_rank_col(ranks_df)
    id_type, id_col = detect_pid_or_name_col(ranks_df)

    if id_col is None:
        logging.error("Could not detect player id or name column in ranking files (tried common names). Columns: %s", list(ranks_df.columns))
        sys.exit(1)
    logging.info("Detected id column type=%s col=%s", id_type, id_col)

    if rank_col is None:
        logging.error("Could not detect numeric rank column in ranking files. Columns: %s", list(ranks_df.columns))
        sys.exit(1)
    logging.info("Detected rank column: %s", rank_col)

    # normalize rank numbers
    ranks_df["__rank_num"] = pd.to_numeric(ranks_df[rank_col].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    valid_ranks = ranks_df.dropna(subset=["__rank_num"]).copy()
    valid_ranks["__rank_num"] = valid_ranks["__rank_num"].astype(int)

    # compute best rank by group key (player_id OR normalized name)
    if id_type == "id":
        group_key = id_col
        best = valid_ranks.groupby(group_key)["__rank_num"].min().rename("best_rank_computed").reset_index()
        logging.info("Computed best ranks (by id) for %d players from %d ranking rows", best.shape[0], len(valid_ranks))
    else:
        # group by normalized name
        valid_ranks["_norm_name"] = valid_ranks[id_col].apply(normalize_text)
        best = valid_ranks.groupby("_norm_name")["__rank_num"].min().rename("best_rank_computed").reset_index()
        logging.info("Computed best ranks (by name) for %d name-keys from %d ranking rows", best.shape[0], len(valid_ranks))

    # detect latest date (if present)
    if "date" in ranks_df.columns:
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["date"], errors="coerce")
    else:
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["__src_file"].str.extract(r"data_(\d{4}_\d{2}_\d{2})")[0].str.replace("_","-"), errors="coerce")
    latest_date = ranks_df["__date_parsed"].max()
    if pd.isna(latest_date):
        logging.warning("Could not determine latest ranking date; will treat all players as potentially active")
        active_keys = None
    else:
        latest_mask = ranks_df["__date_parsed"] == latest_date
        if id_type == "id":
            active_keys = set(ranks_df.loc[latest_mask, id_col].astype(str))
        else:
            active_keys = set(ranks_df.loc[latest_mask, id_col].apply(normalize_text).astype(str))
        logging.info("Latest ranking date detected: %s -> active keys: %d", latest_date.date(), len(active_keys))

    # Load players CSV
    players_path = Path(args.csv)
    if not players_path.exists():
        logging.error("Players CSV not found: %s", players_path)
        sys.exit(1)
    players_df = pd.read_csv(players_path, dtype=str).fillna("")

    # find players columns to update by id or by name
    player_id_col = find_player_id_col_in_players(players_df)
    player_name_col = find_player_name_col_in_players(players_df)
    logging.info("Players CSV columns: id_col=%s name_col=%s", player_id_col, player_name_col)

    # pick target column to write best ranking
    candidate_player_rank_cols = ["highest_ranking","best_rank","best rank","bestRank","highest_rank"]
    player_rank_col = None
    for c in candidate_player_rank_cols:
        if c in players_df.columns:
            player_rank_col = c
            break
    if player_rank_col is None:
        player_rank_col = "best_rank"
        players_df[player_rank_col] = ""

    updated = 0
    # build mapping and update players_df
    if id_type == "id":
        # mapping: id (string) -> best_rank_computed
        best_dict = {str(r[id_col]): int(r["best_rank_computed"]) for _, r in best.iterrows()}
        for idx, row in players_df.iterrows():
            pid = str(row.get(player_id_col, "")).strip() if player_id_col else ""
            if not pid:
                continue
            # only update active players if latest date could be found
            if active_keys is not None and pid not in active_keys:
                continue
            computed = best_dict.get(pid)
            if computed is None:
                computed = args.sentinel
            cur_raw = str(row.get(player_rank_col, "")).strip()
            cur_digits = re.sub(r"[^\d]", "", cur_raw) if cur_raw else ""
            try:
                cur_val = int(cur_digits) if cur_digits else args.sentinel
            except Exception:
                cur_val = args.sentinel
            if computed != cur_val:
                players_df.at[idx, player_rank_col] = "" if computed >= args.sentinel else str(int(computed))
                updated += 1
    else:
        # name-based mapping
        # build dict: norm_name -> best_rank
        best_dict = {str(r["_norm_name"]): int(r["best_rank_computed"]) for _, r in best.iterrows()}
        # build index of players by normalized full_name for fast lookup
        if player_name_col is None:
            logging.error("Players CSV does not have a name column to match ranking names. Columns: %s", list(players_df.columns))
            sys.exit(1)
        # create mapping name -> list of indices (to detect duplicates)
        name_to_idxs = {}
        for idx, row in players_df.iterrows():
            nm = normalize_text(row.get(player_name_col, ""))
            if nm:
                name_to_idxs.setdefault(nm, []).append(idx)

        for norm_name, computed_val in best_dict.items():
            # if active_keys defined, skip names not active
            if active_keys is not None and norm_name not in active_keys:
                continue
            idxs = name_to_idxs.get(norm_name)
            if not idxs:
                # no player found for this name
                continue
            # if multiple players match same name, we update all but log warning
            if len(idxs) > 1:
                logging.warning("Name collision for '%s' matched %d players; updating all matches", norm_name, len(idxs))
            for idx in idxs:
                cur_raw = str(players_df.at[idx, player_rank_col]).strip()
                cur_digits = re.sub(r"[^\d]", "", cur_raw) if cur_raw else ""
                try:
                    cur_val = int(cur_digits) if cur_digits else args.sentinel
                except Exception:
                    cur_val = args.sentinel
                if int(computed_val) != cur_val:
                    players_df.at[idx, player_rank_col] = "" if int(computed_val) >= args.sentinel else str(int(computed_val))
                    updated += 1

    logging.info("Updated %d player rows (active players) in %s (column %s)", updated, players_path.name, player_rank_col)

    if args.dry_run:
        logging.info("Dry-run: not writing CSV")
        # show sample of changed rows
        print(players_df[[player_name_col or player_id_col, player_rank_col]].head(40).to_string(index=False))
        sys.exit(0)

    # backup and write
    backup = players_path.with_suffix(players_path.suffix + ".bak")
    try:
        if backup.exists():
            backup.unlink()
        players_path.rename(backup)
        players_df.to_csv(players_path, index=False)
        logging.info("Wrote updated players CSV %s (backup at %s)", players_path, backup)
    except Exception as e:
        logging.error("Failed to write players CSV: %s", e)
        if backup.exists():
            backup.rename(players_path)
        sys.exit(1)

if __name__ == "__main__":
    main()
