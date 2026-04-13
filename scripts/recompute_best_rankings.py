#!/usr/bin/env python3
"""
Recompute best rankings from ranking CSVs and update:
- ATP: player_data_atp.csv -> highest_ranking, matched by full_name
- WTA: player_data_wta.csv -> best_rank, matched by player_id

Expected ranking file formats:
ATP:
    full_name,ranking,points,date

WTA:
    full_name,player_id,ranking,points,movement,date

Usage examples:
    python recompute_best_ranks.py --mode atp
    python recompute_best_ranks.py --mode wta
    python recompute_best_ranks.py --mode atp --dry-run
    python recompute_best_ranks.py --mode wta --rankings-dir .\wta_rankings --csv .\player_data_wta.csv
"""

import argparse
from pathlib import Path
import logging
import re
import sys
from typing import Optional, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def normalize_text(s) -> str:
    if pd.isna(s) or s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip()).casefold()


def detect_rank_col(df: pd.DataFrame) -> Optional[str]:
    candidates = ["ranking", "rank", "position", "pos", "rank_value"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "rank" in c.lower() or "position" in c.lower():
            return c
    return None


def detect_col(df: pd.DataFrame, candidates) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def load_rankings(rankings_dir: Path) -> pd.DataFrame:
    files = sorted(rankings_dir.glob("data_*.csv"))
    if not files:
        raise FileNotFoundError(f"No ranking files found in {rankings_dir}")

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str).fillna("")
            df["__src_file"] = f.name
            frames.append(df)
        except Exception as e:
            logging.warning("Could not read %s: %s", f, e)

    if not frames:
        raise ValueError(f"No readable ranking files in {rankings_dir}")

    out = pd.concat(frames, ignore_index=True)
    out.columns = [c.strip() for c in out.columns]
    return out


def compute_best_by_name(ranks_df: pd.DataFrame) -> pd.DataFrame:
    rank_col = detect_rank_col(ranks_df)
    if rank_col is None:
        raise ValueError(f"Could not detect ranking column. Columns: {list(ranks_df.columns)}")

    if "full_name" not in ranks_df.columns:
        raise ValueError("ATP mode expects a 'full_name' column in ranking files")

    ranks_df["__rank_num"] = pd.to_numeric(
        ranks_df[rank_col].astype(str).str.extract(r"(\d+)")[0],
        errors="coerce",
    )
    valid = ranks_df.dropna(subset=["__rank_num"]).copy()
    valid["__rank_num"] = valid["__rank_num"].astype(int)

    if valid.empty:
        raise ValueError("No valid ranking values found")

    valid["__norm_name"] = valid["full_name"].apply(normalize_text)
    best = (
        valid.groupby("__norm_name")["__rank_num"]
        .min()
        .rename("best_rank_computed")
        .reset_index()
    )
    return best


def compute_best_by_id(ranks_df: pd.DataFrame) -> pd.DataFrame:
    rank_col = detect_rank_col(ranks_df)
    if rank_col is None:
        raise ValueError(f"Could not detect ranking column. Columns: {list(ranks_df.columns)}")

    if "player_id" not in ranks_df.columns:
        raise ValueError("WTA mode expects a 'player_id' column in ranking files")

    ranks_df["__rank_num"] = pd.to_numeric(
        ranks_df[rank_col].astype(str).str.extract(r"(\d+)")[0],
        errors="coerce",
    )
    valid = ranks_df.dropna(subset=["__rank_num"]).copy()
    valid["__rank_num"] = valid["__rank_num"].astype(int)

    if valid.empty:
        raise ValueError("No valid ranking values found")

    valid["__pid"] = valid["player_id"].astype(str).str.strip()
    best = (
        valid.groupby("__pid")["__rank_num"]
        .min()
        .rename("best_rank_computed")
        .reset_index()
    )
    return best


def update_csv_atp(csv_path: Path, best_by_name: pd.DataFrame, dry_run: bool) -> int:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    name_col = detect_col(df, ["full_name", "fullname", "name", "player_name", "player name"])
    if name_col is None:
        raise ValueError(f"Could not find a name column in {csv_path}. Columns: {list(df.columns)}")

    rank_col = detect_col(df, ["highest_ranking"])
    if rank_col is None:
        rank_col = "highest_ranking"
        df[rank_col] = ""

    best_map = {
        row["__norm_name"]: int(row["best_rank_computed"])
        for _, row in best_by_name.iterrows()
    }

    updated = 0
    for idx, row in df.iterrows():
        key = normalize_text(row.get(name_col, ""))
        if not key:
            continue

        new_val = best_map.get(key)
        if new_val is None:
            continue

        cur_raw = str(row.get(rank_col, "")).strip()
        cur_digits = re.sub(r"[^\d]", "", cur_raw)
        cur_val = int(cur_digits) if cur_digits else None

        if cur_val != new_val:
            df.at[idx, rank_col] = str(new_val)
            updated += 1

    if not dry_run:
        backup = csv_path.with_suffix(csv_path.suffix + ".bak")
        if backup.exists():
            backup.unlink()
        csv_path.rename(backup)
        df.to_csv(csv_path, index=False)
        logging.info("ATP CSV updated: %s (backup: %s)", csv_path, backup)

    return updated


def update_csv_wta(csv_path: Path, best_by_id: pd.DataFrame, dry_run: bool) -> int:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]

    pid_col = detect_col(df, ["player_id", "player id", "id", "pid"])
    if pid_col is None:
        raise ValueError(f"Could not find a player_id column in {csv_path}. Columns: {list(df.columns)}")

    rank_col = detect_col(df, ["best_rank"])
    if rank_col is None:
        rank_col = "best_rank"
        df[rank_col] = ""

    best_map = {
        str(row["__pid"]).strip(): int(row["best_rank_computed"])
        for _, row in best_by_id.iterrows()
    }

    updated = 0
    for idx, row in df.iterrows():
        key = str(row.get(pid_col, "")).strip()
        if not key:
            continue

        new_val = best_map.get(key)
        if new_val is None:
            continue

        cur_raw = str(row.get(rank_col, "")).strip()
        cur_digits = re.sub(r"[^\d]", "", cur_raw)
        cur_val = int(cur_digits) if cur_digits else None

        if cur_val != new_val:
            df.at[idx, rank_col] = str(new_val)
            updated += 1

    if not dry_run:
        backup = csv_path.with_suffix(csv_path.suffix + ".bak")
        if backup.exists():
            backup.unlink()
        csv_path.rename(backup)
        df.to_csv(csv_path, index=False)
        logging.info("WTA CSV updated: %s (backup: %s)", csv_path, backup)

    return updated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["atp", "wta"], required=True, help="Choose ATP or WTA mode")
    parser.add_argument("--rankings-dir", help="Directory containing data_*.csv ranking files")
    parser.add_argument("--csv", help="Target player CSV to update")
    parser.add_argument("--dry-run", action="store_true", help="Do not write files, only print what would change")
    args = parser.parse_args()

    if args.mode == "atp":
        rankings_dir = Path(args.rankings_dir or "atp_rankings")
        csv_path = Path(args.csv or "player_data_atp.csv")
    else:
        rankings_dir = Path(args.rankings_dir or "wta_rankings")
        csv_path = Path(args.csv or "player_data_wta.csv")

    if not rankings_dir.exists() or not rankings_dir.is_dir():
        logging.error("Rankings directory does not exist: %s", rankings_dir)
        sys.exit(1)

    try:
        ranks_df = load_rankings(rankings_dir)
    except Exception as e:
        logging.error("%s", e)
        sys.exit(1)

    try:
        if args.mode == "atp":
            best = compute_best_by_name(ranks_df)
            updated = update_csv_atp(csv_path, best, args.dry_run)
        else:
            best = compute_best_by_id(ranks_df)
            updated = update_csv_wta(csv_path, best, args.dry_run)
    except Exception as e:
        logging.error("%s", e)
        sys.exit(1)

    logging.info("Done. Updated %d rows.", updated)
    if args.dry_run:
        logging.info("Dry-run mode: no files were written.")


if __name__ == "__main__":
    main()