#!/usr/bin/env python3
"""
Expire old 'reviewed_player' flags.

Usage:
  python scripts/expire_reviews.py --csv player_data_wta.csv --months 6 --active-days 365

Behaviour:
 - parsedate date_review and last_appearance (tolerant)
 - a row is expired if reviewed_player is truthy AND date_review <= today - months
 - only expire rows that are "active": last_appearance >= today - active_days
 - writes CSV in place, preserving columns order
"""
import argparse
from pathlib import Path
import pandas as pd
import sys

def truthy(v):
    if pd.isna(v):
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("true", "t", "1", "yes", "y")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to CSV (in-place will be overwritten)")
    p.add_argument("--months", type=int, default=6, help="Expire reviews older than this many months (default: 6)")
    p.add_argument("--active-days", type=int, default=365, help="Consider a player 'active' if last_appearance within this many days (default: 365)")
    p.add_argument("--dry-run", action="store_true", help="Print summary but don't write file")
    args = p.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(csv_path, dtype=str)  # read everything as string (robust)
    # ensure those columns exist (if not, create defaults)
    for c in ("reviewed_player", "date_review", "last_appearance"):
        if c not in df.columns:
            df[c] = ""

    # parse dates tolerant
    df["__date_review_parsed"] = pd.to_datetime(df["date_review"], errors="coerce", infer_datetime_format=True)
    df["__last_app_parsed"] = pd.to_datetime(df["last_appearance"], errors="coerce", infer_datetime_format=True)

    today = pd.Timestamp.today().normalize()
    # expiry threshold = today - months
    expiry_threshold = today - pd.DateOffset(months=args.months)
    active_threshold = today - pd.Timedelta(days=args.active_days)

    # compute boolean masks
    reviewed_mask = df["reviewed_player"].apply(truthy)
    has_review_date = df["__date_review_parsed"].notna()
    review_too_old = df["__date_review_parsed"] <= expiry_threshold
    last_app_recent = df["__last_app_parsed"].notna() & (df["__last_app_parsed"] >= active_threshold)

    to_expire = reviewed_mask & has_review_date & review_too_old & last_app_recent

    n_total_reviewed = int(reviewed_mask.sum())
    n_candidates = int((reviewed_mask & has_review_date & review_too_old).sum())
    n_to_expire = int(to_expire.sum())

    print(f"[INFO] total reviewed_player==True: {n_total_reviewed}")
    print(f"[INFO] reviewed with date older than {args.months} months: {n_candidates}")
    print(f"[INFO] of those, active within last {args.active_days} days -> will expire: {n_to_expire}")

    if n_to_expire == 0:
        print("[INFO] nothing to do.")
    else:
        # apply: set reviewed_player to False and clear date_review
        df.loc[to_expire, "reviewed_player"] = "False"
        df.loc[to_expire, "date_review"] = ""

    # clean helper cols and write
    df = df.drop(columns=["__date_review_parsed","__last_app_parsed"])

    if args.dry_run:
        print("[DRY-RUN] not writing file.")
    else:
        # preserve original column order
        cols = list(df.columns)
        csv_temp = csv_path.with_suffix(csv_path.suffix + ".tmp")
        df.to_csv(csv_temp, index=False)
        # atomic replace
        csv_temp.replace(csv_path)
        print(f"[INFO] wrote updated CSV to {csv_path}")

if __name__ == "__main__":
    main()
