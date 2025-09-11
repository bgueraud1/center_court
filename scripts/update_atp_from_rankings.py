#!/usr/bin/env python3
"""
Update player_data_atp.csv using weekly ranking files placed in atp_rankings/data_YYYY_MM_DD.csv

Behavior:
 - read files in atp_rankings matching data_YYYY_MM_DD.csv
 - process them in chronological order (old -> new)
 - Supports:
    --date YYYY-MM-DD   -> process only the file for that date (data_YYYY_MM_DD.csv)
    --latest             -> process only the most recent file found
 - for each row in a ranking file:
    * match player row in player_data_atp.csv by full_name (case-insensitive exact)
    * if not found and --fuzzy is enabled, try fuzzy match
    * if still not found, append a new player row with minimal fields
    * update:
        - first_appearance = min(existing_first_appearance, date) (if empty set date)
        - last_appearance = max(existing_last_appearance, date)
        - highest_ranking = best (numerically smallest) of existing or new ranking
 - backup the CSV before overwriting
"""
from pathlib import Path
import argparse
import pandas as pd
import html
import re
from datetime import datetime
from difflib import get_close_matches

def parse_date(s):
    if not s or pd.isna(s):
        return ""
    try:
        dt = pd.to_datetime(s, errors='coerce')
        if pd.isna(dt):
            m = re.match(r"^\d{4}-\d{2}-\d{2}$", str(s).strip())
            return str(s).strip() if m else ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def normalize_name(s):
    if s is None:
        return ""
    s2 = re.sub(r"\s+", " ", str(s).strip())
    return s2

def best_rank(old, new):
    try:
        newi = int(str(new).strip())
    except Exception:
        return old or ""
    try:
        oldi = int(str(old).strip()) if old not in (None, "") else None
    except Exception:
        oldi = None
    if oldi is None:
        return str(newi)
    return str(min(oldi, newi))

def min_date(old, new):
    if not new:
        return old
    if not old:
        return new
    try:
        d_old = datetime.strptime(old, "%Y-%m-%d").date()
        d_new = datetime.strptime(new, "%Y-%m-%d").date()
        return d_old.isoformat() if d_old <= d_new else d_new.isoformat()
    except Exception:
        return old or new

def max_date(old, new):
    if not new:
        return old
    if not old:
        return new
    try:
        d_old = datetime.strptime(old, "%Y-%m-%d").date()
        d_new = datetime.strptime(new, "%Y-%m-%d").date()
        return d_old.isoformat() if d_old >= d_new else d_new.isoformat()
    except Exception:
        return new or old

def load_ranking_csv(path):
    df = pd.read_csv(path, dtype=str).fillna("")
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    if 'full_name' in cols:
        mapping[cols['full_name']] = 'full_name'
    else:
        for k in ('player','name','player_name','player full name'):
            if k in cols:
                mapping[cols[k]] = 'full_name'
                break
    for k in ('ranking','rank','#'):
        if k in cols:
            mapping[cols[k]] = 'ranking'
            break
    if 'points' in cols:
        mapping[cols['points']] = 'points'
    if 'date' in cols:
        mapping[cols['date']] = 'date'
    if mapping:
        df = df.rename(columns=mapping)
    for c in ('full_name','ranking','points','date'):
        if c not in df.columns:
            df[c] = ""
    df['date'] = df['date'].apply(parse_date)
    df['full_name'] = df['full_name'].apply(normalize_name)
    return df[['full_name','ranking','points','date']]

def find_match_by_name(df_players, name):
    lname = name.strip().lower()
    if not lname:
        return None
    mask = df_players['full_name'].fillna("").str.strip().str.lower() == lname
    if mask.any():
        idx = df_players[mask].index[0]
        return idx
    return None

def find_match_fuzzy(df_players, name, cutoff=0.85):
    names = df_players['full_name'].fillna("").astype(str).tolist()
    if not name:
        return None
    cm = get_close_matches(name, names, n=1, cutoff=cutoff)
    if cm:
        matched = cm[0]
        mask = df_players['full_name'].fillna("").astype(str) == matched
        if mask.any():
            return df_players[mask].index[0]
    return None

def main(rankings_dir="atp_rankings", csv_path="player_data_atp.csv",
         fuzzy=False, fuzzy_cutoff=0.85, dry_run=False, date_filter=None,
         latest=False, since_csv=False):

    rdir = Path(rankings_dir)
    if not rdir.exists():
        raise SystemExit(f"rankings dir {rdir} not found")
    csv_p = Path(csv_path)
    if not csv_p.exists():
        raise SystemExit(f"CSV {csv_p} not found")

    players_df = pd.read_csv(csv_p, dtype=str).fillna("")
    for col in ("full_name","first_appearance","last_appearance","highest_ranking"):
        if col not in players_df.columns:
            players_df[col] = ""

    players_df['__norm_name'] = players_df['full_name'].apply(normalize_name)

    files = sorted([p for p in rdir.iterdir() if p.is_file() and re.match(r"data_\d{4}_\d{2}_\d{2}\.csv", p.name)])


    # optional: only process files newer than the CSV's max last_appearance
    if since_csv:
        try:
            # parse last_appearance column to datetimes
            last_dates = pd.to_datetime(players_df['last_appearance'], errors='coerce')
            if last_dates.notna().any():
                cutoff_dt = last_dates.max().to_pydatetime().date()
            else:
                cutoff_dt = None
        except Exception:
            cutoff_dt = None

        if cutoff_dt:
            filtered = []
            for pth in files:
                m = re.search(r"data_(\d{4}_\d{2}_\d{2})\.csv$", pth.name)
                if not m:
                    continue
                fdate = datetime.strptime(m.group(1).replace("_", "-"), "%Y-%m-%d").date()
                if fdate > cutoff_dt:
                    filtered.append(pth)
            files = filtered
            if not files:
                print(f"No ranking files newer than last_appearance {cutoff_dt}. Nothing to do.")
                return
            print(f"Processing {len(files)} ranking files newer than {cutoff_dt}.")


    if not files:
        print("No ranking files found (pattern data_YYYY_MM_DD.csv). Nothing to do.")
        return

    # ----- NEW: filtering by --date or --latest -----
    if date_filter:
        wanted = f"data_{date_filter.replace('-', '_')}.csv"
        files = [p for p in files if p.name == wanted]
        if not files:
            print(f"No ranking file for requested date {date_filter} (expected {wanted}). Nothing to do.")
            return
        print(f"Processing only requested date {date_filter} -> file {files[0].name}")
    elif latest:
        files = [files[-1]]
        print(f"Processing only latest ranking file: {files[0].name}")
    else:
        print(f"Found {len(files)} ranking files. Processing in chronological order.")

    changed = False

    for f in files:
        print(f"Processing ranking file: {f.name}")
        df_rank = load_ranking_csv(f)
        file_date = ""
        m = re.search(r"data_(\d{4}_\d{2}_\d{2})\.csv$", f.name)
        if m:
            file_date = m.group(1).replace("_","-")
        for _, row in df_rank.iterrows():
            name = row['full_name'].strip()
            ranking = (row['ranking'] or "").strip()
            date_col = row['date'].strip() or file_date
            date_col = parse_date(date_col)
            if not name:
                continue
            idx = find_match_by_name(players_df, name)
            if idx is None and fuzzy:
                idx = find_match_fuzzy(players_df, name, cutoff=fuzzy_cutoff)
            if idx is None:
                print(f"  -> adding new player: {name} (rank={ranking}) date={date_col}")
                new_row = {c: "" for c in players_df.columns}
                new_row['full_name'] = name
                new_row['first_appearance'] = date_col
                new_row['last_appearance'] = date_col
                new_row['highest_ranking'] = ranking or ""
                players_df = pd.concat([players_df.drop(columns=['__norm_name']), pd.DataFrame([new_row])], ignore_index=True, sort=False)
                players_df['__norm_name'] = players_df['full_name'].apply(normalize_name)
                changed = True
            else:
                cur_first = players_df.at[idx, 'first_appearance'] if 'first_appearance' in players_df.columns else ""
                cur_last = players_df.at[idx, 'last_appearance'] if 'last_appearance' in players_df.columns else ""
                cur_best = players_df.at[idx, 'highest_ranking'] if 'highest_ranking' in players_df.columns else ""
                rank_val = ""
                try:
                    rank_val = str(int(float(ranking))) if ranking not in (None, "") else ""
                except Exception:
                    rank_val = ranking or ""

                new_first = min_date(cur_first, date_col) if date_col else cur_first
                new_last = max_date(cur_last, date_col) if date_col else cur_last
                new_best = best_rank(cur_best, rank_val) if rank_val else cur_best

                update_needed = False
                if new_first != cur_first:
                    players_df.at[idx, 'first_appearance'] = new_first
                    update_needed = True
                if new_last != cur_last:
                    players_df.at[idx, 'last_appearance'] = new_last
                    update_needed = True
                if new_best != cur_best:
                    players_df.at[idx, 'highest_ranking'] = new_best
                    update_needed = True
                if update_needed:
                    print(f"  -> updated '{name}': first={new_first} last={new_last} best={new_best}")
                    changed = True

    if '__norm_name' in players_df.columns:
        players_df = players_df.drop(columns=['__norm_name'])

    if not changed:
        print("No changes detected; leaving CSV untouched.")
        return

    bak = csv_p.with_suffix(csv_p.suffix + ".bak")
    print(f"Backing up original CSV to {bak}")
    csv_p.rename(bak)

    try:
        orig_cols = pd.read_csv(bak, nrows=0).columns.tolist()
        for c in ('full_name','player_id','represented_country','height_inches','height_cm','plays','backhand','birth_date','birthplace','first_appearance','last_appearance','highest_ranking','prize_money','reviewed_player','date_review','biography','turned_pro','retired'):
            if c not in orig_cols and c in players_df.columns:
                orig_cols.append(c)
        write_cols = [c for c in orig_cols if c in players_df.columns]
        if not write_cols:
            write_cols = players_df.columns
    except Exception:
        write_cols = players_df.columns

    if dry_run:
        print("Dry-run mode: not writing CSV. Exiting.")
        return

    players_df.to_csv(csv_p, index=False, columns=write_cols, encoding="utf-8")
    print(f"Wrote updated CSV to {csv_p} (backup at {bak})")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--since-csv", action="store_true",
                   help="Process only ranking files whose date is strictly newer than max(last_appearance) in the CSV")
    p.add_argument("--rankings-dir", default="atp_rankings", help="Directory with ranking CSVs (data_YYYY_MM_DD.csv)")
    p.add_argument("--csv", default="player_data_atp.csv", help="player_data_atp.csv path to update")
    p.add_argument("--fuzzy", action="store_true", help="Enable fuzzy name matching (difflib.get_close_matches)")
    p.add_argument("--fuzzy-cutoff", type=float, default=0.85, help="Fuzzy match cutoff (0..1)")
    p.add_argument("--dry-run", action="store_true", help="Don't write CSV; just show actions")
    p.add_argument("--date", default=None, help="Optional: only process ranking file for this date (YYYY-MM-DD)")
    p.add_argument("--latest", action="store_true", help="Optional: only process the most recent ranking file")
    args = p.parse_args()
    main(rankings_dir=args.rankings_dir,
         csv_path=args.csv,
         fuzzy=args.fuzzy,
         fuzzy_cutoff=args.fuzzy_cutoff,
         dry_run=args.dry_run,
         date_filter=args.date,
         latest=args.latest,
         since_csv=args.since_csv)

