#!/usr/bin/env python3
"""
fill_missing_atp_mondays.py

Pour tous les lundis entre la date la plus basse considérée dans atp_rankings qui a
un fichier non vide et le dernier lundi présent dans la base, si un lundi n'a pas
de fichier ou si le fichier est vide, on lui affecte (ou on remplit) le contenu
du lundi antérieur le plus proche (non vide).

Usage:
    python fill_missing_atp_mondays.py
"""
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import re
import sys
import time

BASE_DIR = Path(__file__).parent.resolve()
OUT_DIR = BASE_DIR / "atp_rankings"
LOG_PATH = OUT_DIR / "fill_missing.log"
FILENAME_RE = re.compile(r"^data_(\d{4})_(\d{2})_(\d{2})\.csv$")

def parse_filename_date(fname: str):
    m = FILENAME_RE.match(fname)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return datetime.strptime(f"{y}-{mo}-{d}", "%Y-%m-%d").date()

def is_csv_empty(path: Path) -> bool:
    """
    Détecte si le CSV est 'vide' au sens utilisé ici:
    - pandas.read_csv -> 0 lignes de données (header possible)
    """
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        # If no rows, it's empty
        return df.shape[0] == 0
    except Exception:
        # Si on ne peut même pas lire le CSV, considérer comme vide (safe fallback)
        return True

def read_csv_preserve(path: Path):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as e:
        # raise to caller
        raise

def write_csv_preserve(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def log(msg: str):
    ts = datetime.utcnow().isoformat()
    line = f"{ts} - {msg}\n"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line.strip())

def monday_of(date_obj):
    # Monday = weekday 0
    return date_obj - timedelta(days=date_obj.weekday())

def daterange_mondays(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur = cur + timedelta(days=7)

def main():
    if not OUT_DIR.exists():
        print(f"Erreur : dossier {OUT_DIR} introuvable.")
        sys.exit(1)

    files = sorted(p.name for p in OUT_DIR.glob("data_*.csv"))
    if not files:
        print("Aucun fichier data_*.csv trouvé dans atp_rankings.")
        sys.exit(0)

    # map filename -> date
    file_date_map = {}
    for f in files:
        dt = parse_filename_date(f)
        if dt:
            file_date_map[f] = dt

    if not file_date_map:
        print("Aucun fichier matching data_YYYY_MM_DD.csv trouvé.")
        sys.exit(0)

    # Determine last_date (max across all parsed filenames)
    last_date = max(file_date_map.values())

    # Determine earliest non-empty date
    non_empty_dates = []
    for fname, dt in file_date_map.items():
        p = OUT_DIR / fname
        try:
            if not is_csv_empty(p):
                non_empty_dates.append(dt)
        except Exception:
            # if error reading, treat as empty (do not include)
            continue

    if not non_empty_dates:
        print("Aucun fichier non-vide trouvé — rien à propager.")
        sys.exit(0)

    earliest_non_empty = min(non_empty_dates)

    # Align to Mondays (should already be Mondays by naming convention)
    earliest_non_empty = monday_of(earliest_non_empty)
    last_date = monday_of(last_date)

    log(f"Starting fill procedure: earliest_non_empty={earliest_non_empty}, last_date={last_date}")

    # Build a set of existing files and which are non-empty
    existing = {}
    for fname, dt in file_date_map.items():
        p = OUT_DIR / fname
        try:
            empty = is_csv_empty(p)
        except Exception:
            empty = True
        existing[dt] = {"path": p, "exists": p.exists(), "empty": empty}

    # For fast lookup of previous non-empty Mondays
    # Build sorted list of non-empty dates
    non_empty_sorted = sorted(set(dt for dt in existing if not existing[dt]["empty"] and dt >= earliest_non_empty))

    # Iterate Mondays
    prev_non_empty_date = None
    # To ensure we can find previous non-empty for any date, we'll update prev_non_empty_date as we go
    # Initialize with the earliest_non_empty if it is non-empty; else we find first non-empty after earliest_non_empty
    # But by definition earliest_non_empty is non-empty.
    prev_non_empty_date = earliest_non_empty if (earliest_non_empty in existing and not existing[earliest_non_empty]["empty"]) else None
    if prev_non_empty_date is None:
        # fallback: choose min non_empty_sorted
        prev_non_empty_date = min(non_empty_sorted) if non_empty_sorted else None

    if prev_non_empty_date is None:
        log("Aucun point de départ non-vide trouvé malgré l'analyse. Rien à faire.")
        sys.exit(0)

    # Iterate from earliest_non_empty to last_date step 7
    created = 0
    filled = 0
    skipped = 0
    for cur in daterange_mondays(earliest_non_empty, last_date):
        expected_name = f"data_{cur.isoformat().replace('-', '_')}.csv"
        expected_path = OUT_DIR / expected_name

        # check if file exists and non-empty
        if cur in existing:
            info = existing[cur]
            if not info["exists"]:
                # Strange but treat as missing
                exists_flag = False
                empty_flag = True
            else:
                exists_flag = True
                empty_flag = info["empty"]
        else:
            exists_flag = expected_path.exists()
            empty_flag = True if not exists_flag else is_csv_empty(expected_path)
            # register for subsequent lookups
            existing[cur] = {"path": expected_path, "exists": exists_flag, "empty": empty_flag}

        if exists_flag and not empty_flag:
            # nothing to do
            log(f"{cur} -> file exists and non-empty ({expected_name}). SKIP.")
            prev_non_empty_date = cur  # update prev
            skipped += 1
            continue

        # Need to fill: find previous non-empty date strictly before cur
        search_date = cur - timedelta(days=7)
        found_source = None
        while search_date >= earliest_non_empty:
            if search_date in existing:
                if existing[search_date]["exists"] and not existing[search_date]["empty"]:
                    found_source = search_date
                    break
            else:
                # check filesystem
                cand = OUT_DIR / f"data_{search_date.isoformat().replace('-', '_')}.csv"
                if cand.exists() and (not is_csv_empty(cand)):
                    # update existing map
                    existing[search_date] = {"path": cand, "exists": True, "empty": False}
                    found_source = search_date
                    break
                else:
                    existing[search_date] = {"path": cand, "exists": cand.exists(), "empty": not cand.exists() or is_csv_empty(cand) if cand.exists() else True}
            search_date -= timedelta(days=7)

        if not found_source:
            log(f"{cur} -> No prior non-empty Monday found to copy from. SKIP.")
            continue

        src_path = OUT_DIR / f"data_{found_source.isoformat().replace('-', '_')}.csv"
        if not src_path.exists():
            log(f"{cur} -> Source expected {src_path} missing unexpectedly. SKIP.")
            continue

        try:
            df = read_csv_preserve(src_path)
        except Exception as e:
            log(f"{cur} -> Failed to read source {src_path}: {e}. SKIP.")
            continue

        # Set/update date column(s) to current monday iso
        # find any column with lower name 'date'
        date_cols = [c for c in df.columns if c.strip().lower() == "date"]
        if date_cols:
            for c in date_cols:
                df[c] = cur.isoformat()
        else:
            # add 'date' column
            df["date"] = cur.isoformat()

        # write to expected_path (overwrite if exists empty)
        try:
            write_csv_preserve(df, expected_path)
            if exists_flag:
                filled += 1
                log(f"{cur} -> Filled empty file {expected_name} from {src_path.name}")
            else:
                created += 1
                log(f"{cur} -> Created file {expected_name} from {src_path.name}")
            # update existing map
            existing[cur] = {"path": expected_path, "exists": True, "empty": False}
            prev_non_empty_date = cur
        except Exception as e:
            log(f"{cur} -> Failed to write {expected_name}: {e}")
            continue

    log(f"Done. created={created}, filled={filled}, skipped={skipped}.")

if __name__ == "__main__":
    main()
